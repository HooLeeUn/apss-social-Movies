"""Phase 4 offline-first CLI: python -m core.tmdb_enrich --help. No ORM imports."""
import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import sys
import time
import uuid

from core.tmdb_enrichment_client import (
    OFFICIAL_BASE, QueryError, TMDbMetadataClient, redact, reject_constant, strict_json,
)
from core.tmdb_enrichment_store import CheckpointStore, StorageError, file_digest, run_lock, utc_now
from core.tmdb_normalizer import normalize_tmdb_metadata
from core.tmdb_eligibility import POLICY_VERSION, evaluate_eligibility

CSV_FIELDS = ("type", "tmdb_id", "original_title", "popularity", "adult", "video")
REPO_ROOT = Path(__file__).resolve().parent.parent


def inspect_input(path):
    path = Path(path).resolve(strict=True)
    with path.open(encoding="utf-8-sig", newline="") as stream:
        fields = csv.DictReader(stream, strict=True).fieldnames
    if (not fields or len(fields) != len(set(fields)) or any(not field for field in fields)
            or not set(CSV_FIELDS).issubset(fields)):
        raise ValueError("CSV requires unique columns: " + ", ".join(CSV_FIELDS))
    return {"path": str(path), "bytes": path.stat().st_size,
            "sha256": file_digest(path), "columns": fields}


def candidates(path, *, after=0):
    """Position counts CSV records, not physical lines (quoted newlines are valid)."""
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream, strict=True)
        for position, row in enumerate(reader, 1):
            if position <= after:
                continue
            source = {name: row.get(name) for name in CSV_FIELDS}
            source.update(position=position, line_end=reader.line_num)
            kind = (row.get("type") or "").strip()
            raw_id = (row.get("tmdb_id") or "").strip()
            valid = (None not in row and all(row.get(key) is not None for key in CSV_FIELDS)
                     and kind in ("movie", "series") and re.fullmatch(r"[0-9]{1,10}", raw_id))
            tmdb_id = int(raw_id) if valid else None
            valid = bool(valid and 0 < tmdb_id <= 2147483647)
            yield position, kind if valid else None, tmdb_id if valid else None, source


class FixtureClient:
    """Read one explicitly supplied response fixture at a time; never use the network."""
    mode = "offline"
    request_count = retry_count = 0
    _token = ""

    def __init__(self, directory):
        self.directory = Path(directory).resolve(strict=True)
        if not self.directory.is_dir():
            raise ValueError("Fixtures must be a directory")

    def fetch_part(self, kind, tmdb_id, language):
        path = self.directory / f"{kind}_{tmdb_id}.json"
        if not path.is_file():
            raise QueryError("offline_fixture_missing", retryable=True)
        if path.resolve().parent != self.directory:
            raise QueryError("unsafe_fixture_path", halt="blocked_security")
        try:
            with path.open(encoding="utf-8") as stream:
                payload = json.load(stream, parse_constant=reject_constant)
            part = payload["english" if language == "en-US" else "spanish"]
            if not isinstance(part, dict):
                raise ValueError("not_an_object")
            strict_json(part)
        except (ValueError, KeyError, TypeError):
            raise QueryError("offline_fixture_invalid", retryable=True) from None
        return part

    def close(self):
        pass


def configuration(client):
    config = {"schema_version": 1, "eligibility_policy_version": POLICY_VERSION,
        "mode": getattr(client, "mode", "live"),
        "languages": ["en-US", "es-ES"], "credits": "standard_credits_appended_to_english",
        "normalizer_sha256": file_digest(REPO_ROOT / "core/tmdb_normalizer.py"),
        "implementation_sha256": {name: file_digest(REPO_ROOT / "core" / name)
            for name in ("tmdb_enrich.py", "tmdb_enrichment_client.py", "tmdb_enrichment_store.py", "tmdb_eligibility.py")}}
    if isinstance(client, FixtureClient):
        digest = hashlib.sha256()
        for path in sorted(client.directory.glob("*.json")):
            digest.update(path.name.encode("utf-8"))
            digest.update(file_digest(path).encode("ascii"))
        config["fixtures"] = {"path": str(client.directory), "sha256": digest.hexdigest()}
    return config


def outside_repository(path):
    path = Path(path).resolve()
    if path.is_relative_to(REPO_ROOT):
        raise ValueError("Enrichment output must be outside the repository")
    return path


def result_envelope(candidate, status, *, normalized=None, error=None, config=None):
    return {"schema_version": 2, "type": candidate["type"], "tmdb_id": candidate["tmdb_id"],
        "source": candidate["source"], "status": status, "normalization": normalized,
        "lookup_error": error, "normalizer_sha256": config["normalizer_sha256"],
        "eligibility": evaluate_eligibility(normalized),
        "provenance": {"provider": "TMDb v3", "mode": config["mode"],
            "details_language": "en-US", "english_language": "en-US", "spanish_language": "es-ES",
            "credits": config["credits"]},
        "publication": {"decision": "not_authorized", "auto_importable": False,
            "normalizer_accepted": status == "accepted", "requires_destination_identity_check": True}}


def process_candidate(store, position, client, config):
    row = store.candidate(position)
    store.event("candidate_attempts", 1)
    if row["type"] is None:
        normalized = normalize_tmdb_metadata(None, None, None)
        store.terminal(position, result_envelope(row, "invalid", normalized=normalized,
            error={"code": "invalid_candidate", "retryable": False}, config=config))
        return None
    try:
        parts = {}
        for language in ("en-US", "es-ES"):
            payload = store.cached(row["type"], row["tmdb_id"], language)
            if payload is None:
                payload = client.fetch_part(row["type"], row["tmdb_id"], language)
                payload = redact(payload, getattr(client, "_token", ""))
                store.cache(row["type"], row["tmdb_id"], language, payload)
            parts[language] = payload
        normalized = normalize_tmdb_metadata(row["type"], row["tmdb_id"], parts["en-US"],
            language="en-US", english=parts["en-US"], spanish=parts["es-ES"],
            credits=parts["en-US"].get("credits"))
        record = result_envelope(row, normalized["status"], normalized=normalized, config=config)
        # Keep export flags and observed detail flags; publication rules for these
        # are deliberately not invented by the enrichment pipeline.
        record["tmdb_flags"] = {key: parts["en-US"].get(key) for key in ("adult", "video", "status")}
        store.terminal(position, redact(record, getattr(client, "_token", "")))
    except QueryError as error:
        if error.status == 404:
            store.terminal(position, result_envelope(row, "invalid", error=error.as_dict(), config=config))
        else:
            store.pending_error(position, error.as_dict())
        return error.halt
    except (ValueError, TypeError, OverflowError):
        store.pending_error(position, {"code": "normalization_or_serialization_error", "retryable": False})
    return None


def run_batch(input_path, run_dir, client, *, resume=False, limit=10,
              now=utc_now, wall_clock=time.time):
    if type(limit) is not int or limit < 0:
        raise ValueError("Limit must be nonnegative; zero explicitly means unlimited")
    identity = inspect_input(input_path)
    config = configuration(client)
    directory = outside_repository(run_dir)
    if resume:
        if not directory.is_dir():
            raise ValueError("Resume directory does not exist")
    else:
        directory.mkdir(parents=True, exist_ok=False)
    with run_lock(directory):
        store = CheckpointStore(directory, identity, config, resume=resume, now=now)
        client.event = store.event
        try:
            if store.meta("state") == "completed":
                return store.manifest()
            store.invocation({"limit": limit, "request_budget": getattr(client, "max_requests", 0),
                "requests_per_second": getattr(client, "rate", None),
                "max_retries": getattr(client, "max_retries", 0),
                "timeouts": getattr(client, "timeout", None)})
            if store.meta("cooldown_until") > wall_clock():
                store.state("waiting")
                return store.manifest()
            store.state("running")
            store.manifest()  # Never leave a previous successful/paused label while working.
            attempted, stop = 0, None
            if resume:
                for position in store.pending_positions():
                    if limit and attempted >= limit:
                        break
                    stop = process_candidate(store, position, client, config)
                    attempted += 1
                    if stop:
                        break
            if not stop:
                stream = iter(candidates(input_path, after=store.meta("cursor")))
                while not limit or attempted < limit:
                    try:
                        position, kind, tmdb_id, source = next(stream)
                    except StopIteration:
                        store.exhausted()
                        break
                    source = redact(source, getattr(client, "_token", ""))
                    duplicate = store.find_identity(kind, tmdb_id) if kind else None
                    if duplicate:
                        store.duplicate(position, source, duplicate[0])
                        continue
                    store.add(position, kind, tmdb_id, source)
                    stop = process_candidate(store, position, client, config)
                    attempted += 1
                    if attempted % 100 == 0:
                        store.manifest()
                    if stop:
                        break
                stream.close()
            pending = store.connection.execute("SELECT count(*) FROM candidates WHERE status='pending'").fetchone()[0]
            store.state(stop or ("completed" if store.meta("input_exhausted") and not pending else
                                "pending" if store.meta("input_exhausted") else "limited"))
            return store.manifest()
        except KeyboardInterrupt:
            store.state("interrupted")
            return store.manifest()
        except BaseException:
            # Incomplete manifests are advisory; the SQLite checkpoint remains
            # authoritative if disk failure also prevents writing a new manifest.
            try:
                store.state("failed")
                store.manifest()
            except Exception:
                pass
            raise
        finally:
            store.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    output = parser.add_mutually_exclusive_group(required=True)
    output.add_argument("--output-dir", type=Path, help="Parent outside repository; a new run subdirectory is created")
    output.add_argument("--resume", type=Path, help="Existing run directory")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--offline", action="store_true")
    mode.add_argument("--live", action="store_true", help="Explicitly authorize live HTTP in this invocation")
    parser.add_argument("--fixtures", type=Path, help="Required offline directory, one file per identity")
    parser.add_argument("--limit", type=int, default=10, help="Candidate attempts per invocation; 0 explicitly means all")
    parser.add_argument("--max-requests", type=int, default=10, help="HTTP attempt budget including retries, per invocation")
    parser.add_argument("--requests-per-second", type=float, default=2)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--connect-timeout", type=float, default=3.05)
    parser.add_argument("--read-timeout", type=float, default=15)
    args = parser.parse_args(argv)
    client = None
    try:
        if args.live:
            if args.fixtures:
                raise ValueError("Live mode does not accept fixtures")
            # Same variables as Django, without importing settings/apps/signals.
            from dotenv import load_dotenv
            load_dotenv(REPO_ROOT / ".env", override=False)
            client = TMDbMetadataClient(os.environ.get("TMDB_READ_ACCESS_TOKEN", ""),
                base_url=os.environ.get("TMDB_BASE_URL", OFFICIAL_BASE),
                requests_per_second=args.requests_per_second, max_retries=args.max_retries,
                connect_timeout=args.connect_timeout, read_timeout=args.read_timeout,
                max_requests=args.max_requests)
        else:
            if not args.fixtures:
                raise ValueError("Offline mode requires --fixtures; use --live only after authorization")
            client = FixtureClient(args.fixtures)
        if args.limit < 0:
            raise ValueError("Limit must be nonnegative")
        directory = args.resume or outside_repository(args.output_dir) / (
            datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:10])
        manifest = run_batch(args.input, directory, client, resume=bool(args.resume), limit=args.limit)
        print(strict_json({"run_directory": str(directory), "state": manifest["state"],
            "complete": manifest["complete"], "counts": manifest["counts"],
            "counters": manifest["counters"], "terminal_results": manifest["terminal_results"]}))
        if manifest["state"] == "interrupted":
            return 130
        return 0 if manifest["state"] in ("completed", "limited") else 2
    except KeyboardInterrupt:
        print("Interrupted before a checkpointed batch started.", file=sys.stderr)
        return 130
    except (ValueError, OSError, StorageError, csv.Error, sqlite3.Error):
        # Never stringify credential-bearing request exceptions or configuration.
        print("Enrichment stopped: invalid configuration/input or storage failure. Check paths, CSV header, checkpoint and credentials.", file=sys.stderr)
        return 3
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    def interrupt(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    raise SystemExit(main())
