"""Database-free preparation, verification and atomic publication of import-ready candidates."""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import uuid

from core.tmdb_importer import (evaluate_record, movie_values, prepare_movie, read_json,
                                snapshot_input, stage_records)
from core.tmdb_enrichment_client import reject_constant, strict_json
from core.tmdb_enrichment_store import atomic_bytes, file_digest


CONTRACT = "reccool.import_ready"
VERSION = 1
RULE_FILES = ("tmdb_import_ready.py", "tmdb_importer.py", "tmdb_eligibility.py",
              "tmdb_normalizer.py", "tmdb_enrich_verify.py", "models.py")
COUNTS = ("examined", "eligible", "included", "excluded_adult", "pending_adult",
          "excluded_genre", "invalid", "duplicates", "pending_enrichment")


def rules():
    return {name: file_digest(Path(__file__).with_name(name)) for name in RULE_FILES}


def source_descriptor(manifest, source, snapshot):
    return {"run_directory": str(Path(source).resolve()), "manifest_sha256": file_digest(snapshot / "manifest.json"),
            "mode": manifest["configuration"]["mode"], "state": manifest["state"],
            "normalizer_sha256": manifest["configuration"]["normalizer_sha256"],
            "enrichment_record_version": 1 if manifest["configuration"].get("eligibility_policy_version") is None else 2,
            "outputs": manifest["outputs"], "upstream_pending": manifest["counts"]["pending"],
            "upstream_duplicates": manifest["counters"]["duplicates"]}


def ready_record(candidate, source):
    if candidate["status"] not in ("accepted", "incomplete"):
        raise ValueError("Only accepted/incomplete can be prepared")
    if candidate["schema_version"] not in (1, 2) or type(candidate["schema_version"]) is not int:
        raise ValueError("Unsupported enrichment contract")
    if (candidate["schema_version"] != source["enrichment_record_version"]
            or candidate["type"] not in ("movie", "series") or type(candidate["tmdb_id"]) is not int
            or not 0 < candidate["tmdb_id"] <= 2147483647):
        raise ValueError("Invalid prepared identity/version")
    if candidate["provenance"]["mode"] != source["mode"] or candidate["normalizer_sha256"] != source["normalizer_sha256"]:
        raise ValueError("Inconsistent prepared provenance")
    # Reserved tags must never be trusted from an unprepared enrichment envelope.
    candidate = {key: value for key, value in candidate.items() if key != "_ready_contract"}
    decision = evaluate_record(candidate, None)
    if (candidate["normalization"]["status"] != candidate["status"]
            or candidate["normalization"]["valid"] is not True
            or candidate["publication"]["auto_importable"] is not False
            or (candidate["schema_version"] == 2 and strict_json(candidate.get("eligibility")) != strict_json(decision["eligibility"]))
            or (candidate["schema_version"] == 1 and "eligibility" in candidate)):
        raise ValueError("Inconsistent candidate contract")
    if decision["decision"] != "proposed_new" or decision["adult"]["resolved"] is not False or decision["adult"]["conflict"]:
        raise ValueError("Candidate is not admissible")
    normalized = candidate["normalization"]
    prepare_movie(normalized, real_spanish_only=True)  # shared model compatibility check, no save/query
    return {"contract": CONTRACT, "schema_version": VERSION, "type": candidate["type"], "tmdb_id": candidate["tmdb_id"],
            "movie_data": movie_values(normalized, real_spanish_only=True),
            "candidate": candidate,  # original diagnostics/metadatas kept, never relabeled as v2
            "eligibility": decision["eligibility"],
            "admission": {"adult": decision["adult"], "video": decision["video"]},
            "title_sources": decision["title_sources"], "warnings": normalized["warnings"],
            "trace": {"source_run": source["run_directory"], "source_manifest_sha256": source["manifest_sha256"],
                      "file": candidate["status"] + ".jsonl.gz", "position": candidate["source"]["position"],
                      "enrichment_schema_version": candidate["schema_version"]},
            "import_authorized": False}


def check_contents(ready_path, omitted_path, manifest):
    """Stream strict JSON/UTF-8/GZIP and recompute policy; disk-bounded uniqueness."""
    counts = Counter({key: 0 for key in COUNTS})
    with tempfile.TemporaryDirectory(prefix="reccool-ready-verify-") as temp:
        with closing(sqlite3.connect(Path(temp) / "seen.sqlite3")) as seen:
            seen.executescript("CREATE TABLE ids(type TEXT,id INTEGER, PRIMARY KEY(type,id));"
                               "CREATE TABLE positions(position INTEGER PRIMARY KEY);")
            last = 0
            with gzip.open(ready_path, "rt", encoding="utf-8") as stream:
                for line in stream:
                    row = json.loads(line, parse_constant=reject_constant)
                    if strict_json(row) != strict_json(ready_record(row["candidate"], manifest["source"])):
                        raise ValueError("Prepared record/policy mismatch")
                    position = row["trace"]["position"]
                    if type(position) is not int or position <= last:
                        raise ValueError("Unstable prepared order")
                    last = position
                    seen.execute("INSERT INTO ids VALUES (?,?)", (row["type"], row["tmdb_id"]))
                    seen.execute("INSERT INTO positions VALUES (?)", (position,))
                    counts.update(included=1, eligible=1, examined=1)
            with Path(omitted_path).open(encoding="utf-8") as stream:
                for line in stream:
                    row = json.loads(line, parse_constant=reject_constant)
                    category = row["classification"]
                    if category not in ("excluded_adult", "pending_adult", "excluded_genre", "invalid", "duplicates", "pending_enrichment"):
                        raise ValueError("Unknown omission classification")
                    counts[category] += 1
                    if category not in ("duplicates", "pending_enrichment"):
                        position = row["position"]
                        if type(position) is not int or position <= 0:
                            raise ValueError("Invalid omission position")
                        seen.execute("INSERT INTO positions VALUES (?)", (position,))
                        counts["examined"] += 1
    expected_terminal = sum(manifest["source"]["outputs"][name]["records"] for name in ("accepted", "incomplete", "excluded", "invalid"))
    if (strict_json(dict(counts)) != strict_json(manifest["counts"]) or counts["examined"] != expected_terminal
            or counts["duplicates"] != manifest["source"]["upstream_duplicates"]
            or counts["pending_enrichment"] != manifest["source"]["upstream_pending"]):
        raise ValueError("Preparation counters do not reconcile")
    return dict(counts)


def verify_ready_run(directory):
    directory = Path(directory).resolve(strict=True)
    manifest = read_json(directory / "preparation_manifest.json")
    if manifest["contract"] != CONTRACT or type(manifest["schema_version"]) is not int or manifest["schema_version"] != VERSION or manifest["state"] != "completed" or manifest["complete"] is not True:
        raise ValueError("Incomplete/unsupported prepared run")
    if manifest["rules_sha256"] != rules() or manifest.get("import_authorized") is not False:
        raise ValueError("Incompatible preparation rules")
    if (manifest["source"]["normalizer_sha256"] != manifest["rules_sha256"]["tmdb_normalizer.py"]
            or manifest["source"]["mode"] not in ("live", "offline")
            or manifest["source"]["enrichment_record_version"] not in (1, 2)
            or (manifest["source"]["enrichment_record_version"] == 1 and manifest.get("convert_legacy") is not True)):
        raise ValueError("Unsupported prepared source")
    if set(manifest["outputs"]) != {"ready", "omitted"}:
        raise ValueError("Unsupported prepared outputs")
    for name, filename in (("ready", "import_ready.jsonl.gz"), ("omitted", "omitted.jsonl")):
        entry = manifest["outputs"][name]
        path = directory / filename
        if entry["file"] != filename or path.resolve().parent != directory or file_digest(path) != entry["sha256"] or path.stat().st_size != entry["bytes"]:
            raise ValueError("Prepared output integrity mismatch")
    check_contents(directory / "import_ready.jsonl.gz", directory / "omitted.jsonl", manifest)
    return manifest


def snapshot_ready_input(source, directory, *, allow_offline=False):
    snapshot = directory / "source_snapshot"
    snapshot.mkdir()
    for filename in ("preparation_manifest.json", "import_ready.jsonl.gz", "omitted.jsonl"):
        path = source / filename
        if path.resolve().parent != source:
            raise ValueError("Unsafe prepared path")
        shutil.copyfile(path, snapshot / filename)
    prepared = verify_ready_run(snapshot)
    mode = prepared["source"]["mode"]
    if mode not in ("live", "offline") or (mode != "live" and not allow_offline):
        raise ValueError("Offline prepared input requires explicit opt-in")
    # Internal adapter only: the original enrichment and prepared versions remain intact on disk.
    return {"input_kind": "import_ready", "schema_version": 1, "state": "completed", "complete": True,
            "configuration": {"mode": mode, "normalizer_sha256": prepared["source"]["normalizer_sha256"]},
            "counts": {"pending": 0}, "counters": {"duplicates": prepared["counts"]["duplicates"]},
            "outputs": prepared["outputs"], "prepared_manifest": prepared}, snapshot


def _export(journal, snapshot, directory, summary):
    ready_part, omitted_part = directory / "import_ready.jsonl.gz.part", directory / "omitted.jsonl.part"
    counts = Counter({key: 0 for key in COUNTS})
    with ready_part.open("xb") as raw, omitted_part.open("x", encoding="utf-8", newline="\n") as omitted:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            for (payload,) in journal.execute("SELECT result FROM results ORDER BY position"):
                result = json.loads(payload)
                counts["examined"] += 1
                if result["classification"] == "included":
                    compressed.write((strict_json(result["ready"]) + "\n").encode("utf-8"))
                    counts.update(included=1, eligible=1)
                else:
                    counts[result["classification"]] += 1
                    omitted.write(strict_json(result) + "\n")
            for name, classification in (("duplicates", "duplicates"), ("pending", "pending_enrichment")):
                with gzip.open(snapshot / f"{name}.jsonl.gz", "rt", encoding="utf-8") as stream:
                    for line in stream:
                        event = json.loads(line, parse_constant=reject_constant)
                        omitted.write(strict_json({"classification": classification, "reason": "duplicate_input" if name == "duplicates" else "upstream_pending",
                                                   "position": event.get("position", event.get("source", {}).get("position")),
                                                   "source_file": name + ".jsonl.gz"}) + "\n")
                        counts[classification] += 1
        raw.flush()
        os.fsync(raw.fileno())
        omitted.flush()
        os.fsync(omitted.fileno())
    summary["counts"] = dict(counts)
    check_contents(ready_part, omitted_part, summary)
    summary["outputs"] = {name: {"file": filename, "bytes": path.stat().st_size, "sha256": file_digest(path)}
                          for name, filename, path in (("ready", "import_ready.jsonl.gz", ready_part), ("omitted", "omitted.jsonl", omitted_part))}
    os.replace(omitted_part, directory / "omitted.jsonl")
    os.replace(ready_part, directory / "import_ready.jsonl.gz")


def prepare_run(source, output_parent, *, allow_offline=False, allow_partial=False, convert_legacy=False, batch_size=100, resume=None):
    if not 1 <= batch_size <= 500:
        raise ValueError("Batch size must be 1..500")
    source = Path(source).resolve(strict=True)
    parent = Path(output_parent).expanduser().resolve()
    if parent.is_relative_to(source) or (resume and parent.is_relative_to(Path(resume).resolve())):
        raise ValueError("Output must be independent from source and previous preparation")
    if (source / "preparation_manifest.json").exists():
        raise ValueError("Preparation requires enrichment input, not another prepared run")
    directory = parent / (datetime.now(timezone.utc).strftime("prepare-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:12])
    directory.mkdir(parents=True, exist_ok=False)
    summary = {"contract": CONTRACT, "schema_version": VERSION, "state": "verifying", "complete": False,
               "run_directory": str(directory), "counts": {}, "rules_sha256": rules(),
               "convert_legacy": convert_legacy, "allow_offline": allow_offline, "allow_partial": allow_partial,
               "import_authorized": False}
    manifest_path = directory / "preparation_manifest.json"

    def publish():
        atomic_bytes(manifest_path, (strict_json(summary) + "\n").encode("utf-8"))

    publish()
    try:
        manifest, snapshot = snapshot_input(source, directory, allow_offline=allow_offline, allow_partial=allow_partial)
        legacy = manifest["configuration"].get("eligibility_policy_version") is None
        if legacy and not convert_legacy:
            raise ValueError("Legacy enrichment requires --convert-legacy")
        summary.update(source=source_descriptor(manifest, source, snapshot), state="running", legacy_conversion=legacy)
        config = {key: summary[key] for key in ("source", "rules_sha256", "convert_legacy", "allow_offline", "allow_partial")}
        checkpoint = directory / "checkpoint.sqlite3"
        with closing(sqlite3.connect(checkpoint)) as journal:
            journal.execute("PRAGMA synchronous=FULL")
            journal.execute("PRAGMA cache_size=-8192")
            if resume:
                previous = Path(resume).resolve(strict=True)
                old = read_json(previous / "preparation_manifest.json")
                old_checkpoint = previous / "checkpoint.sqlite3"
                if old["state"] not in ("interrupted", "failed", "completed") or old.get("configuration") != config or old.get("checkpoint_sha256") != file_digest(old_checkpoint):
                    raise ValueError("Incompatible or active preparation checkpoint")
                with closing(sqlite3.connect(old_checkpoint.as_uri() + "?mode=ro", uri=True)) as original:
                    original.backup(journal)
                if read_json_value(journal) != config:
                    raise ValueError("Checkpoint configuration mismatch")
                summary["resumed_from"] = str(previous)
            else:
                journal.executescript("CREATE TABLE queue(position INTEGER PRIMARY KEY,record TEXT NOT NULL);"
                                      "CREATE TABLE results(position INTEGER PRIMARY KEY,result TEXT NOT NULL);"
                                      "CREATE TABLE metadata(value TEXT NOT NULL);")
                journal.execute("INSERT INTO metadata VALUES (?)", (strict_json(config),))
                stage_records(journal, manifest, snapshot)
            summary["configuration"] = config
            publish()
            last = journal.execute("SELECT coalesce(max(position),0) FROM results").fetchone()[0]
            if journal.execute("SELECT count(*) FROM queue q LEFT JOIN results r ON q.position=r.position WHERE q.position<=? AND r.position IS NULL", (last,)).fetchone()[0]:
                raise ValueError("Preparation checkpoint is not an ordered prefix")
            while True:
                batch = list(journal.execute("SELECT position,record FROM queue WHERE position>? ORDER BY position LIMIT ?", (last, batch_size)))
                if not batch:
                    break
                decisions = []
                for position, payload in batch:
                    candidate = json.loads(payload)
                    decision = evaluate_record(candidate, None)
                    classification = {"proposed_new": "included", "validation_error": "invalid"}.get(decision["decision"], decision["decision"])
                    result = {"classification": classification, "position": position, "type": candidate["type"], "tmdb_id": candidate["tmdb_id"],
                              "source_status": candidate["status"], "reason": decision["reason"],
                              "adult": decision["adult"], "diagnostics": decision["normalization_diagnostics"]}
                    if classification == "included":
                        result["ready"] = ready_record(candidate, summary["source"])
                    decisions.append((position, strict_json(result)))
                with journal:
                    journal.executemany("INSERT INTO results VALUES (?,?)", decisions)
                last = batch[-1][0]
            _export(journal, snapshot, directory, summary)
        summary.update(state="completed", complete=True, checkpoint_sha256=file_digest(checkpoint))
        publish()
        verify_ready_run(directory)
        return summary
    except BaseException as exc:
        # Only remove our newly created final output; never touch an input/previous run.
        final = directory / "import_ready.jsonl.gz"
        if final.exists():
            final.unlink()
        summary.update(state="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed", complete=False, error_type=type(exc).__name__)
        if (directory / "checkpoint.sqlite3").is_file():
            summary["checkpoint_sha256"] = file_digest(directory / "checkpoint.sqlite3")
        publish()
        raise


def read_json_value(journal):
    return json.loads(journal.execute("SELECT value FROM metadata").fetchone()[0])
