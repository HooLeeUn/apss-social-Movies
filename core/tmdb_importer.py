"""Phase 5 planning only. No target INSERT/UPDATE/DELETE implementation exists."""
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import uuid

from django.core.exceptions import ValidationError
from django.db.models import Count, Min, Q

from core.models import Movie, build_genre_key
from core.tmdb_eligibility import evaluate_eligibility
from core.tmdb_enrich_verify import verify_run
from core.tmdb_enrichment_client import reject_constant, strict_json
from core.tmdb_enrichment_store import atomic_bytes, file_digest
from core.tmdb_normalizer import INTERNAL_GENRES


SCHEMA_VERSION = 1
MODEL_FIELDS = ("type", "tmdb_id", "title_english", "title_spanish", "genre",
                "release_year", "director", "cast_members", "synopsis", "synopsis_es",
                "image", "external_rating", "external_votes")
DERIVED_FIELDS = ("genre_key", "title_english_search", "title_spanish_search",
                  "director_search", "cast_members_search")
DECISIONS = ("proposed_new", "existing", "existing_duplicate", "excluded_adult",
             "excluded_genre", "pending_adult", "validation_error")


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"), parse_constant=reject_constant)


def export_flag(value):
    if type(value) is bool:
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    return None


def adult_decision(record):
    # API flags must be JSON booleans; CSV/export flags have a documented string representation.
    detail = (record.get("tmdb_flags") or {}).get("adult")
    detail = detail if type(detail) is bool else None
    exported = export_flag(record.get("source", {}).get("adult"))
    if detail is True or exported is True:
        value = True
    elif detail is False or exported is False:
        value = False
    else:
        value = None
    return {"resolved": value, "api": detail, "export": exported,
            "conflict": detail is not None and exported is not None and detail != exported}


def movie_values(normalized, *, real_spanish_only=False):
    """Normalized source values, before model defaults or derived fields."""
    data, states = normalized["data"], normalized["field_states"]
    values = {name: data[name] for name in MODEL_FIELDS
              if states.get(name) == "valid" and data.get(name) is not None}
    if real_spanish_only and normalized.get("sources", {}).get("title_spanish") != "es":
        values.pop("title_spanish", None)
    return values


def prepare_movie(normalized, *, real_spanish_only=False):
    """Explicit creation projection, not update_data and never **raw_payload."""
    data = normalized["data"]
    values = movie_values(normalized, real_spanish_only=real_spanish_only)
    if "title_english" not in values:
        raise ValidationError({"title_english": "required"})
    if (data.get("type") not in (Movie.MOVIE, Movie.SERIES) or type(data.get("tmdb_id")) is not int
            or not 0 < data["tmdb_id"] <= 2147483647 or values.get("type") != data["type"]
            or values.get("tmdb_id") != data["tmdb_id"]):
        raise ValidationError({"tmdb_id": "invalid_identity"})
    # Do not silently coerce forged normalized JSON (e.g. boolean votes or years).
    for name in ("tmdb_id", "release_year", "external_votes"):
        if name in values and (type(values[name]) is not int or not 0 <= values[name] <= 2147483647):
            raise ValidationError({name: "invalid_integer"})
    if "external_rating" in values:
        raw = values["external_rating"]
        if not isinstance(raw, str):
            raise ValidationError({"external_rating": "expected_normalized_decimal_string"})
        rating = Decimal(raw)
        if not rating.is_finite() or not 0 <= rating <= 10:
            raise ValidationError({"external_rating": "invalid_rating"})
        values["external_rating"] = rating
    if values.get("genre") and (not isinstance(values["genre"], str) or
                                any(part not in INTERNAL_GENRES for part in values["genre"].split(", "))):
        raise ValidationError({"genre": "unsupported_internal_genre"})
    for name, value in values.items():
        if name not in ("tmdb_id", "release_year", "external_votes", "external_rating") and not isinstance(value, str):
            raise ValidationError({name: "invalid_text"})
    movie = Movie(**values)
    # Reuse save()'s pure helpers without save(), signals, relation validation or SQL.
    movie.genre_key = build_genre_key(movie.genre)
    movie.populate_search_fields()
    # Populated by the existing PostgreSQL BEFORE INSERT/UPDATE trigger, not Python.
    movie.full_clean(exclude=["search_vector"], validate_unique=False, validate_constraints=False)
    fields = (*MODEL_FIELDS, *DERIVED_FIELDS)
    projected = {name: getattr(movie, name) for name in fields}
    projected = {name: str(value) if isinstance(value, Decimal) else value for name, value in projected.items()}
    defaults = {name: projected[name] for name in MODEL_FIELDS if name not in values}
    return projected, defaults


def existing_identities(alias, records):
    keys = Q()
    found = False
    for record in records:
        kind, identifier = record.get("type"), record.get("tmdb_id")
        if kind in (Movie.MOVIE, Movie.SERIES) and type(identifier) is int and 0 < identifier <= 2147483647:
            keys |= Q(type=kind, tmdb_id=identifier)
            found = True
    if not found:
        return {}
    rows = (Movie.objects.using(alias).filter(keys).order_by().values("type", "tmdb_id")
            .annotate(count=Count("id"), first_id=Min("id")))
    return {(row["type"], row["tmdb_id"]): {"count": row["count"], "first_id": row["first_id"]}
            for row in rows}


def evaluate_record(record, existing):
    normalized = record.get("normalization")
    admission = adult_decision(record)
    eligibility = evaluate_eligibility(normalized)
    result = {"position": record["source"]["position"], "type": record["type"], "tmdb_id": record["tmdb_id"],
              "source_status": record["status"], "adult": admission,
              "video": {"api": (record.get("tmdb_flags") or {}).get("video"), "export": record["source"].get("video")},
              "title_sources": {name: value for name, value in (normalized or {}).get("sources", {}).items()
                                if name in ("title_english", "title_spanish")},
              "eligibility": eligibility, "normalization_diagnostics": {
                  key: (normalized or {}).get(key) for key in ("field_states", "missing_fields", "warnings", "errors", "exclusion_reasons")},
              "decision": None, "reason": None, "inserted": False}
    if admission["resolved"] is True:
        result.update(decision="excluded_adult", reason="adult_true_in_api_or_export")
    elif (normalized or {}).get("exclusion_reasons") or record["status"] == "excluded":
        result.update(decision="excluded_genre", reason="explicit_normalizer_exclusion")
    elif not eligibility["import"]["eligible"] or (normalized or {}).get("data", {}).get("type") != record["type"] or (normalized or {}).get("data", {}).get("tmdb_id") != record["tmdb_id"]:
        result.update(decision="validation_error", reason="ineligible_or_identity_mismatch")
    elif admission["resolved"] is None:
        result.update(decision="pending_adult", reason="adult_not_confirmed_by_available_data")
    elif existing:
        result.update(decision="existing_duplicate" if existing["count"] > 1 else "existing",
                      reason="destination_identity_exists", existing=existing)
    else:
        try:
            proposal, defaults = prepare_movie(normalized, real_spanish_only=record.get("_ready_contract") == 1)
            result.update(decision="proposed_new", reason="eligible_new_identity", proposed_fields=proposal, model_defaults=defaults)
        except ValidationError as exc:
            result.update(decision="validation_error", reason="model_projection_validation_failed",
                          validation_fields=sorted(exc.message_dict) if hasattr(exc, "message_dict") else [])
        except (ValueError, ArithmeticError, TypeError):
            result.update(decision="validation_error", reason="model_projection_validation_failed")
    return result


def snapshot_input(source, directory, *, allow_offline=False, allow_partial=False):
    source = Path(source).resolve(strict=True)
    if (source / "preparation_manifest.json").is_file():
        from core.tmdb_import_ready import snapshot_ready_input
        return snapshot_ready_input(source, directory, allow_offline=allow_offline)
    manifest_path = source / "manifest.json"
    if manifest_path.resolve().parent != source:
        raise ValueError("Unsafe source manifest")
    manifest = read_json(manifest_path)
    snapshot = directory / "source_snapshot"
    snapshot.mkdir()
    shutil.copyfile(manifest_path, snapshot / "manifest.json")
    for name, entry in manifest["outputs"].items():
        if entry["file"] != name + ".jsonl.gz":
            raise ValueError("Unsafe output path")
        origin = source / entry["file"]
        if origin.resolve().parent != source:
            raise ValueError("Unsafe source output")
        shutil.copyfile(origin, snapshot / entry["file"])
    # Reuse all integrity, GZIP, UTF-8, JSON, classification and eligibility checks.
    verify_run(snapshot)
    if manifest != read_json(snapshot / "manifest.json"):
        raise ValueError("Source changed during snapshot")
    config = manifest["configuration"]
    if config["mode"] not in ("live", "offline") or (config["mode"] != "live" and not allow_offline):
        raise ValueError("Synthetic/offline input requires explicit dry-run opt-in")
    if config["normalizer_sha256"] != file_digest(Path(__file__).with_name("tmdb_normalizer.py")):
        raise ValueError("Unsupported normalizer version")
    if manifest["state"] not in ("completed", "limited") or (not manifest["complete"] and not allow_partial):
        raise ValueError("Partial input requires --allow-partial; failed/active runs are unsupported")
    return manifest, snapshot


def checkpoint_config(manifest, target, allow_offline, allow_partial):
    return {"source_manifest_sha256": hashlib.sha256(strict_json(manifest).encode()).hexdigest(),
            "target": target, "allow_offline_input": allow_offline, "allow_partial": allow_partial,
            "implementation": {name: file_digest(Path(__file__).with_name(name)) for name in
                               ("tmdb_importer.py", "tmdb_import_ready.py", "tmdb_eligibility.py", "tmdb_normalizer.py", "models.py", "tmdb_enrich_verify.py")}}


def stage_records(journal, manifest, snapshot):
    if manifest.get("input_kind") == "import_ready":
        with gzip.open(snapshot / "import_ready.jsonl.gz", "rt", encoding="utf-8") as stream:
            for line in stream:
                prepared = json.loads(line, parse_constant=reject_constant)
                record = prepared["candidate"]
                record["_ready_contract"] = 1
                journal.execute("INSERT INTO queue VALUES (?,?)", (record["source"]["position"], strict_json(record)))
        journal.commit()
        return
    for name in ("accepted", "incomplete", "excluded", "invalid"):
        with gzip.open(snapshot / f"{name}.jsonl.gz", "rt", encoding="utf-8") as stream:
            for line in stream:
                record = json.loads(line, parse_constant=reject_constant)
                record.pop("_ready_contract", None)  # reserved for verified prepared inputs
                if record["provenance"]["mode"] != manifest["configuration"]["mode"]:
                    raise ValueError("Inconsistent input provenance")
                if record.get("tmdb_flags") is not None and not isinstance(record["tmdb_flags"], dict):
                    raise ValueError("Invalid flags payload")
                if record.get("schema_version") == 2 and record["eligibility"] != evaluate_eligibility(record["normalization"]):
                    raise ValueError("Inconsistent eligibility")
                journal.execute("INSERT INTO queue VALUES (?,?)", (record["source"]["position"], strict_json(record)))
    journal.commit()


def plan_import(source, output_parent, target, snapshot_context, *, batch_size=100, limit=None,
                allow_offline=False, allow_partial=False, resume=None):
    """All writes here are to a new local report directory, never the target DB."""
    directory = Path(output_parent) / (datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:12])
    directory.mkdir(parents=True, exist_ok=False)
    summary = {"schema_version": SCHEMA_VERSION, "mode": "dry-run", "state": "verifying", "complete": False,
               "target": target, "apply_enabled": False, "inserted": 0, "counts": {}, "run_directory": str(directory)}
    manifest_path = directory / "import_manifest.json"

    def publish():
        atomic_bytes(manifest_path, (strict_json(summary) + "\n").encode("utf-8"))

    publish()
    try:
        manifest, snapshot = snapshot_input(source, directory, allow_offline=allow_offline, allow_partial=allow_partial)
        config = checkpoint_config(manifest, target, allow_offline, allow_partial)
        source_manifest = "preparation_manifest.json" if manifest.get("input_kind") == "import_ready" else "manifest.json"
        summary.update(source={"directory": str(Path(source).resolve()), "manifest_sha256": file_digest(snapshot / source_manifest),
                               "outputs": manifest["outputs"], "upstream_state": manifest["state"],
                               "pending_enrichment": manifest["counts"]["pending"],
                               "duplicate_input_events": manifest["counters"]["duplicates"]},
                       configuration=config, batch_size=batch_size, limit=limit, state="running",
                       requires_fresh_identity_check_before_apply=True)
        db_path = directory / "checkpoint.sqlite3"
        with closing(sqlite3.connect(db_path)) as journal:
            journal.execute("PRAGMA synchronous=FULL")
            journal.execute("PRAGMA cache_size=-8192")
            if resume:
                previous = Path(resume).resolve(strict=True)
                old_manifest = read_json(previous / "import_manifest.json")
                if old_manifest["state"] not in ("limited", "completed", "interrupted", "failed") or old_manifest.get("configuration") != config:
                    raise ValueError("Incompatible or active resume source")
                old_checkpoint = previous / "checkpoint.sqlite3"
                if old_manifest.get("checkpoint_sha256") != file_digest(old_checkpoint):
                    raise ValueError("Resume checkpoint checksum mismatch")
                with closing(sqlite3.connect(old_checkpoint.as_uri() + "?mode=ro", uri=True)) as old:
                    old.backup(journal)
                if json.loads(journal.execute("SELECT value FROM metadata").fetchone()[0]) != config:
                    raise ValueError("Resume checkpoint configuration mismatch")
                summary.update(resumed_from=str(previous), prior_decisions_not_revalidated=True)
            else:
                journal.executescript("CREATE TABLE queue(position INTEGER PRIMARY KEY, record TEXT NOT NULL);"
                                      "CREATE TABLE decisions(position INTEGER PRIMARY KEY, record TEXT NOT NULL);"
                                      "CREATE TABLE metadata(value TEXT NOT NULL);")
                journal.execute("INSERT INTO metadata VALUES (?)", (strict_json(config),))
                stage_records(journal, manifest, snapshot)
            counts = Counter({name: 0 for name in DECISIONS})
            for (raw,) in journal.execute("SELECT record FROM decisions"):
                counts[json.loads(raw)["decision"]] += 1
            previous_decisions = sum(counts.values())
            last_position = journal.execute("SELECT coalesce(max(position),0) FROM decisions").fetchone()[0]
            if journal.execute("SELECT count(*) FROM queue q LEFT JOIN decisions d ON q.position=d.position WHERE q.position<=? AND d.position IS NULL", (last_position,)).fetchone()[0]:
                raise ValueError("Resume decisions are not a confirmed ordered prefix")
            attempted = 0
            try:
                with snapshot_context():
                    while limit is None or attempted < limit:
                        size = batch_size if limit is None else min(batch_size, limit - attempted)
                        batch = [json.loads(row[0]) for row in journal.execute(
                            "SELECT record FROM queue WHERE position>? ORDER BY position LIMIT ?", (last_position, size))]
                        if not batch:
                            break
                        existing = existing_identities(target["alias"], batch)
                        decisions = [evaluate_record(record, existing.get((record["type"], record["tmdb_id"]))) for record in batch]
                        with journal:
                            journal.executemany("INSERT INTO decisions VALUES (?,?)", [(item["position"], strict_json(item)) for item in decisions])
                        last_position = decisions[-1]["position"]
                        for item in decisions:
                            counts[item["decision"]] += 1
                        attempted += len(batch)
                remaining = journal.execute("SELECT count(*) FROM queue q LEFT JOIN decisions d ON q.position=d.position WHERE d.position IS NULL").fetchone()[0]
                summary.update(state="limited" if remaining else "completed", complete=not remaining)
            except BaseException as exc:
                summary.update(state="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed", error_type=type(exc).__name__)
                raise
            finally:
                # Journal commits are authoritative, including an interrupt just after COMMIT.
                counts = Counter({name: 0 for name in DECISIONS})
                for (raw,) in journal.execute("SELECT record FROM decisions"):
                    counts[json.loads(raw)["decision"]] += 1
                summary.update(counts=dict(counts), examined=sum(counts.values()), examined_this_invocation=sum(counts.values()) - previous_decisions,
                               incomplete_acceptable=journal.execute("SELECT count(*) FROM decisions WHERE json_extract(record,'$.source_status')='incomplete' AND json_extract(record,'$.decision')='proposed_new'").fetchone()[0],
                               skipped=sum(counts.values()) - counts["proposed_new"])
                with (directory / "decisions.jsonl").open("x", encoding="utf-8", newline="\n") as stream:
                    for (raw,) in journal.execute("SELECT record FROM decisions ORDER BY position"):
                        stream.write(raw + "\n")
        summary["checkpoint_sha256"] = file_digest(db_path)
        summary["decisions_sha256"] = file_digest(directory / "decisions.jsonl")
        publish()
        return summary
    except BaseException as exc:
        summary.update(state="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed", complete=False, error_type=type(exc).__name__)
        if (directory / "checkpoint.sqlite3").is_file():
            summary["checkpoint_sha256"] = file_digest(directory / "checkpoint.sqlite3")
        publish()
        raise
