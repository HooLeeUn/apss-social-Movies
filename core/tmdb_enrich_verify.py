"""Verify paused/completed enrichment artifacts offline, without modifying the run."""
import argparse
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile

from core.tmdb_enrichment_client import reject_constant, strict_json
from core.tmdb_enrichment_store import OUTPUTS, STATUSES, file_digest
from core.tmdb_eligibility import POLICY_VERSION, evaluate_eligibility


def verify_run(directory, *, input_path=None):
    directory = Path(directory).resolve(strict=True)
    with (directory / "manifest.json").open(encoding="utf-8") as stream:
        manifest = json.load(stream, parse_constant=reject_constant)
    strict_json(manifest)
    if manifest["schema_version"] != 1 or set(manifest["outputs"]) != {*OUTPUTS, "pending"}:
        raise ValueError("Unsupported manifest")
    policy_version = manifest["configuration"].get("eligibility_policy_version")
    if policy_version is not None and (type(policy_version) is not int or policy_version != POLICY_VERSION):
        raise ValueError("Unsupported eligibility policy")
    if input_path is not None and file_digest(input_path) != manifest["input"]["sha256"]:
        raise ValueError("Input checksum mismatch")
    counts = {name: 0 for name in (*STATUSES, "pending")}
    duplicate_count = 0
    with tempfile.TemporaryDirectory(prefix="reccool-verify-") as temporary:
        connection = sqlite3.connect(Path(temporary) / "seen.sqlite3")
        try:
            connection.execute("CREATE TABLE seen(position INTEGER PRIMARY KEY,type TEXT,tmdb_id INTEGER,UNIQUE(type,tmdb_id))")
            for name, output in manifest["outputs"].items():
                if output["file"] != f"{name}.jsonl.gz":
                    raise ValueError("Unexpected output path")
                path = directory / output["file"]
                if path.stat().st_size != output["bytes"] or file_digest(path) != output["sha256"]:
                    raise ValueError("Output checksum/size mismatch")
                lines = 0
                with gzip.open(path, "rt", encoding="utf-8") as stream:
                    for line in stream:
                        record = json.loads(line, parse_constant=reject_constant)
                        strict_json(record)
                        lines += 1
                        if name == "duplicates":
                            if record["code"] != "duplicate_input":
                                raise ValueError("Invalid duplicate event")
                            duplicate_count += 1
                            continue
                        if record["status"] != name:
                            raise ValueError("Status/output mismatch")
                        position = record["source"]["position"]
                        if type(position) is not int or position < 1:
                            raise ValueError("Invalid trace position")
                        connection.execute("INSERT INTO seen VALUES (?,?,?)", (position, record["type"], record["tmdb_id"]))
                        if record["type"] is not None and (record["type"] not in ("movie", "series") or
                                type(record["tmdb_id"]) is not int or not 0 < record["tmdb_id"] <= 2147483647):
                            raise ValueError("Invalid normalized identity")
                        if name in STATUSES:
                            normalized = record["normalization"]
                            version = record.get("schema_version")
                            if version not in (1, 2) or type(version) is not int:
                                raise ValueError("Unsupported record schema")
                            if version == 2:
                                if policy_version != POLICY_VERSION or strict_json(record.get("eligibility")) != strict_json(evaluate_eligibility(normalized)):
                                    raise ValueError("Eligibility policy mismatch")
                            elif policy_version is not None or "eligibility" in record:
                                raise ValueError("Legacy records cannot declare a new eligibility contract")
                            if record["normalizer_sha256"] != manifest["configuration"]["normalizer_sha256"]:
                                raise ValueError("Normalizer version mismatch")
                            if normalized is None:
                                if name != "invalid" or record["lookup_error"]["code"] != "tmdb_not_found":
                                    raise ValueError("Missing normalizer result")
                            elif normalized["status"] != name or normalized["valid"] != (name != "invalid"):
                                raise ValueError("Normalizer classification mismatch")
                            if record["publication"]["auto_importable"] is not False:
                                raise ValueError("Unexpected publication approval")
                        counts[name] += 1
                if lines != output["records"]:
                    raise ValueError("Output record count mismatch")
        finally:
            connection.close()
    if counts != manifest["counts"] or sum(counts[name] for name in STATUSES) != manifest["terminal_results"]:
        raise ValueError("Manifest counts do not reconcile")
    if (duplicate_count != manifest["counters"]["duplicates"] or
            manifest["counters"]["examined"] != sum(counts.values()) + duplicate_count):
        raise ValueError("Examined/terminal/pending/duplicate counts do not reconcile")
    if manifest["complete"] != (manifest["state"] == "completed"):
        raise ValueError("Invalid completion flag")
    if manifest["complete"] and (not manifest["input_exhausted"] or counts["pending"]):
        raise ValueError("Incomplete run is marked completed")
    return {"verified": True, "state": manifest["state"], "complete": manifest["complete"],
            "counts": counts, "duplicates": duplicate_count}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--input", type=Path)
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args(argv)
    try:
        result = verify_run(args.directory, input_path=args.input)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error, EOFError):
        print('{"verified":false,"error":"artifact_verification_failed"}')
        return 3
    print(strict_json(result))
    return 0 if result["complete"] or args.allow_incomplete else 2


if __name__ == "__main__":
    raise SystemExit(main())
