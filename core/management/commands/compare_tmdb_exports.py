"""Read-only catalog comparison; the writable SQLite index is disposable, not Django's DB."""
import argparse
import csv
import gzip
import json
import sqlite3
import tempfile
import time
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connections, transaction

from core.models import Movie


def positive(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("Expected a positive integer.")
    return parsed


def valid_id(value):
    # JSON booleans, floats and numeric strings are deliberately rejected.
    return type(value) is int and 0 < value <= 2147483647


def read_database(alias):
    connection = connections[alias]
    if connection.vendor != "postgresql":
        raise CommandError("Only PostgreSQL is supported: READ ONLY protection is required.")
    if connection.in_atomic_block or not connection.get_autocommit():
        raise CommandError("Run outside an existing transaction to establish READ ONLY safely.")
    with transaction.atomic(using=alias):
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        # Additional fail-closed guard: only this model's SELECT is permitted.
        def only_catalog_select(execute, sql, params, many, context):
            table = connection.ops.quote_name(Movie._meta.db_table)
            if many or not sql.lstrip().upper().startswith("SELECT ") or table not in sql:
                raise CommandError("Unexpected SQL blocked during catalog comparison.")
            return execute(sql, params, many, context)

        with connection.execute_wrapper(only_catalog_select):
            yield from (Movie.objects.using(alias).order_by("id")
                        .values_list("id", "tmdb_id", "type").iterator(chunk_size=2000))


class Reports:
    FIELDS = {
        "new_candidates": ["type", "tmdb_id", "original_title", "popularity", "adult", "video"],
        "database_duplicates": ["type", "tmdb_id", "database_id", "first_database_id"],
        "export_duplicates": ["type", "tmdb_id", "line", "first_line"],
        "invalid_records": ["source", "type", "database_id", "line", "reason"],
        "database_not_in_export": ["type", "tmdb_id", "database_id", "status"],
    }

    def __init__(self, directory, stack, sample_size):
        self.writers = {}
        self.samples = {name: [] for name in self.FIELDS}
        self.sample_size = sample_size
        for name, fields in self.FIELDS.items():
            handle = stack.enter_context((directory / (name + ".csv")).open(
                "w", encoding="utf-8-sig", newline=""))
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            self.writers[name] = writer

    def row(self, name, **values):
        # Prevent spreadsheet formula execution from untrusted export titles.
        safe = {key: ("'" + value if isinstance(value, str) and
                      value.lstrip().startswith(("=", "+", "-", "@")) else value)
                for key, value in values.items()}
        self.writers[name].writerow(safe)
        if len(self.samples[name]) < self.sample_size:
            self.samples[name].append(safe)


class Command(BaseCommand):
    help = "Compare TMDb GZIP JSON Lines with Movie, using PostgreSQL READ ONLY."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument("--movies-file", required=True)
        parser.add_argument("--series-file", required=True)
        parser.add_argument("--output-dir", required=True)
        parser.add_argument("--limit", type=positive)
        parser.add_argument("--sample-size", type=int, default=20)
        parser.add_argument("--database", default="default")

    def handle(self, *args, **options):
        started = time.monotonic()
        if options["sample_size"] < 0:
            raise CommandError("--sample-size must be nonnegative.")
        paths = [(Movie.MOVIE, Path(options["movies_file"]).expanduser().resolve()),
                 (Movie.SERIES, Path(options["series_file"]).expanduser().resolve())]
        for _, path in paths:
            if not path.is_file():
                raise CommandError(f"Export does not exist: {path}")
        output = Path(options["output_dir"]).expanduser().resolve()
        if output == Path(settings.BASE_DIR).resolve() or Path(settings.BASE_DIR).resolve() in output.parents:
            raise CommandError("Reports must be outside the repository.")
        # Never overwrite previous reports or a source export; each run gets its own directory.
        if output.exists() and any(output.iterdir()):
            raise CommandError("--output-dir must be new or empty.")
        try:
            output.mkdir(parents=True, exist_ok=True)
            with ExitStack() as stack:
                scratch = stack.enter_context(tempfile.TemporaryDirectory(prefix="tmdb-compare-", dir=output))
                index = sqlite3.connect(str(Path(scratch) / "index.sqlite3"))
                stack.callback(index.close)
                index.execute("PRAGMA cache_size=-8192")
                index.execute("PRAGMA temp_store=FILE")
                index.execute("CREATE TABLE catalog (type TEXT, tmdb_id INTEGER, database_id INTEGER, PRIMARY KEY(type, tmdb_id, database_id)) WITHOUT ROWID")
                index.execute("CREATE TABLE seen (type TEXT, tmdb_id INTEGER, line INTEGER, PRIMARY KEY(type, tmdb_id)) WITHOUT ROWID")
                reports = Reports(output, stack, options["sample_size"])
                counts = {kind: Counter(database_valid_rows=0, database_duplicate_extra_rows=0)
                          for kind, _ in paths}
                db_counts = Counter(rows=0, invalid=0, null_tmdb_id=0)
                for pk, tmdb_id, kind in read_database(options["database"]):
                    db_counts["rows"] += 1
                    if kind not in counts or not valid_id(tmdb_id):
                        db_counts["invalid"] += 1
                        if tmdb_id is None:
                            db_counts["null_tmdb_id"] += 1
                        reports.row("invalid_records", source="database", type=kind,
                                    database_id=pk, reason="invalid_type" if kind not in counts else "invalid_id")
                        continue
                    first = index.execute("SELECT database_id FROM catalog WHERE type=? AND tmdb_id=? ORDER BY database_id LIMIT 1", (kind, tmdb_id)).fetchone()
                    if first:
                        counts[kind]["database_duplicate_extra_rows"] += 1
                        reports.row("database_duplicates", type=kind, tmdb_id=tmdb_id,
                                    database_id=pk, first_database_id=first[0])
                    index.execute("INSERT INTO catalog VALUES (?, ?, ?)", (kind, tmdb_id, pk))
                    counts[kind]["database_valid_rows"] += 1
                index.commit()
                for kind, path in paths:
                    self.stdout.write(f"Reading {kind}: {path.name}")
                    self.compare_file(index, reports, counts[kind], kind, path, options["limit"])
                for kind, tmdb_id, pk in index.execute(
                    "SELECT c.type,c.tmdb_id,c.database_id FROM catalog c LEFT JOIN seen s ON c.type=s.type AND c.tmdb_id=s.tmdb_id WHERE s.tmdb_id IS NULL ORDER BY c.type,c.tmdb_id,c.database_id"
                ):
                    counts[kind]["database_not_in_export_rows"] += 1
                    partial = counts[kind]["truncated"] or counts[kind]["invalid"]
                    reports.row("database_not_in_export", type=kind, tmdb_id=tmdb_id,
                                database_id=pk, status="not_observed_in_partial_or_invalid_export" if partial else "absent_from_export_not_confirmed_deleted")
                summary = {
                    "status": "complete", "database_alias": options["database"],
                    "database": dict(db_counts), "types": {k: dict(v) for k, v in counts.items()},
                    "limit": options["limit"], "samples": reports.samples,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "deletions_confirmed": False,
                }
                with (output / "comparison_summary.csv").open("w", encoding="utf-8-sig", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["type", "metric", "count"])
                    for kind, metrics in counts.items():
                        for metric, count in sorted(metrics.items()):
                            writer.writerow([kind, metric, count])
            # Written after CSV handles close and scratch cleanup succeeds.
            # Missing summary means reports are incomplete.
            (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
            self.stdout.write(json.dumps(summary["types"], ensure_ascii=False))
            self.stdout.write(self.style.SUCCESS(f"Reports: {output}; elapsed {summary['elapsed_seconds']}s"))
        except (OSError, EOFError, sqlite3.Error) as exc:
            raise CommandError(f"Comparison failed; reports may be incomplete: {exc}") from exc

    def compare_file(self, index, reports, counts, kind, path, limit):
        for metric in ("lines", "valid", "invalid", "unique", "existing", "new_candidates",
                       "export_duplicate_extra_rows", "truncated", "database_not_in_export_rows"):
            counts[metric] = 0
        with gzip.open(path, "rb") as handle:
            for line_number, raw in enumerate(handle, 1):
                if limit is not None and line_number > limit:
                    counts["truncated"] = 1
                    break
                counts["lines"] += 1
                try:
                    record = json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
                    if not isinstance(record, dict):
                        raise ValueError("expected_object")
                    if "id" not in record:
                        raise ValueError("missing_id")
                    if not valid_id(record["id"]):
                        raise ValueError("invalid_id")
                except (ValueError, UnicodeError, RecursionError) as exc:
                    counts["invalid"] += 1
                    # Do not copy raw lines or arbitrary exception messages into reports.
                    reason = str(exc) if str(exc) in {"expected_object", "missing_id", "invalid_id"} else "invalid_json_or_encoding"
                    reports.row("invalid_records", source="export", type=kind, line=line_number, reason=reason)
                    continue
                counts["valid"] += 1
                tmdb_id = record["id"]
                first = index.execute("SELECT line FROM seen WHERE type=? AND tmdb_id=?", (kind, tmdb_id)).fetchone()
                if first:
                    counts["export_duplicate_extra_rows"] += 1
                    reports.row("export_duplicates", type=kind, tmdb_id=tmdb_id, line=line_number, first_line=first[0])
                    continue
                index.execute("INSERT INTO seen VALUES (?, ?, ?)", (kind, tmdb_id, line_number))
                counts["unique"] += 1
                exists = index.execute("SELECT 1 FROM catalog WHERE type=? AND tmdb_id=? LIMIT 1", (kind, tmdb_id)).fetchone()
                if exists:
                    counts["existing"] += 1
                else:
                    counts["new_candidates"] += 1
                    reports.row("new_candidates", type=kind, tmdb_id=tmdb_id,
                                original_title=record.get("original_title", record.get("original_name", "")),
                                **{field: record.get(field, "") for field in ("popularity", "adult", "video")})
        index.commit()
