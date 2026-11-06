"""Synthetic tests: no Django database or configured production connection is opened."""
import csv
import gzip
import io
import json
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.core.management.base import CommandError

from core.management.commands import compare_tmdb_exports as module


class CompareExportsTests(TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tmdb synthetic tests ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_comparison(self, movies=(), series=(), rows=(), **options):
        for name, records in (("movies", movies), ("series", series)):
            with gzip.open(self.root / f"{name} with spaces.gz", "wb") as handle:
                for record in records:
                    handle.write((record if isinstance(record, bytes) else json.dumps(record).encode()) + b"\n")
        output = self.root / "reports with spaces"
        # Any accidental ORM write fails, even though no actual DB is available.
        with patch.object(module, "read_database", return_value=iter(rows)) as reader, \
                patch.object(module.Movie, "save", side_effect=AssertionError("write")), \
                patch.object(module.Movie, "delete", side_effect=AssertionError("write")), \
                patch.object(module.Movie.objects, "using", side_effect=AssertionError("unexpected ORM access")):
            call_command("compare_tmdb_exports", movies_file=str(self.root / "movies with spaces.gz"),
                         series_file=str(self.root / "series with spaces.gz"), output_dir=str(output),
                         stdout=io.StringIO(), **options)
            reader.assert_called_once_with(options.get("database", "default"))
        self.output = output
        return json.loads((output / "summary.json").read_text(encoding="utf-8"))

    def csv_rows(self, name):
        with (self.output / f"{name}.csv").open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_existing_new_and_same_id_across_types(self):
        summary = self.run_comparison(movies=[{"id": 7}, {"id": 8}], series=[{"id": 7}],
                                      rows=[(1, 7, "movie"), (2, 9, "series")], database="catalog")
        self.assertEqual(summary["types"]["movie"]["existing"], 1)
        self.assertEqual(summary["types"]["series"]["existing"], 0)
        self.assertEqual([(r["type"], r["tmdb_id"]) for r in self.csv_rows("new_candidates")],
                         [("movie", "8"), ("series", "7")])
        self.assertEqual(self.csv_rows("database_not_in_export")[0]["database_id"], "2")
        self.assertFalse(summary["deletions_confirmed"])
        self.assertFalse(list(self.output.glob("tmdb-compare-*")))

    def test_duplicates_are_extra_rows_not_new_candidates(self):
        summary = self.run_comparison(movies=[{"id": 7}, {"id": 7}, {"id": 8}, {"id": 8}],
                                      rows=[(1, 7, "movie"), (2, 7, "movie"), (3, 7, "series")])
        counts = summary["types"]["movie"]
        self.assertEqual(counts["existing"], 1)
        self.assertEqual(counts["new_candidates"], 1)
        self.assertEqual(counts["export_duplicate_extra_rows"], 2)
        self.assertEqual(counts["database_duplicate_extra_rows"], 1)
        self.assertEqual(self.csv_rows("database_duplicates")[0]["first_database_id"], "1")

    def test_invalid_json_ids_encoding_and_database_rows(self):
        invalid = [b"{bad", b"\xff", [], {}, {"id": None}, {"id": ""}, {"id": "7"},
                   {"id": True}, {"id": 1.5}, {"id": 0}, {"id": -1}, {"id": 2147483648}, b'{"id":NaN}']
        summary = self.run_comparison(movies=invalid + [{"id": 8}], rows=[
            (1, None, "movie"), (2, 0, "series"), (3, 8, None), (4, 9, "tv")])
        self.assertEqual(summary["types"]["movie"]["invalid"], len(invalid))
        self.assertEqual(summary["types"]["movie"]["new_candidates"], 1)
        self.assertEqual(summary["database"]["invalid"], 4)
        self.assertEqual(summary["database"]["null_tmdb_id"], 1)
        self.assertEqual(len(self.csv_rows("invalid_records")), len(invalid) + 4)

    def test_empty_files_and_absence(self):
        summary = self.run_comparison(rows=[(1, 4, "movie")])
        self.assertEqual(summary["types"]["movie"]["lines"], 0)
        self.assertEqual(self.csv_rows("new_candidates"), [])
        self.assertEqual(self.csv_rows("database_not_in_export")[0]["status"],
                         "absent_from_export_not_confirmed_deleted")

    def test_limit_counts_physical_lines_and_marks_absence_partial(self):
        summary = self.run_comparison(movies=[b"bad", {"id": 2}, {"id": 3}],
                                      series=[{"id": 1}, {"id": 2}, {"id": 3}],
                                      rows=[(1, 3, "movie")], limit=2)
        for counts in summary["types"].values():
            self.assertEqual(counts["lines"], 2)
            self.assertEqual(counts["truncated"], 1)
        self.assertIn("partial", self.csv_rows("database_not_in_export")[0]["status"])

    def test_exact_limit_is_not_truncated(self):
        summary = self.run_comparison(movies=[{"id": 1}], limit=1)
        self.assertEqual(summary["types"]["movie"]["truncated"], 0)

    def test_metadata_samples_and_formula_protection(self):
        summary = self.run_comparison(series=[{"id": 1, "original_name": "=formula", "popularity": 2,
                                             "adult": False}, {"id": 2}], sample_size=1)
        self.assertEqual(len(summary["samples"]["new_candidates"]), 1)
        records = self.csv_rows("new_candidates")
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["original_title"], "'=formula")
        self.assertEqual(records[0]["adult"], "False")
        self.assertEqual(records[0]["video"], "")

    def test_existing_output_rejected(self):
        self.run_comparison()
        with self.assertRaisesRegex(CommandError, "new or empty"):
            self.run_comparison()

    def test_output_in_repo_rejected_before_database_access(self):
        source = self.root / "empty.gz"
        source.write_bytes(gzip.compress(b""))
        with patch.object(module, "read_database", side_effect=AssertionError("DB access")):
            with self.assertRaisesRegex(CommandError, "outside the repository"):
                call_command("compare_tmdb_exports", movies_file=str(source), series_file=str(source),
                             output_dir=str(Path(module.settings.BASE_DIR) / "forbidden-reports"))

    def test_corrupt_gzip_has_no_success_summary(self):
        source = self.root / "corrupt.gz"
        source.write_bytes(b"not gzip")
        output = self.root / "failed"
        with patch.object(module, "read_database", return_value=iter([])):
            with self.assertRaises(CommandError):
                call_command("compare_tmdb_exports", movies_file=str(source), series_file=str(source),
                             output_dir=str(output), stdout=io.StringIO())
        self.assertFalse((output / "summary.json").exists())


class ReadOnlyBoundaryTests(TestCase):
    def test_postgres_transaction_and_projection(self):
        connection = MagicMock(vendor="postgresql", in_atomic_block=False)
        connection.get_autocommit.return_value = True
        connection.ops.quote_name.side_effect = lambda name: '"' + name + '"'
        events = []
        connection.cursor.return_value.__enter__.return_value.execute.side_effect = lambda sql: events.append(sql)

        @contextmanager
        def wrapper(guard):
            execute = MagicMock()
            for sql in ('UPDATE "core_movie" SET tmdb_id=1', 'SELECT * FROM "auth_user"',
                        'DELETE FROM "core_movie"', 'WITH x AS (DELETE FROM "core_movie") SELECT 1'):
                with self.assertRaises(CommandError):
                    guard(execute, sql, None, False, {})
            guard(execute, 'SELECT "core_movie"."id" FROM "core_movie"', None, False, {})
            execute.assert_called_once()
            yield

        connection.execute_wrapper.side_effect = wrapper
        query = MagicMock()
        query.order_by.return_value.values_list.return_value.iterator.side_effect = lambda **kwargs: (
            events.append("read") or iter([(1, 2, "movie")]))
        with patch.object(module, "connections", {"isolated": connection}), \
                patch.object(module.transaction, "atomic") as atomic, \
                patch.object(module.Movie.objects, "using", return_value=query) as using:
            self.assertEqual(list(module.read_database("isolated")), [(1, 2, "movie")])
        atomic.assert_called_once_with(using="isolated")
        using.assert_called_once_with("isolated")
        query.order_by.assert_called_once_with("id")
        query.order_by.return_value.values_list.assert_called_once_with("id", "tmdb_id", "type")
        self.assertEqual(events, ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", "read"])

    def test_unsupported_backend_and_nested_transaction_fail_closed(self):
        for vendor, nested, autocommit in [("sqlite", False, True), ("postgresql", True, True),
                                            ("postgresql", False, False)]:
            connection = MagicMock(vendor=vendor, in_atomic_block=nested)
            connection.get_autocommit.return_value = autocommit
            with patch.object(module, "connections", {"default": connection}):
                with self.assertRaises(CommandError):
                    list(module.read_database("default"))
            connection.cursor.assert_not_called()
