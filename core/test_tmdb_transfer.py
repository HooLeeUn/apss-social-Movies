import csv
import io
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.management import call_command, CommandError
from django.db import connection
from django.test import TestCase, SimpleTestCase
from django.test.utils import CaptureQueriesContext

from core.models import Movie, MovieQuerySet
from core.management.commands.export_tmdb_rating_votes import FIELDS


class TransferTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.movie = Movie.objects.create(title_english="Original", type="movie", tmdb_id=123,
                                          external_rating=Decimal("6.0"), external_votes=4)
        self.input = self.root / "input.csv"
        self.report = self.root / "report.csv"

    def write_rows(self, rows):
        with self.input.open("w", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(FIELDS)
            writer.writerows(rows)

    def row(self, **changes):
        values = dict(zip(FIELDS, [self.movie.pk, 123, "movie", "8.3", 100]))
        values.update(changes)
        return [values[field] for field in FIELDS]

    def run_import(self, *args):
        out = io.StringIO()
        call_command("import_tmdb_rating_votes_csv", "--input", str(self.input),
                     "--report-csv", str(self.report), *args, stdout=out, stderr=io.StringIO())
        with self.report.open(encoding="utf-8") as report:
            rows = list(csv.DictReader(report))
        return out.getvalue(), rows

    def test_export_order_fields_and_values(self):
        other = Movie.objects.create(title_english="Other", type="series", tmdb_id=234)
        call_command("export_tmdb_rating_votes", output=str(self.input), stdout=io.StringIO())
        with self.input.open(encoding="utf-8") as source:
            reader = csv.DictReader(source)
            self.assertEqual(reader.fieldnames, FIELDS)
            rows = list(reader)
        self.assertEqual([int(row["database_id"]) for row in rows], [self.movie.pk, other.pk])
        self.assertEqual(rows[0]["external_rating"], "6.0")
        self.assertEqual(rows[1]["external_rating"], "")

    def test_export_rejects_missing_tmdb(self):
        Movie.objects.filter(pk=self.movie.pk).update(tmdb_id=None)
        with self.assertRaises(CommandError):
            call_command("export_tmdb_rating_votes", output=str(self.input), stdout=io.StringIO())
        self.assertFalse(self.input.exists())

    def test_dry_run_checks_real_identity_without_writes(self):
        self.write_rows([self.row()])
        before = Movie.objects.values().get(pk=self.movie.pk)
        with CaptureQueriesContext(connection) as queries:
            out, rows = self.run_import("--dry-run")
        self.assertEqual(rows[0]["status"], "would_update")
        self.assertIn("would_update: 1", out)
        self.assertEqual(Movie.objects.values().get(pk=self.movie.pk), before)
        self.assertFalse(any(q["sql"].lstrip().upper().startswith("UPDATE") for q in queries))

    def test_apply_only_two_fields_and_idempotence(self):
        self.write_rows([self.row()])
        before = Movie.objects.values().get(pk=self.movie.pk)
        with patch.object(Movie, "save", side_effect=AssertionError("No save allowed")):
            _, rows = self.run_import("--apply")
        self.assertEqual(rows[0]["status"], "updated")
        after = Movie.objects.values().get(pk=self.movie.pk)
        self.assertEqual(after.pop("external_rating"), Decimal("8.3"))
        self.assertEqual(after.pop("external_votes"), 100)
        before.pop("external_rating")
        before.pop("external_votes")
        self.assertEqual(before, after)
        self.report = self.root / "second.csv"
        _, rows = self.run_import("--apply")
        self.assertEqual(rows[0]["status"], "unchanged")

    def test_identity_and_missing(self):
        for changes, status in (({"tmdb_id": 456}, "identity_mismatch"),
                                ({"type": "series"}, "identity_mismatch"),
                                ({"database_id": self.movie.pk + 9999}, "missing")):
            for apply in (False, True):
                self.report = self.root / f"{len(list(self.root.iterdir()))}.csv"
                self.write_rows([self.row(**changes)])
                _, rows = self.run_import(*(["--apply"] if apply else []))
                self.assertEqual(rows[0]["status"], status)
        self.movie.refresh_from_db()
        self.assertEqual(self.movie.external_votes, 4)

    def test_invalid_rows_and_duplicates(self):
        rows = [self.row(**changes) for changes in (
            {"database_id": "0"}, {"tmdb_id": ""}, {"type": "tv"},
            {"external_rating": "NaN"}, {"external_rating": "8.33"},
            {"external_rating": "11"}, {"external_votes": "-1"},
            {"external_votes": "2147483648"})]
        self.write_rows(rows + [self.row(), self.row()])
        _, report = self.run_import()
        self.assertEqual(sum(row["status"] == "invalid_row" for row in report), 9)
        self.assertEqual(sum(row["status"] == "would_update" for row in report), 1)

    def test_bulk_batches(self):
        others = [Movie.objects.create(title_english="Batch", type="movie", tmdb_id=123) for _ in range(4)]
        self.write_rows([self.row()] + [self.row(database_id=movie.pk) for movie in others])
        original = MovieQuerySet.bulk_update
        calls = []

        def track(queryset, objects, fields, **kwargs):
            calls.append((len(objects), fields))
            return original(queryset, objects, fields, **kwargs)

        with patch.object(MovieQuerySet, "bulk_update", track):
            self.run_import("--apply", "--batch-size", "2")
        self.assertEqual([size for size, _ in calls], [2, 2, 1])
        self.assertTrue(all(fields == ["external_rating", "external_votes"] for _, fields in calls))

    def test_bulk_error_is_reported_and_rolled_back(self):
        self.write_rows([self.row()])
        with patch.object(MovieQuerySet, "bulk_update", side_effect=RuntimeError("failure")):
            out, rows = self.run_import("--apply")
        self.assertEqual(rows[0]["status"], "error")
        self.assertIn("errores: 1", out)
        self.movie.refresh_from_db()
        self.assertEqual(self.movie.external_votes, 4)

    def test_missing_headers_and_bad_batch(self):
        self.input.write_text("database_id\n1\n", encoding="utf-8")
        for args in ([], ["--batch-size", "0"]):
            with self.assertRaises(CommandError):
                self.run_import(*args)


class ManifestTests(SimpleTestCase):
    def test_combination_deduplication_and_invalid_ids_without_db(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            missing, gone, output = [root / name for name in ("missing.csv", "gone.csv", "out.csv")]
            missing.write_text("database_id,title\n1,One\n2,Two\n2,Duplicate\n0,Invalid\n", encoding="utf-8")
            gone.write_text("database_id\n2\n3\nabc\n", encoding="utf-8")
            out = io.StringIO()
            call_command("build_movies_delete_manifest", missing_tmdb_csv=str(missing),
                         tmdb_404_csv=str(gone), output=str(output), stdout=out)
            with output.open(encoding="utf-8") as source:
                rows = list(csv.DictReader(source))
            self.assertEqual(rows, [dict(database_id="1", reason="missing_tmdb_id"),
                                    dict(database_id="2", reason="missing_tmdb_id"),
                                    dict(database_id="3", reason="tmdb_404")])
            self.assertIn("missing_tmdb_id: 2", out.getvalue())
            self.assertIn("tmdb_404: 1", out.getvalue())
            self.assertIn("duplicados: 2", out.getvalue())
            self.assertIn("inválidos: 2", out.getvalue())
            self.assertIn("total único: 3", out.getvalue())
