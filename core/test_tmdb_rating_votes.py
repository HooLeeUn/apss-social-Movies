import csv
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.db import IntegrityError, connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Movie
from core.tmdb import TMDbServiceError


class SyncRatingVotesTests(TestCase):
    def setUp(self):
        self.author = get_user_model().objects.create_user(username="rating-sync")
        self.movie = self.create_movie()
        self.tmdb_patch = patch("core.management.commands.enrich_movies_tmdb.get_tmdb_json")
        self.tmdb = self.tmdb_patch.start()
        self.addCleanup(self.tmdb_patch.stop)
        self.sleep_patch = patch("core.management.commands.enrich_movies_tmdb.time.sleep")
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)
        self.tmdb.return_value = {
            "vote_average": 8.25, "vote_count": 100,
            "title": "Changed title", "overview": "Changed synopsis", "poster_path": "/new.jpg",
        }

    def create_movie(self, **kwargs):
        values = dict(author=self.author, title_english="Original", title_spanish="Original ES",
                      type=Movie.MOVIE, tmdb_id=123, external_rating=Decimal("6.0"),
                      external_votes=42, release_year=2000, synopsis="Original synopsis",
                      image="https://example.com/original.jpg", tmdb_lookup_status="error",
                      tmdb_lookup_error="Existing error")
        values.update(kwargs)
        return Movie.objects.create(**values)

    def run_sync(self, *args):
        out, err = io.StringIO(), io.StringIO()
        call_command("enrich_movies_tmdb", "--sync-rating-votes", "--sleep", "0",
                     *args, stdout=out, stderr=err)
        return out.getvalue(), err.getvalue()

    def snapshot(self):
        return Movie.objects.values().get(pk=self.movie.pk)

    def test_movie_updates_only_rating_votes_and_logs(self):
        before = self.snapshot()
        out, _ = self.run_sync("--movie-id", str(self.movie.pk), "--verify-persistence")
        after = self.snapshot()
        self.assertEqual(after.pop("external_rating"), Decimal("8.3"))
        self.assertEqual(after.pop("external_votes"), 100)
        before.pop("external_rating")
        before.pop("external_votes")
        self.assertEqual(after, before)
        self.tmdb.assert_called_once_with("/movie/123", params={"language": "en-US"})
        self.assertIn(f"Movie(id={self.movie.pk}) tmdb_id=123: rating 6.0 -> 8.3; votes 42 -> 100", out)
        self.assertIn("persistence_mismatches: 0", out)

    def test_series_uses_tv_endpoint(self):
        Movie.objects.filter(pk=self.movie.pk).update(type=Movie.SERIES)
        self.run_sync()
        self.tmdb.assert_called_once_with("/tv/123", params={"language": "en-US"})
        self.movie.refresh_from_db()
        self.assertEqual(self.movie.external_rating, Decimal("8.3"))
        self.assertEqual(self.movie.external_votes, 100)

    def test_zero_votes_are_persisted(self):
        self.tmdb.return_value = {"vote_average": 7, "vote_count": 0}
        self.run_sync()
        self.movie.refresh_from_db()
        self.assertEqual(self.movie.external_votes, 0)

    def test_null_and_zero_rating(self):
        for rating, expected in ((None, None), (0, Decimal("0.0"))):
            with self.subTest(rating=rating):
                self.tmdb.return_value = {"vote_average": rating, "vote_count": 0}
                self.run_sync()
                self.movie.refresh_from_db()
                self.assertEqual(self.movie.external_rating, expected)

    def test_dry_run_has_no_writes(self):
        before = self.snapshot()
        with CaptureQueriesContext(connection) as queries:
            out, _ = self.run_sync("--dry-run")
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(any(q["sql"].lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE")) for q in queries))
        self.assertIn("DRY-RUN", out)
        self.assertIn("ratings_updated: 1", out)

    def test_error_retries_without_modifying_any_fields(self):
        before = self.snapshot()
        self.tmdb.side_effect = TMDbServiceError("Unavailable")
        out, err = self.run_sync()
        self.assertEqual(self.tmdb.call_count, 3)
        self.assertEqual(self.snapshot(), before)
        self.assertIn("errores: 1", out)
        self.assertIn("requests_realizadas: 3", out)
        self.assertIn(f"Movie(id={self.movie.pk}) tmdb_id=123: TMDB error", err)

    def test_retry_can_succeed(self):
        self.tmdb.side_effect = [TMDbServiceError("Temporary"), self.tmdb.return_value]
        out, _ = self.run_sync()
        self.movie.refresh_from_db()
        self.assertEqual(self.movie.external_votes, 100)
        self.assertIn("requests_realizadas: 2", out)

    def test_idempotent_run_has_no_update(self):
        self.run_sync()
        before = self.snapshot()
        with CaptureQueriesContext(connection) as queries:
            out, _ = self.run_sync()
        self.assertEqual(self.snapshot(), before)
        self.assertFalse(any(q["sql"].lstrip().upper().startswith("UPDATE") for q in queries))
        self.assertIn("unchanged: 1", out)

    def test_filters_and_missing_tmdb(self):
        self.create_movie(tmdb_id=None)
        target = self.create_movie(tmdb_id=456)
        self.create_movie(tmdb_id=789)
        self.run_sync("--start-id", str(target.pk), "--limit", "1")
        self.tmdb.assert_called_once_with("/movie/456", params={"language": "en-US"})
        self.tmdb.reset_mock()
        self.run_sync("--movie-id", str(self.movie.pk))
        self.tmdb.assert_called_once_with("/movie/123", params={"language": "en-US"})

    def test_missing_tmdb_is_not_resolved(self):
        missing = self.create_movie(tmdb_id=None)
        out, _ = self.run_sync("--movie-id", str(missing.pk))
        self.tmdb.assert_not_called()
        self.assertIn("Procesadas: 0", out)

    def read_errors(self, path):
        with path.open(newline="", encoding="utf-8") as stream:
            return list(csv.DictReader(stream))

    def test_csv_http_errors_and_deduplication(self):
        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            for status in (404, 429, 500, 503):
                with self.subTest(status=status):
                    self.tmdb.side_effect = TMDbServiceError("HTTP failure", status_code=status)
                    self.run_sync("--errors-csv", str(path))
                    self.run_sync("--errors-csv", str(path))
            rows = self.read_errors(path)
            self.assertEqual(len(rows), 4)
            self.assertEqual([r["status_code"] for r in rows], ["404", "429", "500", "503"])
            self.assertEqual(rows[0], dict(movie_id=str(self.movie.pk), tmdb_id="123",
                             type=Movie.MOVIE, title_english="Original", release_year="2000",
                             error_type="tmdb", status_code="404", error_message="HTTP failure"))

    def test_csv_dry_run_records_errors_without_database_writes(self):
        before = self.snapshot()
        self.tmdb.side_effect = TMDbServiceError("Missing", status_code=404)
        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            self.run_sync("--dry-run", "--verify-persistence", "--errors-csv", str(path))
            self.assertEqual(len(self.read_errors(path)), 1)
        self.assertEqual(self.snapshot(), before)

    def test_csv_resume_appends_and_respects_filters(self):
        second = self.create_movie(tmdb_id=456)
        third = self.create_movie(tmdb_id=789)
        self.tmdb.side_effect = TMDbServiceError("Missing", status_code=404)
        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            self.run_sync("--limit", "1", "--errors-csv", str(path))
            self.run_sync("--start-id", str(second.pk), "--limit", "1", "--errors-csv", str(path))
            self.run_sync("--start-id", str(second.pk), "--limit", "1", "--errors-csv", str(path))
            rows = self.read_errors(path)
            self.assertEqual([r["movie_id"] for r in rows], [str(self.movie.pk), str(second.pk)])
            self.assertNotIn(str(third.pk), [r["movie_id"] for r in rows])

    def test_csv_persistence_failure_isolates_row_and_continues(self):
        good = self.create_movie(tmdb_id=456)
        before = self.snapshot()
        original = Movie.objects.bulk_update

        def fail_bad_movie(objects, fields, **kwargs):
            if any(m.pk == self.movie.pk for m in objects):
                raise IntegrityError("Bad rating row")
            return original(objects, fields, **kwargs)

        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            with patch.object(Movie.objects, "bulk_update", side_effect=fail_bad_movie):
                out, _ = self.run_sync("--verify-persistence", "--errors-csv", str(path))
            rows = self.read_errors(path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["movie_id"], str(self.movie.pk))
            self.assertEqual(rows[0]["error_type"], "persistence")
            self.assertEqual(rows[0]["status_code"], "")
            self.assertIn("errores: 1", out)
        self.assertEqual(self.snapshot(), before)
        good.refresh_from_db()
        self.assertEqual(good.external_votes, 100)

    def test_csv_verification_mismatch_is_recorded(self):
        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            with patch.object(Movie.objects, "bulk_update", return_value=0):
                out, _ = self.run_sync("--verify-persistence", "--errors-csv", str(path))
            self.assertEqual(self.read_errors(path)[0]["error_type"], "verification")
            self.assertIn("persistence_mismatches: 1", out)

    def test_csv_real_database_failure_rolls_back_and_preserves_good_row(self):
        good = self.create_movie(tmdb_id=456)
        before = self.snapshot()
        self.tmdb.side_effect = [
            {"vote_average": 100, "vote_count": 200},
            {"vote_average": 7.5, "vote_count": 300},
        ]
        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            self.run_sync("--verify-persistence", "--errors-csv", str(path))
            rows = self.read_errors(path)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["error_type"], "persistence")
            self.assertEqual(rows[0]["movie_id"], str(self.movie.pk))
        self.assertEqual(self.snapshot(), before)
        good.refresh_from_db()
        self.assertEqual(good.external_rating, Decimal("7.5"))
        self.assertEqual(good.external_votes, 300)

    def test_csv_tmdb_error_does_not_stop_next_movie(self):
        good = self.create_movie(tmdb_id=456)
        failure = TMDbServiceError("Missing", status_code=404)
        self.tmdb.side_effect = [failure, failure, failure, self.tmdb.return_value]
        with TemporaryDirectory(dir=Path.cwd()) as directory:
            path = Path(directory) / "errors.csv"
            self.run_sync("--errors-csv", str(path))
            self.assertEqual(len(self.read_errors(path)), 1)
        good.refresh_from_db()
        self.assertEqual(good.external_votes, 100)
