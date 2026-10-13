import io
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.db import connection
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
        missing = self.create_movie(tmdb_id=None, imdb_id="tt1234567")
        out, _ = self.run_sync("--movie-id", str(missing.pk))
        self.tmdb.assert_not_called()
        self.assertIn("Procesadas: 0", out)
