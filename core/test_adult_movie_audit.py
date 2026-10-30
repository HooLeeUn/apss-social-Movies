import csv
import gzip
import json
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.contrib.auth import get_user_model
from django.db import connection
from django.db.models.deletion import CASCADE, Collector
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.management.commands.audit_adult_movies import classify
from core.management.commands.bulk_delete_movies_from_csv import validate_csv
from core.models import Comment, Movie, MovieListItem, MovieRating, ProfileFavoriteMovie, VideoComment
from core.tmdb import TMDbServiceError


class AdultMovieAuditTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / "results"
        self.movie_file = self.root / "adult_movie.json.gz"
        self.tv_file = self.root / "adult_tv.json.gz"
        self.write_export(self.movie_file, [{"id": 10, "adult": True}, {"id": 10}, {"id": 20}, {"id": 30}])
        self.write_export(self.tv_file, [{"id": 10}, {"id": 40}])
        self.movie = Movie.objects.create(tmdb_id=10, type="movie", title_english="Adult fixture", release_year=2020)
        self.series = Movie.objects.create(tmdb_id=10, type="series", title_english="TV fixture", release_year=2021)
        self.duplicate = Movie.objects.create(tmdb_id=10, type="movie", title_english="Adult fixture", release_year=2020)
        self.false = Movie.objects.create(tmdb_id=20, type="movie", title_english="Not adult", release_year=2020)
        self.error = Movie.objects.create(tmdb_id=30, type="movie", title_english="Unavailable", release_year=2020)
        self.wrong_type = Movie.objects.create(tmdb_id=40, type="movie", title_english="Wrong category", release_year=2020)
        self.mismatch = Movie.objects.create(tmdb_id=10, type="movie", title_english="Different title", release_year=2020)

    @staticmethod
    def write_export(path, rows):
        with gzip.open(path, "wt", encoding="utf-8") as file:
            for row in rows:
                file.write(json.dumps(row) + "\n")

    def audit(self):
        call_command("audit_adult_movies", "audit", movie_export=str(self.movie_file),
                     tv_export=str(self.tv_file), output_dir=str(self.output), batch_size=2, stdout=StringIO())

    def verify(self, **kwargs):
        call_command("audit_adult_movies", "verify", output_dir=str(self.output),
                     interval=0, attempts=2, batch_size=2, stdout=StringIO(), **kwargs)

    def rows(self, name):
        with (self.output / name).open(encoding="utf-8", newline="") as file:
            return list(csv.DictReader(file))

    @staticmethod
    def api(path, **kwargs):
        if path == "/movie/10":
            return {"id": 10, "adult": True, "title": "Adult fixture", "release_date": "2020-01-01"}
        if path == "/tv/10":
            return {"id": 10, "adult": True, "name": "TV fixture", "first_air_date": "2021-01-01"}
        if path == "/movie/20":
            return {"id": 20, "adult": False}
        raise TMDbServiceError("unavailable", status_code=503, retryable=True)

    def test_audit_crosses_type_and_id_without_modifying_catalog(self):
        before = list(Movie.objects.order_by("pk").values())
        with CaptureQueriesContext(connection) as queries:
            self.audit()
        candidates = self.rows("adult_movies_candidates.csv")
        self.assertEqual(len(candidates), 6)
        self.assertNotIn(str(self.wrong_type.pk), [r["database_id"] for r in candidates])
        self.assertEqual(list(Movie.objects.order_by("pk").values()), before)
        self.assertFalse((self.output / "adult_movies_to_delete.csv").exists())
        for query in queries:
            # PostgreSQL iterator() uses a server-side cursor around its SELECT.
            self.assertTrue(query["sql"].lstrip().upper().startswith(("SELECT", "DECLARE", "FETCH", "CLOSE")), query["sql"])

    @patch("core.management.commands.audit_adult_movies.time.sleep")
    def test_confirmation_review_deduplication_and_resume(self, sleep):
        self.audit()
        before = list(Movie.objects.order_by("pk").values())
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=self.api) as api:
            self.verify()
        self.assertEqual(api.call_count, 5)  # 4 keys, error retried, duplicates share API evidence.
        confirmed = self.rows("adult_movies_confirmed.csv")
        expected_ids = sorted([self.movie.pk, self.series.pk, self.duplicate.pk])
        self.assertEqual([int(r["database_id"]) for r in confirmed], expected_ids)
        manifest = self.rows("adult_movies_to_delete.csv")
        self.assertEqual(manifest, [{"database_id": str(pk)} for pk in expected_ids])
        self.assertEqual(list(manifest[0]), ["database_id"])
        review = {int(r["database_id"]): r for r in self.rows("adult_movies_review.csv")}
        self.assertEqual(set(review), {self.false.pk, self.error.pk, self.mismatch.pk})
        self.assertEqual(review[self.error.pk]["adult"], "")
        self.assertEqual(review[self.error.pk]["validation_status"], "error")
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", return_value={
            "id": 30, "adult": True, "title": "Unavailable", "release_date": "2020-01-01",
        }) as api:
            self.verify()
        api.assert_called_once_with("/movie/30", params={"language": "en-US"})
        self.assertEqual(len(self.rows("adult_movies_to_delete.csv")), 4)
        self.assertEqual(list(Movie.objects.order_by("pk").values()), before)
        with (self.output / "adult_movies_to_delete.csv").open(encoding="utf-8") as file:
            self.assertEqual(file.readline().strip(), "database_id")
        spool = StringIO()
        self.assertEqual(validate_csv(self.output / "adult_movies_to_delete.csv", spool)["errors"], 0)
        # The ORM deletion command's actual parser also accepts this one-column manifest.
        from core.management.commands.delete_movies_from_csv import Command
        parsed = Command(stdout=StringIO())._read_csv(self.output / "adult_movies_to_delete.csv", "dry-run")
        self.assertEqual(parsed[1], sorted(expected_ids + [self.error.pk]))

    def test_api_requires_boolean_and_matching_identity(self):
        movie = dict(database_id=1, tmdb_id=10, type="movie", title_english="Fixture", release_year=2020)
        good = {"id": 10, "adult": True, "title": "Fixture", "release_date": "2020-01-01"}
        self.assertEqual(classify(movie, good)[0], "confirmed")
        for changes in ({"adult": "true"}, {"adult": 1}, {"adult": None}, {"id": 11},
                        {"id": True}, {"release_date": "2022-01-01"}, {"title": "Other"},
                        {"release_date": ""}, {"adult": False}):
            with self.subTest(changes=changes):
                self.assertEqual(classify(movie, dict(good, **changes))[0], "review")
        self.assertEqual(classify(movie, {})[0], "review")

    def test_invalid_exports_do_not_publish_state(self):
        for invalid in ([{"id": True}], [{"id": 0}], [{"id": "10"}], [{"id": 10, "adult": False}], [[]], []):
            with self.subTest(invalid=invalid):
                self.write_export(self.tv_file, invalid)
                with self.assertRaises(CommandError):
                    self.audit()
                self.assertFalse((self.output / "audit.sqlite3").exists())
                self.assertFalse((self.output / "adult_movies_to_delete.csv").exists())

    def test_changed_catalog_is_excluded_even_with_cached_confirmation(self):
        self.audit()
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=self.api), \
                patch("core.management.commands.audit_adult_movies.time.sleep"):
            self.verify()
        Movie.objects.filter(pk=self.movie.pk).update(tmdb_id=999)
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=self.api), \
                patch("core.management.commands.audit_adult_movies.time.sleep"):
            self.verify()
        self.assertNotIn(str(self.movie.pk), [r["database_id"] for r in self.rows("adult_movies_to_delete.csv")])

    def test_audit_state_cannot_be_silently_overwritten(self):
        self.audit()
        with self.assertRaisesMessage(CommandError, "already exists"):
            self.audit()

    def test_response_cache_reuses_completed_tmdb_keys_without_internal_ids(self):
        self.audit()
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=self.api), \
                patch("core.management.commands.audit_adult_movies.time.sleep"):
            self.verify()
        new_output = self.root / "independent_audit"
        call_command("audit_adult_movies", "audit", movie_export=str(self.movie_file), tv_export=str(self.tv_file),
                     output_dir=str(new_output), response_cache=str(self.output / "audit.sqlite3"), stdout=StringIO())
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", return_value={}) as api:
            call_command("audit_adult_movies", "verify", output_dir=str(new_output), interval=0, stdout=StringIO())
        # Only the failed key is queried, never the already verified records.
        api.assert_called_once_with("/movie/30", params={"language": "en-US"})

    def test_database_mismatch_is_rejected(self):
        self.audit()
        with patch("core.management.commands.audit_adult_movies.database_identity", return_value="different"):
            with self.assertRaisesMessage(CommandError, "different database"):
                self.verify()

    def test_interrupted_verification_resumes_completed_keys(self):
        self.audit()
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=[self.api("/movie/10"), KeyboardInterrupt]):
            with self.assertRaises(KeyboardInterrupt):
                self.verify()
        self.assertFalse((self.output / "adult_movies_to_delete.csv").exists())
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=self.api) as api, \
                patch("core.management.commands.audit_adult_movies.time.sleep"):
            self.verify()
        self.assertNotIn("/movie/10", [call.args[0] for call in api.call_args_list])

    def test_rate_limit_retry_after_is_respected(self):
        self.audit()
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=[
            TMDbServiceError("rate limited", status_code=429, retryable=True, retry_after="3"),
            self.api("/movie/10"), self.api("/movie/20"), {}, self.api("/tv/10"),
        ]), patch("core.management.commands.audit_adult_movies.time.sleep") as sleep:
            self.verify()
        self.assertTrue(any(call.args[0] > 2 for call in sleep.call_args_list))

    def test_truncated_gzip_is_rejected(self):
        self.movie_file.write_bytes(self.movie_file.read_bytes()[:-8])
        with self.assertRaises(CommandError):
            self.audit()
        self.assertFalse((self.output / "audit.sqlite3").exists())

    def test_parallel_verification_has_same_manifest(self):
        self.audit()
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json", side_effect=self.api), \
                patch("core.management.commands.audit_adult_movies.time.sleep"):
            self.verify(workers=4)
        self.assertEqual([int(r["database_id"]) for r in self.rows("adult_movies_to_delete.csv")],
                         sorted([self.movie.pk, self.series.pk, self.duplicate.pk]))

    def test_cascade_collector_includes_interactions_without_deleting(self):
        user = get_user_model().objects.create_user(username="adult-audit-cascade")
        Comment.objects.create(movie=self.movie, author=user, body="Fixture")
        MovieRating.objects.create(movie=self.movie, user=user, score=6)
        MovieListItem.objects.create(movie=self.movie, user=user)
        ProfileFavoriteMovie.objects.create(movie=self.movie, user=user, slot=1)
        VideoComment.objects.create(movie=self.movie, user=user, video="test-only.mp4",
                                    duration_seconds=1, file_size=10, mime_type="video/mp4")
        self.assertTrue(all(r.field.remote_field.on_delete is CASCADE for r in Movie._meta.related_objects))
        collector = Collector(using="default", origin=self.movie)
        with patch.object(VideoComment, "delete") as delete_video:
            collector.collect([self.movie])
        collected_models = set(collector.data) | {qs.model for qs in collector.fast_deletes}
        self.assertTrue({Movie, Comment, MovieRating, MovieListItem, ProfileFavoriteMovie, VideoComment} <= collected_models)
        delete_video.assert_not_called()
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())
