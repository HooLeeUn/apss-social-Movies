import csv
import json
import sqlite3
from datetime import date
from contextlib import closing, contextmanager
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command, CommandError
from django.db import connection, IntegrityError, transaction
from django.test import SimpleTestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext

from core.adult_cleanup import assess_identity, digest_file, metadata_fingerprint, write_json
from core.management.commands.audit_adult_movies import FIELDS, Command as AuditCommand, database_identity
from core.models import (Comment, CommentReaction, ContentReport, Movie, MovieListItem,
                         MovieRating, MovieRecommendationItem, ProfileFavoriteMovie,
                         UserTasteProfile, VideoComment, VideoCommentReaction,
                         WeeklyRecommendationItem, WeeklyRecommendationSnapshot)


class AdultIdentityTests(SimpleTestCase):
    def test_metadata_variants_and_conflicts(self):
        movie = dict(database_id=1, tmdb_id=10, type="movie", title_english="The Test Fixture", release_year=2020)
        api = dict(id=10, adult=True, title="Test Fixture", release_date="2021-01-01")
        self.assertTrue(assess_identity(movie, api, {})[0])
        self.assertTrue(assess_identity(dict(movie, title_english="Translated title"), api, {"title_spanish": "Test Fixture"})[0])
        self.assertFalse(assess_identity(movie, dict(api, release_date="1990-01-01"), {})[0])
        self.assertEqual(assess_identity(movie, dict(api, adult=False), {})[1], "adult_false")
        self.assertFalse(assess_identity(movie, dict(api, adult="true"), {})[0])
        self.assertFalse(assess_identity(movie, dict(api, id=99), {})[0])
        self.assertFalse(assess_identity(dict(movie, title_english="Test Fixture 2"), api, {})[0])
        self.assertTrue(assess_identity(dict(movie, title_english="Test Fixture"), dict(api, release_date=""), {})[0])
        self.assertFalse(assess_identity(dict(movie, title_english="Unknown translation"), api, {})[0])

    def test_synopsis_requires_independent_distinctive_evidence(self):
        movie = dict(database_id=1, tmdb_id=10, type="movie", title_english="Local translation", release_year=2020)
        synopsis = "A distinctive story with many characters and several specific events. " * 4
        api = dict(id=10, adult=True, title="Another language title", release_date="2020-01-01", overview=synopsis)
        self.assertFalse(assess_identity(movie, api, {"synopsis": synopsis})[0])
        self.assertTrue(assess_identity(movie, api, {"synopsis": synopsis, "_overview_unique": True})[0])


class AdultCleanupTests(TransactionTestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(dir=Path(__file__).resolve().parent.parent / "tmdb_exports")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.audit = self.root
        self.journal = self.root / "cleanup.sqlite3"
        self.selection = self.root / "adult_movies_final_selection.json"
        self.user = get_user_model().objects.create_user(username="cleanup-fixture")
        self.movie = Movie.objects.create(type="movie", tmdb_id=10, title_english="Test Fixture", release_year=2020)
        self.keep = Movie.objects.create(type="movie", tmdb_id=20, title_english="Keep Fixture", release_year=2020)
        self.comment = Comment.objects.create(movie=self.movie, author=self.user, body="fixture")
        CommentReaction.objects.create(comment=self.comment, user=self.user, reaction_type="like")
        MovieRating.objects.create(movie=self.movie, user=self.user, score=6)
        MovieListItem.objects.create(movie=self.movie, user=self.user)
        MovieRecommendationItem.objects.create(movie=self.movie, user=self.user)
        ProfileFavoriteMovie.objects.create(movie=self.movie, user=self.user, slot=1)
        self.video = VideoComment.objects.create(movie=self.movie, user=self.user, video="videos/selected.mp4",
                    duration_seconds=1, mime_type="video/mp4", file_size=10)
        VideoCommentReaction.objects.create(video_comment=self.video, user=self.user, reaction_type="like")
        self.report = ContentReport.objects.create(reporter=self.user, content_type="comment", object_id=self.comment.pk,
                                                  reason="sexual_content")
        self.snapshot = {key: getattr(self.movie, "pk" if key == "database_id" else key) for key in FIELDS}
        self.make_selection()

    def make_selection(self):
        manifest = self.root / "adult_movies_final_to_delete.csv"
        AuditCommand.write_csv(manifest, ["database_id"], [{"database_id": self.movie.pk}])
        write_json(self.selection, {"version": 2, "database": database_identity("default"),
                   "manifest_sha256": digest_file(manifest), "movies": [dict(self.snapshot,
                       identity_metadata_sha256=metadata_fingerprint(self.movie))]})

    def run_phase(self, phase, **kwargs):
        call_command("adult_movie_cleanup", phase, selection=str(self.selection), journal=str(self.journal),
                     expected_database=str(connection.settings_dict["NAME"]),
                     expected_host=str(connection.settings_dict["HOST"]), stdout=StringIO(), **kwargs)

    def journal_rows(self, table):
        with closing(sqlite3.connect(self.journal)) as db:
            return db.execute(f"SELECT * FROM {table}").fetchall()

    def test_plan_and_dry_run_preserve_database_and_storage(self):
        self.run_phase("plan")
        self.assertEqual(self.journal_rows("media")[0][1:3], ("videos/selected.mp4", "pending"))
        self.run_phase("delete")
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())
        self.assertTrue(VideoComment.objects.filter(pk=self.video.pk).exists())
        self.assertEqual(self.journal_rows("movies")[0][1], "planned")

    def test_delete_cascades_and_media_only_after_commit(self):
        self.run_phase("plan")
        storage = VideoComment._meta.get_field("video").storage
        with patch.object(storage, "delete") as delete:
            self.run_phase("delete", apply=True)
        delete.assert_not_called()
        self.assertFalse(Movie.objects.filter(pk=self.movie.pk).exists())
        self.assertTrue(Movie.objects.filter(pk=self.keep.pk).exists())
        self.assertFalse(CommentReaction.objects.filter(comment_id=self.comment.pk).exists())
        self.assertFalse(VideoCommentReaction.objects.filter(video_comment_id=self.video.pk).exists())
        self.assertFalse(ContentReport.objects.filter(pk=self.report.pk).exists())
        self.assertEqual(UserTasteProfile.objects.get(user=self.user).ratings_count, 0)
        self.assertEqual(self.journal_rows("movies")[0][1], "deleted")
        self.run_phase("verify")
        def after_commit(name):
            self.assertTrue(connection.get_autocommit() is False)  # Independent media lock transaction.
            self.assertFalse(Movie.objects.filter(pk=self.movie.pk).exists())
            self.assertEqual(self.journal_rows("movies")[0][1], "deleted")
        with patch.object(storage, "delete", side_effect=after_commit) as delete, patch.object(storage, "exists", return_value=False):
            self.run_phase("clean-media", apply=True)
        delete.assert_called_once_with("videos/selected.mp4")
        self.assertEqual(self.journal_rows("media")[0][2], "deleted")

    def test_storage_failure_is_retryable_and_not_reported_as_success(self):
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        storage = VideoComment._meta.get_field("video").storage
        with patch.object(storage, "delete", side_effect=OSError("R2 unavailable")):
            with self.assertRaises(CommandError):
                self.run_phase("clean-media", apply=True)
        self.assertEqual(self.journal_rows("media")[0][2], "error")
        with patch.object(storage, "delete") as delete, patch.object(storage, "exists", return_value=False):
            self.run_phase("clean-media", apply=True)
        delete.assert_called_once()
        self.assertEqual(self.journal_rows("media")[0][2], "deleted")

    def test_shared_key_is_not_deleted(self):
        VideoComment.objects.create(movie=self.keep, user=self.user, video=self.video.video.name,
                    duration_seconds=1, mime_type="video/mp4", file_size=10)
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        storage = VideoComment._meta.get_field("video").storage
        with patch.object(storage, "delete") as delete:
            with self.assertRaises(CommandError):
                self.run_phase("clean-media", apply=True)
        delete.assert_not_called()

    def test_wrong_target_or_manifest_is_rejected(self):
        with self.assertRaises(CommandError):
            call_command("adult_movie_cleanup", "plan", selection=str(self.selection), journal=str(self.journal),
                         expected_database="wrong-database", expected_host=str(connection.settings_dict["HOST"]))
        self.run_phase("plan")
        (self.root / "adult_movies_final_to_delete.csv").write_text("database_id\n999\n", encoding="utf-8")
        with self.assertRaises(CommandError):
            self.run_phase("delete", apply=True)
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())

    def test_failed_batch_does_not_publish_success(self):
        self.run_phase("plan")
        with patch("core.management.commands.adult_movie_cleanup.Command._delete_batch", side_effect=RuntimeError("commit failed")):
            with self.assertRaises(CommandError):
                self.run_phase("delete", apply=True)
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())
        self.assertEqual(self.journal_rows("movies")[0][1], "error")

    def test_outer_transaction_failure_rolls_back_and_reports_error(self):
        self.run_phase("plan")
        real_atomic = transaction.atomic

        @contextmanager
        def failing_commit(*args, **kwargs):
            outer = not connection.in_atomic_block
            with real_atomic(*args, **kwargs):
                yield
                if outer:
                    raise IntegrityError("deferred constraint failed at commit")

        with patch("core.management.commands.delete_movies_from_csv.transaction.atomic", side_effect=failing_commit):
            with self.assertRaises(CommandError):
                self.run_phase("delete", apply=True)
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())
        self.assertTrue(VideoComment.objects.filter(pk=self.video.pk).exists())
        self.assertEqual(self.journal_rows("movies")[0][1], "error")

    def test_storage_is_never_deleted_before_parent_commit(self):
        self.run_phase("plan")
        storage = VideoComment._meta.get_field("video").storage
        with patch.object(storage, "delete") as delete:
            with self.assertRaises(CommandError):
                self.run_phase("clean-media", apply=True)
        delete.assert_not_called()
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())

    def test_read_only_media_verification_detects_existing_object(self):
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        storage = VideoComment._meta.get_field("video").storage
        with patch.object(storage, "exists", return_value=True), patch.object(storage, "delete") as delete:
            with self.assertRaises(CommandError):
                self.run_phase("verify", check_media=True)
        delete.assert_not_called()

    def test_changed_snapshot_is_not_deleted(self):
        self.run_phase("plan")
        Movie.objects.filter(pk=self.movie.pk).update(tmdb_id=99)
        with self.assertRaises(CommandError):
            self.run_phase("delete", apply=True)
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())

    def test_weekly_snapshot_count_is_consistent_after_cascade(self):
        snapshot = WeeklyRecommendationSnapshot.objects.create(week_start=date(2026, 10, 19),
                    week_end=date(2026, 10, 25), items_count=2)
        for position, movie in enumerate((self.movie, self.keep), 1):
            WeeklyRecommendationItem.objects.create(snapshot=snapshot, movie=movie, position=position,
                weekly_score=6, week_ratings_count=1, week_ratings_sum=6)
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        snapshot.refresh_from_db()
        self.assertEqual(snapshot.items_count, 1)
        self.assertEqual(snapshot.items.get().movie_id, self.keep.pk)

    def test_media_created_after_plan_is_saved_before_delete(self):
        self.run_phase("plan")
        VideoComment.objects.create(movie=self.movie, user=self.user, video="videos/later.mp4",
                    duration_seconds=1, mime_type="video/mp4", file_size=10)
        self.run_phase("delete", apply=True)
        self.assertEqual({r[1] for r in self.journal_rows("media")}, {"videos/selected.mp4", "videos/later.mp4"})

    def test_prepared_state_recovers_after_commit_before_journal_update(self):
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE movies SET status='prepared'")
            db.commit()
        self.run_phase("delete", apply=True)
        self.assertEqual(self.journal_rows("movies")[0][1], "deleted")

    def test_readonly_inspect_and_dry_run_do_not_modify_journal_or_reports(self):
        self.run_phase("plan")
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE movies SET status='prepared'")
            db.commit()
        before = {str(p.relative_to(self.root)): digest_file(p) for p in self.root.rglob("*") if p.is_file()}
        self.run_phase("inspect")
        self.run_phase("delete", dry_run=True)
        with self.assertRaises(CommandError):
            self.run_phase("verify")
        after = {str(p.relative_to(self.root)): digest_file(p) for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(before, after)
        self.assertEqual(self.journal_rows("movies")[0][1], "prepared")

    def test_readonly_inspect_does_not_reconcile_absent_prepared(self):
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE movies SET status='prepared'")
            db.commit()
        before = digest_file(self.journal)
        self.run_phase("inspect")
        self.assertEqual(before, digest_file(self.journal))
        self.assertEqual(self.journal_rows("movies")[0][1], "prepared")

    def test_confirmed_deletions_never_reach_orm_again(self):
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        with patch("core.management.commands.adult_movie_cleanup.Command._delete_batch") as delete:
            self.run_phase("delete", apply=True)
        delete.assert_not_called()

    def test_unexpected_reappearance_of_deleted_id_aborts(self):
        self.run_phase("plan")
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE movies SET status='deleted'")
            db.commit()
        with patch("core.management.commands.adult_movie_cleanup.Command._delete_batch") as delete:
            with self.assertRaisesMessage(CommandError, "exists again"):
                self.run_phase("delete", apply=True)
        delete.assert_not_called()
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())

    def test_protected_movie_is_checked_and_never_deleted(self):
        proof = json.loads(self.selection.read_text(encoding="utf-8"))
        proof["protected_movies"] = [dict(
            {key: getattr(self.keep, "pk" if key == "database_id" else key) for key in FIELDS},
            identity_metadata_sha256=metadata_fingerprint(self.keep),
        )]
        write_json(self.selection, proof)
        self.run_phase("plan")
        self.run_phase("delete", apply=True)
        self.run_phase("verify")
        self.assertTrue(Movie.objects.filter(pk=self.keep.pk).exists())

    def test_protected_overlap_aborts_before_deletion(self):
        proof = json.loads(self.selection.read_text(encoding="utf-8"))
        proof["protected_movies"] = proof["movies"]
        write_json(self.selection, proof)
        with self.assertRaisesMessage(CommandError, "Protected IDs overlap"):
            self.run_phase("plan")
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())

    def test_protected_change_aborts_before_deletion(self):
        proof = json.loads(self.selection.read_text(encoding="utf-8"))
        proof["protected_movies"] = [dict(
            {key: getattr(self.keep, "pk" if key == "database_id" else key) for key in FIELDS},
            identity_metadata_sha256=metadata_fingerprint(self.keep),
        )]
        write_json(self.selection, proof)
        self.run_phase("plan")
        Movie.objects.filter(pk=self.keep.pk).update(title_english="Changed protected record")
        with self.assertRaisesMessage(CommandError, "protected Movie changed"):
            self.run_phase("delete", apply=True)
        self.assertTrue(Movie.objects.filter(pk=self.movie.pk).exists())

    def test_prepared_present_is_retried_using_same_journal(self):
        self.run_phase("plan")
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE movies SET status='prepared'")
            db.commit()
        self.run_phase("delete", apply=True)
        self.assertFalse(Movie.objects.filter(pk=self.movie.pk).exists())
        self.assertEqual(self.journal_rows("movies")[0][1], "deleted")

    def test_membership_is_one_query_per_batch_and_absent_prepared_is_not_deleted_again(self):
        from core.management.commands.adult_movie_cleanup import Command
        with closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE movies(id INTEGER PRIMARY KEY,status TEXT,error TEXT)")
            db.executemany("INSERT INTO movies VALUES (?,?,?)", [
                (self.movie.pk, "prepared", ""), (self.keep.pk, "planned", ""), (999999999, "prepared", ""),
            ])
            db.commit()
            command = Command(stdout=StringIO())
            command.journal = db
            with patch.object(command, "_delete_batch") as delete, self.assertNumQueries(1):
                command.delete([self.movie.pk, self.keep.pk, 999999999], {"apply": True, "batch_size": 100})
            self.assertEqual(delete.call_args.args[0], [self.movie.pk, self.keep.pk])
            self.assertEqual(db.execute("SELECT status FROM movies WHERE id=999999999").fetchone()[0], "deleted")

    def test_cached_finalization_has_no_network_calls(self):
        with closing(sqlite3.connect(self.root / "audit.sqlite3")) as db:
            db.executescript("""CREATE TABLE metadata(key TEXT,value TEXT);
                CREATE TABLE candidates(database_id INTEGER,snapshot TEXT);
                CREATE TABLE exports(type TEXT,id INTEGER);
                CREATE TABLE responses(type TEXT,id INTEGER,status TEXT,evidence TEXT);""")
            db.execute("INSERT INTO metadata VALUES ('database',?)", (database_identity("default"),))
            db.execute("INSERT INTO candidates VALUES (?,?)", (self.movie.pk, json.dumps(self.snapshot)))
            db.execute("INSERT INTO exports VALUES ('movie',10)")
            db.execute("INSERT INTO responses VALUES ('movie',10,'complete',?)", (json.dumps({
                "id": 10, "adult": True, "title": "The Test Fixture", "release_date": "2021-01-01",
            }),))
            db.commit()
        with patch("core.management.commands.audit_adult_movies.get_tmdb_json") as api:
            call_command("finalize_adult_movies", audit_dir=str(self.root),
                         expected_database=str(connection.settings_dict["NAME"]), expected_host=str(connection.settings_dict["HOST"]),
                         stdout=StringIO())
        api.assert_not_called()
        proof = json.loads(self.selection.read_text(encoding="utf-8"))
        self.assertEqual([r["database_id"] for r in proof["movies"]], [self.movie.pk])

    def test_query_profile_for_movie_without_interactions(self):
        self.movie = self.keep
        self.snapshot = {key: getattr(self.movie, "pk" if key == "database_id" else key) for key in FIELDS}
        self.make_selection()
        self.run_phase("plan")
        with CaptureQueriesContext(connection) as queries:
            self.run_phase("delete", apply=True)
        selects = sum(q["sql"].lstrip().upper().startswith("SELECT") for q in queries)
        print(f"Adult cleanup fixture: {len(queries)} PostgreSQL statements, {selects} SELECTs (one empty movie, includes target/verification)")
        self.assertFalse(Movie.objects.filter(pk=self.movie.pk).exists())
