from contextlib import contextmanager
from decimal import Decimal
import io
import json
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test.utils import CaptureQueriesContext

from core import test_local_movie_duplicates as fixtures
from core.management.commands import plan_local_movie_duplicate_cleanup as command
from core.movie_duplicate_cleanup_plan import choose_survivor


movie = fixtures.movie


class SurvivorTests(TestCase):
    def test_coverage_wins_over_rating_magnitude_and_length(self):
        rows = [movie(1, title_english="A very long title", external_rating=Decimal("9.9")),
                movie(2, director="D", image="https://example.test/p", synopsis_es="S",
                      external_rating=Decimal("5.5")), movie(3, title_spanish="T")]
        plan = choose_survivor(rows)
        self.assertEqual((plan["keep_id"], plan["remove_ids"]), (2, [1, 3]))
        self.assertEqual(plan, choose_survivor(list(reversed(rows))))

    def test_equivalent_genres_and_empty_values_use_stable_id(self):
        genres = "Drama, Comedy, Fantasy, Horror"
        rows = [movie(20, genre=genres, director=" "), movie(5, genre=" horror, Drama, comedy, Fantasy ")]
        plan = choose_survivor(rows)
        self.assertEqual(plan["keep_id"], 5)
        self.assertEqual(plan["scores"][5], plan["scores"][20])
        self.assertEqual(rows[0]["genre"], genres)
        self.assertEqual(choose_survivor([movie(2, genre=", ,"), movie(3, genre=genres)])["keep_id"], 3)


class PlanTests(TestCase):
    add_rows = fixtures.DiagnosticQueryTests.add_rows
    cleanup_database = fixtures.DiagnosticQueryTests.cleanup_database

    def setUp(self):
        fixtures.DiagnosticQueryTests.setUp(self)
        with connection.cursor() as cursor:
            cursor.execute("CREATE TABLE core_movielistitem (id integer PRIMARY KEY, movie_id integer)")
            cursor.execute("INSERT INTO core_movielistitem VALUES (1, 2), (2, 2)")
        self.addCleanup(self.cleanup_list)

    def cleanup_list(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE core_movielistitem")

    def plan(self, output, *, behavior="CASCADE", resume=None):
        @contextmanager
        def synthetic_snapshot(alias, **options):
            with connection.cursor() as cursor:
                cursor.execute("PRAGMA query_only=ON")

            def substitute(execute, sql, params, many, context):
                if "SELECT current_database()" in sql:
                    return execute("SELECT 'synthetic', 5432, 1", None, many, context)
                return execute(sql, params, many, context)

            try:
                with connection.execute_wrapper(substitute):
                    yield
            finally:
                with connection.cursor() as cursor:
                    cursor.execute("PRAGMA query_only=OFF")

        edges = [{"parent": "core.Movie", "dependent": "core.MovieListItem", "table": "core_movielistitem",
                  "field": "movie", "on_delete": behavior}]
        target = Mock(settings_dict={"HOST": "127.0.0.1", "PORT": "5432", "NAME": "synthetic"},
                      cursor=connection.cursor)
        with patch.object(command, "require_local"), patch.object(command, "local_readonly_snapshot", synthetic_snapshot), \
                patch.object(command, "django_dependencies", return_value=edges), \
                patch.object(command, "read_schema", return_value={"fk_graph_matches": True, "blockers": []}), \
                patch.object(command, "connections", {"default": target}), \
                CaptureQueriesContext(connection) as queries:
            call_command("plan_local_movie_duplicate_cleanup", output_dir=str(output), confirm_database="synthetic",
                         resume=resume, stdout=io.StringIO())
        self.assertFalse(any(q["sql"].lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) for q in queries))
        manifest = json.loads((output / "cleanup_manifest.json").read_text(encoding="utf-8"))
        plan = [json.loads(line) for line in (output / "cleanup_plan.jsonl").read_text(encoding="utf-8").splitlines()]
        return manifest, plan

    def test_candidates_exclude_null_invalid_and_unrecognized_types_and_count_dependents(self):
        self.add_rows(movie(1), movie(2), movie(3, tmdb_id=None), movie(4, tmdb_id=0),
                      movie(5, kind="other"), movie(6, kind="other"), movie(7, kind="series"))
        report, (group,) = self.plan(self.output)
        self.assertEqual((report["total_movies"], report["null_tmdb"], report["invalid_tmdb"]), (7, 1, 1))
        self.assertEqual(report["excluded_nonnull_duplicate_groups"], 1)
        self.assertEqual(group["remove_ids"], [2])
        self.assertEqual(group["dependencies"][0]["rows"], 2)
        self.assertEqual(group["status"], "planned")

    def test_protect_is_recorded_as_blocker(self):
        self.add_rows(movie(1), movie(2))
        report, (group,) = self.plan(self.output, behavior="PROTECT")
        self.assertEqual((report["protected_groups"], group["status"]), (1, "blocked_by_relation"))

    def test_repeat_and_resume_recompute_and_reject_corruption(self):
        self.add_rows(movie(1), movie(2))
        first, _ = self.plan(self.output)
        next_output = self.output.with_name("second")
        second, _ = self.plan(next_output, resume=str(self.output))
        self.assertEqual(first["plan_sha256"], second["plan_sha256"])
        self.assertTrue(second["previous_plan_matches"])
        self.add_rows(movie(3, synopsis="New metadata"))
        third, (group,) = self.plan(self.output.with_name("third"), resume=str(self.output))
        self.assertFalse(third["previous_plan_matches"])
        self.assertEqual(group["keep_id"], 3)
        (self.output / "cleanup_plan.jsonl").write_text("corrupt", encoding="utf-8")
        with self.assertRaisesRegex(CommandError, "Previous plan"):
            self.plan(self.output.with_name("fourth"), resume=str(self.output))

    def test_no_duplicates(self):
        self.add_rows(movie(1), movie(2, kind="series"))
        report, plan = self.plan(self.output)
        self.assertEqual((report["groups"], report["remove_movies"], plan), (0, 0, []))


class PlanBoundaryTests(TestCase):
    def test_remote_or_unconfirmed_target_does_not_create_output(self):
        remote = Mock(vendor="postgresql", connection=None, in_atomic_block=False)
        remote.settings_dict = {"HOST": "example.render.com", "NAME": "socialapp", "PORT": "5432"}
        with patch.object(command, "connections", {"default": remote}), patch.object(command.Path, "mkdir") as mkdir:
            with self.assertRaisesRegex(CommandError, "HOST"):
                call_command("plan_local_movie_duplicate_cleanup", output_dir=str(Path.home() / "unused-plan"),
                             confirm_database="socialapp")
            remote.settings_dict["HOST"] = "127.0.0.1"
            with self.assertRaisesRegex(CommandError, "Confirmed database"):
                call_command("plan_local_movie_duplicate_cleanup", output_dir=str(Path.home() / "unused-plan"),
                             confirm_database="other")
            mkdir.assert_not_called()
            remote.get_autocommit.assert_not_called()
