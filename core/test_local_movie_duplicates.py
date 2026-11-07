import io
import json
import csv
import tempfile
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from unittest import TestCase
from unittest.mock import MagicMock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, DatabaseError
from django.test.utils import CaptureQueriesContext

from core.management.commands import diagnose_local_movie_duplicates as command
from core.movie_duplicate_diagnosis import analyze_group, selected_fields, technical_dependencies


def movie(pk, tmdb_id=10, kind="movie", **changes):
    row = {name: None for name in selected_fields()}
    row.update(id=pk, tmdb_id=tmdb_id, type=kind, title_english="Title", synopsis="", external_votes=0)
    row.update(changes)
    return row


class MetadataAnalysisTests(TestCase):
    def test_identical_records_stable_tie_uses_lowest_id(self):
        result = analyze_group([movie(9), movie(2)])
        self.assertEqual(result["recommended_id"], 2)
        self.assertFalse(result["manual_review_required"])

    def test_completeness_is_preferred(self):
        result = analyze_group([movie(1), movie(5, director="Director", synopsis="Plot", image="https://example.test/poster")])
        self.assertEqual(result["recommended_id"], 5)
        self.assertFalse(result["manual_review_required"])

    def test_three_records_and_complementary_information(self):
        result = analyze_group([movie(1, director="Director"), movie(2, synopsis="Plot"), movie(3)])
        self.assertEqual(result["recommended_id"], 1)
        self.assertTrue(result["manual_review_required"])
        self.assertEqual(result["complementary_fields"], ["synopsis"])

    def test_conflicts_do_not_prefer_higher_rating_or_recency(self):
        result = analyze_group([
            movie(1, external_rating=Decimal("6.0"), external_votes=100, release_year=2000, updated_at="2000-01-01"),
            movie(2, external_rating=Decimal("9.0"), external_votes=500, release_year=2001, updated_at="2026-01-01")])
        self.assertEqual(result["recommended_id"], 1)
        self.assertTrue(result["manual_review_required"])
        self.assertIn("release_year", result["conflicts"])
        self.assertIn("external_rating", result["conflicts"])

    def test_null_empty_and_defaults_remain_distinguishable(self):
        result = analyze_group([movie(1, director=None), movie(2, director=" ")])
        self.assertEqual(result["records"][1]["states"]["director"], "null")
        self.assertEqual(result["records"][2]["states"]["director"], "empty")
        self.assertEqual(result["records"][1]["states"]["external_votes"], "default")
        self.assertEqual(result["records"][1]["states"]["synopsis"], "empty")
        self.assertIn("synopsis", result["records"][1]["default_matches"])

    def test_invalid_metadata_and_incomplete_rating_pair_require_review(self):
        result = analyze_group([movie(1, image="bad", release_year=12, external_rating=Decimal("8.0")), movie(2)])
        self.assertEqual(result["recommended_id"], 2)
        self.assertTrue(result["manual_review_required"])
        self.assertIn("rating_votes_incomplete_pair", result["records"][1]["issues"])

    def test_whitespace_case_and_genre_order_are_normalized(self):
        result = analyze_group([movie(1, director="A  B", genre="Drama, Comedy"),
                                movie(2, director="a b", genre="comedy,drama")])
        self.assertFalse(result["manual_review_required"])
        self.assertIn("genre", result["different_fields"])

    def test_dependencies_include_direct_transitive_and_constraints_without_sql(self):
        with patch.object(connection, "cursor", side_effect=AssertionError("query")):
            report = technical_dependencies()
        direct = {edge["dependent"] for edge in report["incoming_relations"] if edge["direct_movie_dependency"]}
        self.assertIn("core.MovieRating", direct)
        self.assertIn("core.StreamingProviderLink", direct)
        self.assertIn("core.VideoCommentReaction", {edge["dependent"] for edge in report["incoming_relations"]})
        rating = next(edge for edge in report["incoming_relations"] if edge["dependent"] == "core.MovieRating")
        self.assertEqual(rating["on_delete"], "CASCADE")
        self.assertTrue(rating["constraints"])
        self.assertEqual(report["movie_model"]["constraints"], [])


class DiagnosticQueryTests(TestCase):
    """Exercise actual ORM SQL on an isolated synthetic SQLite table, never PostgreSQL."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="duplicate diagnostic tests ")
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "reports with spaces"
        self.assertEqual(connection.vendor, "sqlite")
        self.assertEqual(connection.settings_dict["NAME"], ":memory:")
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA query_only=OFF")
            cursor.execute('DROP TABLE IF EXISTS "core_movie"')
            fields = []
            for name in selected_fields():
                db_type = ("integer" if name in {"id", "tmdb_id", "release_year", "external_votes"}
                           else "decimal" if name == "external_rating" else "datetime" if name.endswith("_at") else "text")
                fields.append(f'"{name}" {db_type}')
            cursor.execute('CREATE TABLE "core_movie" (' + ",".join(fields) + ")")
        self.addCleanup(self.cleanup_database)

    def cleanup_database(self):
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA query_only=OFF")
            cursor.execute('DROP TABLE "core_movie"')

    def add_rows(self, *rows):
        names = selected_fields()
        sql = ('INSERT INTO "core_movie" (' + ",".join(f'"{name}"' for name in names) +
               ") VALUES (" + ",".join(["%s"] * len(names)) + ")")
        with connection.cursor() as cursor:
            for row in rows:
                cursor.execute(sql, [row[name] for name in names])

    def run_diagnosis(self, **options):
        @contextmanager
        def synthetic_snapshot(alias):
            self.assertEqual(alias, "default")
            with connection.cursor() as cursor:
                cursor.execute("PRAGMA query_only=ON")
            yield

        with patch.object(command, "require_local"), patch.object(command, "local_readonly_snapshot", synthetic_snapshot), \
                patch.object(command.Movie, "save", side_effect=AssertionError("write")), \
                patch.object(command.Movie, "delete", side_effect=AssertionError("write")):
            with CaptureQueriesContext(connection) as queries:
                call_command("diagnose_local_movie_duplicates", output_dir=str(self.output), stdout=io.StringIO(), **options)
        self.queries = [item["sql"] for item in queries if item["sql"].lstrip().upper().startswith("SELECT")]
        self.assertTrue(all('"core_movie"' in sql for sql in self.queries))
        return json.loads((self.output / "duplicate_summary.json").read_text(encoding="utf-8"))

    def csv_rows(self, name):
        with (self.output / f"{name}.csv").open(encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_real_aggregation_pair_keys_reports_and_no_writes(self):
        self.add_rows(movie(1), movie(2), movie(3, kind="series"), movie(4, 20), movie(5, 20), movie(6, 20))
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM "core_movie" ORDER BY id')
            before = cursor.fetchall()
        result = self.run_diagnosis()
        self.assertEqual(result["total_movies"], 5)
        self.assertEqual(result["total_series"], 1)
        self.assertEqual(result["duplicate_group_count"], 2)
        self.assertEqual(result["duplicate_extra_rows"], 3)
        self.assertEqual(result["group_size_distribution"], {"2": 1, "3": 1})
        self.assertEqual(len(self.queries), 3)  # totals + grouped keys + one batched record query
        self.assertEqual(len(self.csv_rows("duplicate_records")), 5)
        self.assertEqual(self.csv_rows("duplicate_conflicts"), [])
        self.assertEqual(len(list(self.output.iterdir())), 5)
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM "core_movie" ORDER BY id')
            self.assertEqual(cursor.fetchall(), before)
            with self.assertRaises(DatabaseError):
                cursor.execute('UPDATE "core_movie" SET title_english=\'changed\'')

    def test_limit_keeps_global_counts_but_bounds_analyzed_groups(self):
        self.add_rows(movie(1), movie(2), movie(3, 20), movie(4, 20))
        result = self.run_diagnosis(limit_groups=1, sample_size=0)
        self.assertEqual(result["duplicate_group_count"], 2)
        self.assertEqual(result["analyzed_groups"], 1)
        self.assertTrue(result["partial_analysis"])
        self.assertEqual(result["samples"], [])
        self.assertEqual(len(self.csv_rows("duplicate_groups")), 1)
        self.assertEqual(len(self.csv_rows("duplicate_records")), 2)

    def test_no_duplicates_invalid_keys_excluded(self):
        self.add_rows(movie(1), movie(2, kind="series"), movie(3, None), movie(4, None), movie(5, 0), movie(6, 0))
        result = self.run_diagnosis()
        self.assertEqual(result["duplicate_group_count"], 0)
        self.assertEqual(result["invalid_key_rows_excluded"], 4)
        self.assertEqual(len(self.queries), 2)
        self.assertEqual(self.csv_rows("duplicate_groups"), [])

    def test_conflicts_and_formula_escaping(self):
        self.add_rows(movie(1, title_english="=Title", release_year=2000), movie(2, release_year=2001))
        result = self.run_diagnosis(sample_size=1)
        self.assertEqual(result["manual_review_required_groups"], 1)
        self.assertEqual(len(self.csv_rows("duplicate_conflicts")), 1)
        self.assertEqual(self.csv_rows("duplicate_records")[0]["title_english"], "'=Title")

    def test_batching_is_per_group_batch_not_per_movie(self):
        self.add_rows(*(movie(pk, (pk + 1) // 2) for pk in range(1, 205)))
        result = self.run_diagnosis()
        self.assertEqual(result["duplicate_group_count"], 102)
        self.assertEqual(result["record_batch_queries"], 2)
        self.assertEqual(len(self.queries), 4)

    def test_existing_reports_are_not_overwritten(self):
        self.run_diagnosis()
        original = (self.output / "duplicate_summary.json").read_bytes()
        with self.assertRaisesRegex(CommandError, "new or empty"):
            self.run_diagnosis()
        self.assertEqual((self.output / "duplicate_summary.json").read_bytes(), original)

    def test_database_failure_does_not_leave_completion_marker(self):
        with patch.object(command, "catalog_totals", side_effect=DatabaseError("synthetic failure")):
            with self.assertRaisesRegex(CommandError, "Database diagnosis failed"):
                self.run_diagnosis()
        self.assertFalse((self.output / "duplicate_summary.json").exists())


class LocalSafetyTests(TestCase):
    def local_connection(self, **config):
        result = MagicMock(vendor="postgresql", connection=None, in_atomic_block=False)
        result.settings_dict = {"HOST": "127.0.0.1", "PORT": "5432", "NAME": "synthetic", "OPTIONS": {}, **config}
        result.get_autocommit.return_value = True
        result.ops.quote_name.side_effect = lambda name: '"' + name + '"'
        return result

    def test_remote_hosts_overrides_and_unknown_destinations_rejected_before_connect(self):
        bad = [{"HOST": "some.render.com"}, {"HOST": "10.0.0.1"}, {"HOST": ""}, {"HOST": "/tmp"},
               {"HOST": "127.0.0.1,remote"}, {"PORT": ""}, {"NAME": "postgresql://remote/db"},
               {"OPTIONS": {"service": "render"}}, {"OPTIONS": {"hostaddr": "1.2.3.4"}},
               {"OPTIONS": {"pool": {"conninfo": "remote"}}}]
        with patch.dict(command.os.environ, {}, clear=True):
            for config in bad:
                with self.subTest(config=config):
                    local = self.local_connection(**config)
                    with self.assertRaises(CommandError):
                        command.require_local(local)
                    local.get_autocommit.assert_not_called()
                    local.cursor.assert_not_called()
            local = self.local_connection()
            local.vendor = "sqlite"
            with self.assertRaises(CommandError):
                command.require_local(local)

    def test_environment_overrides_bad_localhost_and_reused_connection_rejected(self):
        with patch.dict(command.os.environ, {"PGHOSTADDR": "1.2.3.4"}, clear=True):
            with self.assertRaises(CommandError):
                command.require_local(self.local_connection())
        with patch.dict(command.os.environ, {}, clear=True):
            with patch.object(command.socket, "getaddrinfo", return_value=[(None, None, None, None, ("8.8.8.8", 5432))]):
                with self.assertRaises(CommandError):
                    command.require_local(self.local_connection(HOST="localhost"))
            local = self.local_connection()
            local.connection = object()
            with self.assertRaises(CommandError):
                command.require_local(local)

    def test_readonly_set_before_queries_and_write_guard_and_session_close(self):
        local = self.local_connection()
        cursor = local.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("127.0.0.1", "on")
        events = []
        cursor.execute.side_effect = lambda sql: events.append(sql)

        @contextmanager
        def wrapper(guard):
            execute = MagicMock()
            for sql in ('UPDATE "core_movie" SET id=1', 'DELETE FROM "core_movie"',
                        'SELECT * FROM "auth_user"', 'WITH x AS (DELETE FROM "core_movie") SELECT 1'):
                with self.assertRaises(CommandError):
                    guard(execute, sql, None, False, {})
            guard(execute, 'SELECT * FROM "core_movie"', None, False, {})
            execute.assert_called_once()
            yield

        local.execute_wrapper.side_effect = wrapper
        with patch.dict(command.os.environ, {}, clear=True), patch.object(command, "connections", {"local": local}), \
                patch.object(command.transaction, "atomic") as atomic:
            with command.local_readonly_snapshot("local"):
                events.append("analysis")
        self.assertEqual(events, ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY", command.SESSION_CHECK, "analysis"])
        atomic.assert_called_once_with(using="local")
        local.close.assert_called_once()

    def test_server_must_confirm_local_and_readonly(self):
        for address, readonly in [("10.0.0.1", "on"), ("127.0.0.1", "off"), (None, "on")]:
            local = self.local_connection()
            local.cursor.return_value.__enter__.return_value.fetchone.return_value = (address, readonly)
            with patch.dict(command.os.environ, {}, clear=True), patch.object(command, "connections", {"local": local}), \
                    patch.object(command.transaction, "atomic"):
                with self.assertRaises(CommandError):
                    with command.local_readonly_snapshot("local"):
                        self.fail("analysis must not start")
            local.close.assert_called_once()

    def test_server_inet_text_netmask_is_removed_by_host_function(self):
        # PostgreSQL inet::text can return '127.0.0.1/32', not a bare IP.
        # Model the actual server output so reverting to ::text reproduces the failure.
        self.assertFalse(command.is_loopback("127.0.0.1/32"))
        local = self.local_connection()
        cursor = local.cursor.return_value.__enter__.return_value

        def execute(sql):
            if sql == command.SESSION_CHECK:
                address = "127.0.0.1" if "host(inet_server_addr())" in sql else "127.0.0.1/32"
                cursor.fetchone.return_value = (address, "on")

        cursor.execute.side_effect = execute
        with patch.dict(command.os.environ, {}, clear=True), patch.object(command, "connections", {"local": local}), \
                patch.object(command.transaction, "atomic"):
            with command.local_readonly_snapshot("local"):
                pass
        local.close.assert_called_once()

    def test_nested_transactions_and_failure_to_set_readonly_never_reach_analysis(self):
        local = self.local_connection()
        local.in_atomic_block = True
        with patch.dict(command.os.environ, {}, clear=True), patch.object(command, "connections", {"local": local}):
            with self.assertRaises(CommandError):
                with command.local_readonly_snapshot("local"):
                    self.fail("nested transaction")
        local.get_autocommit.assert_not_called()
        local = self.local_connection()
        local.cursor.return_value.__enter__.return_value.execute.side_effect = DatabaseError("cannot set read only")
        with patch.dict(command.os.environ, {}, clear=True), patch.object(command, "connections", {"local": local}), \
                patch.object(command.transaction, "atomic"):
            with self.assertRaises(DatabaseError):
                with command.local_readonly_snapshot("local"):
                    self.fail("unprotected analysis")
        local.execute_wrapper.assert_not_called()
        local.close.assert_called_once()

    def test_command_rejects_remote_before_connecting_or_creating_reports(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "must not exist"
            local = self.local_connection(HOST="production.render.com")
            with patch.object(command, "connections", {"default": local}):
                with self.assertRaises(CommandError):
                    call_command("diagnose_local_movie_duplicates", output_dir=str(output))
            local.get_autocommit.assert_not_called()
            self.assertFalse(output.exists())

    def test_command_rejects_repo_output_before_database_access(self):
        with patch.object(command, "require_local", side_effect=AssertionError("database validation")):
            with self.assertRaisesRegex(CommandError, "outside the repository"):
                call_command("diagnose_local_movie_duplicates", output_dir=str(command.settings.BASE_DIR / "forbidden"))
