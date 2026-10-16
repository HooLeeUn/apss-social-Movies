import io
import json
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import MagicMock, patch

from django.core.management import CommandError, call_command

from core.management.commands.bulk_delete_movies_from_csv import Command, validate_csv
from core.models import Movie


class BulkMoviePreflightTests(TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory(dir=Path(__file__).resolve().parent.parent)
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "movies.csv"

    def write(self, content):
        self.path.write_text(content, encoding="utf-8")

    def test_valid_ids_spooled_without_reason(self):
        self.write('\ufeffdatabase_id,reason\n1,"a,b"\n9223372036854775807,404\n')
        spool = io.StringIO()
        self.assertEqual(validate_csv(self.path, spool),
                         dict(rows_read=2, unique_ids=2, errors=0))
        self.assertEqual(spool.read(), "1\n9223372036854775807\n")

    def test_invalid_or_duplicate_reject_entire_manifest(self):
        for values in ("1\n1", "0", "-1", "01", "1.0", "", "9223372036854775808", "1,extra"):
            with self.subTest(values=values):
                self.write(f"database_id\n{values}\n")
                with self.assertRaises(CommandError):
                    validate_csv(self.path, io.StringIO())

    def test_missing_or_duplicate_header(self):
        for content in ("id\n1\n", "database_id,database_id\n1,2\n"):
            self.write(content)
            with self.assertRaises(CommandError):
                validate_csv(self.path, io.StringIO())

    @patch("core.management.commands.bulk_delete_movies_from_csv.connections")
    def test_apply_requires_expected_database_before_connection_access(self, connections):
        with self.assertRaises(CommandError):
            call_command("bulk_delete_movies_from_csv", "--affected-csv", "missing.csv", "--apply")
        connections.__getitem__.assert_not_called()

    @patch("core.management.commands.bulk_delete_movies_from_csv.connections")
    def test_invalid_manifest_never_accesses_connection(self, connections):
        self.write("database_id\n-1\n")
        with self.assertRaises(CommandError):
            call_command("bulk_delete_movies_from_csv", affected_csv=str(self.path),
                         expected_database="socialapp_render_clone")
        connections.__getitem__.assert_not_called()

    @patch.object(Command, "inspect_database")
    @patch("core.management.commands.bulk_delete_movies_from_csv.connections")
    def test_default_is_dry_run_and_reports_counts(self, connections, inspect):
        self.write("database_id,reason\n1,404\n2,missing\n")
        db = connections.__getitem__.return_value
        db.vendor = "postgresql"
        db.settings_dict = {"NAME": "socialapp_render_clone"}
        inspect.return_value = dict(existing_before=1, not_found=1, related_counts={})
        out = io.StringIO()
        call_command("bulk_delete_movies_from_csv", affected_csv=str(self.path),
                     expected_database="socialapp_render_clone", stdout=out)
        self.assertIn('"deleted": 0', out.getvalue())
        self.assertIn('"rows_read": 2', out.getvalue())
        self.assertIn('"errors": 0', out.getvalue())
        self.assertFalse(inspect.call_args.kwargs["apply"])
        inspect.assert_called_once()

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_sql_preflight_uses_copy_and_rolls_back_without_delete(self, transaction):
        db, cursor = self.database(existing=1)
        summary = Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone")
        self.assertEqual(summary["not_found"], 1)
        cursor.copy.assert_called_once_with("COPY pg_temp.bulk_movie_ids (id) FROM STDIN")
        transaction.set_rollback.assert_called_once_with(True, using="default")
        sql = " ".join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertNotIn("DELETE", sql)
        self.assertNotIn("UPDATE", sql)
        self.assertIn("REPEATABLE READ", sql)

    @patch.object(Command, "inspect_database")
    @patch("core.management.commands.bulk_delete_movies_from_csv.connections")
    def test_wrong_database_rejected_before_queries(self, connections, inspect):
        self.write("database_id\n1\n")
        db = connections.__getitem__.return_value
        db.vendor = "postgresql"
        db.settings_dict = {"NAME": "production"}
        with self.assertRaises(CommandError):
            call_command("bulk_delete_movies_from_csv", affected_csv=str(self.path),
                         expected_database="socialapp_render_clone")
        inspect.assert_not_called()

    def database(self, counts=None, existing=2, remaining=0, deleted=2,
                 active_database="socialapp_render_clone", candidate_flow=False,
                 candidate_stats=None, candidate_deleted=None, candidate_remaining=0,
                 after_counts=None):
        db = MagicMock(alias="default")
        db.in_atomic_block = False
        db.ops.quote_name.side_effect = lambda name: f'"{name}"'
        cursor = db.cursor.return_value.__enter__.return_value
        relations = list(Movie._meta.related_objects)
        cursor.fetchall.return_value = [
            (r.related_model._meta.db_table, r.field.column, "a") for r in relations
        ]
        cursor.fetchall.side_effect = lambda: (
            cursor.fetchall.return_value if "SELECT c.conrelid" in cursor.execute.call_args.args[0] else []
        )
        candidate_rows = (counts or {}).get("UserDailyFeedCandidate", 0)
        stats = candidate_stats if candidate_stats is not None else (candidate_rows, min(candidate_rows, 2), min(candidate_rows, 2))
        cursor.fetchone.side_effect = [(active_database,), (existing,), (2,)] + [
            ((counts or {}).get(r.related_model.__name__, 0),) for r in relations
        ] + [stats] + ([(candidate_remaining,)] + [
            ((after_counts or {}).get(r.related_model.__name__, 0),) for r in relations
        ] if candidate_flow else []) + [(remaining,)]
        cursor.rowcount = deleted

        def execute(sql, *args):
            if sql.startswith('DELETE FROM "core_userdailyfeedcandidate"'):
                cursor.rowcount = candidate_rows if candidate_deleted is None else candidate_deleted
            elif sql.startswith('DELETE FROM "core_movie"'):
                cursor.rowcount = deleted

        cursor.execute.side_effect = execute
        return db, cursor

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_apply_success_without_relations(self, transaction):
        db, cursor = self.database()
        result = Command().inspect_database(db, io.StringIO("1\n2\n"),
                                            "socialapp_render_clone", apply=True)
        self.assertEqual(result["deleted"], 2)
        self.assertEqual(result["remaining_after"], 0)
        transaction.set_rollback.assert_not_called()
        sql = [call.args[0] for call in cursor.execute.call_args_list]
        lock = next(i for i, stmt in enumerate(sql) if stmt.startswith("LOCK TABLE"))
        count = next(i for i, stmt in enumerate(sql) if stmt.startswith("SELECT count(*) FROM \"core_movie\""))
        delete = next(i for i, stmt in enumerate(sql) if stmt.startswith("DELETE"))
        self.assertLess(lock, count)
        self.assertLess(count, delete)
        self.assertIn('DELETE FROM "core_movie" m USING pg_temp.bulk_movie_ids s WHERE m."id" = s.id', sql)
        self.assertEqual(sql[-1], "SET CONSTRAINTS ALL IMMEDIATE")
        transaction.atomic.return_value.__exit__.assert_called_once_with(None, None, None)

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_every_audited_relation_blocks_apply_including_rating(self, transaction):
        for relation in Movie._meta.related_objects:
            name = relation.related_model.__name__
            with self.subTest(relation=name):
                db, cursor = self.database(counts={name: 1})
                with self.assertRaisesRegex(CommandError, "Relaciones no vacías"):
                    Command().inspect_database(db, io.StringIO("1\n2\n"),
                                               "socialapp_render_clone", apply=True)
                self.assertFalse(any(c.args[0].startswith("DELETE") for c in cursor.execute.call_args_list))
                self.assertIs(transaction.atomic.return_value.__exit__.call_args.args[0], CommandError)

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_remaining_targets_or_wrong_delete_count_roll_back(self, transaction):
        for remaining, deleted in ((1, 2), (0, 1)):
            with self.subTest(remaining=remaining, deleted=deleted):
                db, cursor = self.database(remaining=remaining, deleted=deleted)
                with self.assertRaisesRegex(CommandError, "Verificación final"):
                    Command().inspect_database(db, io.StringIO("1\n2\n"),
                                               "socialapp_render_clone", apply=True)
                self.assertIs(transaction.atomic.return_value.__exit__.call_args.args[0], CommandError)

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_missing_ids_abort_apply(self, transaction):
        db, cursor = self.database(existing=1)
        with self.assertRaisesRegex(CommandError, "IDs ausentes"):
            Command().inspect_database(db, io.StringIO("1\n2\n"),
                                       "socialapp_render_clone", apply=True)
        self.assertFalse(any(c.args[0].startswith("DELETE") for c in cursor.execute.call_args_list))

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_actual_database_mismatch_aborts(self, transaction):
        db, cursor = self.database(active_database="production")
        with self.assertRaisesRegex(CommandError, "current_database"):
            Command().inspect_database(db, io.StringIO("1\n2\n"),
                                       "socialapp_render_clone", apply=True)
        self.assertFalse(any(c.args[0].startswith("COPY") or c.args[0].startswith("DELETE")
                             for c in cursor.execute.call_args_list))

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_schema_drift_aborts_apply(self, transaction):
        for extra in ([("external_table", "movie_id", "a")], [], [("external_table", "movie_id", "c")]):
            db, cursor = self.database()
            cursor.fetchall.return_value = extra
            with self.assertRaisesRegex(CommandError, "FK PostgreSQL"):
                Command().inspect_database(db, io.StringIO("1\n2\n"),
                                           "socialapp_render_clone", apply=True)
            self.assertFalse(any(c.args[0].startswith("DELETE") for c in cursor.execute.call_args_list))

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_commit_failure_never_reports_success(self, transaction):
        db, cursor = self.database()
        transaction.atomic.return_value.__exit__.side_effect = RuntimeError("commit failed")
        with self.assertRaisesRegex(RuntimeError, "commit failed"):
            Command().inspect_database(db, io.StringIO("1\n2\n"),
                                       "socialapp_render_clone", apply=True)

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_lock_timeout_aborts_before_delete(self, transaction):
        db, cursor = self.database()

        def execute(sql, *args):
            if sql.startswith("LOCK TABLE"):
                raise RuntimeError("lock timeout")

        cursor.execute.side_effect = execute
        with self.assertRaisesRegex(RuntimeError, "lock timeout"):
            Command().inspect_database(db, io.StringIO("1\n2\n"),
                                       "socialapp_render_clone", apply=True)
        self.assertFalse(any(c.args[0].startswith("DELETE") for c in cursor.execute.call_args_list))
        self.assertIs(transaction.atomic.return_value.__exit__.call_args.args[0], RuntimeError)

    @patch.object(Command, "inspect_database")
    @patch("core.management.commands.bulk_delete_movies_from_csv.connections")
    def test_apply_report_preserves_committed_deleted_count(self, connections, inspect):
        self.write("database_id\n1\n2\n")
        db = connections.__getitem__.return_value
        db.vendor = "postgresql"
        db.settings_dict = {"NAME": "socialapp_render_clone"}
        inspect.return_value = dict(existing_before=2, not_found=0, related_counts={},
                                    deleted=2, remaining_after=0, database="socialapp_render_clone")
        out = io.StringIO()
        call_command("bulk_delete_movies_from_csv", affected_csv=str(self.path),
                     expected_database="socialapp_render_clone", apply=True, stdout=out)
        report = json.loads(out.getvalue().splitlines()[0])
        self.assertEqual(report["deleted"], 2)
        self.assertEqual(report["remaining_after"], 0)
        self.assertEqual(report["mode"], "apply")
        self.assertTrue(inspect.call_args.kwargs["apply"])

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_candidates_with_flag_delete_both_and_count_distinct_movies(self, transaction):
        db, cursor = self.database(counts={"UserDailyFeedCandidate": 3}, candidate_flow=True,
                                   candidate_stats=(3, 1, 3))
        result = Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                            apply=True, delete_feed_candidates=True)
        self.assertEqual(result["candidate_rows_before"], 3)
        self.assertEqual(result["unique_movies_with_candidates"], 1)
        self.assertEqual(result["affected_pools"], 3)
        self.assertEqual(result["candidate_rows_deleted"], 3)
        self.assertEqual(result["candidate_rows_remaining"], 0)
        self.assertEqual(result["deleted"], 2)
        sql = [c.args[0] for c in cursor.execute.call_args_list]
        deletes = [s for s in sql if s.startswith("DELETE")]
        self.assertEqual(deletes, [
            'DELETE FROM "core_userdailyfeedcandidate" c USING pg_temp.bulk_movie_ids s WHERE c.movie_id = s.id',
            'DELETE FROM "core_movie" m USING pg_temp.bulk_movie_ids s WHERE m."id" = s.id',
        ])
        for relation in Movie._meta.related_objects:
            table = relation.related_model._meta.db_table
            counts = [s for s in sql if s.startswith(f'SELECT count(*) FROM "{table}" r')]
            self.assertEqual(len(counts), 2)
        transaction.atomic.return_value.__exit__.assert_called_once_with(None, None, None)

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_other_ten_relations_still_block_with_flag(self, transaction):
        for relation in Movie._meta.related_objects:
            name = relation.related_model.__name__
            if name == "UserDailyFeedCandidate":
                continue
            with self.subTest(relation=name):
                db, cursor = self.database(counts={name: 1, "UserDailyFeedCandidate": 3})
                with self.assertRaisesRegex(CommandError, "Relaciones no vacías"):
                    Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                               apply=True, delete_feed_candidates=True)
                self.assertFalse(any(c.args[0].startswith("DELETE") for c in cursor.execute.call_args_list))

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_candidate_count_discrepancies_abort(self, transaction):
        for changes in ({"candidate_deleted": 2}, {"candidate_remaining": 1},
                        {"candidate_stats": (2, 1, 1)}):
            with self.subTest(changes=changes):
                db, cursor = self.database(counts={"UserDailyFeedCandidate": 3},
                                           candidate_flow=True, **changes)
                with self.assertRaisesRegex(CommandError, "candidatos"):
                    Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                               apply=True, delete_feed_candidates=True)
                self.assertFalse(any(c.args[0].startswith('DELETE FROM "core_movie"')
                                     for c in cursor.execute.call_args_list))
                self.assertIs(transaction.atomic.return_value.__exit__.call_args.args[0], CommandError)

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_revalidation_of_relations_aborts_movie_delete(self, transaction):
        db, cursor = self.database(counts={"UserDailyFeedCandidate": 3}, candidate_flow=True,
                                   after_counts={"MovieRating": 1})
        with self.assertRaisesRegex(CommandError, "tras limpiar"):
            Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                       apply=True, delete_feed_candidates=True)
        self.assertFalse(any(c.args[0].startswith('DELETE FROM "core_movie"')
                             for c in cursor.execute.call_args_list))

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_flag_in_dry_run_only_counts_candidates(self, transaction):
        db, cursor = self.database(counts={"UserDailyFeedCandidate": 3}, candidate_stats=(3, 1, 3))
        result = Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                            delete_feed_candidates=True)
        self.assertEqual(result["candidate_rows_before"], 3)
        self.assertEqual(result["candidate_rows_remaining"], 3)
        self.assertEqual(result["candidate_rows_deleted"], 0)
        self.assertEqual(result["deleted"], 0)
        self.assertFalse(any(c.args[0].startswith(("DELETE", "UPDATE", "LOCK"))
                             for c in cursor.execute.call_args_list))
        transaction.set_rollback.assert_called_once_with(True, using="default")

    @patch("core.management.commands.bulk_delete_movies_from_csv.transaction")
    def test_unexpected_candidate_fk_or_delete_trigger_aborts(self, transaction):
        for kind in ("candidate_fk", "movie_trigger", "candidate_trigger"):
            with self.subTest(kind=kind):
                db, cursor = self.database()
                fks = cursor.fetchall.return_value

                def fetchall():
                    sql, *params = cursor.execute.call_args.args
                    if "SELECT c.conrelid" in sql:
                        return fks
                    if "SELECT conname" in sql and kind == "candidate_fk":
                        return [("unexpected_fk",)]
                    if "FROM pg_trigger" in sql:
                        target = params[0][0]
                        if ((kind == "movie_trigger" and target == "core_movie") or
                                (kind == "candidate_trigger" and target == "core_userdailyfeedcandidate")):
                            return [("unexpected_delete_trigger",)]
                    return []

                cursor.fetchall.side_effect = fetchall
                with self.assertRaisesRegex(CommandError, "inesperad"):
                    Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                               apply=True, delete_feed_candidates=True)
                self.assertFalse(any(c.args[0].startswith("DELETE") for c in cursor.execute.call_args_list))

    def run_memory_scenario(self, *, fail_movie=False, dry_run=False, final_remaining=None):
        """Transactional state simulator, NOT an SQL engine or PostgreSQL integration test."""
        state = {"movies": {1, 2, 99}, "candidates": {(10, 1, 7), (11, 1, 8), (12, 99, 7)},
                 "pools": {7, 8}, "other_user_data": {"ratings": [(99, 5)]}}
        original = deepcopy(state)
        db, cursor = self.database()
        targets = set()
        cursor.copy.return_value.__enter__.return_value.write.side_effect = lambda chunk: targets.update(
            int(line) for line in chunk.splitlines()
        )
        rolled_back = False

        @contextmanager
        def atomic(**kwargs):
            nonlocal rolled_back
            try:
                yield
                if dry_run:
                    state.clear()
                    state.update(deepcopy(original))
                    rolled_back = True
            except Exception:
                state.clear()
                state.update(deepcopy(original))
                rolled_back = True
                raise

        movie_deleted = False

        def execute(sql, *params):
            nonlocal movie_deleted
            candidates = [row for row in state["candidates"] if row[1] in targets]
            if sql.startswith("SELECT current_database"):
                cursor.fetchone.return_value = ("socialapp_render_clone",)
            elif sql.startswith("SELECT count(*) FROM pg_temp"):
                cursor.fetchone.return_value = (len(targets),)
            elif sql.startswith('SELECT count(*) FROM "core_movie"'):
                cursor.fetchone.return_value = (
                    final_remaining if movie_deleted and final_remaining is not None
                    else len(state["movies"] & targets),
                )
            elif sql.startswith("SELECT count(*),"):
                cursor.fetchone.return_value = (len(candidates), len({r[1] for r in candidates}),
                                                 len({r[2] for r in candidates}))
            elif sql.startswith('SELECT count(*) FROM "core_userdailyfeedcandidate"'):
                cursor.fetchone.return_value = (len(candidates),)
            elif sql.startswith("SELECT count(*)"):
                cursor.fetchone.return_value = (0,)
            elif sql.startswith('DELETE FROM "core_userdailyfeedcandidate"'):
                self.assertTrue(sql.endswith("WHERE c.movie_id = s.id"))
                state["candidates"].difference_update(candidates)
                cursor.rowcount = len(candidates)
            elif sql.startswith('DELETE FROM "core_movie"'):
                if fail_movie:
                    raise RuntimeError("movie delete failed")
                self.assertTrue(sql.endswith('WHERE m."id" = s.id'))
                cursor.rowcount = len(state["movies"] & targets)
                state["movies"].difference_update(targets)
                movie_deleted = True

        cursor.fetchone.side_effect = None
        cursor.execute.side_effect = execute
        with patch("core.management.commands.bulk_delete_movies_from_csv.transaction") as transaction:
            transaction.atomic.side_effect = atomic
            try:
                result = Command().inspect_database(db, io.StringIO("1\n2\n"), "socialapp_render_clone",
                                                    apply=not dry_run, delete_feed_candidates=True)
            except (RuntimeError, CommandError):
                self.assertTrue(rolled_back)
                self.assertEqual(state, original)
                raise
        return state, result, original

    def test_outside_csv_and_other_user_data_preserved_in_state_simulator(self):
        state, result, original = self.run_memory_scenario()
        self.assertEqual(state["movies"], {99})
        self.assertEqual(state["candidates"], {(12, 99, 7)})
        self.assertEqual(state["pools"], original["pools"])
        self.assertEqual(state["other_user_data"], original["other_user_data"])
        self.assertEqual(result["unique_movies_with_candidates"], 1)
        self.assertEqual(result["affected_pools"], 2)

    def test_movie_delete_failure_restores_candidates_in_state_simulator(self):
        with self.assertRaisesRegex(RuntimeError, "movie delete failed"):
            self.run_memory_scenario(fail_movie=True)

    def test_final_count_failure_restores_both_in_state_simulator(self):
        with self.assertRaisesRegex(CommandError, "Verificación final"):
            self.run_memory_scenario(final_remaining=1)

    def test_dry_run_preserves_all_rows_in_state_simulator(self):
        state, result, original = self.run_memory_scenario(dry_run=True)
        self.assertEqual(state, original)
        self.assertEqual(result["candidate_rows_deleted"], 0)
