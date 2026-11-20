"""Apply orchestration and rollback on a synthetic SQLite target."""
from contextlib import contextmanager
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, connection, connections

from core.models import Movie
from core import test_tmdb_importer as fixtures
from core.test_tmdb_importer import record
from core.tmdb_import_apply import apply_import, check_schema, check_session, resolve_author
from core.tmdb_import_ready import prepare_run
from core.tmdb_importer import read_json


class ApplyTests(unittest.TestCase):
    source_run = fixtures.PlanningTests.source_run

    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="reccool-apply-synthetic-")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.source_run([record(1, optional=False, video=True), record(2, kind="series"), record(3)])
        self.prepared = prepare_run(self.source, self.root / "prepared")["run_directory"]
        self.target = {"alias": "default", "host": "127.0.0.1", "port": "5432", "name": "isolated"}
        self.assertEqual(connection.vendor, "sqlite")
        self.assertEqual(connection.settings_dict["NAME"], ":memory:")
        with patch.object(Movie._meta, "indexes", []), connection.schema_editor() as schema:
            schema.execute("CREATE TABLE auth_user (id INTEGER PRIMARY KEY, username TEXT UNIQUE)")
            schema.execute("INSERT INTO auth_user VALUES (71,'Admin'), (93,'catalog_import')")
            schema.create_model(Movie)
            schema.execute("CREATE UNIQUE INDEX synthetic_identity ON core_movie(type, tmdb_id)")
            schema.execute("CREATE TABLE synthetic_relation (movie_id INTEGER REFERENCES core_movie(id))")
            schema.execute("""
                CREATE TRIGGER synthetic_vector AFTER INSERT ON core_movie
                BEGIN UPDATE core_movie SET search_vector = 'synthetic' WHERE id = NEW.id; END
            """)
        self.addCleanup(self.drop_tables)

    def drop_tables(self):
        with connection.cursor() as cursor:
            for table in ("synthetic_relation", "core_movie", "auth_user"):
                cursor.execute(f"DROP TABLE {table}")

    def apply(self, limit, *, resume=None, forbidden_schema=False, author="Admin"):
        def substitute_postgres_commands(execute, sql, params, many, context):
            if sql.startswith(("SET TRANSACTION", "SET LOCAL", "LOCK TABLE")):
                return None
            return execute(sql, params, many, context)

        with patch("core.tmdb_import_apply.require_local"), patch("core.tmdb_import_apply.check_session"), \
                patch("core.tmdb_import_apply.check_schema",
                      side_effect=CommandError("schema blocked") if forbidden_schema else None), \
                connection.execute_wrapper(substitute_postgres_commands):
            return apply_import(self.prepared, self.root / "reports", connection, self.target,
                                batch_size=2, limit=limit, author_username=author, resume=resume)

    def test_limit_resume_repeat_and_existing_social_link(self):
        existing = Movie.objects.create(type="movie", tmdb_id=3, title_english="Existing", author_id=93)
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO synthetic_relation VALUES (%s)", [existing.pk])
        first = self.apply(1)
        self.assertEqual((first["state"], first["inserted"]), ("limited", 1))
        japanese = Movie.objects.get(type="movie", tmdb_id=1)
        self.assertEqual(japanese.title_english, "日本語")
        self.assertIsNone(japanese.title_spanish)
        self.assertEqual(japanese.author_id, 71)
        self.assertEqual(first["author"], {"username": "Admin", "id": 71, "status": "resolved"})
        self.assertEqual(japanese.search_vector, "synthetic")
        second = self.apply(5, resume=first["run_directory"])
        self.assertEqual(second["counts"], {"existing": 2, "inserted": 1})
        third = self.apply(5)
        self.assertEqual(third["counts"], {"existing": 3})
        existing.refresh_from_db()
        self.assertEqual(existing.title_english, "Existing")
        self.assertEqual(existing.author_id, 93)
        self.assertEqual(Movie.objects.get(tmdb_id=2).author_id, 71)
        with connection.cursor() as cursor:
            cursor.execute("SELECT movie_id FROM synthetic_relation")
            self.assertEqual(cursor.fetchall(), [(existing.pk,)])

    def test_missing_vectors_roll_back_entire_batch(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP TRIGGER synthetic_vector")
        with self.assertRaisesRegex(CommandError, "rolled back"):
            self.apply(5)
        self.assertFalse(Movie.objects.exists())
        report = next((self.root / "reports").glob("*/import_manifest.json"))
        self.assertEqual(read_json(report)["inserted"], 0)

    def test_resume_after_later_batch_fails_preserves_committed_ids(self):
        with connection.cursor() as cursor:
            cursor.execute("DROP TRIGGER synthetic_vector")
            cursor.execute("""
                CREATE TRIGGER synthetic_vector AFTER INSERT ON core_movie WHEN NEW.tmdb_id != 3
                BEGIN UPDATE core_movie SET search_vector = 'synthetic' WHERE id = NEW.id; END
            """)
        with self.assertRaisesRegex(CommandError, "rolled back"):
            self.apply(5)
        before = dict(Movie.objects.values_list("tmdb_id", "id"))
        self.assertEqual(set(before), {1, 2})
        report = next((self.root / "reports").glob("*/import_manifest.json"))
        self.assertEqual(read_json(report)["counts"], {"inserted": 2})
        with connection.cursor() as cursor:
            cursor.execute("DROP TRIGGER synthetic_vector")
            cursor.execute("""
                CREATE TRIGGER synthetic_vector AFTER INSERT ON core_movie
                BEGIN UPDATE core_movie SET search_vector = 'synthetic' WHERE id = NEW.id; END
            """)
        resumed = self.apply(5, resume=report.parent)
        self.assertEqual(resumed["counts"], {"existing": 2, "inserted": 1})
        self.assertEqual(dict(Movie.objects.filter(tmdb_id__in=before).values_list("tmdb_id", "id")), before)

    def test_failed_preflight_blocks_insertions(self):
        with self.assertRaisesRegex(CommandError, "schema blocked"):
            self.apply(5, forbidden_schema=True)
        self.assertFalse(Movie.objects.exists())

    def test_missing_or_unprovided_author_blocks_apply(self):
        for author, status in [("missing", "not_found"), (None, "not_provided")]:
            with self.assertRaisesRegex(CommandError, status):
                self.apply(5, author=author)
        self.assertFalse(Movie.objects.exists())
        self.target["name"] = "socialapp"
        with self.assertRaisesRegex(CommandError, "expected_local_admin"):
            self.apply(5, author="catalog_import")
        self.assertFalse(Movie.objects.exists())

    def test_author_pk_is_resolved_in_selected_alias(self):
        other = connections["alternate"]
        self.assertEqual((other.vendor, other.settings_dict["NAME"]), ("sqlite", ":memory:"))
        with other.cursor() as cursor:
            cursor.execute("CREATE TABLE auth_user (id INTEGER PRIMARY KEY, username TEXT UNIQUE)")
            try:
                cursor.execute("INSERT INTO auth_user VALUES (817,'Admin'), (992,'catalog_import')")
                self.assertEqual(resolve_author({**self.target, "alias": "alternate"}, "Admin")["id"], 817)
                self.assertEqual(resolve_author({**self.target, "alias": "alternate"}, "catalog_import")["id"], 992)
            finally:
                cursor.execute("DROP TABLE auth_user")
        self.assertEqual(self.apply(5, author="catalog_import")["author"]["id"], 93)
        self.assertEqual(set(Movie.objects.values_list("author_id", flat=True)), {93})

    def test_resume_rejects_different_username_or_recreated_author(self):
        first = self.apply(1)
        with self.assertRaisesRegex(CommandError, "explicit author"):
            self.apply(5, resume=first["run_directory"], author="catalog_import")
        with connection.cursor() as cursor:
            cursor.execute("UPDATE auth_user SET username='FormerAdmin' WHERE id=71")
            cursor.execute("INSERT INTO auth_user VALUES (72,'Admin')")
        with self.assertRaisesRegex(CommandError, "author ID no longer matches"):
            self.apply(5, resume=first["run_directory"])
        self.assertEqual(list(Movie.objects.values_list("author_id", flat=True)), [71])

    def test_foreign_key_error_rolls_back_batch(self):
        # Django's SQLite FK is deferred, so force the failure at transaction commit.
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TRIGGER synthetic_unrelated_fk AFTER INSERT ON core_movie
                BEGIN INSERT INTO synthetic_relation VALUES (999999); END
            """)
        with self.assertRaises(IntegrityError):
            self.apply(5)
        self.assertFalse(Movie.objects.exists())
        report = next((self.root / "reports").glob("*/import_manifest.json"))
        self.assertEqual(read_json(report)["inserted"], 0)

    def test_dry_run_reports_author_without_writes_or_eligibility_changes(self):
        @contextmanager
        def snapshot(alias, **kwargs):
            with connection.cursor() as cursor:
                cursor.execute("PRAGMA query_only=ON")
            try:
                yield
            finally:
                with connection.cursor() as cursor:
                    cursor.execute("PRAGMA query_only=OFF")

        command = "core.management.commands.import_tmdb_productions"
        for username, status, pk in [("Admin", "resolved", 71), ("missing", "not_found", None),
                                     (None, "not_provided", None)]:
            parent = self.root / status
            with patch(command + ".require_local"), patch(command + ".local_readonly_snapshot", snapshot):
                call_command("import_tmdb_productions", run_dir=self.prepared, output_dir=str(parent),
                             author_username=username, stdout=io.StringIO())
            manifest = read_json(next(parent.glob("*/import_manifest.json")))
            self.assertEqual(manifest["author"], {"username": username, "status": status, "id": pk})
            self.assertEqual(manifest["author_blocks_apply"], pk is None)
            self.assertEqual((manifest["counts"]["proposed_new"], manifest["inserted"]), (3, 0))
        self.assertFalse(Movie.objects.exists())

    def test_resume_and_corrupt_source_are_validated_before_postgres(self):
        first = self.apply(1)
        self.target["name"] = "another"
        with self.assertRaisesRegex(CommandError, "same verified source and target"):
            self.apply(5, resume=first["run_directory"])
        (Path(self.prepared) / "import_ready.jsonl.gz").write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "output integrity"):
            self.apply(5)
        self.assertEqual(Movie.objects.count(), 1)


class ApplyBoundaryTests(unittest.TestCase):
    def test_requires_unique_index_then_enabled_trigger(self):
        for results, message in [([(False,)], "unique index"), ([(True,), (False,)], "trigger"),
                                 ([(True,), (True,), (False,)], "foreign key")]:
            cursor = Mock()
            cursor.fetchone.side_effect = results
            with self.assertRaisesRegex(CommandError, message):
                check_schema(cursor, '"core_movie"')
        cursor = Mock()
        cursor.fetchone.side_effect = [(True,), (True,), (True,)]
        check_schema(cursor, '"core_movie"')
        self.assertEqual(cursor.execute.call_count, 3)

    def test_apply_without_author_rejects_before_getting_connection(self):
        command = "core.management.commands.import_tmdb_productions"
        with patch(command + ".connections") as targets:
            with self.assertRaisesRegex(CommandError, "--author-username"):
                call_command("import_tmdb_productions", run_dir="unused", output_dir="unused",
                             apply=True, confirm_write=True, confirm_database="socialapp", limit=1)
            targets.__getitem__.assert_not_called()

    def test_session_must_confirm_endpoint_database_and_transaction_mode(self):
        target = {"port": "5432", "name": "isolated"}
        cursor = Mock()
        for row in [("192.0.2.1", 5432, "isolated", "off", False),
                    ("127.0.0.1", 5433, "isolated", "off", False),
                    ("127.0.0.1", 5432, "other", "off", False),
                    ("127.0.0.1", 5432, "isolated", "on", False),
                    ("127.0.0.1", 5432, "isolated", "off", True)]:
            cursor.fetchone.return_value = row
            with self.assertRaises(CommandError):
                check_session(cursor, target, read_only=False)
        cursor.fetchone.return_value = ("127.0.0.1", 5432, "isolated", "off", False)
        check_session(cursor, target, read_only=False)
