import csv
import json
import sqlite3
from contextlib import closing
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.management import call_command, CommandError
from django.db import connection
from django.test import TransactionTestCase
from django.test.utils import CaptureQueriesContext

from core.adult_cleanup import digest_file, metadata_fingerprint
from core.management.commands.adult_movie_cleanup import storage_identity
from core.management.commands.audit_adult_movies import FIELDS, database_identity
from core.models import Movie
from core.protected_metadata_approval import IDENTITY, load_approval, recover_known_metadata


class ProtectedMetadataApprovalTests(TransactionTestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(dir=Path(__file__).resolve().parent.parent / "tmdb_exports")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.selection = self.root / "adult_movies_final_selection.json"
        self.journal = self.root / "existing.sqlite3"
        self.approval = self.root / "metadata_approval.json"
        self.identity = database_identity("default")
        self.movie = Movie.objects.create(id=525617, type="movie", tmdb_id=812137,
             title_english="Power Rangers Jungle Fury", release_year=2008, synopsis="Old synopsis", synopsis_es="")
        self.others = [Movie.objects.create(title_english=f"Other protected {i}", tmdb_id=2000+i, type="movie", release_year=2000)
                       for i in range(7)]

        def snap(movie):
            return dict({key: getattr(movie, "pk" if key == "database_id" else key) for key in FIELDS},
                        identity_metadata_sha256=metadata_fingerprint(movie))

        selected = [dict(database_id=10000+i, type="movie", tmdb_id=3000+i, title_english=f"Selected {i}", release_year=2020,
                         identity_metadata_sha256=metadata_fingerprint({"title_spanish": None, "synopsis": "", "synopsis_es": None}))
                    for i in range(337)]
        manifest = self.root / "adult_movies_final_to_delete.csv"
        with manifest.open("w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["database_id"])
            writer.writerows([row["database_id"]] for row in selected)
        self.proof = {"version": 2, "database": self.identity, "manifest_sha256": digest_file(manifest),
                      "movies": selected, "protected_movies": [snap(self.movie)] + [snap(m) for m in self.others]}
        self.selection.write_text(json.dumps(self.proof), encoding="utf-8")
        with closing(sqlite3.connect(self.journal)) as db:
            db.executescript("""CREATE TABLE metadata(key TEXT,value TEXT);
                CREATE TABLE movies(id INTEGER PRIMARY KEY,status TEXT,error TEXT);
                CREATE TABLE media(movie_id INTEGER,name TEXT,status TEXT,error TEXT);
                CREATE TABLE related(model TEXT,id INTEGER);""")
            db.executemany("INSERT INTO metadata VALUES (?,?)", [("database", self.identity), ("selection", digest_file(self.selection)),
                                                                  ("storage", storage_identity())])
            db.executemany("INSERT INTO movies VALUES (?,'planned','')", [(r["database_id"],) for r in selected])
            db.commit()
        self.old = metadata_fingerprint(self.movie)
        Movie.objects.filter(pk=525617).update(synopsis="Explicitly reviewed enriched synopsis")
        self.movie.refresh_from_db()
        self.new = metadata_fingerprint(self.movie)

    def create_approval(self, **kwargs):
        options = dict(selection=self.selection, journal=self.journal,
                       expected_database="reccool_postgres_production", expected_host="external.example",
                       expected_metadata_sha256=self.old, approved_metadata_sha256=self.new,
                       reason="Reviewed importer enrichment", output=self.approval, stdout=StringIO())
        options.update(kwargs)
        with patch("core.management.commands.approve_protected_metadata.target_identity", return_value=self.identity):
            call_command("approve_protected_metadata", **options)

    def inspect(self, with_approval=True):
        options = dict(selection=str(self.selection), journal=str(self.journal),
                       expected_database="reccool_postgres_production", expected_host="external.example", stdout=StringIO())
        if with_approval:
            options["protected_metadata_approval"] = self.approval
        with patch("core.management.commands.adult_movie_cleanup.target_identity", return_value=self.identity):
            call_command("adult_movie_cleanup", "inspect", **options)

    def test_explicit_sidecar_allows_exact_metadata_only_and_keeps_existing_files(self):
        before = {p: digest_file(p) for p in self.root.iterdir()}
        with CaptureQueriesContext(connection) as queries:
            self.create_approval()
        self.assertTrue(all(q["sql"].lstrip().upper().startswith("SELECT") for q in queries))
        self.assertTrue(all(digest_file(path) == value for path, value in before.items()))
        self.inspect()
        self.assertTrue(all(digest_file(path) == value for path, value in before.items()))
        with self.assertRaisesMessage(CommandError, "protected Movie changed"):
            self.inspect(with_approval=False)
        with self.assertRaisesMessage(CommandError, "refusing to overwrite"):
            self.create_approval()

    def test_wrong_expected_or_current_hash_cannot_be_approved(self):
        for options in ({"expected_metadata_sha256": "0"*64}, {"approved_metadata_sha256": "0"*64}):
            with self.subTest(options=options), self.assertRaises(CommandError):
                self.create_approval(**options)
            self.assertFalse(self.approval.exists())

    def test_future_metadata_change_including_reverting_to_old_hash_is_rejected(self):
        self.create_approval()
        for text in ("Another later enrichment", "Old synopsis"):
            Movie.objects.filter(pk=525617).update(synopsis=text)
            with self.assertRaisesMessage(CommandError, "protected Movie changed"):
                self.inspect()

    def test_all_primary_identity_changes_are_rejected(self):
        self.create_approval()
        for field, value in (("type", "series"), ("tmdb_id", 320832), ("title_english", "Other movie"), ("release_year", 2009)):
            with self.subTest(field=field):
                Movie.objects.filter(pk=525617).update(**{field: value})
                with self.assertRaisesMessage(CommandError, "protected Movie changed"):
                    self.inspect()
                Movie.objects.filter(pk=525617).update(**{field: IDENTITY[field]})

    def test_other_seven_protected_remain_strict(self):
        self.create_approval()
        Movie.objects.filter(pk=self.others[0].pk).update(synopsis="Changed unrelated protected metadata")
        with self.assertRaisesMessage(CommandError, "protected Movie changed"):
            self.inspect()

    def test_selected_record_validation_still_rejects_changes(self):
        self.create_approval()
        Movie.objects.create(id=10000, title_english="Unauthorized selected change", type="movie", tmdb_id=3000, release_year=2020)
        with patch("core.management.commands.adult_movie_cleanup.target_identity", return_value=self.identity):
            with self.assertRaisesMessage(CommandError, "differs from the approved selection"):
                call_command("adult_movie_cleanup", "delete", selection=str(self.selection), journal=str(self.journal),
                             expected_database="reccool_postgres_production", expected_host="external.example",
                             protected_metadata_approval=self.approval, dry_run=True, stdout=StringIO())

    def test_approval_binds_original_selection_and_journal_but_allows_progress(self):
        self.create_approval()
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE movies SET status='prepared' WHERE id=10000")
            db.commit()
        load_approval(self.approval, self.proof, self.selection, self.journal, self.identity)
        self.selection.write_text(self.selection.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        with self.assertRaisesMessage(CommandError, "selection checksum"):
            load_approval(self.approval, self.proof, self.selection, self.journal, self.identity)

    def test_different_journal_binding_is_rejected(self):
        self.create_approval()
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE metadata SET value='another' WHERE key='storage'")
            db.commit()
        with self.assertRaisesMessage(CommandError, "journal binding"):
            load_approval(self.approval, self.proof, self.selection, self.journal, self.identity)

    def install_recoverable_baseline(self, title):
        baseline = {"title_spanish": "Power Rangers Jungle Fury", "synopsis": "", "synopsis_es": ""}
        self.old = metadata_fingerprint(baseline)
        self.proof["protected_movies"][0]["identity_metadata_sha256"] = self.old
        self.selection.write_text(json.dumps(self.proof), encoding="utf-8")
        with closing(sqlite3.connect(self.journal)) as db:
            db.execute("UPDATE metadata SET value=? WHERE key='selection'", (digest_file(self.selection),))
            db.commit()
        Movie.objects.filter(pk=525617).update(title_spanish=title, synopsis="Enriched synopsis", synopsis_es="")
        self.movie.refresh_from_db()
        self.new = metadata_fingerprint(self.movie)
        return baseline

    def test_recovered_empty_synopses_match_original_hash_exactly(self):
        baseline = self.install_recoverable_baseline("Power Rangers Jungle Fury")
        self.assertEqual(self.old, "56042e6d85ab8f40f85ef14e57e7c552961e814c71bfe537021db290eabfad76")
        self.assertEqual(recover_known_metadata(self.old), baseline)
        self.assertIsNone(recover_known_metadata("0"*64))
        self.create_approval()
        self.assertEqual(json.loads(self.approval.read_text(encoding="utf-8"))["expected_metadata"], baseline)
        self.inspect()

    def test_unexplained_spanish_title_change_cannot_be_approved_with_known_baseline(self):
        self.install_recoverable_baseline("Different Spanish title")
        with self.assertRaisesMessage(CommandError, "title_spanish changed"):
            self.create_approval()
        self.assertFalse(self.approval.exists())
