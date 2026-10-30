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
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.adult_cleanup import digest_file
from core.management.commands.audit_adult_movies import FIELDS, database_identity
from core.models import Movie


class ManualAdultPreparationTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.audit = self.root / "render"
        self.local_audit = self.root / "local_audit"
        self.output = self.root / "second_local"
        self.audit.mkdir()
        self.local_audit.mkdir()
        self.identity = database_identity("default")
        self.movies = Movie.objects.bulk_create([
            Movie(type="movie" if i % 3 else "series", tmdb_id=1000 + i,
                  title_english=f"Manual fixture {i}", release_year=2000 + i % 20)
            for i in range(345)
        ])
        self.originals, self.reviewed = [], []
        self.review_fields = ["database_id", "type", "tmdb_id", "reccool_title_english", "reccool_release_year", "decision", "review_notes"]
        for i, movie in enumerate(self.movies):
            prod_id = movie.pk + 1000000  # Internal production IDs deliberately differ.
            self.originals.append(dict(database_id=prod_id, type=movie.type, tmdb_id=movie.tmdb_id,
                                       title_english=movie.title_english, release_year=movie.release_year))
            self.reviewed.append(dict(database_id=prod_id, type=movie.type, tmdb_id=movie.tmdb_id,
                                     reccool_title_english=movie.title_english, reccool_release_year=movie.release_year,
                                     decision="PENDIENTE", review_notes="ELIMINAR" if i < 337 else "CONSERVAR"))
        self.write_sources()
        for directory, identity, snapshots in (
            (self.audit, "render-cache-identity", self.originals),
            (self.local_audit, self.identity, [dict(database_id=m.pk, type=m.type, tmdb_id=m.tmdb_id,
                title_english=m.title_english, release_year=m.release_year) for m in self.movies]),
        ):
            with closing(sqlite3.connect(directory / "audit.sqlite3")) as db:
                db.executescript("""CREATE TABLE metadata(key TEXT,value TEXT);
                    CREATE TABLE candidates(database_id INTEGER,snapshot TEXT);
                    CREATE TABLE exports(type TEXT,id INTEGER);
                    CREATE TABLE responses(type TEXT,id INTEGER,status TEXT,evidence TEXT);""")
                db.execute("INSERT INTO metadata VALUES ('database',?)", (identity,))
                for row in snapshots:
                    db.execute("INSERT INTO candidates VALUES (?,?)", (row["database_id"], json.dumps(row)))
                    db.execute("INSERT INTO exports VALUES (?,?)", (row["type"], row["tmdb_id"]))
                    # API titles/dates intentionally disagree; manual approval is authoritative.
                    db.execute("INSERT INTO responses VALUES (?,?,'complete',?)", (row["type"], row["tmdb_id"], json.dumps({
                        "id": row["tmdb_id"], "adult": True, "title": "Another title", "release_date": "1900-01-01",
                    })))
                db.commit()
            (directory / "adult_movies_final_selection.json").write_text(json.dumps({"database": identity, "movies": []}), encoding="utf-8")

    def write_sources(self):
        for name, fields, rows, delimiter in (
            ("adult_movies_final_review.csv", FIELDS, self.originals, ","),
            ("adult_movies_review_enriched.csv", self.review_fields, self.reviewed, ";"),
        ):
            with (self.audit / name).open("w", encoding="utf-8-sig", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fields, delimiter=delimiter)
                writer.writeheader()
                writer.writerows(rows)

    def prepare(self, **kwargs):
        with patch("core.management.commands.prepare_manual_adult_cleanup.target_identity", return_value=self.identity):
            call_command("prepare_manual_adult_cleanup", target="local", audit_dir=self.audit,
                         local_audit_dir=self.local_audit, output_dir=self.output,
                         expected_database="socialapp", expected_host="127.0.0.1", stdout=StringIO(), **kwargs)

    def test_local_mapping_and_manual_decisions_are_preserved_without_writes_or_api(self):
        before = {p: digest_file(p) for p in self.root.rglob("*") if p.is_file()}
        with CaptureQueriesContext(connection) as queries, patch("core.management.commands.audit_adult_movies.get_tmdb_json") as api:
            self.prepare()
        api.assert_not_called()
        self.assertTrue(all(q["sql"].lstrip().upper().startswith("SELECT") for q in queries))
        self.assertTrue(all(digest_file(path) == digest for path, digest in before.items()))
        selection = json.loads((self.output / "adult_movies_final_selection.json").read_text(encoding="utf-8"))
        self.assertEqual([r["database_id"] for r in selection["movies"]], [m.pk for m in self.movies[:337]])
        self.assertEqual([r["database_id"] for r in selection["protected_movies"]], [m.pk for m in self.movies[337:]])
        self.assertEqual(selection["database"], self.identity)
        self.assertEqual(selection["manifest_sha256"], digest_file(self.output / "adult_movies_final_to_delete.csv"))
        self.prepare(validate_only=True)
        with self.assertRaisesMessage(CommandError, "already exists"):
            self.prepare()

    def test_invalid_empty_decision_and_duplicate_id_are_rejected(self):
        self.reviewed[0]["review_notes"] = ""
        self.write_sources()
        with self.assertRaisesMessage(CommandError, "Invalid manual decisions"):
            self.prepare()
        self.reviewed[0]["review_notes"] = "ELIMINAR"
        self.reviewed[1]["database_id"] = self.reviewed[0]["database_id"]
        self.write_sources()
        with self.assertRaisesMessage(CommandError, "Duplicate database_id"):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_changed_audited_identity_aborts_without_partial_files(self):
        Movie.objects.filter(pk=self.movies[0].pk).update(title_english="Changed after audit")
        with self.assertRaisesMessage(CommandError, "Identity changed"):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_ambiguous_local_pair_aborts(self):
        movie = self.movies[0]
        Movie.objects.create(type=movie.type, tmdb_id=movie.tmdb_id, title_english=movie.title_english, release_year=movie.release_year)
        with self.assertRaisesMessage(CommandError, "ambiguous"):
            self.prepare()

    def test_keep_identity_discrepancy_is_not_silently_accepted(self):
        self.reviewed[-1]["tmdb_id"] = 999999
        self.write_sources()
        with self.assertRaisesMessage(CommandError, "Reviewed identities differ"):
            self.prepare()
        self.assertFalse(self.output.exists())

    def test_explicit_retained_discrepancy_never_enters_selection_or_changes_decision(self):
        retained = self.reviewed[-1]
        retained["tmdb_id"] = 999999
        self.write_sources()
        before = digest_file(self.audit / "adult_movies_review_enriched.csv")
        self.prepare(retain_audit_identity=[int(retained["database_id"])])
        proof = json.loads((self.output / "adult_movies_final_selection.json").read_text(encoding="utf-8"))
        self.assertNotIn(self.movies[-1].pk, {row["database_id"] for row in proof["movies"]})
        protected = {row["database_id"]: row for row in proof["protected_movies"]}
        self.assertEqual(protected[self.movies[-1].pk]["tmdb_id"], self.movies[-1].tmdb_id)
        self.assertEqual(proof["manual_review"]["retained_identity_discrepancies"][0]["reviewed_tmdb_id"], 999999)
        self.assertEqual(before, digest_file(self.audit / "adult_movies_review_enriched.csv"))
        self.prepare(validate_only=True, retain_audit_identity=[int(retained["database_id"])])

    def test_approved_identity_discrepancy_cannot_be_overridden(self):
        self.reviewed[0]["tmdb_id"] = 999999
        self.write_sources()
        with self.assertRaisesMessage(CommandError, "Reviewed identities differ"):
            self.prepare(retain_audit_identity=[int(self.reviewed[0]["database_id"])])
        self.assertFalse(self.output.exists())

    def test_render_generation_rejects_local_connection_identity(self):
        with patch("core.management.commands.prepare_manual_adult_cleanup.target_identity", return_value=self.identity):
            with self.assertRaisesMessage(CommandError, "audited production database"):
                call_command("prepare_manual_adult_cleanup", target="render", audit_dir=self.audit,
                             output_dir=self.output, expected_database="reccool_postgres_production",
                             expected_host="external.example", stdout=StringIO())
        self.assertFalse(self.output.exists())

    def test_previous_duplicate_tmdb_pair_does_not_reselect_deleted_internal_id(self):
        row = dict(self.originals[0], database_id=999999999, title_english="Other historical duplicate")
        for directory, identity in ((self.audit, "render-cache-identity"), (self.local_audit, self.identity)):
            (directory / "adult_movies_final_selection.json").write_text(json.dumps({"database": identity, "movies": [row]}), encoding="utf-8")
        with closing(sqlite3.connect(self.local_audit / "audit.sqlite3")) as db:
            db.execute("INSERT INTO candidates VALUES (?,?)", (row["database_id"], json.dumps(row)))
            db.commit()
        self.prepare()
        proof = json.loads((self.output / "adult_movies_final_selection.json").read_text(encoding="utf-8"))
        self.assertNotIn(999999999, {r["database_id"] for r in proof["movies"]})
        self.assertEqual(len(proof["movies"]), 337)

    def test_render_generation_checks_actual_production_internal_ids(self):
        for source, reviewed, movie in zip(self.originals, self.reviewed, self.movies):
            source["database_id"] = reviewed["database_id"] = movie.pk
        self.write_sources()
        with closing(sqlite3.connect(self.audit / "audit.sqlite3")) as db:
            db.execute("DELETE FROM candidates")
            for row in self.originals:
                db.execute("INSERT INTO candidates VALUES (?,?)", (row["database_id"], json.dumps(row)))
            db.commit()
        with patch("core.management.commands.prepare_manual_adult_cleanup.target_identity", return_value="render-cache-identity"):
            call_command("prepare_manual_adult_cleanup", target="render", audit_dir=self.audit,
                         output_dir=self.output, expected_database="reccool_postgres_production",
                         expected_host="external.example", stdout=StringIO())
        proof = json.loads((self.output / "adult_movies_final_selection.json").read_text(encoding="utf-8"))
        self.assertEqual(proof["database"], "render-cache-identity")
        self.assertEqual(len(proof["movies"]), 337)
        self.assertEqual(len(proof["protected_movies"]), 8)

    def install_confirmed_correction(self, target):
        if target == "render":
            for source, reviewed, movie in zip(self.originals, self.reviewed, self.movies):
                source["database_id"] = reviewed["database_id"] = movie.pk
            with closing(sqlite3.connect(self.audit / "audit.sqlite3")) as db:
                db.execute("DELETE FROM candidates")
                for row in self.originals:
                    db.execute("INSERT INTO candidates VALUES (?,?)", (row["database_id"], json.dumps(row)))
                db.commit()
        old_pk = self.movies[-1].pk
        old_prod_pk = self.originals[-1]["database_id"]
        Movie.objects.filter(pk=old_pk).update(id=525617, type="series" if target == "local" else "movie",
                tmdb_id=812137, title_english="Power Rangers Jungle Fury", release_year=2008)
        self.movies[-1] = Movie.objects.get(pk=525617)
        original = dict(database_id=525617, type="series", tmdb_id=320832,
                        title_english="Power Rangers Jungle Fury", release_year=2008)
        self.originals[-1] = original
        self.reviewed[-1] = dict(database_id=525617, type="series", tmdb_id=812137,
                reccool_title_english="Power Rangers Jungle Fury", reccool_release_year=2008,
                decision="PENDIENTE", review_notes="CONSERVAR")
        self.write_sources()
        for directory, previous_pk in ((self.audit, old_prod_pk), (self.local_audit, old_pk)):
            with closing(sqlite3.connect(directory / "audit.sqlite3")) as db:
                db.execute("UPDATE candidates SET database_id=?,snapshot=? WHERE database_id=?", (525617, json.dumps(original), previous_pk))
                db.execute("INSERT INTO exports VALUES ('series',320832)")
                db.execute("INSERT INTO responses VALUES ('series',320832,'complete',?)", (json.dumps({"id": 320832, "adult": True}),))
                db.commit()

    def test_confirmed_correction_protects_local_series_without_changing_database(self):
        self.install_confirmed_correction("local")
        self.prepare(protect_corrected_525617=True)
        self.prepare(validate_only=True, protect_corrected_525617=True)
        proof = json.loads((self.output / "adult_movies_final_selection.json").read_text(encoding="utf-8"))
        self.assertEqual(len(proof["movies"]), 337)
        self.assertNotIn(525617, {row["database_id"] for row in proof["movies"]})
        protected = next(row for row in proof["protected_movies"] if row["database_id"] == 525617)
        self.assertEqual((protected["type"], protected["tmdb_id"]), ("series", 812137))
        self.assertEqual(Movie.objects.get(pk=525617).type, "series")

    def test_confirmed_correction_protects_render_movie(self):
        self.install_confirmed_correction("render")
        with patch("core.management.commands.prepare_manual_adult_cleanup.target_identity", return_value="render-cache-identity"):
            call_command("prepare_manual_adult_cleanup", target="render", audit_dir=self.audit,
                         output_dir=self.output, expected_database="reccool_postgres_production",
                         expected_host="external.example", protect_corrected_525617=True, stdout=StringIO())
        proof = json.loads((self.output / "adult_movies_final_selection.json").read_text(encoding="utf-8"))
        protected = next(row for row in proof["protected_movies"] if row["database_id"] == 525617)
        self.assertEqual((protected["type"], protected["tmdb_id"]), ("movie", 812137))
        self.assertNotIn(525617, {row["database_id"] for row in proof["movies"]})

    def test_correction_does_not_allow_unconfirmed_current_values(self):
        self.install_confirmed_correction("local")
        Movie.objects.filter(pk=525617).update(type="movie")
        with self.assertRaisesMessage(CommandError, "Identity changed"):
            self.prepare(protect_corrected_525617=True)
        self.assertFalse(self.output.exists())
