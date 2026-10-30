import json
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.core.management import call_command, CommandError
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.adult_cleanup import digest_file, metadata_fingerprint
from core.management.commands.audit_adult_movies import FIELDS, database_identity
from core.models import Movie


class ProtectedMovieDiagnosisTests(TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(dir=Path(__file__).resolve().parent.parent / "tmdb_exports")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.selection = self.root / "selection.json"
        self.journal = self.root / "untouched.sqlite3"
        self.journal.write_bytes(b"Not a database: diagnostic must never open this file")
        self.movie = Movie.objects.create(title_english="Protected fixture", type="series", tmdb_id=812137,
                                          release_year=2008, synopsis="Current synopsis", title_spanish="Titulo")
        self.expected = {key: getattr(self.movie, "pk" if key == "database_id" else key) for key in FIELDS}
        self.expected["identity_metadata_sha256"] = metadata_fingerprint(self.movie)
        self.write_selection()

    def write_selection(self):
        self.selection.write_text(json.dumps({"version": 2, "database": database_identity("default"),
                                              "protected_movies": [self.expected]}), encoding="utf-8")

    def diagnose(self):
        output = StringIO()
        with patch("core.management.commands.diagnose_protected_movies.target_identity", return_value=database_identity("default")):
            call_command("diagnose_protected_movies", selection=self.selection, journal=str(self.journal),
                         expected_database="reccool_postgres_production", expected_host="external.example",
                         batch_size=25, stdout=output)
        return json.loads(output.getvalue())

    def test_unchanged_record_and_files_with_only_select_queries(self):
        before = {p: digest_file(p) for p in self.root.iterdir()}
        with CaptureQueriesContext(connection) as queries:
            report = self.diagnose()
        self.assertEqual(report["records_with_differences"], 0)
        self.assertEqual(report["checked"], 1)
        self.assertTrue(report["selection_unchanged"])
        self.assertFalse(report["journal_opened"])
        self.assertEqual(before, {p: digest_file(p) for p in self.root.iterdir()})
        self.assertEqual(len(queries), 1)
        self.assertTrue(queries[0]["sql"].lstrip().upper().startswith("SELECT"))

    def test_real_primary_value_difference_is_reported_without_cleanup_validation(self):
        self.expected["type"] = "movie"
        self.write_selection()
        with patch("core.management.commands.adult_movie_cleanup.Command.handle", side_effect=AssertionError("Must not call cleanup")) as cleanup:
            report = self.diagnose()
        cleanup.assert_not_called()
        difference = report["records"][0]["differences"][0]
        self.assertEqual(difference["field"], "type")
        self.assertEqual((difference["expected"], difference["actual"]), ("movie", "series"))
        self.assertEqual(difference["classification"], "field_value_difference")
        self.assertEqual(Movie.objects.get(pk=self.movie.pk).type, "series")

    def test_json_number_representation_is_identified(self):
        self.expected["release_year"] = "2008"
        self.write_selection()
        difference = self.diagnose()["records"][0]["differences"][0]
        self.assertEqual(difference["classification"], "comparison_representation")
        self.assertEqual(difference["expected_python_type"], "str")
        self.assertEqual(difference["actual_python_type"], "int")

    def test_hash_only_difference_does_not_invent_original_text_or_changed_field(self):
        self.expected["identity_metadata_sha256"] = "old-digest"
        self.write_selection()
        differences = self.diagnose()["records"][0]["differences"]
        self.assertEqual(differences[0]["classification"], "undetermined_hash_only")
        self.assertEqual({d["field"] for d in differences[1:]}, {"title_spanish", "synopsis", "synopsis_es"})
        self.assertTrue(all(d["classification"] == "expected_value_unavailable" and not d["expected_available"] for d in differences[1:]))
        self.assertEqual(next(d["actual"] for d in differences if d["field"] == "synopsis"), "Current synopsis")

    def test_missing_record_is_reported(self):
        self.expected["database_id"] = 999999999
        self.write_selection()
        self.assertEqual(self.diagnose()["records"][0]["differences"][0]["classification"], "record_missing")

    def test_wrong_database_fingerprint_is_rejected(self):
        self.selection.write_text(json.dumps({"database": "another", "protected_movies": [self.expected]}), encoding="utf-8")
        with self.assertRaisesMessage(CommandError, "database fingerprint"):
            self.diagnose()
