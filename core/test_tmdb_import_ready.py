"""Synthetic-only preparation tests. Preparation itself must never open a Django connection."""
from contextlib import ExitStack
import copy
import gzip
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection

from core import tmdb_import_ready as ready
from core import tmdb_importer
from core import test_tmdb_importer as fixtures
from core.test_tmdb_importer import record
from core.tmdb_enrichment_store import file_digest
from core.tmdb_enrichment_client import strict_json
from core.tmdb_normalizer import normalize_tmdb_metadata


class PreparationTests(unittest.TestCase):
    source_run = fixtures.PlanningTests.source_run

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="reccool-ready-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "enrichment"
        self.source.mkdir()
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")))
        self.stack.enter_context(patch.object(connection, "cursor", side_effect=AssertionError("Django DB forbidden")))

    def prepare(self, **options):
        return ready.prepare_run(self.source, self.root / "output", **options)

    def rows(self, manifest):
        with gzip.open(Path(manifest["run_directory"]) / "import_ready.jsonl.gz", "rt", encoding="utf-8") as stream:
            return [json.loads(line) for line in stream]

    def omitted(self, manifest):
        return [json.loads(line) for line in (Path(manifest["run_directory"]) / "omitted.jsonl").read_text(encoding="utf-8").splitlines()]

    def test_accepted_and_incomplete_are_combined_in_position_order_without_defaults(self):
        self.source_run([record(3), record(1, optional=False), record(2)])
        source_hashes = {path.name: file_digest(path) for path in self.source.iterdir()}
        result = self.prepare(batch_size=1)
        rows = self.rows(result)
        self.assertEqual([row["tmdb_id"] for row in rows], [1, 2, 3])
        self.assertEqual(result["counts"]["included"], 3)
        self.assertEqual(rows[0]["candidate"]["status"], "incomplete")
        self.assertEqual(rows[0]["movie_data"]["title_english"], "日本語")
        self.assertNotIn("title_spanish", rows[0]["movie_data"])
        self.assertNotIn("external_votes", rows[0]["movie_data"])
        self.assertNotIn("genre_key", rows[0]["movie_data"])
        self.assertNotIn("synopsis", rows[0]["movie_data"])
        self.assertEqual(rows[0]["candidate"]["normalization"]["sources"]["title_spanish"], "details.original")
        self.assertFalse(rows[0]["import_authorized"])
        self.assertTrue(ready.verify_ready_run(result["run_directory"])["complete"])
        self.assertEqual(source_hashes, {path.name: file_digest(path) for path in self.source.iterdir()})

    def test_adult_flags_and_conflicts_and_video_values(self):
        rows = [record(1, adult=True), record(2, adult=False), record(3, adult=None),
                record(4, adult=False), record(5, adult=True), record(6, video=None), record(7, video=True)]
        rows[2]["source"]["adult"] = ""
        rows[3]["source"]["adult"] = "True"
        rows[4]["source"]["adult"] = "False"
        rows[5]["tmdb_flags"].pop("video")
        self.source_run(rows)
        result = self.prepare()
        self.assertEqual(result["counts"]["excluded_adult"], 3)
        self.assertEqual(result["counts"]["pending_adult"], 1)
        self.assertEqual([row["tmdb_id"] for row in self.rows(result)], [2, 6, 7])
        self.assertTrue(any(row["adult"]["conflict"] for row in self.omitted(result)))

    def test_excluded_and_invalid_cannot_be_reintroduced(self):
        rows = [record(1, kind="series"), record(2, kind="series"), record(3, kind="series"), record(4)]
        for row, genre in zip(rows[:3], (10764, 10763, 10767)):
            row["normalization"] = normalize_tmdb_metadata("series", row["tmdb_id"], {"id": row["tmdb_id"], "name": "Series", "genre_ids": [genre]})
            row["status"] = "excluded"
            from core.tmdb_eligibility import evaluate_eligibility
            row["eligibility"] = evaluate_eligibility(row["normalization"])
        rows[3]["normalization"] = normalize_tmdb_metadata("movie", 4, {"id": 4})
        rows[3]["status"] = "invalid"
        rows[3]["eligibility"] = evaluate_eligibility(rows[3]["normalization"])
        self.source_run(rows)
        result = self.prepare()
        self.assertEqual(result["counts"]["excluded_genre"], 3)
        self.assertEqual(result["counts"]["invalid"], 1)
        self.assertEqual(self.rows(result), [])

    def test_legacy_conversion_requires_explicit_option_and_preserves_original(self):
        self.source_run([record(1, optional=False)], version=1)
        with self.assertRaisesRegex(ValueError, "convert-legacy"):
            self.prepare()
        result = self.prepare(convert_legacy=True)
        row = self.rows(result)[0]
        self.assertEqual(row["schema_version"], 1)
        self.assertEqual(row["candidate"]["schema_version"], 1)
        self.assertNotIn("eligibility", row["candidate"])
        self.assertTrue(row["eligibility"]["import"]["eligible"])
        self.assertEqual(row["trace"]["enrichment_schema_version"], 1)

    def test_offline_and_limited_require_explicit_options(self):
        self.source_run([record(1)], mode="offline", state="limited")
        with self.assertRaises(ValueError):
            self.prepare()
        with self.assertRaises(ValueError):
            self.prepare(allow_offline=True)
        result = self.prepare(allow_offline=True, allow_partial=True)
        self.assertEqual(result["counts"]["included"], 1)

    def test_duplicate_events_reported_but_duplicate_terminal_identity_blocks_publication(self):
        self.source_run([record(1)], duplicate_events=2)
        result = self.prepare()
        self.assertEqual(result["counts"]["duplicates"], 2)
        self.assertEqual(len(self.rows(result)), 1)
        self.assertEqual(len(self.omitted(result)), 2)
        self.source_run([record(1), record(1)])
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.prepare()
        failed = [read for path in (self.root / "output").iterdir()
                  if (read := json.loads((path / "preparation_manifest.json").read_text()))["state"] == "failed"]
        self.assertTrue(failed)
        self.assertFalse((Path(failed[-1]["run_directory"]) / "import_ready.jsonl.gz").exists())

    def test_corrupt_files_and_manifest_counts_never_publish(self):
        for corrupt_manifest in (True, False):
            self.source_run([record(1)])
            if corrupt_manifest:
                path = self.source / "manifest.json"
                manifest = json.loads(path.read_text())
                manifest["terminal_results"] = 99
                path.write_text(strict_json(manifest))
            else:
                (self.source / "accepted.jsonl.gz").write_bytes(b"corrupt")
            with self.assertRaises(ValueError):
                self.prepare()
        self.assertFalse(list((self.root / "output").glob("*/import_ready.jsonl.gz")))

    def test_determinism_unicode_and_no_mutation(self):
        rows = [record(1, optional=False, original="日本語だけ"), record(2, optional=False, original="Titre français")]
        self.source_run(rows)
        before = copy.deepcopy(rows)
        first, second = self.prepare(), self.prepare()
        self.assertEqual(first["outputs"], second["outputs"])
        self.assertEqual(self.rows(first), self.rows(second))
        self.assertEqual(rows, before)

    def test_interruption_during_evaluation_can_resume_without_modifying_previous(self):
        self.source_run([record(1), record(2)])
        original = ready.evaluate_record
        calls = 0
        def interrupt(candidate, existing):
            nonlocal calls
            calls += 1
            if candidate["tmdb_id"] == 2:
                raise KeyboardInterrupt()
            return original(candidate, existing)
        with patch.object(ready, "evaluate_record", side_effect=interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.prepare(batch_size=1)
        previous = next((self.root / "output").iterdir())
        self.assertFalse((previous / "import_ready.jsonl.gz").exists())
        before = {path.name: file_digest(path) for path in previous.iterdir() if path.is_file()}
        result = self.prepare(resume=previous)
        self.assertEqual(len(self.rows(result)), 2)
        self.assertEqual(before, {path.name: file_digest(path) for path in previous.iterdir() if path.is_file()})

    def test_interrupted_writing_and_failed_publish_leave_no_final_ready(self):
        self.source_run([record(1)])
        with patch.object(ready.gzip.GzipFile, "write", side_effect=KeyboardInterrupt()):
            with self.assertRaises(KeyboardInterrupt):
                self.prepare()
        self.assertFalse(list((self.root / "output").glob("*/import_ready.jsonl.gz")))
        original = ready.os.replace
        def fail_publish(source, target):
            if Path(target).name == "import_ready.jsonl.gz":
                raise OSError("synthetic rename failure")
            return original(source, target)
        with patch.object(ready.os, "replace", side_effect=fail_publish):
            with self.assertRaises(OSError):
                self.prepare()
        self.assertFalse(list((self.root / "output").glob("*/import_ready.jsonl.gz")))

    def test_final_gzip_does_not_appear_until_verification_passes(self):
        self.source_run([record(1)])
        original = ready.check_contents
        observed = []
        def checked(path, omitted, manifest):
            if str(path).endswith(".part"):
                self.assertFalse((Path(path).parent / "import_ready.jsonl.gz").exists())
                observed.append(True)
            return original(path, omitted, manifest)
        with patch.object(ready, "check_contents", side_effect=checked):
            result = self.prepare()
        self.assertTrue(observed)
        self.assertEqual(result["state"], "completed")

    def test_ready_tamper_with_rehashed_file_still_rejected(self):
        self.source_run([record(1)])
        result = self.prepare()
        directory = Path(result["run_directory"])
        row = self.rows(result)[0]
        row["movie_data"]["title_english"] = "altered"
        path = directory / "import_ready.jsonl.gz"
        path.write_bytes(gzip.compress((strict_json(row) + "\n").encode(), mtime=0))
        result["outputs"]["ready"].update(bytes=path.stat().st_size, sha256=file_digest(path))
        (directory / "preparation_manifest.json").write_text(strict_json(result))
        with self.assertRaises(ValueError):
            ready.verify_ready_run(directory)

    def test_preparation_command_works_without_django_connection(self):
        self.source_run([record(1)])
        call_command("prepare_tmdb_import", run_dir=str(self.source), output_dir=str(self.root / "output"), stdout=io.StringIO())
        directory = next((self.root / "output").iterdir())
        call_command("prepare_tmdb_import", verify=str(directory), stdout=io.StringIO())
        with self.assertRaises(CommandError):
            call_command("prepare_tmdb_import", run_dir=str(self.source), output_dir=str(ready.Path(__file__).resolve().parent))

    def test_equal_tmdb_id_across_types_is_not_a_duplicate(self):
        rows = [record(77), record(77, kind="series")]
        for index, row in enumerate(rows, 1):
            row["source"]["position"] = index
        self.source_run(rows)
        result = self.prepare()
        self.assertEqual([(row["type"], row["tmdb_id"]) for row in self.rows(result)], [("movie", 77), ("series", 77)])

    def test_legacy_unknown_adult_is_pending_not_included(self):
        row = record(1)
        row.pop("tmdb_flags")
        row["source"]["adult"] = ""
        self.source_run([row], version=1)
        result = self.prepare(convert_legacy=True)
        self.assertEqual(result["counts"]["pending_adult"], 1)
        self.assertEqual(self.rows(result), [])

    def test_output_cannot_be_created_inside_original_source(self):
        self.source_run([record(1)])
        before = {path.name for path in self.source.iterdir()}
        with self.assertRaisesRegex(ValueError, "independent"):
            ready.prepare_run(self.source, self.source / "nested-output")
        self.assertEqual(before, {path.name for path in self.source.iterdir()})


class ReadyDryRunCompatibilityTests(unittest.TestCase):
    def test_prepared_file_uses_existing_grouped_identity_check_and_preserves_spanish_policy(self):
        helper = fixtures.PlanningTests()
        helper.setUp()
        self.addCleanup(helper.doCleanups)
        helper.source_run([record(1, optional=False), record(2), record(3)])
        prepared = ready.prepare_run(helper.source, helper.root / "prepared")
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO core_movie VALUES(1,'movie',2),(2,'movie',3),(3,'movie',3)")
        result = tmdb_importer.plan_import(prepared["run_directory"], helper.root / "plans", helper.target, helper.snapshot, batch_size=10)
        self.assertEqual(result["counts"]["proposed_new"], 1)
        self.assertEqual(result["counts"]["existing"], 1)
        self.assertEqual(result["counts"]["existing_duplicate"], 1)
        self.assertEqual(result["inserted"], 0)
        proposed = next(row for row in helper.decisions(result) if row["decision"] == "proposed_new")
        self.assertIsNone(proposed["proposed_fields"]["title_spanish"])
        self.assertEqual(proposed["proposed_fields"]["title_english"], "日本語")
