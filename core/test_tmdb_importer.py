from contextlib import contextmanager, nullcontext
from concurrent.futures import ThreadPoolExecutor
import copy
import gzip
import hashlib
import io
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, DatabaseError
from django.test.utils import CaptureQueriesContext

from core import tmdb_importer as importer
from core.management.commands import import_tmdb_productions as command
from core.tmdb_enrich import configuration, result_envelope
from core.tmdb_enrichment_store import file_digest
from core.tmdb_enrichment_client import strict_json
from core.tmdb_enrich_verify import verify_run
from core.tmdb_normalizer import normalize_tmdb_metadata
from scripts.create_tmdb_enrichment_demo import fixture_pair


def record(identifier=12, *, adult=False, video=False, original="日本語", optional=True, kind="movie"):
    pair = fixture_pair(kind, identifier)
    en, es = pair["english"], pair["spanish"]
    key = "title" if kind == "movie" else "name"
    original_key = "original_title" if kind == "movie" else "original_name"
    if not optional:
        en = {"id": identifier, original_key: original}
        es = {"id": identifier, original_key: original}
    result = normalize_tmdb_metadata(kind, identifier, en, english=en, spanish=es, credits=en.get("credits"))
    config = configuration(type("Live", (), {"mode": "live"})())
    envelope = result_envelope({"type": kind, "tmdb_id": identifier,
        "source": {"position": identifier, "adult": "False", "video": "True", "original_title": original}},
        result["status"], normalized=result, config=config)
    envelope["tmdb_flags"] = {"adult": adult, "video": video}
    return envelope


class AdmissionTests(unittest.TestCase):
    def test_adult_resolution_truth_table_and_video_never_vetoes(self):
        for api, export, expected in [(True, "False", True), (False, "True", True),
                                      (None, "True", True), (False, "", False),
                                      (None, "False", False), (None, None, None),
                                      ("false", "", None), (0, 0, None)]:
            with self.subTest(api=api, export=export):
                item = record(adult=api)
                item["source"]["adult"] = export
                self.assertIs(importer.adult_decision(item)["resolved"], expected)
                for video in (True, False, None):
                    item["tmdb_flags"]["video"] = video
                    decision = importer.evaluate_record(item, None)
                    self.assertEqual(decision["decision"], "excluded_adult" if expected is True else
                                     "pending_adult" if expected is None else "proposed_new")
        item["tmdb_flags"] = None
        item["source"]["adult"] = None
        self.assertEqual(importer.evaluate_record(item, None)["decision"], "pending_adult")

    def test_missing_optional_data_unicode_defaults_and_no_database_access(self):
        with patch.object(connection, "cursor", side_effect=AssertionError("query")):
            result = importer.evaluate_record(record(optional=False), None)
        self.assertEqual(result["decision"], "proposed_new")
        values = result["proposed_fields"]
        self.assertEqual(values["title_english"], "日本語")
        self.assertEqual(values["synopsis"], "")
        self.assertEqual(values["external_votes"], 0)
        for name in ("director", "cast_members", "genre", "image", "external_rating", "release_year", "synopsis_es"):
            self.assertIsNone(values[name])
        self.assertEqual(result["source_status"], "incomplete")
        self.assertFalse(result["inserted"])

    def test_derived_fields_use_model_helpers(self):
        item = record()
        item["normalization"]["data"].update(title_english="Árbol   日本", director="Ána", genre="Drama, Comedy")
        result = importer.evaluate_record(item, None)
        self.assertEqual(result["proposed_fields"]["genre_key"], "Comedy|Drama")
        self.assertEqual(result["proposed_fields"]["title_english_search"], "arbol 日本")
        self.assertEqual(result["proposed_fields"]["director_search"], "ana")
        self.assertNotIn("search_vector", result["proposed_fields"])

    def test_model_projection_rejects_incompatible_values_and_never_extra_fields(self):
        for name, value in [("title_english", "X" * 256), ("director", "X" * 256),
                            ("external_votes", True), ("external_rating", "NaN"),
                            ("image", "not a URL"), ("genre", "Invented")]:
            with self.subTest(field=name):
                item = record()
                item["normalization"]["data"][name] = value
                self.assertEqual(importer.evaluate_record(item, None)["decision"], "validation_error")
        item = record()
        item["normalization"]["data"]["id"] = 999
        self.assertNotIn("id", importer.evaluate_record(item, None)["proposed_fields"])

    def test_existing_and_duplicate_existing_are_skipped_without_updates(self):
        for count, status in [(1, "existing"), (3, "existing_duplicate")]:
            self.assertEqual(importer.evaluate_record(record(), {"count": count, "first_id": 1})["decision"], status)

    def test_identity_and_exclusions_and_titles(self):
        for title in ("Solo español", "Titre français", "日本語だけ"):
            self.assertEqual(importer.evaluate_record(record(optional=False, original=title), None)["decision"], "proposed_new")
        item = record()
        item["tmdb_id"] = 99
        self.assertEqual(importer.evaluate_record(item, None)["decision"], "validation_error")
        for genres in ([10764], [10763], [10767], [10763, 10767]):
            item = record(kind="series")
            item["normalization"] = normalize_tmdb_metadata("series", 12, {"id": 12, "name": "Series", "genre_ids": genres})
            item["status"] = item["normalization"]["status"]
            self.assertEqual(importer.evaluate_record(item, None)["decision"], "excluded_genre")
            item["tmdb_flags"]["adult"] = True
            self.assertEqual(importer.evaluate_record(item, None)["decision"], "excluded_adult")

    def test_diagnosis_is_deterministic_and_does_not_mutate_normalization(self):
        item = record(optional=False)
        original = copy.deepcopy(item)
        self.assertEqual(importer.evaluate_record(item, None), importer.evaluate_record(item, None))
        self.assertEqual(item, original)
        item["normalization"]["field_states"]["tmdb_id"] = "missing"
        self.assertEqual(importer.evaluate_record(item, None)["decision"], "validation_error")


class PlanningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="tmdb-import-synthetic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "source"
        self.source.mkdir()
        self.target = {"alias": "default", "host": "127.0.0.1", "port": "5432", "name": "isolated"}
        self.assertEqual(connection.vendor, "sqlite")
        self.assertEqual(connection.settings_dict["NAME"], ":memory:")
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA query_only=OFF")
            cursor.execute("CREATE TABLE core_movie(id INTEGER PRIMARY KEY,type TEXT,tmdb_id INTEGER)")
            cursor.execute("CREATE TABLE synthetic_social(id INTEGER PRIMARY KEY,value TEXT)")
            cursor.execute("INSERT INTO synthetic_social VALUES(1,'unchanged')")
        self.addCleanup(self.cleanup)

    def cleanup(self):
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA query_only=OFF")
            cursor.execute("DROP TABLE core_movie")
            cursor.execute("DROP TABLE synthetic_social")

    @contextmanager
    def snapshot(self):
        with connection.cursor() as cursor:
            cursor.execute("PRAGMA query_only=ON")
        yield

    def source_run(self, rows, *, version=2, mode="live", state="completed", duplicate_events=0):
        config = configuration(type("Client", (), {"mode": mode})())
        if version == 1:
            config.pop("eligibility_policy_version")
        outputs = {}
        counts = {name: 0 for name in ("accepted", "incomplete", "excluded", "invalid", "pending")}
        for row in rows:
            row["provenance"]["mode"] = mode
            row["schema_version"] = version
            if version == 1:
                row.pop("eligibility", None)
            counts[row["status"]] += 1
        for name in ("accepted", "incomplete", "excluded", "invalid", "pending", "duplicates"):
            selected = [row for row in rows if row["status"] == name]
            if name == "duplicates":
                selected = [{"code": "duplicate_input"} for _ in range(duplicate_events)]
            content = gzip.compress("".join(strict_json(row) + "\n" for row in selected).encode(), mtime=0)
            path = self.source / f"{name}.jsonl.gz"
            path.write_bytes(content)
            outputs[name] = {"file": path.name, "bytes": len(content), "sha256": file_digest(path), "records": len(selected)}
        manifest = {"schema_version": 1, "configuration": config, "counts": counts, "outputs": outputs,
                    "terminal_results": len(rows), "state": state, "complete": state == "completed", "input_exhausted": state == "completed",
                    "counters": {"duplicates": duplicate_events, "examined": len(rows) + duplicate_events}}
        (self.source / "manifest.json").write_text(strict_json(manifest), encoding="utf-8")

    def plan(self, **options):
        return importer.plan_import(self.source, self.root / "reports", self.target, self.snapshot, **options)

    def decisions(self, result):
        return [json.loads(line) for line in (Path(result["run_directory"]) / "decisions.jsonl").read_text(encoding="utf-8").splitlines()]

    def test_default_plan_real_selects_deduplicates_and_never_writes_target(self):
        self.source_run([record(1), record(2, kind="series"), record(3), record(4, optional=False)], duplicate_events=1)
        with connection.cursor() as cursor:
            cursor.execute("INSERT INTO core_movie VALUES(1,'movie',1),(2,'movie',3),(3,'movie',3),(4,'movie',2)")
        before = [(path.name, file_digest(path)) for path in self.source.iterdir()]
        with CaptureQueriesContext(connection) as queries, patch.object(importer.Movie, "save", side_effect=AssertionError("save")):
            result = self.plan(batch_size=2)
        selects = [query["sql"] for query in queries if query["sql"].startswith("SELECT")]
        self.assertEqual(len(selects), 2)
        self.assertTrue(all('"core_movie"' in sql for sql in selects))
        self.assertEqual(result["counts"]["existing"], 1)
        self.assertEqual(result["counts"]["existing_duplicate"], 1)
        self.assertEqual(result["counts"]["proposed_new"], 2)
        self.assertEqual(result["incomplete_acceptable"], 1)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual(result["source"]["duplicate_input_events"], 1)
        self.assertEqual(before, [(path.name, file_digest(path)) for path in self.source.iterdir()])
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM core_movie")
            self.assertEqual(cursor.fetchone()[0], 4)
            cursor.execute("SELECT value FROM synthetic_social")
            self.assertEqual(cursor.fetchone()[0], "unchanged")
            with self.assertRaises(DatabaseError):
                cursor.execute("UPDATE core_movie SET tmdb_id=999")

    def test_contract_v1_and_repeated_plans_have_same_decisions(self):
        self.source_run([record(1, optional=False)], version=1)
        first, second = self.plan(), self.plan()
        self.assertEqual(self.decisions(first), self.decisions(second))
        self.assertNotEqual(first["run_directory"], second["run_directory"])

    def test_offline_and_partial_inputs_require_opt_in(self):
        for mode, state, flags in [("offline", "completed", {"allow_offline": True}), ("live", "limited", {"allow_partial": True})]:
            self.source_run([record(1)], mode=mode, state=state)
            with self.assertRaises(ValueError):
                self.plan()
            self.assertEqual(self.plan(**flags)["counts"]["proposed_new"], 1)

    def test_corrupt_input_and_repeated_identity_fail_before_database_snapshot(self):
        self.source_run([record(1), record(1)])
        with patch.object(self, "snapshot", side_effect=AssertionError("DB connected")):
            with self.assertRaises(sqlite3.IntegrityError) as caught:
                self.plan()
            self.assertNotIsInstance(caught.exception, AssertionError)
        self.source_run([record(1)])
        (self.source / "accepted.jsonl.gz").write_bytes(b"corrupt")
        with patch.object(self, "snapshot", side_effect=AssertionError("DB connected")):
            with self.assertRaises(ValueError):
                self.plan()

    def test_deterministic_position_order_across_status_files_and_resume(self):
        self.source_run([record(3), record(1, optional=False), record(2)])
        first = self.plan(limit=1)
        self.assertEqual(first["state"], "limited")
        self.assertEqual(self.decisions(first)[0]["position"], 1)
        previous = Path(first["run_directory"])
        old = {p.name: file_digest(p) for p in previous.iterdir() if p.is_file()}
        final = self.plan(resume=previous)
        self.assertEqual([row["position"] for row in self.decisions(final)], [1, 2, 3])
        self.assertEqual(final["examined_this_invocation"], 2)
        self.assertTrue(final["complete"])
        self.assertEqual(old, {p.name: file_digest(p) for p in previous.iterdir() if p.is_file()})

    def test_interrupt_checkpoint_can_resume_without_repeating_confirmed_decisions(self):
        self.source_run([record(1), record(2), record(3)])
        original = importer.existing_identities
        calls = 0
        def stop(alias, records):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise KeyboardInterrupt()
            return original(alias, records)
        with patch.object(importer, "existing_identities", side_effect=stop):
            with self.assertRaises(KeyboardInterrupt):
                self.plan(batch_size=1)
        previous = next((self.root / "reports").iterdir())
        final = self.plan(resume=previous)
        self.assertEqual(final["examined"], 3)
        self.assertEqual(final["examined_this_invocation"], 2)

    def test_changed_target_source_or_checkpoint_rejects_resume(self):
        self.source_run([record(1), record(2)])
        first = self.plan(limit=1)
        self.target["name"] = "different"
        with self.assertRaises(ValueError):
            self.plan(resume=first["run_directory"])
        self.target["name"] = "isolated"
        (Path(first["run_directory"]) / "checkpoint.sqlite3").write_bytes(b"corrupt")
        with self.assertRaises(ValueError):
            self.plan(resume=first["run_directory"])

    def test_future_apply_and_remote_configuration_rejected_before_connection(self):
        with patch.object(command, "connections") as connections:
            with self.assertRaisesRegex(CommandError, "Apply is disabled"):
                call_command("import_tmdb_productions", run_dir=str(self.source), output_dir=str(self.root / "output"), apply=True,
                             confirm_database="socialapp", limit=10, stdout=io.StringIO())
            connections.__getitem__.assert_not_called()
        from core.test_local_movie_duplicates import LocalSafetyTests
        remote = LocalSafetyTests().local_connection(HOST="example.render.com")
        with patch.object(command, "connections", {"default": remote}):
            with self.assertRaises(CommandError):
                call_command("import_tmdb_productions", run_dir=str(self.source), output_dir=str(self.root / "output"))
            remote.get_autocommit.assert_not_called()

    def test_command_defaults_to_dry_run_and_writes_only_unique_report(self):
        self.source_run([record(1)])
        with patch.object(command, "require_local"), patch.object(command, "local_readonly_snapshot", side_effect=lambda alias: self.snapshot()):
            call_command("import_tmdb_productions", run_dir=str(self.source), output_dir=str(self.root / "reports"), stdout=io.StringIO())
        manifest = json.loads(next((self.root / "reports").glob("*/import_manifest.json")).read_text())
        self.assertEqual(manifest["mode"], "dry-run")
        self.assertFalse(manifest["apply_enabled"])

    def test_concurrent_planners_have_separate_journals_and_never_offer_apply(self):
        self.source_run([record(1), record(2)])
        # This tests concurrent planning/report isolation, not unimplemented PostgreSQL insert locks.
        with patch.object(importer, "existing_identities", return_value={}), ThreadPoolExecutor(max_workers=2) as workers:
            futures = [workers.submit(importer.plan_import, self.source, self.root / "reports", self.target, nullcontext)
                       for _ in range(2)]
            results = [future.result() for future in futures]
        self.assertNotEqual(results[0]["run_directory"], results[1]["run_directory"])
        self.assertEqual(self.decisions(results[0]), self.decisions(results[1]))
        self.assertTrue(all(not result["apply_enabled"] and result["inserted"] == 0 for result in results))

    def test_source_contract_mismatch_with_correct_gzip_hash_is_rejected(self):
        item = record(1)
        item["eligibility"]["import"]["authorized"] = True
        self.source_run([item])
        with patch.object(self, "snapshot", side_effect=AssertionError("target connected")):
            with self.assertRaises(ValueError):
                self.plan()

    def test_report_destination_and_batch_limits_fail_before_any_connection(self):
        with patch.object(command, "connections") as connections:
            for options in ({"batch_size": 501}, {"limit": 0}, {"output_dir": str(command.settings.BASE_DIR / "forbidden")}):
                with self.subTest(options=options):
                    args = {"run_dir": str(self.source), "output_dir": str(self.root / "reports"), **options}
                    with self.assertRaises(CommandError):
                        call_command("import_tmdb_productions", **args)
            connections.__getitem__.assert_not_called()
