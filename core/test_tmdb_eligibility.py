"""Offline policy/contract coverage; no Django, PostgreSQL or live HTTP."""
import copy
import csv
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from core.tmdb_eligibility import evaluate_eligibility
from core.tmdb_normalizer import normalize_tmdb_metadata
from core.tmdb_enrich import CSV_FIELDS, FixtureClient, configuration, run_batch
from core.tmdb_enrich_verify import verify_run
from core.tmdb_enrichment_store import StorageError
from scripts.create_tmdb_enrichment_demo import fixture_pair


def normalize(pair):
    en, es = pair["english"], pair["spanish"]
    return normalize_tmdb_metadata("movie", en["id"], en, english=en, spanish=es,
                                   credits=en.get("credits"))


class EligibilityTests(unittest.TestCase):
    def test_titles_any_language_use_existing_fallback_and_provenance(self):
        cases = [
            ("English", "Español", "Original", "English", "en"),
            ("English", "", "", "English", "en"),
            ("", "Solo español", "", "Solo español", "spanish.localized"),
            ("", "", "日本語だけ", "日本語だけ", "details.original"),
            ("", "", "Titre français", "Titre français", "details.original"),
            ("", "", "Nur Deutsch", "Nur Deutsch", "details.original"),
        ]
        for en_title, es_title, original, expected, source in cases:
            with self.subTest(title=expected):
                pair = fixture_pair("movie", 12)
                for language, title in (("english", en_title), ("spanish", es_title)):
                    pair[language].update(title=title, original_title=original)
                before = copy.deepcopy(pair)
                result = normalize(pair)
                policy = evaluate_eligibility(result)
                self.assertEqual(result["data"]["title_english"], expected)
                self.assertEqual(result["sources"]["title_english"], source)
                self.assertTrue(policy["import"]["eligible"])
                self.assertTrue(policy["publication"]["eligible"])
                self.assertEqual(pair, before)
                self.assertEqual(result, normalize(pair))
                if source != "en":
                    self.assertIn({"field": "title_english", "code": "title_fallback"}, result["warnings"])
                    # update_data protects existing titles; creation must use data's primary title.
                    self.assertNotIn("title_english", result["update_data"])

    def test_no_empty_or_oversized_title_is_eligible(self):
        for title in (None, "", "   ", "X" * 256):
            with self.subTest(title=repr(title)):
                pair = fixture_pair("movie", 12)
                for payload in pair.values():
                    payload.update(title=title, original_title=title)
                result = normalize(pair)
                self.assertFalse(result["valid"])
                self.assertFalse(evaluate_eligibility(result)["import"]["eligible"])

    def test_optional_absences_keep_diagnostics_and_both_minimum_eligibilities(self):
        changes = ["synopsis_es", "synopsis", "director", "cast_members", "image",
                   "genre", "release_year", "ratings"]
        for missing in changes:
            with self.subTest(missing=missing):
                pair = fixture_pair("movie", 12)
                en, es = pair["english"], pair["spanish"]
                if missing in ("synopsis_es", "synopsis"):
                    pair["spanish" if missing.endswith("_es") else "english"]["overview"] = ""
                elif missing in ("director", "cast_members"):
                    en["credits"]["crew" if missing == "director" else "cast"] = []
                else:
                    for key in {"image": ["poster_path"], "genre": ["genres"],
                                "release_year": ["release_date"], "ratings": ["vote_average", "vote_count"]}[missing]:
                        en.pop(key)
                result = normalize(pair)
                policy = evaluate_eligibility(result)
                self.assertTrue(result["valid"])
                self.assertEqual(result["status"], "incomplete")
                self.assertFalse(policy["metadata_complete"])
                self.assertTrue(policy["non_valid_fields"])
                self.assertTrue(policy["import"]["eligible"])
                self.assertTrue(policy["publication"]["eligible"])
                self.assertFalse(policy["import"]["authorized"])
                self.assertFalse(policy["publication"]["authorized"])

    def test_title_only_import_and_publication_evaluation_are_distinct_from_quality(self):
        result = normalize_tmdb_metadata("movie", 12, {"id": 12, "original_title": "日本語"})
        policy = evaluate_eligibility(result)
        self.assertTrue(policy["import"]["eligible"])
        self.assertTrue(policy["publication"]["eligible"])
        self.assertEqual(policy["publication"]["quality_indicators"],
                         {"has_genre": False, "has_poster": False, "has_synopsis": False})
        self.assertEqual(len(policy["publication"]["editorial_review_flags"]), 3)
        self.assertNotIn("quality_indicators", policy["import"])

    def test_complete_and_partial_and_invalid_and_exclusions(self):
        complete = normalize(fixture_pair("movie", 12))
        self.assertTrue(evaluate_eligibility(complete)["metadata_complete"])
        pair = fixture_pair("movie", 12)
        pair["english"]["genres"].append({"id": 999999})
        partial = normalize(pair)
        self.assertEqual(partial["field_states"]["genre"], "partial")
        self.assertTrue(partial["warnings"])
        self.assertTrue(evaluate_eligibility(partial)["import"]["eligible"])
        for ids in ([10764, 18], [10763], [10767], [10763, 10767]):
            result = normalize_tmdb_metadata("series", 12, {"id": 12, "name": "Series", "genre_ids": ids})
            self.assertTrue(result["valid"])
            self.assertFalse(evaluate_eligibility(result)["import"]["eligible"])
            self.assertFalse(evaluate_eligibility(result)["publication"]["eligible"])
        for result in (None, normalize_tmdb_metadata("movie", 0, {}),
                       normalize_tmdb_metadata("movie", 12, {"id": 13, "title": "Wrong"})):
            self.assertFalse(evaluate_eligibility(result)["technical_valid"])
            self.assertFalse(evaluate_eligibility(result)["import"]["eligible"])

    def test_series_original_title_without_translation(self):
        result = normalize_tmdb_metadata("series", 12, {"id": 12, "original_name": "Série française"})
        self.assertEqual(result["data"]["title_english"], "Série française")
        self.assertTrue(evaluate_eligibility(result)["publication"]["eligible"])

    def test_synthetic_ten_candidate_pilot_keeps_classification_but_all_are_eligible(self):
        pairs = [fixture_pair("movie", identifier) for identifier in range(1, 11)]
        for index in (0, 1, 2, 4, 5, 9):
            pairs[index]["spanish"]["overview"] = ""
        pairs[0]["english"]["genres"] = []
        pairs[1]["english"]["credits"] = {"crew": [], "cast": []}
        pairs[2]["english"]["poster_path"] = None
        pairs[5]["english"]["credits"]["cast"] = []
        pairs[9]["english"]["poster_path"] = None
        pairs[9]["english"]["credits"]["cast"] = []
        results = [normalize(pair) for pair in pairs]
        self.assertEqual(sum(row["status"] == "accepted" for row in results), 4)
        self.assertEqual(sum(row["status"] == "incomplete" for row in results), 6)
        self.assertTrue(all(evaluate_eligibility(row)["import"]["eligible"] for row in results))
        self.assertTrue(all(evaluate_eligibility(row)["publication"]["eligible"] for row in results))


class EligibilityArtifactTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="reccool-eligibility-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixtures = self.root / "fixtures"
        self.fixtures.mkdir()
        self.input = self.root / "input.csv"
        with self.input.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for identifier in range(1, 4):
                writer.writerow(dict(type="movie", tmdb_id=identifier, original_title="日本語", popularity=0, adult=False, video=False))
                pair = fixture_pair("movie", identifier)
                pair["spanish"]["overview"] = ""
                (self.fixtures / f"movie_{identifier}.json").write_text(json.dumps(pair), encoding="utf-8")
        self.network = patch("socket.socket.connect", side_effect=AssertionError("Network forbidden"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def run_batch(self, **options):
        return run_batch(self.input, self.root / "run", FixtureClient(self.fixtures), **options)

    def test_limited_terminal_results_and_resume_keep_contract(self):
        first = self.run_batch(limit=2)
        self.assertEqual(first["state"], "limited")
        self.assertFalse(first["complete"])
        self.assertEqual(first["counts"]["pending"], 0)
        self.assertEqual(first["counts"]["incomplete"], 2)
        self.assertTrue(verify_run(self.root / "run")["verified"])
        final = self.run_batch(resume=True, limit=0)
        self.assertTrue(final["complete"])
        with gzip.open(self.root / "run/incomplete.jsonl.gz", "rt", encoding="utf-8") as stream:
            rows = [json.loads(line) for line in stream]
        self.assertEqual(len(rows), 3)
        for row in rows:
            self.assertEqual(row["schema_version"], 2)
            self.assertTrue(row["eligibility"]["import"]["eligible"])
            self.assertFalse(row["publication"]["auto_importable"])
        self.assertTrue(verify_run(self.root / "run")["verified"])

    def rewrite(self, transform):
        directory = self.root / "run"
        manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
        for name, entry in manifest["outputs"].items():
            path = directory / entry["file"]
            with gzip.open(path, "rt", encoding="utf-8") as stream:
                rows = [json.loads(line) for line in stream]
            for row in rows:
                transform(row)
            content = gzip.compress("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows).encode("utf-8"), mtime=0)
            path.write_bytes(content)
            entry.update(bytes=len(content), sha256=hashlib.sha256(content).hexdigest())
        return directory, manifest

    def test_verifier_rejects_tampered_evaluation_even_with_correct_file_hash(self):
        self.run_batch(limit=0)
        directory, manifest = self.rewrite(lambda row: row["eligibility"]["import"].update(authorized=True))
        (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Eligibility policy mismatch"):
            verify_run(directory)

    def test_legacy_schema_one_remains_verifiable(self):
        self.run_batch(limit=0)
        def legacy(row):
            row["schema_version"] = 1
            row.pop("eligibility")
        directory, manifest = self.rewrite(legacy)
        manifest["configuration"].pop("eligibility_policy_version")
        manifest["configuration"]["implementation_sha256"].pop("tmdb_eligibility.py")
        (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        self.assertTrue(verify_run(directory)["verified"])

    def test_semantic_policy_change_cannot_silently_resume(self):
        self.run_batch(limit=1)
        changed = configuration(FixtureClient(self.fixtures))
        changed["eligibility_policy_version"] = 99
        with patch("core.tmdb_enrich.configuration", return_value=changed):
            with self.assertRaises(StorageError):
                self.run_batch(resume=True, limit=0)
