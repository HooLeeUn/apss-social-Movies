"""Regression tests for JSON-key comparison, legacy review binding and receipts."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
from unittest import TestCase
from unittest.mock import patch

from django.core.management.base import CommandError

from core.management.commands.diagnose_local_movie_duplicates import json_text
from core.movie_duplicate_cleanup import load_receipts, validate_group_metadata
from core.movie_duplicate_cleanup_plan import choose_survivor
from core.movie_duplicate_resume import (LEGACY_RULES, LEGACY_COMPONENTS, ALIAS_AWARE_LEGACY_COMPONENTS, failure_report,
    preference_component_hashes, update_progress, verify_execution_preferences)
from core.movie_duplicate_preferences import build_preference_review
from core.test_local_movie_duplicates import movie
from core.test_movie_duplicate_preferences import state


class ResumeRegressionTests(TestCase):
    def test_mixed_length_ids_have_equal_json_values_but_unequal_old_json_strings(self):
        rows = [movie(77116), movie(340370)]
        computed = choose_survivor(rows)
        planned = json.loads(json_text(computed))
        self.assertNotEqual(json_text(computed), json_text(planned))
        self.assertEqual(validate_group_metadata(planned, rows), planned)
        self.assertEqual(validate_group_metadata(planned, list(reversed(rows))), planned)

    def test_metadata_and_membership_changes_still_fail(self):
        rows = [movie(77116), movie(340370)]
        planned = {"type": "movie", "tmdb_id": 78916, **json.loads(json_text(choose_survivor(rows)))}
        for changed in ([rows[0]], [*rows, movie(999999)], [{**rows[0], "title_english": "Different"}, rows[1]]):
            with self.assertRaises(CommandError) as caught:
                validate_group_metadata(planned, changed, index=74)
            self.assertEqual(caught.exception.cleanup_group["index"], 74)
            self.assertIn("expected_metadata_sha256", caught.exception.cleanup_details)

    def test_normalization_does_not_coerce_json_numeric_types_or_scores(self):
        rows = [movie(77116), movie(340370)]
        planned = {"type": "movie", "tmdb_id": 78916, **json.loads(json_text(choose_survivor(rows)))}
        key = next(iter(planned["scores"]))
        planned["scores"][key]["score"] = float(planned["scores"][key]["score"])
        with self.assertRaises(CommandError):
            validate_group_metadata(planned, rows)

    def test_unknown_or_semantically_changed_legacy_reviews_are_rejected(self):
        review = build_preference_review(state(drift=True), [91])
        self.assertEqual(verify_execution_preferences(review, [91]), "current")
        legacy = deepcopy(review)
        legacy["rules_sha256"] = LEGACY_RULES
        self.assertEqual(preference_component_hashes(), ALIAS_AWARE_LEGACY_COMPONENTS)
        self.assertEqual(verify_execution_preferences(legacy, [91]), "legacy-serialization-fix-v1")
        original = deepcopy(legacy)
        with patch("core.movie_duplicate_resume.preference_component_hashes", return_value=LEGACY_COMPONENTS):
            self.assertEqual(verify_execution_preferences(legacy, [91]), "legacy-serialization-fix-v1")
        self.assertEqual(legacy, original)  # Never rewrite the approved review.
        with patch("core.movie_duplicate_resume.preference_component_hashes", return_value={}):
            with self.assertRaises(CommandError):
                verify_execution_preferences(legacy, [91])
        legacy["rules_sha256"] = "f" * 64
        with self.assertRaises(CommandError):
            verify_execution_preferences(legacy, [91])

    def test_receipt_counts_and_sequence_are_validated_without_database_access(self):
        groups = [{"type": "movie", "tmdb_id": 1, "keep_id": 1, "remove_ids": [2], "dependencies": []}]
        receipt = {"state": "committed", "keep_id": 1, "remove_ids": [2],
            "deleted_by_model": {"core.Movie": 1}, **{key: "a" * 64 for key in
                ("survivor_sha256", "preference_scope_sha256", "preferences_after_sha256")}}
        with tempfile.TemporaryDirectory(prefix="resume-receipts-") as temporary:
            path = Path(temporary) / "group-000000.json"
            path.write_text(json_text(receipt), encoding="utf-8")
            self.assertEqual(load_receipts(path.parent, groups), {0: receipt})
            for changes in ({"deleted_by_model": {"core.Movie": 99}}, {"keep_id": 3},
                            {"deleted_by_model": {"core.Movie": True}}, {"state": "unknown"}):
                path.write_text(json_text({**receipt, **changes}), encoding="utf-8")
                with self.assertRaises(CommandError):
                    load_receipts(path.parent, groups)

    def test_failure_report_distinguishes_verified_progress_from_checkpoint_claims(self):
        groups = [{"type": "movie", "tmdb_id": i, "keep_id": i, "remove_ids": [i + 20]} for i in range(12)]
        receipts = {i: {"state": "committed", "deleted_by_model": {"core.Movie": 1}} for i in range(10)}
        progress = {}
        update_progress(progress, groups, receipts, set(), complete=False)
        self.assertIsNone(progress["committed_groups"])
        update_progress(progress, groups, receipts, set(receipts), complete=True)
        progress["active_group"] = {"index": 10}
        result = failure_report({"plan_sha256": "a"}, progress, CommandError("metadata mismatch"))
        self.assertEqual((result["committed_groups"], result["pending_groups"], result["removed_movies"]), (10, 2, 10))
        self.assertEqual(result["last_committed_group"]["index"], 9)
        self.assertEqual(result["failed_group"]["index"], 10)
        self.assertEqual(result["state"], "failed")
