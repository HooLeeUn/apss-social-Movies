"""Synthetic baseline/delta tests; no real database or account data."""
from copy import deepcopy
from decimal import Decimal
from unittest import TestCase

from django.core.management.base import CommandError

from core.movie_duplicate_preferences import (assert_reviewed_preferences,
    build_preference_review, canonical, metrics, project_preferences, verify_preference_review)


def preference(pk, field, value, scores):
    counts = {f"count_{score}": scores.count(score) for score in range(1, 11)}
    return {"id": pk, "user_id": 2, field: value, **metrics(counts)}


def state(*, drift=False):
    timestamp = "2026-11-18T12:00:00+00:00"
    ratings = [dict(id=pk, user_id=2, movie_id=movie_id, score=score,
                    created_at=timestamp, updated_at=timestamp, movie__type="movie",
                    movie__genre="Drama", movie__director=director)
               for pk, movie_id, score, director in ((160, 91, 8, "Target"),
                   (32, 92, 5, "Old A, Co A"), (97, 93, 7, "Old B, Co B"))]
    directors = [preference(1, "director", "Target", [8]),
                 preference(2, "director", "Old A" if drift else "Old A, Co A", [5]),
                 preference(3, "director", "Old B" if drift else "Old B, Co B", [7])]
    return {"users": {"2": {"ratings": ratings,
        "profile": {"id": 1, "user_id": 2, "ratings_count": 3, "last_updated_at": timestamp},
        "preferences": {
            "core.UserGenrePreference": [preference(4, "genre", "Drama", [5, 7, 8])],
            "core.UserTypePreference": [preference(5, "content_type", "movie", [5, 7, 8])],
            "core.UserDirectorPreference": directors,
        }}}}


class ReviewedPreferenceTests(TestCase):
    def test_consistent_initial_state_remains_strict(self):
        baseline = state()
        review = build_preference_review(baseline, [91])
        self.assertEqual(review["diagnosis"]["preexisting_unrelated_director_discrepancies"], [])
        assert_reviewed_preferences(baseline, review, [])
        projected, _ = project_preferences(review, [91])
        self.assertEqual(projected["users"]["2"]["profile"]["ratings_count"], 2)
        assert_reviewed_preferences(projected, review, [91])

    def test_four_historical_keys_are_reported_without_repairing_any_row(self):
        baseline = state(drift=True)
        original = deepcopy(baseline)
        review = build_preference_review(baseline, [91])
        differences = review["diagnosis"]["preexisting_unrelated_director_discrepancies"]
        self.assertEqual(len(differences), 4)
        self.assertTrue(all(not row["affected_by_deletion"] for row in differences))
        self.assertEqual(baseline, original)
        self.assertEqual(review["diagnosis"]["historical_rows_reconciled"], 0)
        verify_preference_review(review, [91])

    def test_only_the_deleted_rating_contributions_change(self):
        baseline = state(drift=True)
        review = build_preference_review(baseline, [91])
        after, _ = project_preferences(review, [91])
        user = after["users"]["2"]
        self.assertEqual(user["preferences"]["core.UserDirectorPreference"],
                         baseline["users"]["2"]["preferences"]["core.UserDirectorPreference"][1:])
        self.assertEqual(user["preferences"]["core.UserGenrePreference"][0]["count_8"], 0)
        self.assertEqual(user["preferences"]["core.UserGenrePreference"][0]["score"], "6.00")
        self.assertEqual(user["preferences"]["core.UserTypePreference"][0]["ratings_count"], 2)
        assert_reviewed_preferences(after, review, [91])

    def test_historical_drift_on_touched_director_is_blocked(self):
        baseline = state(drift=True)
        row = baseline["users"]["2"]["preferences"]["core.UserDirectorPreference"][0]
        row.update(metrics({f"count_{i}": 2 if i == 8 else 0 for i in range(1, 11)}))
        with self.assertRaisesRegex(CommandError, "deletion target"):
            build_preference_review(baseline, [91])

    def test_unrelated_genre_drift_is_not_whitelisted(self):
        baseline = state(drift=True)
        baseline["users"]["2"]["preferences"]["core.UserGenrePreference"].append(preference(9, "genre", "Comedy", [9]))
        with self.assertRaisesRegex(CommandError, "non-director"):
            build_preference_review(baseline, [91])

    def test_invalid_profile_or_distribution_metrics_block_initial_review(self):
        for field in ("profile", "distribution"):
            baseline = state(drift=True)
            if field == "profile":
                baseline["users"]["2"]["profile"]["ratings_count"] = 99
            else:
                baseline["users"]["2"]["preferences"]["core.UserDirectorPreference"][1]["score"] = "1.00"
            with self.assertRaises(CommandError):
                build_preference_review(baseline, [91])

    def test_changes_to_unrelated_rows_fail_even_if_internally_consistent(self):
        baseline = state(drift=True)
        review = build_preference_review(baseline, [91])
        after, _ = project_preferences(review, [91])
        row = after["users"]["2"]["preferences"]["core.UserDirectorPreference"][0]
        row.update(metrics({f"count_{i}": 2 if i == 5 else 0 for i in range(1, 11)}))
        with self.assertRaisesRegex(CommandError, "exact reviewed deletion delta"):
            assert_reviewed_preferences(after, review, [91])

    def test_new_zero_count_rows_or_deleted_unrelated_rows_are_also_blocked(self):
        review = build_preference_review(state(drift=True), [91])
        for action in ("create", "delete"):
            after, _ = project_preferences(review, [91])
            rows = after["users"]["2"]["preferences"]["core.UserDirectorPreference"]
            if action == "create":
                rows.append(preference(42, "director", "Unrelated empty row", []))
            else:
                rows.pop()
            with self.assertRaises(CommandError):
                assert_reviewed_preferences(after, review, [91])

    def test_wrong_related_delta_is_not_hidden_by_old_director_drift(self):
        review = build_preference_review(state(drift=True), [91])
        after, _ = project_preferences(review, [91])
        row = after["users"]["2"]["preferences"]["core.UserGenrePreference"][0]
        row.update(metrics({f"count_{i}": int(i in (5, 8)) for i in range(1, 11)}))
        with self.assertRaises(CommandError):
            assert_reviewed_preferences(after, review, [91])

    def test_an_unrelated_rating_metadata_change_invalidates_the_review(self):
        review = build_preference_review(state(drift=True), [91])
        after, _ = project_preferences(review, [91])
        after["users"]["2"]["ratings"][0]["movie__director"] = "Changed after review"
        with self.assertRaises(CommandError):
            assert_reviewed_preferences(after, review, [91])

    def test_policy_code_scope_and_report_are_pinned(self):
        review = build_preference_review(state(drift=True), [91])
        for key in ("policy", "rules_sha256", "removed_movie_ids", "diagnosis"):
            changed = deepcopy(review)
            changed[key] = "changed"
            with self.assertRaises(CommandError):
                verify_preference_review(changed, [91])
        with self.assertRaises(CommandError):
            project_preferences(review, [92])

    def test_two_group_projections_preserve_the_same_historical_residual(self):
        baseline = state(drift=True)
        # A second approved deletion has its own consistent director, not either
        # of the historical keys that must remain unchanged.
        extra = {**baseline["users"]["2"]["ratings"][0], "id": 161, "movie_id": 94, "score": 4}
        user = baseline["users"]["2"]
        user["ratings"].append(extra)
        user["profile"]["ratings_count"] = 4
        user["preferences"]["core.UserGenrePreference"] = [preference(4, "genre", "Drama", [4, 5, 7, 8])]
        user["preferences"]["core.UserTypePreference"] = [preference(5, "content_type", "movie", [4, 5, 7, 8])]
        user["preferences"]["core.UserDirectorPreference"][0] = preference(1, "director", "Target", [4, 8])
        review = build_preference_review(baseline, [91, 94])
        for removed in ([91], [91, 94]):
            after, _ = project_preferences(review, removed)
            assert_reviewed_preferences(after, review, removed)
            self.assertEqual(after["users"]["2"]["preferences"]["core.UserDirectorPreference"][-2:],
                             user["preferences"]["core.UserDirectorPreference"][1:])

    def test_timestamps_cannot_change_without_a_rating_removal_or_go_backwards(self):
        baseline = state(drift=True)
        review = build_preference_review(baseline, [91])
        changed = deepcopy(baseline)
        changed["users"]["2"]["profile"]["last_updated_at"] = "2026-11-18T13:00:00+00:00"
        with self.assertRaises(CommandError):
            assert_reviewed_preferences(changed, review, [])
        after, _ = project_preferences(review, [91])
        after["users"]["2"]["profile"]["last_updated_at"] = "2026-11-17T12:00:00+00:00"
        with self.assertRaises(CommandError):
            assert_reviewed_preferences(after, review, [91])

    def test_decimal_serialization_and_existing_model_rounding_are_preserved(self):
        self.assertEqual(canonical({"score": Decimal("8.00")}), {"score": "8.00"})
        self.assertEqual(metrics({f"count_{i}": 1 if i in (5, 7) else 0 for i in range(1, 11)})["score"], "6.00")
