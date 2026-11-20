"""Reviewed preference deltas for duplicate cleanup; never repairs historical rows."""
from collections import Counter
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

from django.core.management.base import CommandError

from core.management.commands.diagnose_local_movie_duplicates import json_text
from core.models import (MovieRating, UserDirectorPreference, UserGenrePreference,
                         UserTasteProfile, UserTypePreference)
from core.services import COUNT_FIELDS, get_movie_preference_targets

PREFERENCES = {UserGenrePreference: "genre", UserDirectorPreference: "director",
               UserTypePreference: "content_type"}
REVIEW_KEY = "__preference_review__"
POLICY = "reviewed-unrelated-director-drift-and-exact-deletion-delta-v1"


def canonical(value):
    return json.loads(json_text(value))


def rules_digest():
    root = Path(__file__).parent
    digest = hashlib.sha256()
    for name in ("movie_duplicate_preferences.py", "movie_duplicate_cleanup.py",
                 "services.py", "signals.py", "models.py"):
        digest.update(name.encode())
        digest.update((root / name).read_bytes())
    return digest.hexdigest()


def capture_preferences(user_ids):
    """Minimal technical inputs; no username, email or account profile fields."""
    users = {}
    for user_id in sorted(user_ids):
        users[str(user_id)] = {
            "profile": UserTasteProfile.objects.filter(user_id=user_id).values().first(),
            "ratings": list(MovieRating.objects.filter(user_id=user_id).order_by("pk").values(
                "id", "user_id", "movie_id", "score", "created_at", "updated_at",
                "movie__type", "movie__genre", "movie__director")),
            "preferences": {model._meta.label: list(model.objects.filter(user_id=user_id)
                .order_by(field).values()) for model, field in PREFERENCES.items()},
        }
    return canonical({"users": users})


def contributions(ratings):
    result = Counter()
    for rating in ratings:
        if type(rating["score"]) is not int or not 1 <= rating["score"] <= 10:
            raise CommandError("Preference audit found an invalid rating score.")
        movie = SimpleNamespace(type=rating["movie__type"], genre=rating["movie__genre"],
                                director=rating["movie__director"])
        for model, _, value in get_movie_preference_targets(movie):
            result[model._meta.label, value, f"count_{rating['score']}"] += 1
    return result


def metrics(counts):
    total = sum(counts.values())
    weighted = sum(index * counts[key] for index, key in enumerate(COUNT_FIELDS, 1))
    # Match PreferenceDistributionMixin.save(), including its existing rounding.
    score = Decimal(str(round(weighted / total, 2))) if total else Decimal(0)
    return {**counts, "ratings_count": total, "score": format(score, ".2f")}


def assert_internal_metrics(state):
    for user in state["users"].values():
        if user["profile"] is None or user["profile"]["ratings_count"] != len(user["ratings"]):
            raise CommandError("Taste profile disagrees with its ratings.")
        for rows in user["preferences"].values():
            for row in rows:
                expected = metrics({key: row[key] for key in COUNT_FIELDS})
                if (row["ratings_count"] != expected["ratings_count"] or
                        Decimal(row["score"]) != Decimal(expected["score"])):
                    raise CommandError("Preference total or score disagrees with its distribution.")


def build_preference_review(baseline, removed_movie_ids):
    """Only unrelated director distribution drift may be carried unchanged."""
    assert_internal_metrics(baseline)
    removed = set(removed_movie_ids)
    differences, impacts = [], []
    for user_id, user in baseline["users"].items():
        expected = contributions(user["ratings"])
        deleted_ratings = [row for row in user["ratings"] if row["movie_id"] in removed]
        delta = contributions(deleted_ratings)
        touched = {(label, value) for label, value, _ in delta}
        impacts.append({"user_id": int(user_id),
            "ratings": [{key: row[key] for key in ("id", "movie_id", "score")} for row in deleted_ratings],
            "profile_before": user["profile"]["ratings_count"],
            "profile_after": user["profile"]["ratings_count"] - len(deleted_ratings),
            "contributions_removed": [{"model": label, "value": value, "bucket": bucket, "count": count}
                for (label, value, bucket), count in sorted(delta.items())]})
        for model, field in PREFERENCES.items():
            label = model._meta.label
            actual = {row[field]: row for row in user["preferences"][label]}
            keys = set(actual) | {value for model_label, value, _ in expected if model_label == label}
            for value in sorted(keys):
                row = actual.get(value)
                wanted = {key: expected[label, value, key] for key in COUNT_FIELDS}
                counts = {key: row[key] if row else 0 for key in COUNT_FIELDS}
                if counts == wanted:
                    continue
                if model is not UserDirectorPreference or (label, value) in touched:
                    raise CommandError("Preference drift affects a deletion target or a non-director preference; cleanup blocked.")
                differences.append({"user_id": int(user_id), "model": label, "value": value,
                    "preference_id": row["id"] if row else None,
                    "actual": metrics(counts) if row else None, "expected": metrics(wanted),
                    "expected_row_exists": bool(sum(wanted.values())),
                    "affected_by_deletion": False, "policy": "preserve_exactly"})
    return {"policy": POLICY, "rules_sha256": rules_digest(),
            "removed_movie_ids": sorted(removed), "baseline": baseline,
            "diagnosis": {"preexisting_unrelated_director_discrepancies": differences,
                          "deletion_impacts": impacts, "historical_rows_reconciled": 0}}


def verify_preference_review(review, removed_movie_ids):
    if (not isinstance(review, dict) or review.get("policy") != POLICY or
            review.get("rules_sha256") != rules_digest() or
            review.get("removed_movie_ids") != sorted(set(removed_movie_ids))):
        raise CommandError("Preference review is missing or its rules/scope changed; generate a new dry-run.")
    rebuilt = build_preference_review(review["baseline"], removed_movie_ids)
    if rebuilt != review:
        raise CommandError("Preference review diagnosis does not match its baseline.")


def project_preferences(review, removed_movie_ids):
    """Subtract only approved rating contributions from the actual stored rows."""
    removed = set(removed_movie_ids)
    if not removed <= set(review["removed_movie_ids"]):
        raise CommandError("Preference projection exceeds the reviewed deletion scope.")
    result = deepcopy(review["baseline"])
    changed_profiles = set()
    for user_id, user in result["users"].items():
        deleted = [row for row in user["ratings"] if row["movie_id"] in removed]
        if not deleted:
            continue
        changed_profiles.add(user_id)
        user["ratings"] = [row for row in user["ratings"] if row["movie_id"] not in removed]
        user["profile"]["ratings_count"] -= len(deleted)
        delta = contributions(deleted)
        for model, field in PREFERENCES.items():
            label = model._meta.label
            rows = {row[field]: row for row in user["preferences"][label]}
            for model_label, value, bucket in delta:
                if model_label != label:
                    continue
                row = rows.get(value)
                if row is None or row[bucket] < delta[model_label, value, bucket]:
                    raise CommandError("Reviewed preference cannot supply the deleted rating's contribution.")
                row[bucket] -= delta[model_label, value, bucket]
            projected = []
            for value, row in rows.items():
                if any(model_label == label and key == value for model_label, key, _ in delta):
                    recalculated = metrics({key: row[key] for key in COUNT_FIELDS})
                    if not recalculated["ratings_count"]:
                        continue
                    row.update(recalculated)
                projected.append(row)
            user["preferences"][label] = projected
    return result, changed_profiles


def assert_reviewed_preferences(actual, review, removed_movie_ids):
    expected, changed_profiles = project_preferences(review, removed_movie_ids)
    assert_internal_metrics(actual)
    if set(actual["users"]) != set(expected["users"]):
        raise CommandError("Preference user scope changed.")
    for user_id, wanted in expected["users"].items():
        observed = deepcopy(actual["users"][user_id])
        wanted = deepcopy(wanted)
        if user_id in changed_profiles:
            before = wanted["profile"].pop("last_updated_at")
            after = observed["profile"].pop("last_updated_at")
            if not after or datetime.fromisoformat(after) < datetime.fromisoformat(before):
                raise CommandError("Taste profile timestamp moved backwards.")
        if observed != wanted:
            raise CommandError("Preference/rating state differs from the exact reviewed deletion delta; cleanup blocked.")
