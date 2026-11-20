"""Compatibility and progress for reviewed duplicate cleanup; no database writes."""
from collections import Counter
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from django.core.management.base import CommandError

from core.movie_duplicate_preferences import rules_digest, verify_preference_review

# The exact pre-fix reviewed implementation. Only cleanup validation/reporting
# changes are compatible: the preference implementation and its inputs must be
# byte-identical. Unknown rule digests never get an exemption.
LEGACY_RULES = "61815825840a7ffc31b3532a07d00a39ccb5b6bbad0aaae15ac870acd2ae8d69"
LEGACY_COMPONENTS = {
    "movie_duplicate_preferences.py": "ab1a2c4454c62e0033292e4d8a9fba61c81720c7550da8e2e97ca1a4a4b8b488",
    "services.py": "1bbb741b236adeb0fe73f3dcddff9179393b75b52a8d166d9e407e29f78f220d",
    "signals.py": "f7320b968832e2897eaf70de640b9af5c131a3ae10913471bb7b07f195ccc941",
    "models.py": "ee3f60b91a7b12217d3ab6fdb13242b7825ccf40f261a0e2a1ad96e06385d1bb",
}
# Exact reviewed alias-only variant: removal uses the owning database's atomic
# block. For default/local data the preference calculations and writes are
# identical. No other component is allowed to differ from the pinned legacy set.
ALIAS_AWARE_LEGACY_COMPONENTS = {
    **LEGACY_COMPONENTS,
    'services.py': '37467e4530c085439b7f92483ee650e5d17c92594de52d83d2c683d5ec9010a3',
}


def preference_component_hashes():
    return {name: hashlib.sha256((Path(__file__).parent / name).read_bytes()).hexdigest()
            for name in LEGACY_COMPONENTS}


def verify_execution_preferences(review, removed_ids):
    if isinstance(review, dict) and review.get("rules_sha256") == LEGACY_RULES and rules_digest() != LEGACY_RULES:
        if preference_component_hashes() not in (LEGACY_COMPONENTS, ALIAS_AWARE_LEGACY_COMPONENTS):
            raise CommandError("Legacy preference review is incompatible with changed preference/model/signal rules.")
        compatible = deepcopy(review)
        compatible["rules_sha256"] = rules_digest()
        verify_preference_review(compatible, removed_ids)
        return "legacy-serialization-fix-v1"
    verify_preference_review(review, removed_ids)
    return "current"


def load_review(directory, execution):
    directory = Path(directory)
    report = json.loads((directory / "execution_summary.json").read_text(encoding="utf-8"))
    if (report.get("state") != "validated" or report.get("mode") != "dry-run"
            or report.get("unsupported_dependencies") or any(report.get(key) != execution[key]
                for key in ("target", "plan_sha256", "source_manifest_sha256"))):
        raise CommandError("Review must be a successful dry-run for the exact plan and destination.")
    snapshot = (directory / "review_snapshot.json").read_bytes()
    review_digest = hashlib.sha256(snapshot).hexdigest()
    if report.get("review_sha256") != review_digest:
        raise CommandError("Review snapshot checksum mismatch.")
    return json.loads(snapshot), review_digest


def group_descriptor(index, group):
    return {"index": index, "type": group["type"], "tmdb_id": group["tmdb_id"],
            "keep_id": group["keep_id"], "remove_ids": group["remove_ids"]}


def update_progress(progress, groups, receipts, verified, *, complete):
    if progress is None:
        return
    removed = Counter()
    for index in verified:
        removed.update(receipts[index]["deleted_by_model"])
    progress.setdefault("checkpoint_committed_groups_at_start", sum(r["state"] == "committed" for r in receipts.values()))
    progress.update(groups=len(groups), remove_movies=sum(len(group["remove_ids"]) for group in groups),
        checkpoint_count=len(receipts),
        verified_groups_count=len(verified), progress_verification_complete=complete,
        committed_groups=len(verified) if complete else None,
        pending_groups=len(groups) - len(verified) if complete else None,
        removed_movies=removed.get("core.Movie", 0), deleted_by_model=dict(removed),
        last_committed_group=group_descriptor(max(verified), groups[max(verified)]) if verified else None,
        next_pending_group=next((group_descriptor(i, group) for i, group in enumerate(groups) if i not in verified), None)
            if complete else None)


def failure_report(execution, progress, error):
    return {**execution, **progress, "state": "failed", "error_type": type(error).__name__,
        **({"reason": str(error)} if isinstance(error, CommandError) else {}),
        "failed_group": getattr(error, "cleanup_group", progress.get("active_group")),
        "integrity_details": getattr(error, "cleanup_details", None),
        "recovery": "Revalidate checkpoints and PostgreSQL with verify_local_movie_duplicate_resume; never infer commits from this summary alone."}
