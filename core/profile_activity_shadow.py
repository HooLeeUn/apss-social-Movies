"""Read-only Phase E shadow comparison for profile activity."""

import hashlib
import json
import logging
import random
from time import perf_counter

from django.conf import settings

from .social_feed import SocialActivityFeedService


logger = logging.getLogger("core.profile_activity_shadow")

SAFE_FALLBACK_REASONS = {
    "max_batches", "max_candidate_rows", "hydrated_order_mismatch",
    "phase_d_scope_not_supported",
}


def _shape(value):
    """Describe payload structure without retaining values or PII."""
    if isinstance(value, dict):
        return {key: _shape(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [_shape(value[0])] if value else []
    return type(value).__name__


def safe_fingerprint(items):
    safe = [{
        "id": item.get("id"),
        "activity_type": item.get("activity_type"),
        "created_at": str(item.get("created_at")),
        "activity_at": str(item.get("activity_at")),
        "shape": _shape(item),
    } for item in items]
    return hashlib.sha256(json.dumps(safe, sort_keys=True).encode()).hexdigest()


def _safe_fallback_reason(reason):
    if reason is None or reason in SAFE_FALLBACK_REASONS:
        return reason
    return "candidate_error"


def report_profile_activity_shadow(metadata):
    """Sample a structured, PII-free record; kept separate for test patching."""
    rate = max(0.0, min(1.0, settings.PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE))
    if rate and random.random() < rate:
        logger.info(
            "PROFILE_ACTIVITY_SHADOW matched=%s count_matched=%s certified=%s "
            "fallback=%s fallback_reason=%s k=%s legacy_count=%s "
            "candidate_count=%s rows_inspected=%s logical_candidates=%s "
            "hydrated_rows=%s legacy_ms=%.3f candidate_ms=%.3f "
            "mismatch_reason=%s error=%s batches=%s legacy_fingerprint=%s "
            "candidate_fingerprint=%s",
            metadata["matched"], metadata["count_matched"],
            metadata["candidate_certified"], metadata["fallback_used"],
            metadata["fallback_reason"], metadata["k"],
            metadata["legacy_item_count"], metadata["candidate_item_count"],
            metadata["source_rows_inspected"],
            metadata["logical_candidates_inspected"], metadata["hydrated_rows"],
            metadata["duration_legacy_seconds"] * 1000,
            metadata["duration_candidate_seconds"] * 1000,
            metadata["mismatch_reason"], metadata["error_type"],
            metadata["batches_by_family"],
            metadata.get("legacy_fingerprint"),
            metadata.get("candidate_fingerprint"),
        )


def run_profile_activity_shadow(*, user, scope, legacy, k, legacy_duration):
    """Compare candidates and always swallow failures to protect legacy."""
    started = perf_counter()
    metadata = {
        "matched": False,
        "count_matched": False,
        "candidate_certified": False,
        "fallback_used": False,
        "fallback_reason": None,
        "legacy_item_count": len(legacy),
        "candidate_item_count": 0,
        "candidate_logical_count": None,
        "k": k,
        "batches_by_family": {},
        "source_rows_inspected": 0,
        "logical_candidates_inspected": 0,
        "hydrated_rows": 0,
        "duration_legacy_seconds": legacy_duration,
        "duration_candidate_seconds": 0.0,
        "mismatch_reason": None,
        "error_type": None,
    }
    try:
        candidate, adaptive = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=user, scope=scope, k=k, fallback_to_legacy=False,
            return_metadata=True,
        )
        metadata.update({
            "candidate_certified": adaptive["certified"],
            "fallback_used": adaptive["fallback_reason"] is not None,
            "fallback_reason": _safe_fallback_reason(adaptive["fallback_reason"]),
            "candidate_item_count": len(candidate),
            "batches_by_family": adaptive["batches_by_family"],
            "source_rows_inspected": adaptive["source_rows_inspected"],
            "logical_candidates_inspected": adaptive["logical_candidates_inspected"],
            "hydrated_rows": adaptive["hydrated_rows"],
        })
        expected = legacy[:k]
        metadata["matched"] = adaptive["certified"] and candidate == expected
        if not metadata["matched"]:
            metadata["mismatch_reason"] = (
                "candidate_uncertified" if not adaptive["certified"]
                else "length" if len(candidate) != len(expected)
                else "strict_payload_or_order"
            )
        metadata["legacy_fingerprint"] = safe_fingerprint(expected)
        metadata["candidate_fingerprint"] = safe_fingerprint(candidate)
        candidate_count = SocialActivityFeedService.count_feed_candidate_logical(
            user=user, scope=scope
        )
        metadata["candidate_logical_count"] = candidate_count
        metadata["count_matched"] = candidate_count == len(legacy)
        if not metadata["count_matched"]:
            metadata["mismatch_reason"] = metadata["mismatch_reason"] or "logical_count"
    except Exception as exc:  # Shadow must never alter the legacy response.
        metadata["fallback_used"] = True
        metadata["fallback_reason"] = exc.__class__.__name__
        metadata["error_type"] = exc.__class__.__name__
        metadata["mismatch_reason"] = metadata["mismatch_reason"] or "shadow_error"
    finally:
        metadata["duration_candidate_seconds"] = perf_counter() - started
        report_profile_activity_shadow(metadata)
    return metadata
