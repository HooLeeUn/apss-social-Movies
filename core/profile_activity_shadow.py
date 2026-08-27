"""Read-only Phase E shadow comparison for profile activity."""

import hashlib
import json
import logging
import random
from time import perf_counter

from django.conf import settings
from django.db import connection

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


def report_candidate_profile(metadata):
    """Emit one compact numeric-only Phase F record for a sampled run."""
    profile = metadata.get("profile")
    if not profile:
        return
    logger.info(
        "PROFILE_ACTIVITY_CANDIDATE_PROFILE k=%s total_ms=%s queries=%s "
        "legacy_count=%s candidate_count=%s rows_inspected=%s "
        "logical_candidates=%s hydrated_rows=%s frontier_ms=%s "
        "certification_ms=%s hydration_ms=%s hydration_queries=%s "
        "hydration_sql_ms=%s hydration_python_ms=%s "
        "hydration_query_build_ms=%s hydration_sql_execute_ms=%s "
        "hydration_row_fetch_conversion_ms=%s hydration_model_materialization_ms=%s "
        "hydration_serialize_ms=%s hydration_wall_ms=%s hydration_cpu_ms=%s "
        "hydration_accounted_ms=%s hydration_unaccounted_ms=%s "
        "hydration_families=%s hydration_components=%s "
        "logical_count_ms=%s logical_count_queries=%s families=%s",
        metadata["k"], profile["total_ms"], profile["candidate_queries_total"],
        metadata["legacy_item_count"], metadata["candidate_item_count"],
        metadata["source_rows_inspected"], metadata["logical_candidates_inspected"],
        metadata["hydrated_rows"], profile["frontier_ms"],
        profile["certification_ms"], profile["hydration_ms"],
        profile["hydration_queries"], profile["hydration_sql_ms"],
        profile["hydration_python_ms"], profile["hydration_query_build_ms"],
        profile["hydration_sql_execute_ms"],
        profile["hydration_row_fetch_conversion_ms"],
        profile["hydration_model_materialization_ms"],
        profile["hydration_serialize_ms"], profile["hydration_wall_ms"],
        profile["hydration_cpu_ms"], profile["hydration_accounted_ms"],
        profile["hydration_unaccounted_ms"], profile["hydration_families"],
        profile["hydration_components"], profile["logical_count_ms"],
        profile["logical_count_queries"], profile["families"],
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
    profile_enabled = settings.PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED
    try:
        candidate, adaptive = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=user, scope=scope, k=k, fallback_to_legacy=False,
            return_metadata=True, profile_enabled=profile_enabled,
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
        if profile_enabled and "profile" in adaptive:
            metadata["profile"] = adaptive["profile"]
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
        if profile_enabled:
            query_count = 0
            def count_query(execute, sql, params, many, context):
                nonlocal query_count
                query_count += 1
                return execute(sql, params, many, context)
            count_started = perf_counter()
            with connection.execute_wrapper(count_query):
                candidate_count = SocialActivityFeedService.count_feed_candidate_logical(
                    user=user, scope=scope
                )
            metadata["profile"].update({
                "logical_count_ms": round((perf_counter() - count_started) * 1000, 3),
                "logical_count_queries": query_count,
            })
            metadata["profile"]["candidate_queries_total"] += query_count
        else:
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
        if profile_enabled and "profile" in metadata:
            rate = max(0.0, min(1.0, settings.PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE))
            if rate and random.random() < rate:
                report_candidate_profile(metadata)
    return metadata
