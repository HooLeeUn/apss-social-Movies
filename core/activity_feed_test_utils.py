"""Test-only semantic helpers for the legacy profile activity feed.

Nothing in the request path imports this module.  Phase B can use these
helpers to compare a candidate implementation with the legacy response.
"""

from copy import deepcopy
from urllib.parse import parse_qsl, urlsplit


# ``list.sort`` is stable.  These ranks describe the order in which the
# current build_feed() appends families before its reverse sort.  A larger
# rank therefore wins an otherwise total tie.  Received rows are removed by
# consolidation and their summaries are appended last, in first-seen order.
LEGACY_STABLE_FAMILY_RANK = {
    "rating": 7,
    "public_comment": 6,
    "public_comment_reaction": 5,
    "private_message": 4,
    "private_comment_reaction": 3,
    "video_reaction_created": 2,
    "video_reaction_given": 1,
    "video_reaction_received": 1,
    "comment_reactions_received_summary": 0,
    "video_reactions_received_summary": -1,
}

CREATION_TIMESTAMP_FAMILIES = frozenset({
    "public_comment",
    "private_message",
    "video_reaction_created",
})


def legacy_effective_timestamp(activity):
    """Mirror the current timestamp selection without touching production."""
    if activity["activity_type"] in CREATION_TIMESTAMP_FAMILIES:
        return activity["created_at"]
    return activity["activity_at"]


def legacy_characterization_sort_key(activity):
    """Expose timestamp, priority, entity id and stable family rank for tests."""
    return (
        legacy_effective_timestamp(activity),
        activity["_sort_activity_priority"],
        activity["_sort_entity_id"],
        LEGACY_STABLE_FAMILY_RANK[activity["activity_type"]],
    )


def _normalize_page_url(value):
    if value is None:
        return None
    parts = urlsplit(str(value))
    return {"path": parts.path, "query": list(parse_qsl(parts.query, keep_blank_values=True))}


def normalize_activity_response(response):
    """Return a domain-independent, lossless semantic pagination snapshot."""
    data = response.data if hasattr(response, "data") else response
    normalized = deepcopy(data)
    normalized["next"] = _normalize_page_url(normalized.get("next"))
    normalized["previous"] = _normalize_page_url(normalized.get("previous"))
    return normalized


def assert_activity_responses_equivalent(test_case, legacy_result, candidate_result):
    """Assert complete contract equality, including result order and payloads."""
    test_case.assertEqual(
        normalize_activity_response(legacy_result),
        normalize_activity_response(candidate_result),
    )
