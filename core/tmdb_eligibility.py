"""Approved minimum-data policy. Pure evaluation; never authorizes database actions."""

POLICY_VERSION = 1


def evaluate_eligibility(normalized):
    normalized = normalized or {}
    data = normalized.get("data", {})
    states = normalized.get("field_states", {})
    technical_valid = normalized.get("valid") is True and not normalized.get("errors", [])
    reasons = []
    if not technical_valid:
        reasons.append("technical_validation_failed")
    if data.get("type") not in ("movie", "series") or type(data.get("tmdb_id")) is not int or not 0 < data["tmdb_id"] <= 2147483647:
        reasons.append("invalid_identity")
    title = data.get("title_english")
    if not isinstance(title, str) or not title.strip() or len(title) > 255:
        reasons.append("no_model_compatible_primary_title")
    if normalized.get("exclusion_reasons") or normalized.get("status") == "excluded":
        reasons.append("explicit_exclusion")
    eligible = not reasons
    quality = {
        "has_genre": states.get("genre") == "valid" and bool(data.get("genre")),
        "has_poster": states.get("image") == "valid" and bool(data.get("image")),
        "has_synopsis": any(states.get(field) == "valid" and bool(data.get(field))
                            for field in ("synopsis", "synopsis_es")),
    }
    return {
        "policy_version": POLICY_VERSION,
        "technical_valid": technical_valid,
        "metadata_complete": bool(states) and all(value == "valid" for value in states.values()),
        "non_valid_fields": {key: value for key, value in states.items() if value != "valid"},
        "import": {"eligible": eligible, "blocking_reasons": reasons,
                   "authorized": False, "requires_destination_identity_check": True},
        "publication": {"eligible": eligible, "blocking_reasons": list(reasons),
                        "authorized": False, "quality_indicators": quality,
                        "editorial_review_flags": ["missing_" + key[4:] for key, value in quality.items() if not value],
                        "scope": "approved_minimum_metadata_only"},
    }
