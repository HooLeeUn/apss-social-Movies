"""Pure metadata analysis and model introspection. No queries or model writes."""
import json
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit

from django.db.models import CASCADE, NOT_PROVIDED

from core.models import Movie


METADATA = (
    "title_english", "title_spanish", "release_year", "director", "genre",
    "synopsis", "synopsis_es", "image", "external_rating", "external_votes",
    "cast_members", "imdb_id", "trailer_es_key", "trailer_en_key",
)
AUDIT = ("created_at", "updated_at", "tmdb_lookup_status", "tmdb_lookup_checked_at",
         "trailer_checked_at")
WEIGHTS = {
    "title_english": 3, "title_spanish": 2, "release_year": 3, "director": 2,
    "genre": 2, "synopsis": 2, "synopsis_es": 2, "image": 2,
    "external_rating": 1, "external_votes": 1, "cast_members": 1,
    "imdb_id": 1, "trailer_es_key": 1, "trailer_en_key": 1,
}


def selected_fields():
    available = {field.name for field in Movie._meta.concrete_fields}
    return [name for name in ("id", "type", "tmdb_id", *METADATA, *AUDIT) if name in available]


def state(name, value):
    if value is None:
        return "null"
    if isinstance(value, str) and not value.strip():
        return "empty"
    field = Movie._meta.get_field(name)
    if field.default is not NOT_PROVIDED and not callable(field.default) and value == field.default:
        return "default"
    return "present"


def normalized(name, value):
    if isinstance(value, str):
        value = " ".join(value.split()).casefold()
        if name == "genre":
            return tuple(sorted({part.strip() for part in value.split(",") if part.strip()}))
    return value


def usable(name, value):
    if state(name, value) != "present":
        return False
    try:
        if name == "release_year":
            return type(value) is int and 1800 <= value <= 2200
        if name == "external_rating":
            rating = Decimal(str(value))
            return rating.is_finite() and 0 < rating <= 10
        if name == "external_votes":
            return type(value) is int and value > 0
        if name == "image":
            url = urlsplit(value.strip())
            return url.scheme in {"https", "http"} and bool(url.hostname)
    except (ValueError, InvalidOperation):
        return False
    return True


def analyze_group(records):
    fields = [name for name in METADATA if name in records[0]]
    conflicts = {}
    differences = []
    scores = {}
    details = {}
    any_issues = False
    for name in fields:
        values = {normalized(name, row[name]) for row in records if usable(name, row[name])}
        raw = {json.dumps(row[name], default=str, ensure_ascii=False) for row in records}
        if len(raw) > 1:
            differences.append(name)
        if len(values) > 1:
            conflicts[name] = [{"id": row["id"], "value": row[name]} for row in records]
    for row in records:
        states = {name: state(name, row[name]) for name in fields}
        issues = [f"invalid_{name}" for name in fields
                  if states[name] == "present" and not usable(name, row[name])]
        if "external_rating" in row and "external_votes" in row:
            if usable("external_rating", row["external_rating"]) != usable("external_votes", row["external_votes"]):
                issues.append("rating_votes_incomplete_pair")
        completeness = sum(WEIGHTS[name] for name in fields if usable(name, row[name]))
        # Penalize incoherent data before comparing completeness, never rating magnitude or recency.
        score = (len(issues), -completeness, row["id"])
        scores[row["id"]] = score
        default_matches = [name for name in fields if Movie._meta.get_field(name).default is not NOT_PROVIDED
                           and not callable(Movie._meta.get_field(name).default)
                           and row[name] == Movie._meta.get_field(name).default]
        details[row["id"]] = {"states": states, "default_matches": default_matches,
                              "issues": issues, "completeness": completeness}
        any_issues = any_issues or bool(issues)
    recommended = min(records, key=lambda row: scores[row["id"]])["id"]
    best = details[recommended]
    # Review if the recommendation would discard complementary valid information.
    best_row = next(row for row in records if row["id"] == recommended)
    complementary = [name for name in fields if not usable(name, best_row[name]) and
                     any(usable(name, row[name]) for row in records)]
    manual = bool(conflicts or any_issues or complementary)
    return {
        "recommended_id": recommended,
        "reason": (f"coherence_issues={len(best['issues'])}; completeness={best['completeness']}; "
                   "ordered by fewer coherence issues, greater weighted completeness, lowest id; "
                   + ("provisional: manual review required" if manual else "no unresolved conflict detected")),
        "manual_review_required": manual,
        "conflicts": conflicts, "different_fields": differences,
        "complementary_fields": complementary, "records": details,
    }


def technical_dependencies():
    """Inspect the Django registry, including transitive reverse cascades, without SQL."""
    edges = []
    visited = set()

    def walk(model):
        if model in visited:
            return
        visited.add(model)
        for rel in model._meta.get_fields(include_hidden=True):
            if not rel.auto_created or rel.concrete or not rel.is_relation:
                continue
            field = rel.field
            child = rel.related_model
            behavior = getattr(field.remote_field.on_delete, "__name__", str(field.remote_field.on_delete))
            edges.append({
                "parent": model._meta.label, "dependent": child._meta.label,
                "table": child._meta.db_table, "field": field.name,
                "relation": "one_to_one" if rel.one_to_one else "many_to_one",
                "on_delete": behavior, "nullable": field.null,
                "db_constraint": getattr(field, "db_constraint", None),
                "direct_movie_dependency": model is Movie,
                "constraints": [constraint.deconstruct() for constraint in child._meta.constraints],
                "unique_together": child._meta.unique_together,
            })
            if field.remote_field.on_delete is CASCADE:
                walk(child)

    walk(Movie)
    fields = [{"name": field.name, "type": field.get_internal_type(), "null": field.null,
               "primary_key": field.primary_key, "unique": field.unique,
               "db_index": field.db_index, "has_default": field.has_default(),
               "default": None if not field.has_default() or callable(field.default) else field.default}
              for field in Movie._meta.concrete_fields]
    return {
        "source": "Django model registry; not live schema introspection",
        "movie_model": {"table": Movie._meta.db_table, "fields": fields,
                        "type_choices": Movie.TYPE_CHOICES,
                        "constraints": [item.deconstruct() for item in Movie._meta.constraints],
                        "unique_together": Movie._meta.unique_together,
                        "indexes": [item.deconstruct() for item in Movie._meta.indexes]},
        "incoming_relations": sorted(edges, key=lambda edge: (edge["parent"], edge["dependent"], edge["field"])),
        "outgoing_relations": [{"field": field.name, "target": field.related_model._meta.label,
                                "on_delete": field.remote_field.on_delete.__name__,
                                "note": "Applies when referenced parent is deleted, not when Movie is deleted."}
                               for field in Movie._meta.fields if field.is_relation],
        "risks": [
            "Django CASCADE is collector behavior; raw SQL may be blocked by deferred database FKs, not cascade identically.",
            "MovieRating post_delete updates preferences and active feed caches via core.signals.",
            "MovieRecommendationItem pre_delete has origin-sensitive history behavior; preserve collector origin semantics.",
            "VideoComment.delete removes storage files, but queryset/collector cascades do not call each child delete override; orphan files are possible.",
            "ContentReport content_type/object_id logically references comments/videos without FK; reports can become dangling after cascades.",
            "UserTasteProfile/feed summaries, snapshot counts and caches can become stale; FK integrity alone does not ensure consistency.",
            "StreamingProviderLink has both movie FK and independent TMDb/IMDb identifiers; inspect technical links before any future cleanup.",
            "Repointing dependent rows can violate user/movie, snapshot/movie and other composite unique constraints.",
            "No dependent rows, users, counts of social interactions, live constraints, triggers or storage objects were queried.",
        ],
    }
