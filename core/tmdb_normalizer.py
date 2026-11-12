"""Pure, offline TMDb metadata normalization. No Django or network imports."""
from collections.abc import Mapping
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import re

INTERNAL_GENRES = frozenset(("Action", "Animation", "Comedy", "Documentary",
                             "Drama", "Horror", "Musical", "Sci-Fi"))

# Historical business rules supplied and approved by RecCool (previously SQL).
MOVIE_GENRES = {
    28: ("Action",), 12: ("Action",), 16: ("Animation",), 35: ("Comedy",),
    80: ("Drama", "Action"), 99: ("Documentary",), 18: ("Drama",),
    10751: ("Drama", "Comedy"), 14: ("Animation", "Sci-Fi"),
    36: ("Drama", "Documentary"), 27: ("Horror",), 10402: ("Musical",),
    9648: ("Drama",), 10749: ("Drama",), 878: ("Sci-Fi",),
    53: ("Drama", "Horror"), 10770: (), 10752: ("Action",), 37: ("Action",),
}
SERIES_GENRES = {
    10759: ("Action",), 16: ("Animation",), 35: ("Comedy",),
    80: ("Drama", "Action"), 99: ("Documentary",), 18: ("Drama",),
    10751: ("Drama", "Comedy"), 10762: ("Action", "Animation", "Comedy"),
    9648: ("Drama",), 10765: ("Animation", "Sci-Fi"), 10766: ("Drama",),
    10768: ("Drama", "Action"), 37: ("Action",),
    10763: (), 10767: (), 10764: (),
}


def normalize_tmdb_metadata(production_type, tmdb_id, details, *, language="en-US",
                            english=None, spanish=None, credits=None, genre_mapping=None):
    """Return a JSON-safe record using approved historical genre rules.

    Optional genre_mapping replaces equivalences, not exclusion rules; keys are
    (model type, TMDb genre ID), values are internal strings or sequences thereof.
    Never pass this result directly to an ORM. Check status as well as valid.
    """
    result = dict(data={}, errors=[], warnings=[], missing_fields=[],
                  field_states={}, sources={}, update_data={}, valid=False,
                  status="invalid", exclusion_reasons=[], requires_review=False,
                  genre_candidates=[], genre_selection=None, director_entries=[])
    data = result["data"]

    def issue(bucket, field, code, **context):
        result[bucket].append(dict(field=field, code=code, **context))

    def put(field, value, state="valid", source="details"):
        data[field] = value
        result["field_states"][field] = state
        result["sources"][field] = source
        if state == "missing":
            result["missing_fields"].append(field)

    def text(value):
        return value.strip() if isinstance(value, str) else ""

    def positive(value):
        return type(value) is int and 0 < value <= 2147483647

    if production_type not in ("movie", "series"):
        issue("errors", "type", "invalid_type")
    if not positive(tmdb_id):
        issue("errors", "tmdb_id", "invalid_id")
    if result["errors"]:
        return result
    if not isinstance(details, Mapping):
        issue("errors", "details", "invalid_payload")
        return result
    is_movie = production_type == "movie"
    title_key, original_key, date_key = (("title", "original_title", "release_date")
        if is_movie else ("name", "original_name", "first_air_date"))
    expected_media = "movie" if is_movie else "tv"
    payloads = [("details", details)]
    for label, payload in (("english", english), ("spanish", spanish)):
        if payload is not None:
            if not isinstance(payload, Mapping):
                issue("errors", label, "invalid_payload")
            else:
                payloads.append((label, payload))
    for label, payload in payloads:
        if not positive(payload.get("id")) or payload.get("id") != tmdb_id:
            issue("errors", label, "identity_mismatch")
        if (payload.get("media_type", expected_media) != expected_media or
                (title_key not in payload and ("name" if is_movie else "title") in payload)):
            issue("errors", label, "type_mismatch")
    put("type", production_type)
    put("tmdb_id", tmdb_id)
    localized = {"en": english, "es": spanish}
    if isinstance(language, str) and language.split("-")[0] in localized:
        localized[language.split("-")[0]] = localized[language.split("-")[0]] or details
    for lang, field in (("en", "title_english"), ("es", "title_spanish")):
        preferred = localized[lang] if isinstance(localized[lang], Mapping) else {}
        candidates = [(text(preferred.get(title_key)), lang)]
        candidates += [(text(p.get(original_key)), label + ".original") for label, p in payloads]
        candidates += [(text(p.get(title_key)), label + ".localized") for label, p in payloads]
        value, source = next(((v, s) for v, s in candidates if v), (None, "none"))
        put(field, value, "valid" if value else
            ("empty" if title_key in preferred or any(original_key in p or title_key in p
                                                      for _, p in payloads) else "missing"), source)
        if value and source != lang:
            issue("warnings", field, "title_fallback")
        if value and len(value) > 255:
            issue("errors", field, "max_length")
    if not data["title_english"]:
        issue("errors", "title_english", "required_title")
    for lang, field in (("en", "synopsis"), ("es", "synopsis_es")):
        payload = localized[lang] if isinstance(localized[lang], Mapping) else {}
        value = text(payload.get("overview"))
        put(field, value or None, "valid" if value else
            ("empty" if "overview" in payload else "missing"), lang)

    def raw_state(key):
        return "missing" if key not in details else "empty"

    raw_date = details.get(date_key)
    year = None
    state = raw_state(date_key)
    if raw_date:
        try:
            if not isinstance(raw_date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
                raise ValueError
            year = date.fromisoformat(raw_date).year
            state = "valid"
        except ValueError:
            state = "invalid"
            issue("warnings", "release_year", "invalid_date")
    put("release_year", year, state)
    path = details.get("poster_path")
    image, state = None, raw_state("poster_path")
    if path:
        if isinstance(path, str) and re.fullmatch(r"/[A-Za-z0-9_-]+\.(?:jpg|jpeg|png|webp)", path):
            image = "https://image.tmdb.org/t/p/w500" + path
            state = "valid"
        else:
            state = "invalid"
            issue("warnings", "image", "invalid_poster_path")
    put("image", image, state)

    genre_key = "genres" if "genres" in details else "genre_ids"
    genres = details.get(genre_key)
    categories = []
    genre_state = raw_state(genre_key)
    if genres is not None and not isinstance(genres, list):
        genre_state = "invalid"
        issue("warnings", "genre", "invalid_genres")
    elif isinstance(genres, list):
        genre_state = "valid" if genres else "empty"
        ids = {item.get("id") if isinstance(item, Mapping) else item
               for item in genres if type(item.get("id") if isinstance(item, Mapping) else item) is int}
        if not is_movie:
            if 10764 in ids:
                result["exclusion_reasons"].append("reality")
            elif ids and ids <= {10763, 10767} and all(
                    type(item.get("id") if isinstance(item, Mapping) else item) is int for item in genres):
                result["exclusion_reasons"].append("news_talk_only")
        for item in genres:
            gid = item.get("id") if isinstance(item, Mapping) else item
            mapping = MOVIE_GENRES if is_movie else SERIES_GENRES
            category = (mapping.get(gid) if genre_mapping is None else
                        genre_mapping.get((production_type, gid))) if type(gid) is int else None
            if isinstance(category, str):
                category = (category,)
            if not isinstance(category, (tuple, list)) or any(
                    not isinstance(c, str) or c not in INTERNAL_GENRES for c in category):
                issue("warnings", "genre", "unmapped_genre", genre_id=gid if type(gid) is int else None)
                genre_state = "partial"
            else:
                for name in category:
                    if name not in categories:
                        categories.append(name)
        if is_movie and ids == {10770} and all(
                (item.get("id") if isinstance(item, Mapping) else item) == 10770 for item in genres):
            categories = ["Drama"]
    result["genre_candidates"] = sorted(categories)
    value = ", ".join(sorted(categories)) or None
    put("genre", value, genre_state)
    if value and len(value) > 100:
        issue("errors", "genre", "max_length")

    credit_payload = credits if credits is not None else details.get("credits")
    if credit_payload is not None and not isinstance(credit_payload, Mapping):
        issue("errors", "credits", "invalid_payload")
        credit_payload = None
    if isinstance(credit_payload, Mapping) and "id" in credit_payload and (
            not positive(credit_payload["id"]) or credit_payload["id"] != tmdb_id):
        issue("errors", "credits", "identity_mismatch")
    credit_payload = credit_payload or {}

    def people(items, ordered=False):
        if not isinstance(items, list):
            issue("warnings", "credits", "invalid_people")
            return []
        items = [p for p in items if isinstance(p, Mapping)]
        if ordered:
            items = sorted(items, key=lambda p: p.get("order") if
                           type(p.get("order")) is int and p["order"] >= 0 else float("inf"))
        names, seen, seen_names = [], set(), set()
        for person in items:
            name = text(person.get("name"))
            key = ("id", person["id"]) if positive(person.get("id")) else ("name", name.casefold())
            if name and key not in seen and name.casefold() not in seen_names:
                seen.add(key)
                seen_names.add(name.casefold())
                names.append(name)
        return names

    crew = credit_payload.get("crew", [])
    directors = people([p for p in crew if isinstance(p, Mapping) and p.get("job") == "Director"]
                       if isinstance(crew, list) else [])
    result["creators"] = people(details.get("created_by", [])) if not is_movie else []
    director_source, role = "credits", "Director"
    if result["creators"] and not directors:
        directors = result["creators"]
        director_source, role = "details.created_by", "Creator"
    result["director_entries"] = [dict(name=name, job=role) for name in directors]
    for field, names, key in (("director", directors, "crew"),
                              ("cast_members", people(credit_payload.get("cast", []), True), "cast")):
        put(field, ", ".join(names) or None, "valid" if names else
            ("empty" if key in credit_payload else "missing"), "credits")
    result["sources"]["director"] = director_source
    if data["director"] and len(data["director"]) > 255:
        issue("errors", "director", "max_length")
    for key, field in (("vote_average", "external_rating"), ("vote_count", "external_votes")):
        raw = details.get(key)
        value, state = None, raw_state(key)
        if raw is not None and raw != "":
            try:
                if key == "vote_count":
                    if type(raw) is not int or not 0 <= raw <= 2147483647:
                        raise ValueError
                    value = raw
                else:
                    if isinstance(raw, bool) or not isinstance(raw, (int, float, Decimal)):
                        raise ValueError
                    number = Decimal(str(raw))
                    if not number.is_finite() or not 0 <= number <= 10:
                        raise ValueError
                    value = str(number.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP))
                state = "valid"
            except (ValueError, InvalidOperation):
                state = "invalid"
                issue("errors", field, "invalid_number")
        put(field, value, state)
    rating, votes = data["external_rating"], data["external_votes"]
    if (rating is None) != (votes is None) or (votes == 0 and rating not in (None, "0.0")):
        issue("warnings", "external_rating", "inconsistent_rating_votes")
    result["valid"] = not result["errors"]
    if result["valid"]:
        result["status"] = ("excluded" if result["exclusion_reasons"] else
            "incomplete" if result["warnings"] or result["requires_review"] or any(
                state != "valid" for state in result["field_states"].values()) else "accepted")
    if result["valid"] and result["status"] != "excluded":
        result["update_data"] = {k: v for k, v in data.items() if
            result["field_states"][k] == "valid" and v is not None and k not in ("type", "tmdb_id")}
        for field, source in (("title_english", "en"), ("title_spanish", "es")):
            if result["sources"][field] != source:
                result["update_data"].pop(field, None)
        if any(w["code"] == "inconsistent_rating_votes" for w in result["warnings"]):
            for field in ("external_rating", "external_votes"):
                result["update_data"].pop(field, None)
    return result
