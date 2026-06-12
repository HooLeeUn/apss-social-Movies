from __future__ import annotations

from typing import Any

from django.core.cache import cache

from .models import Movie
from .tmdb import TMDbServiceError, get_tmdb_json

TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w185"
TMDB_WEB_BASE_URL = "https://www.themoviedb.org"
TMDB_CREDITS_CACHE_TIMEOUT = 60 * 60 * 24
TMDB_PERSON_CACHE_TIMEOUT = 60 * 60 * 24 * 7
TMDB_PERSON_DETAIL_CACHE_TIMEOUT = 60 * 60 * 24 * 7
TMDB_PERSON_EXTERNAL_IDS_CACHE_TIMEOUT = 60 * 60 * 24 * 7
TMDB_PERSON_PARTIAL_CACHE_TIMEOUT = 60 * 60
TMDB_PERSON_FAILURE_CACHE_TIMEOUT = 60 * 5
TMDB_PERSON_REQUEST_TIMEOUT = 4
TMDB_TV_DETAILS_CACHE_TIMEOUT = 60 * 60 * 24
TMDB_PERSON_DETAIL_LIMIT = 20

GENDER_LABELS = {
    0: "Not specified/Unknown",
    1: "Female",
    2: "Male",
    3: "Non-binary",
}

PERSON_DETAIL_FIELDS = {
    "profile_url": None,
    "known_for_department": "",
    "gender": {"code": 0, "label": GENDER_LABELS[0]},
    "birthday": None,
    "deathday": None,
    "place_of_birth": "",
    "facebook_url": None,
    "instagram_url": None,
    "x_url": None,
    "tmdb_url": None,
}


def get_movie_credits_payload(movie: Movie) -> dict[str, Any]:
    payload = build_empty_credits_payload(movie)
    if not movie.tmdb_id:
        return payload

    credits = get_cached_tmdb_credits(movie)
    return {
        **payload,
        "director": build_director_entries(movie, credits),
        "cast": build_cast_entries(credits),
    }


def get_person_payload(person_id: int) -> dict[str, Any]:
    return get_cached_person_payload(person_id)


def build_empty_credits_payload(movie: Movie) -> dict[str, Any]:
    return {
        "movie_id": movie.id,
        "tmdb_id": movie.tmdb_id,
        "type": get_tmdb_content_kind(movie),
        "director": [],
        "cast": [],
    }


def get_cached_tmdb_credits(movie: Movie) -> dict[str, Any]:
    content_kind = get_tmdb_content_kind(movie)
    cache_key = f"tmdb-credits:v1:{content_kind}:{movie.tmdb_id}"
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    tmdb_payload = get_tmdb_json(f"/{content_kind}/{movie.tmdb_id}/credits")
    cache.set(cache_key, tmdb_payload, TMDB_CREDITS_CACHE_TIMEOUT)
    return tmdb_payload


def get_cached_tv_details(tmdb_id: int) -> dict[str, Any]:
    cache_key = f"tmdb-tv-details:v1:{tmdb_id}"
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    tmdb_payload = get_tmdb_json(f"/tv/{tmdb_id}")
    cache.set(cache_key, tmdb_payload, TMDB_TV_DETAILS_CACHE_TIMEOUT)
    return tmdb_payload


def get_cached_person_payload(person_id: int) -> dict[str, Any]:
    cache_key = f"tmdb-person:v2:{person_id}"
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    try:
        details = get_cached_person_details(person_id)
    except TMDbServiceError:
        payload = build_minimal_person_payload(person_id)
        cache.set(cache_key, payload, TMDB_PERSON_FAILURE_CACHE_TIMEOUT)
        return payload

    external_ids_loaded = True
    try:
        external_ids = get_cached_person_external_ids(person_id)
    except TMDbServiceError:
        external_ids_loaded = False
        external_ids = {}

    payload = serialize_person_payload(details, external_ids)
    cache_timeout = (
        TMDB_PERSON_CACHE_TIMEOUT
        if external_ids_loaded
        else TMDB_PERSON_PARTIAL_CACHE_TIMEOUT
    )
    cache.set(cache_key, payload, cache_timeout)
    return payload


def get_cached_person_details(person_id: int) -> dict[str, Any]:
    cache_key = f"tmdb-person-details:v1:{person_id}"
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    tmdb_payload = get_tmdb_json(
        f"/person/{person_id}", timeout=TMDB_PERSON_REQUEST_TIMEOUT
    )
    cache.set(cache_key, tmdb_payload, TMDB_PERSON_DETAIL_CACHE_TIMEOUT)
    return tmdb_payload


def get_cached_person_external_ids(person_id: int) -> dict[str, Any]:
    cache_key = f"tmdb-person-external-ids:v1:{person_id}"
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    tmdb_payload = get_tmdb_json(
        f"/person/{person_id}/external_ids", timeout=TMDB_PERSON_REQUEST_TIMEOUT
    )
    cache.set(cache_key, tmdb_payload, TMDB_PERSON_EXTERNAL_IDS_CACHE_TIMEOUT)
    return tmdb_payload


def build_minimal_person_payload(person_id: int) -> dict[str, Any]:
    return serialize_person_payload({"id": person_id}, {})


def build_director_entries(
    movie: Movie, credits: dict[str, Any]
) -> list[dict[str, Any]]:
    crew = credits.get("crew", [])
    if not isinstance(crew, list):
        crew = []

    directors = [
        serialize_credit_person(person, role_key="job")
        for person in crew
        if isinstance(person, dict) and person.get("job") == "Director"
    ]
    if directors or movie.type != Movie.SERIES or not movie.tmdb_id:
        return directors

    try:
        tv_details = get_cached_tv_details(movie.tmdb_id)
    except TMDbServiceError:
        return []

    created_by = tv_details.get("created_by", [])
    if not isinstance(created_by, list):
        return []

    return [
        {
            **serialize_credit_person(person, role_key="job"),
            "job": person.get("job") or "Creator",
        }
        for person in created_by
        if isinstance(person, dict)
    ]


def build_cast_entries(credits: dict[str, Any]) -> list[dict[str, Any]]:
    cast = credits.get("cast", [])
    if not isinstance(cast, list):
        return []

    entries = [
        serialize_credit_person(person, role_key="character")
        for person in cast
        if isinstance(person, dict)
    ]
    return sorted(
        entries,
        key=lambda entry: (
            entry.get("order") if entry.get("order") is not None else 999999
        ),
    )


def serialize_credit_person(person: dict[str, Any], role_key: str) -> dict[str, Any]:
    profile_path = person.get("profile_path")
    return {
        "tmdb_person_id": person.get("id"),
        "name": person.get("name") or "",
        role_key: person.get(role_key) or "",
        "order": person.get("order") if role_key == "character" else None,
        "profile_url": build_profile_url(profile_path),
        "known_for_department": person.get("known_for_department") or "",
        "gender": serialize_gender(person.get("gender")),
        "birthday": None,
        "deathday": None,
        "place_of_birth": "",
        "facebook_url": None,
        "instagram_url": None,
        "x_url": None,
        "tmdb_url": build_tmdb_person_url(person.get("id")),
    }


def serialize_person_payload(
    details: dict[str, Any], external_ids: dict[str, Any]
) -> dict[str, Any]:
    person_id = details.get("id")
    return {
        "tmdb_person_id": person_id,
        "name": details.get("name") or "",
        "profile_url": build_profile_url(details.get("profile_path")),
        "known_for_department": details.get("known_for_department") or "",
        "gender": serialize_gender(details.get("gender")),
        "birthday": details.get("birthday") or None,
        "deathday": details.get("deathday") or None,
        "place_of_birth": details.get("place_of_birth") or "",
        "facebook_url": build_social_url("facebook", external_ids.get("facebook_id")),
        "instagram_url": build_social_url(
            "instagram", external_ids.get("instagram_id")
        ),
        "x_url": build_social_url("x", external_ids.get("twitter_id")),
        "tmdb_url": build_tmdb_person_url(person_id),
    }


def enrich_person_entry(
    person: dict[str, Any], details_by_person_id: dict[int, dict[str, Any]]
) -> dict[str, Any]:
    person_id = person.get("tmdb_person_id")
    details = details_by_person_id.get(person_id)
    if not details:
        return person

    enriched = {**person}
    for key in PERSON_DETAIL_FIELDS:
        enriched[key] = details.get(key)
    if details.get("name"):
        enriched["name"] = details["name"]
    return enriched


def get_tmdb_content_kind(movie: Movie) -> str:
    if movie.type == Movie.SERIES:
        return "tv"
    return "movie"


def build_profile_url(profile_path: str | None) -> str | None:
    if not profile_path:
        return None
    return f"{TMDB_IMAGE_BASE_URL}{profile_path}"


def build_tmdb_person_url(person_id: int | None) -> str | None:
    if not person_id:
        return None
    return f"{TMDB_WEB_BASE_URL}/person/{person_id}"


def build_social_url(provider: str, value: str | None) -> str | None:
    if not value:
        return None
    if provider == "facebook":
        return f"https://www.facebook.com/{value}"
    if provider == "instagram":
        return f"https://www.instagram.com/{value}"
    if provider == "x":
        return f"https://x.com/{value}"
    return None


def serialize_gender(value: Any) -> dict[str, Any]:
    try:
        code = int(value)
    except (TypeError, ValueError):
        code = 0
    return {"code": code, "label": GENDER_LABELS.get(code, GENDER_LABELS[0])}
