from __future__ import annotations

import logging
import unicodedata
from concurrent.futures import ThreadPoolExecutor
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
TMDB_SEASON_CAST_LIMIT = 5
TRUE_DETECTIVE_TMDB_ID = 46648
TMDB_SEASON_CREDITS_MAX_WORKERS = 6

logger = logging.getLogger(__name__)

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
        return build_local_credits_payload(movie, payload)

    credits = get_cached_tmdb_credits(movie)
    tv_details = (
        get_cached_tv_details(movie.tmdb_id) if movie.type == Movie.SERIES else None
    )
    cast = (
        build_series_cast_entries(movie, tv_details)
        if movie.type == Movie.SERIES
        else build_cast_entries(credits)
    )
    return {
        **payload,
        "director": build_director_entries(movie, credits, tv_details=tv_details),
        "cast": cast,
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


def build_local_credits_payload(
    movie: Movie, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    payload = payload or build_empty_credits_payload(movie)
    return {
        **payload,
        "director": [
            serialize_local_person(name, "job", "Director")
            for name in split_local_people(movie.director)
        ],
        "cast": [
            {**serialize_local_person(name, "character", ""), "order": index}
            for index, name in enumerate(split_local_people(movie.cast_members))
        ],
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


def get_cached_tv_season_credits(tmdb_id: int, season_number: int) -> dict[str, Any]:
    cache_key = f"tmdb-tv-season-credits:v1:{tmdb_id}:{season_number}"
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    tmdb_payload = get_tmdb_json(f"/tv/{tmdb_id}/season/{season_number}/credits")
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
    movie: Movie, credits: dict[str, Any], tv_details: dict[str, Any] | None = None
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

    if tv_details is None:
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


def build_series_cast_entries(
    movie: Movie, tv_details: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    if tv_details is None:
        tv_details = get_cached_tv_details(movie.tmdb_id)

    seasons = tv_details.get("seasons", [])
    if not isinstance(seasons, list):
        seasons = []
    season_numbers = [
        season.get("season_number")
        for season in seasons
        if isinstance(season, dict) and season.get("season_number") not in (None, 0)
    ]

    if movie.tmdb_id == TRUE_DETECTIVE_TMDB_ID:
        logger.info("True Detective TMDb seasons consulted: %s", season_numbers)

    entries_by_key: dict[tuple[str, Any], dict[str, Any]] = {}
    season_credits_by_number = get_tv_season_credits_payloads(
        movie.tmdb_id, season_numbers
    )
    for season_number in season_numbers:
        season_credits = season_credits_by_number.get(season_number, {})
        top_cast = build_cast_entries(season_credits)[:TMDB_SEASON_CAST_LIMIT]
        if movie.tmdb_id == TRUE_DETECTIVE_TMDB_ID:
            logger.info(
                "True Detective TMDb season %s top 5 before dedupe: %s",
                season_number,
                [
                    (person.get("tmdb_person_id"), person.get("name"))
                    for person in top_cast
                ],
            )
        for person in top_cast:
            key = build_cast_dedupe_key(person)
            if key not in entries_by_key:
                entries_by_key[key] = {
                    **person,
                    "seasons": [season_number],
                    "first_season": season_number,
                }
            elif season_number not in entries_by_key[key]["seasons"]:
                entries_by_key[key]["seasons"].append(season_number)

    final_cast = list(entries_by_key.values())
    if movie.tmdb_id == TRUE_DETECTIVE_TMDB_ID:
        logger.info(
            "True Detective TMDb final cast after dedupe: %s",
            [
                (
                    person.get("tmdb_person_id"),
                    person.get("name"),
                    person.get("seasons"),
                )
                for person in final_cast
            ],
        )
    return final_cast


def get_tv_season_credits_payloads(
    tmdb_id: int, season_numbers: list[int]
) -> dict[int, dict[str, Any]]:
    if not season_numbers:
        return {}

    max_workers = min(TMDB_SEASON_CREDITS_MAX_WORKERS, len(season_numbers))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return dict(
            zip(
                season_numbers,
                executor.map(
                    lambda season_number: get_cached_tv_season_credits(
                        tmdb_id, season_number
                    ),
                    season_numbers,
                ),
            )
        )


def build_cast_dedupe_key(person: dict[str, Any]) -> tuple[str, Any]:
    person_id = person.get("tmdb_person_id")
    if person_id:
        return ("id", person_id)
    return ("name", normalize_person_name(person.get("name")))


def normalize_person_name(name: Any) -> str:
    normalized = (
        unicodedata.normalize("NFKD", str(name or ""))
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return " ".join(normalized.casefold().split())


def split_local_people(value: str | None) -> list[str]:
    return [name.strip() for name in (value or "").split(",") if name.strip()]


def serialize_local_person(name: str, role_key: str, role_value: str) -> dict[str, Any]:
    return {
        "tmdb_person_id": None,
        "name": name,
        role_key: role_value,
        "order": None,
        "profile_url": None,
        "known_for_department": "",
        "gender": serialize_gender(None),
        "birthday": None,
        "deathday": None,
        "place_of_birth": "",
        "facebook_url": None,
        "instagram_url": None,
        "x_url": None,
        "tmdb_url": None,
    }


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
