from __future__ import annotations

from datetime import timedelta
from typing import Any

import requests
from django.conf import settings
from django.utils import timezone

from .models import Movie
from .tmdb import get_tmdb_json

YOUTUBE_EMBED_BASE_URL = "https://www.youtube.com/embed/"
YOUTUBE_WATCH_BASE_URL = "https://www.youtube.com/watch?v="
YOUTUBE_OEMBED_URL = "https://www.youtube.com/oembed"
TRAILER_NEGATIVE_CACHE_TTL = timedelta(days=30)
YOUTUBE_VALIDATION_TIMEOUT = 3
ENGLISH_COUNTRIES = {"US", "CA", "UK", "BZ"}


def language_for_country(country: str | None) -> str:
    normalized = (country or "").strip().upper()
    return "en" if normalized in ENGLISH_COUNTRIES else "es"


def build_trailer_payload(
    youtube_key: str | None,
    language: str | None,
    source: str,
    fallback_watch_key: str | None = None,
) -> dict[str, Any]:
    if not youtube_key:
        return {
            "trailer_url": None,
            "watch_url": f"{YOUTUBE_WATCH_BASE_URL}{fallback_watch_key}" if fallback_watch_key else None,
            "youtube_key": None,
            "language": None,
            "source": source,
            "available": False,
            "external_only": bool(fallback_watch_key),
        }

    return {
        "trailer_url": f"{YOUTUBE_EMBED_BASE_URL}{youtube_key}",
        "watch_url": f"{YOUTUBE_WATCH_BASE_URL}{youtube_key}",
        "youtube_key": youtube_key,
        "language": language,
        "source": source,
        "available": True,
        "external_only": False,
    }


def get_movie_trailer_payload(movie: Movie, country: str | None = None) -> dict[str, Any]:
    requested_language = language_for_country(country)
    cached_key = _get_cached_key(movie, requested_language)
    if cached_key and is_youtube_video_embeddable(cached_key):
        return build_trailer_payload(cached_key, requested_language, "cache")
    if cached_key:
        _clear_cached_key(movie, requested_language)

    fallback_key = _get_cached_key(movie, "en") if requested_language != "en" else None
    if fallback_key and is_youtube_video_embeddable(fallback_key):
        return build_trailer_payload(fallback_key, "en", "cache")
    if fallback_key:
        _clear_cached_key(movie, "en")

    if _has_recent_negative_cache(movie):
        return build_trailer_payload(None, None, "tmdb")

    if not movie.tmdb_id:
        return build_trailer_payload(None, None, "tmdb")

    content_kind = "tv" if movie.type == Movie.SERIES else "movie"
    tmdb_payload = get_tmdb_json(f"/{content_kind}/{movie.tmdb_id}/videos")
    videos = tmdb_payload.get("results", [])
    if not isinstance(videos, list):
        videos = []

    selected_language, youtube_key, external_watch_key = select_first_embeddable_trailer(videos, requested_language)
    movie.trailer_checked_at = timezone.now()
    update_fields = ["trailer_checked_at"]

    if youtube_key and selected_language == "es":
        movie.trailer_es_key = youtube_key
        update_fields.append("trailer_es_key")
    elif youtube_key and selected_language == "en":
        movie.trailer_en_key = youtube_key
        update_fields.append("trailer_en_key")

    movie.save(update_fields=update_fields)
    return build_trailer_payload(youtube_key, selected_language, "tmdb", external_watch_key)


def select_first_embeddable_trailer(
    videos: list[dict[str, Any]], requested_language: str
) -> tuple[str | None, str | None, str | None]:
    first_watch_key = None
    for language, candidate in iter_trailer_candidates(videos, requested_language):
        youtube_key = candidate.get("key")
        if not youtube_key:
            continue
        youtube_key = str(youtube_key)
        if first_watch_key is None:
            first_watch_key = youtube_key
        if is_youtube_video_embeddable(youtube_key):
            return language, youtube_key, None
    return None, None, first_watch_key


def iter_trailer_candidates(videos: list[dict[str, Any]], requested_language: str):
    languages = [requested_language]
    if requested_language != "en":
        languages.append("en")

    for language in languages:
        for candidate in _candidates_for_language(videos, language):
            yield language, candidate


def _candidates_for_language(videos: list[dict[str, Any]], language: str) -> list[dict[str, Any]]:
    candidates = [
        video for video in videos
        if isinstance(video, dict)
        and (video.get("site") or "").lower() == "youtube"
        and (video.get("type") or "").lower() == "trailer"
        and (video.get("iso_639_1") or "").lower() == language
        and video.get("key")
    ]
    return sorted(
        candidates,
        key=lambda video: (video.get("official") is True, video.get("published_at") or ""),
        reverse=True,
    )


def is_youtube_video_embeddable(youtube_key: str) -> bool:
    try:
        response = requests.get(
            YOUTUBE_OEMBED_URL,
            params={"url": f"{YOUTUBE_EMBED_BASE_URL}{youtube_key}", "format": "json"},
            timeout=getattr(settings, "YOUTUBE_VALIDATION_TIMEOUT", YOUTUBE_VALIDATION_TIMEOUT),
        )
    except requests.RequestException:
        return False
    return response.status_code == 200


def _get_cached_key(movie: Movie, language: str) -> str:
    return (movie.trailer_en_key if language == "en" else movie.trailer_es_key) or ""


def _clear_cached_key(movie: Movie, language: str) -> None:
    field_name = "trailer_en_key" if language == "en" else "trailer_es_key"
    setattr(movie, field_name, "")
    movie.save(update_fields=[field_name])


def _has_recent_negative_cache(movie: Movie) -> bool:
    if movie.trailer_es_key or movie.trailer_en_key or not movie.trailer_checked_at:
        return False
    return movie.trailer_checked_at >= timezone.now() - TRAILER_NEGATIVE_CACHE_TTL
