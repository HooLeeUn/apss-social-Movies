from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.utils import timezone

from .models import Movie
from .tmdb import get_tmdb_json

YOUTUBE_EMBED_BASE_URL = "https://www.youtube.com/embed/"
YOUTUBE_WATCH_BASE_URL = "https://www.youtube.com/watch?v="
TRAILER_NEGATIVE_CACHE_TTL = timedelta(days=30)
ENGLISH_COUNTRIES = {"US", "CA", "UK", "BZ"}


def language_for_country(country: str | None) -> str:
    normalized = (country or "").strip().upper()
    return "en" if normalized in ENGLISH_COUNTRIES else "es"


def build_trailer_payload(youtube_key: str | None, language: str | None, source: str) -> dict[str, Any]:
    if not youtube_key:
        return {
            "trailer_url": None,
            "watch_url": None,
            "youtube_key": None,
            "language": None,
            "source": source,
            "available": False,
        }

    return {
        "trailer_url": f"{YOUTUBE_EMBED_BASE_URL}{youtube_key}",
        "watch_url": f"{YOUTUBE_WATCH_BASE_URL}{youtube_key}",
        "youtube_key": youtube_key,
        "language": language,
        "source": source,
        "available": True,
    }


def get_movie_trailer_payload(movie: Movie, country: str | None = None) -> dict[str, Any]:
    requested_language = language_for_country(country)
    cached_key = _get_cached_key(movie, requested_language)
    if cached_key:
        return build_trailer_payload(cached_key, requested_language, "cache")

    fallback_key = _get_cached_key(movie, "en") if requested_language != "en" else None
    if fallback_key:
        return build_trailer_payload(fallback_key, "en", "cache")

    if _has_recent_negative_cache(movie):
        return build_trailer_payload(None, None, "tmdb")

    if not movie.tmdb_id:
        return build_trailer_payload(None, None, "tmdb")

    content_kind = "tv" if movie.type == Movie.SERIES else "movie"
    tmdb_payload = get_tmdb_json(f"/{content_kind}/{movie.tmdb_id}/videos")
    videos = tmdb_payload.get("results", [])
    if not isinstance(videos, list):
        videos = []

    selected_language, youtube_key = select_best_trailer(videos, requested_language)
    movie.trailer_checked_at = timezone.now()
    update_fields = ["trailer_checked_at"]

    if youtube_key and selected_language == "es":
        movie.trailer_es_key = youtube_key
        update_fields.append("trailer_es_key")
    elif youtube_key and selected_language == "en":
        movie.trailer_en_key = youtube_key
        update_fields.append("trailer_en_key")

    movie.save(update_fields=update_fields)
    return build_trailer_payload(youtube_key, selected_language, "tmdb")


def select_best_trailer(videos: list[dict[str, Any]], requested_language: str) -> tuple[str | None, str | None]:
    languages = [requested_language]
    if requested_language != "en":
        languages.append("en")

    for language in languages:
        candidate = _select_for_language(videos, language)
        if candidate:
            return language, candidate.get("key")
    return None, None


def _select_for_language(videos: list[dict[str, Any]], language: str) -> dict[str, Any] | None:
    candidates = [
        video for video in videos
        if isinstance(video, dict)
        and (video.get("site") or "").lower() == "youtube"
        and (video.get("type") or "").lower() == "trailer"
        and (video.get("iso_639_1") or "").lower() == language
        and video.get("key")
    ]
    if not candidates:
        return None

    official = [video for video in candidates if video.get("official") is True]
    pool = official or candidates
    return sorted(pool, key=lambda video: video.get("published_at") or "", reverse=True)[0]


def _get_cached_key(movie: Movie, language: str) -> str:
    return (movie.trailer_en_key if language == "en" else movie.trailer_es_key) or ""


def _has_recent_negative_cache(movie: Movie) -> bool:
    if movie.trailer_es_key or movie.trailer_en_key or not movie.trailer_checked_at:
        return False
    return movie.trailer_checked_at >= timezone.now() - TRAILER_NEGATIVE_CACHE_TTL
