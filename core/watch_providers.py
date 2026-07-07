from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from django.core.cache import cache
from django.db.models import Q
from django.utils import timezone

from .models import Movie, StreamingProviderLink, TMDbPayloadCache
from .streaming_provider_links_seed import get_global_pattern_landing_url
from .tmdb import TMDbServiceError, get_tmdb_json

TMDB_LOGO_BASE_URL = "https://image.tmdb.org/t/p/w92"
WATCH_PROVIDER_CACHE_TIMEOUT = 60 * 60 * 24 * 3
WATCH_PROVIDER_GROUPS = ("flatrate", "rent", "buy")
TRUE_DETECTIVE_TMDB_ID = 46648

logger = logging.getLogger(__name__)


def get_movie_watch_providers(movie: Movie, country_code: str) -> dict[str, Any]:
    country = normalize_country_code(country_code)
    empty_payload = build_empty_watch_provider_payload(movie, country)

    if not movie.tmdb_id:
        return empty_payload

    raw_payload = get_cached_country_watch_providers(movie, country)
    if not raw_payload:
        return empty_payload

    provider_links = get_active_provider_links(movie, raw_payload, country)
    providers = {
        group: serialize_provider_group(
            raw_payload.get(group, []), raw_payload.get("link", ""), provider_links, country
        )
        for group in WATCH_PROVIDER_GROUPS
    }

    return {
        "movie_id": movie.id,
        "tmdb_id": movie.tmdb_id,
        "type": movie.type,
        "country": country,
        "link": raw_payload.get("link", ""),
        "tmdb_url": build_tmdb_url(movie),
        **providers,
    }


def normalize_country_code(country_code: str | None) -> str:
    country = (country_code or "").strip().upper()
    if len(country) != 2 or not country.isalpha():
        return "US"
    return country


def build_empty_watch_provider_payload(movie: Movie, country: str) -> dict[str, Any]:
    return {
        "movie_id": movie.id,
        "tmdb_id": movie.tmdb_id,
        "type": movie.type,
        "country": country,
        "link": "",
        "tmdb_url": build_tmdb_url(movie),
        "flatrate": [],
        "rent": [],
        "buy": [],
    }


def get_cached_country_watch_providers(movie: Movie, country: str) -> dict[str, Any]:
    cache_key = build_watch_provider_cache_key(movie, country)
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        log_true_detective_providers_source(movie.tmdb_id, country, "memory-cache")
        return cached_payload

    content_kind = get_tmdb_content_kind(movie)
    persistent_cache = get_persistent_watch_provider_payload(movie, content_kind, country)
    if persistent_cache and persistent_cache.is_fresh():
        cache.set(cache_key, persistent_cache.payload, WATCH_PROVIDER_CACHE_TIMEOUT)
        log_true_detective_providers_source(movie.tmdb_id, country, "persistent-cache")
        return persistent_cache.payload

    try:
        tmdb_payload = fetch_tmdb_watch_providers(movie)
    except TMDbServiceError:
        if persistent_cache:
            log_true_detective_providers_source(movie.tmdb_id, country, "stale-persistent-cache")
            return persistent_cache.payload
        log_true_detective_providers_source(movie.tmdb_id, country, "fallback")
        raise

    country_payload = tmdb_payload.get("results", {}).get(country, {})
    if not isinstance(country_payload, dict):
        country_payload = {}

    store_persistent_watch_provider_payload(movie, content_kind, country, country_payload)
    cache.set(cache_key, country_payload, WATCH_PROVIDER_CACHE_TIMEOUT)
    log_true_detective_providers_source(movie.tmdb_id, country, "tmdb")
    return country_payload


def build_watch_provider_cache_key(movie: Movie, country: str) -> str:
    content_kind = get_tmdb_content_kind(movie)
    return f"movie-watch-providers:v1:{movie.id}:{content_kind}:{movie.tmdb_id}:{country}"


def fetch_tmdb_watch_providers(movie: Movie) -> dict[str, Any]:
    content_kind = get_tmdb_content_kind(movie)
    return get_tmdb_json(f"/{content_kind}/{movie.tmdb_id}/watch/providers")


def get_tmdb_content_kind(movie: Movie) -> str:
    if movie.type == Movie.SERIES:
        return "tv"
    return "movie"


def build_tmdb_url(movie: Movie) -> str | None:
    if not movie.tmdb_id:
        return None
    return f"https://www.themoviedb.org/{get_tmdb_content_kind(movie)}/{movie.tmdb_id}"


def get_provider_ids(raw_payload: dict[str, Any]) -> set[int]:
    provider_ids = set()
    for group in WATCH_PROVIDER_GROUPS:
        providers = raw_payload.get(group, [])
        if not isinstance(providers, list):
            continue
        provider_ids.update(
            provider.get("provider_id")
            for provider in providers
            if isinstance(provider, dict) and provider.get("provider_id") is not None
        )
    return provider_ids


def get_active_provider_links(
    movie: Movie,
    raw_payload: dict[str, Any],
    country: str,
) -> dict[int, StreamingProviderLink]:
    provider_ids = get_provider_ids(raw_payload)
    if not provider_ids:
        return {}

    content_kind = get_tmdb_content_kind(movie)
    specific_filter = Q(movie=movie)
    if movie.tmdb_id:
        specific_filter |= Q(tmdb_id=movie.tmdb_id)
    if movie.imdb_id:
        specific_filter |= Q(imdb_id=movie.imdb_id)

    general_filter = Q(tmdb_id__isnull=True, movie__isnull=True, imdb_id__isnull=True)

    links = (
        StreamingProviderLink.objects.filter(
            provider_id__in=provider_ids,
            country_code=country,
            is_active=True,
        )
        .filter(specific_filter | general_filter)
        .order_by("provider_id", "-updated_at", "-id")
    )

    specific_links = {}
    matching_general_links = {}
    fallback_general_links = {}
    for link in links:
        content_type_matches = link.content_type == content_kind
        is_specific_link = (
            link.movie_id == movie.id
            or link.tmdb_id == movie.tmdb_id
            or (movie.imdb_id and link.imdb_id == movie.imdb_id)
        )
        if is_specific_link and content_type_matches:
            specific_links.setdefault(link.provider_id, link)
        elif link.tmdb_id is None and link.movie_id is None and link.imdb_id is None:
            if content_type_matches:
                matching_general_links.setdefault(link.provider_id, link)
            else:
                fallback_general_links.setdefault(link.provider_id, link)

    return {
        provider_id: (
            specific_links.get(provider_id)
            or matching_general_links.get(provider_id)
            or fallback_general_links.get(provider_id)
        )
        for provider_id in provider_ids
        if (
            specific_links.get(provider_id)
            or matching_general_links.get(provider_id)
            or fallback_general_links.get(provider_id)
        )
    }


def serialize_provider_group(
    providers: Any,
    tmdb_watch_url: str,
    provider_links: dict[int, StreamingProviderLink],
    country: str,
) -> list[dict[str, Any]]:
    if not isinstance(providers, list):
        return []

    serialized = []
    for provider in providers:
        if not isinstance(provider, dict):
            continue

        provider_id = provider.get("provider_id")
        provider_link = provider_links.get(provider_id)
        direct_url = get_link_url(provider_link, "direct_url")
        affiliate_url = get_link_url(provider_link, "affiliate_url")
        landing_url = get_link_url(provider_link, "landing_url")
        if landing_url is None:
            pattern_landing_url = get_global_pattern_landing_url(provider.get("provider_name", ""), country)
            landing_url = pattern_landing_url or None
        monetized_url = affiliate_url or direct_url or landing_url
        monetization_type = (
            provider_link.monetization_type
            if provider_link
            else StreamingProviderLink.MonetizationType.NONE
        )

        logo_path = provider.get("logo_path") or ""
        serialized.append(
            {
                "provider_id": provider_id,
                "provider_name": provider.get("provider_name", ""),
                "logo_url": f"{TMDB_LOGO_BASE_URL}{logo_path}" if logo_path else "",
                "display_priority": provider.get("display_priority"),
                "tmdb_watch_url": tmdb_watch_url or "",
                "direct_url": direct_url,
                "affiliate_url": affiliate_url,
                "landing_url": landing_url,
                "monetized_url": monetized_url,
                "is_clickable": monetized_url is not None,
                "monetization_type": monetization_type,
            }
        )

    return serialized


def get_link_url(provider_link: StreamingProviderLink | None, field_name: str) -> str | None:
    if provider_link is None:
        return None
    value = getattr(provider_link, field_name, "")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def get_persistent_watch_provider_payload(movie: Movie, content_kind: str, country: str) -> TMDbPayloadCache | None:
    cache_entry = (
        TMDbPayloadCache.objects.filter(
            tmdb_id=movie.tmdb_id,
            content_type=content_kind,
            payload_type=TMDbPayloadCache.PayloadType.WATCH_PROVIDERS,
            country_code=country,
            season_number=0,
        )
        .order_by("-updated_at", "-id")
        .first()
    )
    if cache_entry and cache_entry.movie_id != movie.id:
        TMDbPayloadCache.objects.filter(pk=cache_entry.pk, movie__isnull=True).update(movie=movie)
    return cache_entry


def store_persistent_watch_provider_payload(
    movie: Movie, content_kind: str, country: str, payload: dict[str, Any]
) -> TMDbPayloadCache:
    cache_entry, _created = TMDbPayloadCache.objects.update_or_create(
        tmdb_id=movie.tmdb_id,
        content_type=content_kind,
        payload_type=TMDbPayloadCache.PayloadType.WATCH_PROVIDERS,
        country_code=country,
        season_number=0,
        defaults={
            "movie": movie,
            "payload": payload,
            "source": "tmdb",
            "expires_at": timezone.now() + timedelta(seconds=WATCH_PROVIDER_CACHE_TIMEOUT),
        },
    )
    return cache_entry


def log_true_detective_providers_source(tmdb_id: int | None, country: str, source: str) -> None:
    if tmdb_id == TRUE_DETECTIVE_TMDB_ID:
        logger.info("True Detective providers source for %s: %s", country, source)
