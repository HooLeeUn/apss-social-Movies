from __future__ import annotations

from typing import Any

from django.core.cache import cache
from django.db.models import Q

from .models import Movie, StreamingProviderLink
from .tmdb import get_tmdb_json

TMDB_LOGO_BASE_URL = "https://image.tmdb.org/t/p/w92"
WATCH_PROVIDER_CACHE_TIMEOUT = 60 * 60 * 24
WATCH_PROVIDER_GROUPS = ("flatrate", "rent", "buy")


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
            raw_payload.get(group, []), raw_payload.get("link", ""), provider_links
        )
        for group in WATCH_PROVIDER_GROUPS
    }

    return {
        "movie_id": movie.id,
        "tmdb_id": movie.tmdb_id,
        "type": movie.type,
        "country": country,
        "link": raw_payload.get("link", ""),
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
        "flatrate": [],
        "rent": [],
        "buy": [],
    }


def get_cached_country_watch_providers(movie: Movie, country: str) -> dict[str, Any]:
    cache_key = build_watch_provider_cache_key(movie, country)
    cached_payload = cache.get(cache_key)
    if cached_payload is not None:
        return cached_payload

    tmdb_payload = fetch_tmdb_watch_providers(movie)
    country_payload = tmdb_payload.get("results", {}).get(country, {})
    if not isinstance(country_payload, dict):
        country_payload = {}

    cache.set(cache_key, country_payload, WATCH_PROVIDER_CACHE_TIMEOUT)
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
    links = StreamingProviderLink.objects.filter(
        provider_id__in=provider_ids,
        country_code=country,
        is_active=True,
        content_type=content_kind,
    ).filter(
        Q(tmdb_id=movie.tmdb_id)
        | Q(tmdb_id__isnull=True, movie__isnull=True, imdb_id__isnull=True)
    ).order_by("provider_id", "-updated_at", "-id")

    specific_links = {}
    general_links = {}
    for link in links:
        if link.tmdb_id == movie.tmdb_id:
            specific_links.setdefault(link.provider_id, link)
        elif link.tmdb_id is None and link.movie_id is None and link.imdb_id is None:
            general_links.setdefault(link.provider_id, link)

    return {
        provider_id: specific_links.get(provider_id) or general_links.get(provider_id)
        for provider_id in provider_ids
        if specific_links.get(provider_id) or general_links.get(provider_id)
    }


def serialize_provider_group(
    providers: Any,
    tmdb_watch_url: str,
    provider_links: dict[int, StreamingProviderLink],
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
        monetized_url = affiliate_url or direct_url
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
