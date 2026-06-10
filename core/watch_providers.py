from __future__ import annotations

from typing import Any

from django.core.cache import cache

from .models import Movie, StreamingAffiliateLink
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

    affiliate_links = get_active_affiliate_links(raw_payload, country)
    providers = {
        group: serialize_provider_group(
            raw_payload.get(group, []), raw_payload.get("link", ""), affiliate_links
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


def get_active_affiliate_links(raw_payload: dict[str, Any], country: str) -> dict[int, StreamingAffiliateLink]:
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

    if not provider_ids:
        return {}

    links = StreamingAffiliateLink.objects.filter(
        provider_id__in=provider_ids,
        country_code=country,
        is_active=True,
    ).order_by("provider_id", "-updated_at", "-id")

    by_provider = {}
    for link in links:
        by_provider.setdefault(link.provider_id, link)
    return by_provider


def serialize_provider_group(
    providers: Any,
    default_link: str,
    affiliate_links: dict[int, StreamingAffiliateLink],
) -> list[dict[str, Any]]:
    if not isinstance(providers, list):
        return []

    serialized = []
    for provider in providers:
        if not isinstance(provider, dict):
            continue

        provider_id = provider.get("provider_id")
        affiliate_link = affiliate_links.get(provider_id)
        link = default_link or ""
        monetized_url = link
        monetization_type = StreamingAffiliateLink.MonetizationType.NONE

        if affiliate_link:
            monetized_url = affiliate_link.affiliate_url
            monetization_type = affiliate_link.monetization_type

        logo_path = provider.get("logo_path") or ""
        serialized.append(
            {
                "provider_id": provider_id,
                "provider_name": provider.get("provider_name", ""),
                "logo_url": f"{TMDB_LOGO_BASE_URL}{logo_path}" if logo_path else "",
                "display_priority": provider.get("display_priority"),
                "link": link,
                "monetized_url": monetized_url,
                "monetization_type": monetization_type,
            }
        )

    return serialized
