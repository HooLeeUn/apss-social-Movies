from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from django.db.models import Q

from core.models import StreamingProviderLink


GLOBAL_PROVIDER_PATTERN_NOTE = "Global fallback landing URL by provider name pattern"
AMAZON_CHANNEL_PROVIDER_PATTERN_NOTE = "Global fallback landing URL for Amazon Channel providers"
AMAZON_CHANNEL_LANDING_URL = "https://www.primevideo.com/"
LEGACY_AMAZON_CHANNEL_LANDING_URL = "https://www.amazon.com"
MINIMUM_SUPPORTED_COUNTRY_CODES: tuple[str, ...] = ("AR", "CA", "CL", "CO", "ES", "MX", "PE", "UK", "US")


@dataclass(frozen=True)
class StreamingProviderLinkSeed:
    provider_id: int
    provider_name: str
    country_code: str
    landing_url: str
    provider_name_aliases: tuple[str, ...] = field(default_factory=tuple)
    notes: str = ""

    @property
    def provider_names(self) -> tuple[str, ...]:
        return (self.provider_name, *self.provider_name_aliases)


STREAMING_PROVIDER_LINK_SEEDS: tuple[StreamingProviderLinkSeed, ...] = (
    # Colombia
    StreamingProviderLinkSeed(8, "Netflix", "CO", "https://www.netflix.com/co/"),
    StreamingProviderLinkSeed(337, "Disney Plus", "CO", "https://www.disneyplus.com/es-co"),
    StreamingProviderLinkSeed(119, "Amazon Prime Video", "CO", "https://www.primevideo.com/"),
    StreamingProviderLinkSeed(10, "Amazon Video", "CO", "https://www.primevideo.com/"),
    StreamingProviderLinkSeed(2, "Apple TV", "CO", "https://tv.apple.com/co"),
    StreamingProviderLinkSeed(3, "Google Play Movies", "CO", "https://play.google.com/store/movies"),
    StreamingProviderLinkSeed(192, "YouTube", "CO", "https://www.youtube.com/movies"),
    StreamingProviderLinkSeed(1899, "Max", "CO", "https://www.max.com/co/es"),
    StreamingProviderLinkSeed(531, "Paramount Plus", "CO", "https://www.paramountplus.com/co/"),
    StreamingProviderLinkSeed(257, "Fubo", "CO", "https://www.fubo.tv/"),
    StreamingProviderLinkSeed(43, "Starz", "CO", "https://www.starz.com/"),
    StreamingProviderLinkSeed(7, "Fandango At Home", "CO", "https://www.vudu.com/"),
    StreamingProviderLinkSeed(538, "Plex", "CO", "https://www.plex.tv/"),
    StreamingProviderLinkSeed(331, "FlixFling", "CO", "https://www.flixfling.com/"),
    StreamingProviderLinkSeed(486, "Spectrum On Demand", "CO", "https://ondemand.spectrum.net/"),
    StreamingProviderLinkSeed(
        339,
        "MovistarTV",
        "CO",
        "https://tv.movistar.co/",
        provider_name_aliases=("Movistar TV", "Movistar Plus"),
    ),
    StreamingProviderLinkSeed(
        1855,
        "Starz Apple TV Channel",
        "CO",
        "https://tv.apple.com/co",
        provider_name_aliases=("STARZ Apple TV Channel",),
        notes=GLOBAL_PROVIDER_PATTERN_NOTE,
    ),
    StreamingProviderLinkSeed(
        582,
        "Paramount+ Amazon Channel",
        "CO",
        AMAZON_CHANNEL_LANDING_URL,
        provider_name_aliases=("Paramount Plus Amazon Channel",),
        notes=AMAZON_CHANNEL_PROVIDER_PATTERN_NOTE,
    ),
    StreamingProviderLinkSeed(
        633,
        "Paramount+ Roku Premium Channel",
        "CO",
        "https://therokuchannel.roku.com/enguard/",
        provider_name_aliases=("Paramount Plus Roku Premium Channel",),
        notes=GLOBAL_PROVIDER_PATTERN_NOTE,
    ),
    StreamingProviderLinkSeed(
        207,
        "The Roku Channel",
        "CO",
        "https://therokuchannel.roku.com/enguard/",
        provider_name_aliases=("Roku", "Roku Channel"),
        notes=GLOBAL_PROVIDER_PATTERN_NOTE,
    ),
    # United States
    StreamingProviderLinkSeed(8, "Netflix", "US", "https://www.netflix.com/"),
    StreamingProviderLinkSeed(337, "Disney Plus", "US", "https://www.disneyplus.com"),
    StreamingProviderLinkSeed(119, "Amazon Prime Video", "US", "https://www.primevideo.com/"),
    StreamingProviderLinkSeed(10, "Amazon Video", "US", "https://www.primevideo.com/"),
    StreamingProviderLinkSeed(2, "Apple TV", "US", "https://tv.apple.com/us"),
    StreamingProviderLinkSeed(3, "Google Play Movies", "US", "https://play.google.com/store/movies"),
    StreamingProviderLinkSeed(192, "YouTube", "US", "https://www.youtube.com/movies"),
    StreamingProviderLinkSeed(1899, "Max", "US", "https://www.max.com"),
    StreamingProviderLinkSeed(531, "Paramount Plus", "US", "https://www.paramountplus.com/"),
    StreamingProviderLinkSeed(257, "Fubo", "US", "https://www.fubo.tv/"),
    StreamingProviderLinkSeed(43, "Starz", "US", "https://www.starz.com/"),
    StreamingProviderLinkSeed(7, "Fandango At Home", "US", "https://www.vudu.com/"),
    StreamingProviderLinkSeed(538, "Plex", "US", "https://www.plex.tv/"),
    StreamingProviderLinkSeed(331, "FlixFling", "US", "https://www.flixfling.com/"),
    StreamingProviderLinkSeed(486, "Spectrum On Demand", "US", "https://ondemand.spectrum.net/"),
)


def normalize_seed_country_code(country_code: str) -> str:
    return country_code.strip().upper()


def normalize_provider_name(provider_name: str) -> str:
    return " ".join(provider_name.strip().lower().split())


def is_amazon_channel_provider(provider_name: str) -> bool:
    return "amazon channel" in normalize_provider_name(provider_name)


def get_global_pattern_note(provider_name: str) -> str:
    if is_amazon_channel_provider(provider_name):
        return AMAZON_CHANNEL_PROVIDER_PATTERN_NOTE
    return GLOBAL_PROVIDER_PATTERN_NOTE


def seed_uses_amazon_channel_rule(seed: StreamingProviderLinkSeed) -> bool:
    return seed.notes == AMAZON_CHANNEL_PROVIDER_PATTERN_NOTE or is_amazon_channel_provider(seed.provider_name)


def should_update_landing_url_from_seed(
    link: StreamingProviderLink,
    seed: StreamingProviderLinkSeed,
    *,
    update_static_provider: bool = True,
) -> bool:
    if not link.landing_url:
        return True
    if link.affiliate_url:
        return False
    if link.landing_url == seed.landing_url:
        return False
    if seed_uses_amazon_channel_rule(seed):
        return (
            link.landing_url == LEGACY_AMAZON_CHANNEL_LANDING_URL
            or AMAZON_CHANNEL_PROVIDER_PATTERN_NOTE in link.notes
        )
    return update_static_provider or bool(seed.notes)


def should_add_notes_from_seed(link: StreamingProviderLink, seed: StreamingProviderLinkSeed) -> bool:
    if not seed.notes or seed.notes in link.notes:
        return False
    if (
        seed_uses_amazon_channel_rule(seed)
        and not link.affiliate_url
        and link.landing_url != seed.landing_url
    ):
        return False
    return True


def get_global_pattern_landing_url(provider_name: str, country_code: str) -> str:
    normalized_name = normalize_provider_name(provider_name)
    country = normalize_seed_country_code(country_code).lower()

    # Amazon channel rules must win before plain HBO/HBO Max matching.
    if is_amazon_channel_provider(provider_name):
        return AMAZON_CHANNEL_LANDING_URL
    if normalized_name in {"hbo", "hbo max", "max hbo"}:
        return "https://www.hbomax.com"
    if normalized_name == "youtube tv":
        return "https://tv.youtube.com/welcome/"
    if (
        normalized_name in {"roku", "roku channel", "the roku channel", "paramount+ roku premium channel"}
        or "roku premium channel" in normalized_name
    ):
        return "https://therokuchannel.roku.com/enguard/"
    if "apple tv channel" in normalized_name:
        return f"https://tv.apple.com/{country}"
    return ""


def get_supported_country_codes() -> tuple[str, ...]:
    db_country_codes = (
        StreamingProviderLink.objects.exclude(country_code="")
        .values_list("country_code", flat=True)
        .distinct()
    )
    seed_country_codes = (seed.country_code for seed in STREAMING_PROVIDER_LINK_SEEDS)
    country_codes = {
        normalize_seed_country_code(country_code)
        for country_code in (*MINIMUM_SUPPORTED_COUNTRY_CODES, *seed_country_codes, *db_country_codes)
        if country_code
    }
    return tuple(sorted(country_codes))


def iter_global_provider_sources() -> Iterable[tuple[int, str]]:
    seen: set[tuple[int, str]] = set()

    for seed in STREAMING_PROVIDER_LINK_SEEDS:
        if get_global_pattern_landing_url(seed.provider_name, seed.country_code):
            key = (seed.provider_id, seed.provider_name)
            if key not in seen:
                seen.add(key)
                yield seed.provider_id, seed.provider_name

    existing_provider_rows = (
        StreamingProviderLink.objects.exclude(provider_name="")
        .values_list("provider_id", "provider_name")
        .distinct()
        .order_by("provider_id", "provider_name")
    )
    for provider_id, provider_name in existing_provider_rows:
        if get_global_pattern_landing_url(provider_name, "US"):
            key = (provider_id, provider_name)
            if key not in seen:
                seen.add(key)
                yield provider_id, provider_name


def build_streaming_provider_link_seeds() -> tuple[StreamingProviderLinkSeed, ...]:
    country_codes = get_supported_country_codes()
    seeds_by_provider_country: dict[tuple[int, str], StreamingProviderLinkSeed] = {
        (seed.provider_id, seed.country_code): seed for seed in STREAMING_PROVIDER_LINK_SEEDS
    }

    for provider_id, provider_name in iter_global_provider_sources():
        for country_code in country_codes:
            landing_url = get_global_pattern_landing_url(provider_name, country_code)
            if not landing_url:
                continue
            seeds_by_provider_country[(provider_id, country_code)] = StreamingProviderLinkSeed(
                provider_id=provider_id,
                provider_name=provider_name,
                country_code=country_code,
                landing_url=landing_url,
                notes=get_global_pattern_note(provider_name),
            )

    return tuple(seeds_by_provider_country.values())


def get_general_provider_link(seed: StreamingProviderLinkSeed) -> StreamingProviderLink | None:
    provider_id_link = (
        StreamingProviderLink.objects.filter(
            provider_id=seed.provider_id,
            country_code=seed.country_code,
            movie__isnull=True,
            tmdb_id__isnull=True,
            imdb_id__isnull=True,
        )
        .order_by("id")
        .first()
    )
    if provider_id_link is not None:
        return provider_id_link

    provider_name_filter = Q()
    for provider_name in seed.provider_names:
        provider_name_filter |= Q(provider_name__iexact=provider_name)

    return (
        StreamingProviderLink.objects.filter(
            provider_name_filter,
            country_code=seed.country_code,
            movie__isnull=True,
            tmdb_id__isnull=True,
            imdb_id__isnull=True,
        )
        .order_by("id")
        .first()
    )
