from __future__ import annotations

from dataclasses import dataclass, field

from django.db.models import Q

from core.models import StreamingProviderLink


@dataclass(frozen=True)
class StreamingProviderLinkSeed:
    provider_id: int
    provider_name: str
    country_code: str
    landing_url: str
    provider_name_aliases: tuple[str, ...] = field(default_factory=tuple)

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
    ),
    StreamingProviderLinkSeed(
        582,
        "Paramount+ Amazon Channel",
        "CO",
        "https://www.amazon.com",
        provider_name_aliases=("Paramount Plus Amazon Channel",),
    ),
    StreamingProviderLinkSeed(
        633,
        "Paramount+ Roku Premium Channel",
        "CO",
        "https://therokuchannel.roku.com/enguard/",
        provider_name_aliases=("Paramount Plus Roku Premium Channel",),
    ),
    StreamingProviderLinkSeed(
        207,
        "The Roku Channel",
        "CO",
        "https://therokuchannel.roku.com/enguard/",
        provider_name_aliases=("Roku", "Roku Channel"),
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
