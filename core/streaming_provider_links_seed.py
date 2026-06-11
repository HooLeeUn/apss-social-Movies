from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StreamingProviderLinkSeed:
    provider_id: int
    provider_name: str
    country_code: str
    landing_url: str


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
