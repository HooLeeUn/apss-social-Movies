"""TMDb catalogue resolution and enrichment, deliberately independent of ORM."""
from __future__ import annotations

import email.utils
import re
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from core.tmdb import TMDbServiceError, get_tmdb_json

IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"
IMDB_RE = re.compile(r"^tt\d+$", re.IGNORECASE)


def normalize_text(value: Any) -> str:
    value = unicodedata.normalize("NFKD", str(value or ""))
    return " ".join("".join(c for c in value if not unicodedata.combining(c)).casefold().split())


def normalize_imdb_id(value: Any) -> str:
    value = str(value or "").strip().lower()
    return value if IMDB_RE.fullmatch(value) else ""


def split_directors(value: Any) -> list[str]:
    """Return non-empty directors in CSV order (commas are the contract)."""
    return [part.strip() for part in str(value or "").split(",") if part.strip()]


def normalize_media_type(value: Any) -> str:
    value = normalize_text(value)
    if value in {"movie", "film", "feature", "feature film", "pelicula", "peliculas"}:
        return "movie"
    if value in {"tv", "series", "serie", "show", "tv show", "tv series", "tv-series", "tv_series", "television"}:
        return "tv"
    return ""


def poster_url(path: Any) -> str:
    path = str(path or "").strip()
    return f"{IMAGE_BASE_URL}/{path.lstrip('/')}" if path else ""


@dataclass
class CatalogResult:
    status: str
    tmdb_id: int | None = None
    match_reason: str = "none"
    matched_director: str = ""
    candidate_tmdb_ids: list[int] = field(default_factory=list)
    image: str = ""
    synopsis: str = ""
    synopsis_es: str = ""
    synopsis_en_source: str = "empty"
    retry_count: int = 0
    request_count: int = 0
    notes: str = ""

    @property
    def candidate_count(self):
        return len(self.candidate_tmdb_ids)


class CatalogEnricher:
    def __init__(self, *, year_tolerance=0, requests_per_second=3.0, max_retries=4,
                 request: Callable[..., dict[str, Any]] | None = None,
                 sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.year_tolerance = max(0, int(year_tolerance))
        self.requests_per_second = max(0.0, float(requests_per_second))
        self.max_retries = max(0, int(max_retries))
        self.request = request or get_tmdb_json
        self.sleep = sleep
        self.clock = clock
        self.request_count = 0
        self.retry_count = 0
        self._last_request_at: float | None = None

    def _retry_after_seconds(self, value):
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except (TypeError, ValueError):
            try:
                parsed = email.utils.parsedate_to_datetime(str(value))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return None

    def _get(self, path, params=None):
        attempt = 0
        while True:
            if self.requests_per_second and self._last_request_at is not None:
                wait = 1 / self.requests_per_second - (self.clock() - self._last_request_at)
                if wait > 0:
                    self.sleep(wait)
            self.request_count += 1
            self._last_request_at = self.clock()
            try:
                return self.request(path, params=params or {})
            except TMDbServiceError as exc:
                if not getattr(exc, "retryable", False) or attempt >= self.max_retries:
                    raise
                retry_after = self._retry_after_seconds(getattr(exc, "retry_after", None))
                self.sleep(retry_after if retry_after is not None else min(30.0, 2 ** attempt))
                attempt += 1
                self.retry_count += 1

    @staticmethod
    def _year(value):
        match = re.match(r"\s*(\d{4})", str(value or ""))
        return int(match.group(1)) if match else None

    def _candidate_year_matches(self, candidate, media_type, year):
        field = "release_date" if media_type == "movie" else "first_air_date"
        found = self._year(candidate.get(field))
        return found is not None and abs(found - year) <= self.year_tolerance

    @staticmethod
    def _candidate_title_matches(candidate, media_type, title):
        fields = ("title", "original_title") if media_type == "movie" else ("name", "original_name")
        wanted = normalize_text(title)
        return any(normalize_text(candidate.get(field)) == wanted for field in fields)

    def _candidate_directors(self, media_type, tmdb_id):
        detail = self._get(f"/{media_type}/{tmdb_id}", {"language": "en-US", "append_to_response": "credits"})
        credits = detail.get("credits") or {}
        names = [p.get("name", "") for p in credits.get("crew", []) if p.get("job") == "Director"]
        if media_type == "tv":
            names.extend(p.get("name", "") for p in detail.get("created_by", []))
        return {normalize_text(name) for name in names if name}

    def resolve(self, row: dict[str, Any]) -> CatalogResult:
        start_requests, start_retries = self.request_count, self.retry_count
        media_type = normalize_media_type(row.get("type"))
        imdb_id = normalize_imdb_id(row.get("imdb_id"))
        title = str(row.get("title_english") or row.get("title_spanish") or "").strip()
        year = self._year(row.get("release_year"))
        directors = split_directors(row.get("director"))
        result = CatalogResult(status="not_found")
        try:
            if not media_type or (not imdb_id and (not title or year is None or not directors)):
                result.status, result.notes = "invalid_input", "type or title/year/director is invalid"
                return result

            if imdb_id:
                found = self._get(f"/find/{imdb_id}", {"external_source": "imdb_id"})
                key = "movie_results" if media_type == "movie" else "tv_results"
                ids = list(dict.fromkeys(int(x["id"]) for x in found.get(key, []) if x.get("id") is not None))
                result.candidate_tmdb_ids = ids
                if len(ids) == 1:
                    result.tmdb_id, result.match_reason = ids[0], "imdb_id"
                elif len(ids) > 1:
                    result.status, result.candidate_tmdb_ids = "ambiguous", ids
                    return result

            if result.tmdb_id is None:
                if not title or year is None or not directors:
                    result.status, result.notes = "invalid_input", "IMDb did not resolve and fallback fields are invalid"
                    return result
                endpoint = f"/search/{media_type}"
                params = {"query": title, "language": "en-US"}
                params["year" if media_type == "movie" else "first_air_date_year"] = year
                payload = self._get(endpoint, params)
                candidates = [c for c in payload.get("results", []) if c.get("id") is not None
                              and self._candidate_title_matches(c, media_type, title)
                              and self._candidate_year_matches(c, media_type, year)]
                director_sets = {int(c["id"]): self._candidate_directors(media_type, int(c["id"])) for c in candidates}
                result.candidate_tmdb_ids = list(director_sets)
                for director in directors:
                    normalized = normalize_text(director)
                    ids = [candidate_id for candidate_id, names in director_sets.items() if normalized in names]
                    if len(ids) == 1:
                        result.tmdb_id, result.match_reason, result.matched_director = ids[0], "title_year_director", director
                        break
                    if len(ids) > 1:
                        result.status, result.matched_director, result.candidate_tmdb_ids = "ambiguous", director, ids
                        return result
                if result.tmdb_id is None:
                    result.candidate_tmdb_ids = list(director_sets)
                    return result

            # Details are intentionally never requested until identity is confirmed.
            en = self._get(f"/{media_type}/{result.tmdb_id}", {"language": "en-US"})
            es = self._get(f"/{media_type}/{result.tmdb_id}", {"language": "es-ES"})
            en_overview = str(en.get("overview") or "").strip()
            original = str(row.get("synopsis") or "").strip()
            result.synopsis = en_overview or original
            result.synopsis_en_source = "tmdb" if en_overview else ("original_csv" if original else "empty")
            result.synopsis_es = str(es.get("overview") or "").strip()
            result.image = poster_url(en.get("poster_path"))
            result.status = "enriched" if result.image and result.synopsis and result.synopsis_es else "matched_partial"
            return result
        except TMDbServiceError as exc:
            result.status = "error_retryable" if getattr(exc, "retryable", False) else "error_permanent"
            result.notes = str(exc)
            return result
        finally:
            result.request_count = self.request_count - start_requests
            result.retry_count = self.retry_count - start_retries
