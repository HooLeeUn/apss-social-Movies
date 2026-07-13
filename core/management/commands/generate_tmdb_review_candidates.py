import csv
import re
import string
import time
import unicodedata
from collections import OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from core.models import Movie


OUTPUT_COLUMNS = [
    "database_id", "source_type", "source_title_en", "source_title_es", "source_year",
    "source_director", "source_imdb_id", "source_cast", "candidate_rank", "candidate_tmdb_id",
    "candidate_type", "candidate_title", "candidate_original_title", "candidate_release_date",
    "candidate_year", "candidate_imdb_id", "candidate_director_or_creators", "candidate_cast",
    "candidate_overview", "candidate_original_language", "candidate_popularity", "candidate_vote_average",
    "candidate_vote_count", "candidate_poster_path", "match_method", "search_query", "year_difference",
    "title_normalized_equal", "director_normalized_match", "imdb_exact_match", "review_decision",
    "approved_tmdb_id", "review_notes", "error_message",
]


class TMDbReviewCandidateError(Exception):
    pass


class LRUCache:
    def __init__(self, max_size=512):
        self.max_size = max_size
        self._data = OrderedDict()

    def get(self, key):
        if key not in self._data:
            return None
        self._data.move_to_end(key)
        return self._data[key]

    def set(self, key, value):
        self._data[key] = value
        self._data.move_to_end(key)
        if len(self._data) > self.max_size:
            self._data.popitem(last=False)


class TMDbReadOnlyClient:
    RETRY_STATUSES = {429, 500, 502, 503, 504}

    def __init__(self, timeout, request_delay, max_retries=3):
        self.base_url = getattr(settings, "TMDB_BASE_URL", "https://api.themoviedb.org/3").rstrip("/")
        self.timeout = timeout if timeout is not None else getattr(settings, "TMDB_REQUEST_TIMEOUT", 10)
        self.request_delay = max(0.0, request_delay)
        self.max_retries = max_retries
        self.requests_made = 0
        token = getattr(settings, "TMDB_READ_ACCESS_TOKEN", "")
        if not token:
            raise CommandError("TMDB_READ_ACCESS_TOKEN is not configured")
        self.session = requests.Session()
        self.headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    def get(self, path, params=None):
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{normalized_path}"
        last_error = None
        for attempt in range(self.max_retries + 1):
            if self.request_delay:
                time.sleep(self.request_delay)
            self.requests_made += 1
            try:
                response = self.session.get(url, params=params or {}, headers=self.headers, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_error = "TMDb connection error or timeout"
                if attempt < self.max_retries:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise TMDbReviewCandidateError(last_error) from exc
            except requests.RequestException as exc:
                raise TMDbReviewCandidateError("TMDb request failed") from exc

            if response.status_code == 404:
                return {}
            if response.status_code in self.RETRY_STATUSES and attempt < self.max_retries:
                retry_after = response.headers.get("Retry-After")
                try:
                    sleep_for = float(retry_after) if retry_after else min(2 ** attempt, 8)
                except ValueError:
                    sleep_for = min(2 ** attempt, 8)
                time.sleep(max(0.0, sleep_for))
                continue
            if response.status_code != 200:
                raise TMDbReviewCandidateError(f"TMDb returned status {response.status_code}")
            try:
                data = response.json()
            except ValueError as exc:
                raise TMDbReviewCandidateError("TMDb returned invalid JSON") from exc
            if not isinstance(data, dict):
                raise TMDbReviewCandidateError("TMDb response JSON must be an object")
            return data
        raise TMDbReviewCandidateError(last_error or "TMDb request failed after retries")


class Command(BaseCommand):
    help = "Genera CSV de candidatos TMDb para revisión manual sin modificar la base de datos."

    MOVIE_ALIASES = {Movie.MOVIE, "movies", "film", "films", "pelicula", "película"}
    SERIES_ALIASES = {Movie.SERIES, "serie", "series", "tv", "show", "tv_show", "tv-series"}

    def add_arguments(self, parser):
        parser.add_argument("--input", required=True, dest="input_path")
        parser.add_argument("--output", default="tmdb_exports/tmdb_review_candidates.csv")
        parser.add_argument("--min-database-id", type=int, default=None)
        parser.add_argument("--max-database-id", type=int, default=None)
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--start-row", type=int, default=1)
        parser.add_argument("--max-candidates", type=int, default=5)
        parser.add_argument("--language", default="en-US")
        parser.add_argument("--request-delay", type=float, default=0.25)
        parser.add_argument("--timeout", type=float, default=None)
        parser.add_argument("--overwrite", action="store_true")
        parser.add_argument("--resume", action="store_true")

    def handle(self, *args, **options):
        self._validate_options(options)
        input_path = Path(options["input_path"])
        output_path = Path(options["output"])
        if not input_path.exists():
            raise CommandError(f"Input CSV does not exist: {input_path}")
        if output_path.exists() and not options["overwrite"] and not options["resume"]:
            raise CommandError("Output CSV already exists. Use --overwrite or --resume.")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        processed_ids = self._read_processed_ids(output_path) if options["resume"] and output_path.exists() else set()
        mode = "a" if options["resume"] and output_path.exists() else "w"
        write_header = mode == "w" or output_path.stat().st_size == 0 if output_path.exists() else True
        client = TMDbReadOnlyClient(options["timeout"], options["request_delay"])
        detail_cache = LRUCache(512)
        stats = defaultdict(int)
        first_id = last_id = None
        started = time.monotonic()
        self._print_start(options, input_path, output_path)

        with input_path.open("r", encoding="utf-8-sig", newline="") as in_fh, output_path.open(mode, encoding="utf-8-sig", newline="") as out_fh:
            reader = csv.DictReader(in_fh)
            writer = csv.DictWriter(out_fh, fieldnames=OUTPUT_COLUMNS)
            if write_header:
                writer.writeheader()
            for logical_row, source in enumerate(reader, start=1):
                stats["rows_read"] += 1
                if logical_row < options["start_row"]:
                    stats["skipped_before_start_row"] += 1
                    continue
                database_id = self._parse_positive_int(source.get("database_id"))
                if database_id is None:
                    stats["errors"] += 1
                    writer.writerow(self._base_output(source, "", 0, "error", "Invalid database_id"))
                    continue
                if not self._in_range(database_id, options):
                    stats["out_of_range"] += 1
                    continue
                if database_id in processed_ids:
                    stats["skipped_by_resume"] += 1
                    continue
                if options["limit"] is not None and stats["processed_sources"] >= options["limit"]:
                    break
                if first_id is None:
                    first_id = database_id
                last_id = database_id
                stats["processed_sources"] += 1
                try:
                    rows = self._process_source(source, database_id, client, detail_cache, options)
                except TMDbReviewCandidateError as exc:
                    rows = [self._base_output(source, database_id, 0, "error", str(exc))]
                for row in rows:
                    writer.writerow({key: self._clean_cell(row.get(key, "")) for key in OUTPUT_COLUMNS})
                    if row["match_method"] == "no_match":
                        stats["no_match"] += 1
                    elif row["match_method"] == "error":
                        stats["errors"] += 1
                    else:
                        stats["candidates"] += 1
                if stats["processed_sources"] % 100 == 0:
                    self.stdout.write(f"Processed {stats['processed_sources']} source records...")
        self._print_finish(stats, client.requests_made, first_id, last_id, started, output_path)

    def _validate_options(self, options):
        for name in ("min_database_id", "max_database_id"):
            if options[name] is not None and options[name] < 0:
                raise CommandError(f"--{name.replace('_', '-')} must be zero or positive")
        if options["min_database_id"] is not None and options["max_database_id"] is not None and options["min_database_id"] > options["max_database_id"]:
            raise CommandError("--min-database-id cannot be greater than --max-database-id")
        if options["limit"] is not None and options["limit"] <= 0:
            raise CommandError("--limit must be greater than 0")
        if options["max_candidates"] <= 0:
            raise CommandError("--max-candidates must be greater than 0")
        if options["start_row"] <= 0:
            raise CommandError("--start-row must be greater than 0")

    def _process_source(self, source, database_id, client, detail_cache, options):
        content_type = self._map_type(source.get("type"))
        if content_type is None:
            return [self._base_output(source, database_id, 0, "error", f"Unknown source type: {source.get('type', '')}")]
        query = (source.get("title_en") or source.get("title_english") or "").strip() or (source.get("title_es") or source.get("title_spanish") or "").strip()
        if not query:
            return [self._base_output(source, database_id, 0, "error", "Missing title_en and title_es")]
        year = self._parse_positive_int(source.get("year") or source.get("release_year"))
        candidates, method = self._find_candidates(client, content_type, source.get("imdb_id", ""), query, year, options)
        candidates = candidates[: options["max_candidates"]]
        if not candidates:
            return [self._base_output(source, database_id, 0, "no_match", "", search_query=query)]
        rows = []
        for rank, candidate in enumerate(candidates, start=1):
            tmdb_id = candidate.get("id")
            details = self._get_details(client, detail_cache, content_type, tmdb_id, options["language"])
            rows.append(self._candidate_row(source, database_id, content_type, rank, details, method, query, year))
        return rows

    def _find_candidates(self, client, content_type, imdb_id, query, year, options):
        imdb_id = (imdb_id or "").strip()
        if re.fullmatch(r"tt\d+", imdb_id):
            data = client.get(f"/find/{imdb_id}", {"external_source": "imdb_id", "language": options["language"]})
            key = "movie_results" if content_type == Movie.MOVIE else "tv_results"
            results = data.get(key) or []
            if results:
                return results, "imdb_exact"
        endpoint = "/search/movie" if content_type == Movie.MOVIE else "/search/tv"
        params = {"query": query, "language": options["language"], "include_adult": "false"}
        if year:
            params["year" if content_type == Movie.MOVIE else "first_air_date_year"] = year
        data = client.get(endpoint, params)
        results = data.get("results") or []
        if not results and year:
            params.pop("year", None); params.pop("first_air_date_year", None)
            data = client.get(endpoint, params)
            results = data.get("results") or []
        return results, "text_search"

    def _get_details(self, client, cache, content_type, tmdb_id, language):
        key = (content_type, tmdb_id, language)
        cached = cache.get(key)
        if cached is not None:
            return cached
        if content_type == Movie.MOVIE:
            details = client.get(f"/movie/{tmdb_id}", {"language": language, "append_to_response": "credits,external_ids"})
        else:
            details = client.get(f"/tv/{tmdb_id}", {"language": language, "append_to_response": "aggregate_credits,external_ids"})
            if not details.get("aggregate_credits"):
                details["aggregate_credits"] = client.get(f"/tv/{tmdb_id}/aggregate_credits", {"language": language})
        cache.set(key, details)
        return details

    def _candidate_row(self, source, database_id, content_type, rank, details, method, query, source_year):
        is_movie = content_type == Movie.MOVIE
        release_date = details.get("release_date") if is_movie else details.get("first_air_date")
        candidate_year = self._year_from_date(release_date)
        title = details.get("title") if is_movie else details.get("name")
        original_title = details.get("original_title") if is_movie else details.get("original_name")
        external_ids = details.get("external_ids") or {}
        candidate_imdb = external_ids.get("imdb_id") or (details.get("imdb_id") if is_movie else "")
        people = self._movie_directors(details) if is_movie else self._tv_creators_or_directors(details)
        cast = self._movie_cast(details) if is_movie else self._tv_cast(details)
        row = self._base_output(source, database_id, rank, method, "", search_query=query)
        row.update({
            "candidate_tmdb_id": details.get("id"), "candidate_type": content_type,
            "candidate_title": title, "candidate_original_title": original_title,
            "candidate_release_date": release_date, "candidate_year": candidate_year,
            "candidate_imdb_id": candidate_imdb, "candidate_director_or_creators": people,
            "candidate_cast": cast, "candidate_overview": details.get("overview"),
            "candidate_original_language": details.get("original_language"),
            "candidate_popularity": details.get("popularity"), "candidate_vote_average": details.get("vote_average"),
            "candidate_vote_count": details.get("vote_count"), "candidate_poster_path": details.get("poster_path"),
            "year_difference": abs(source_year - candidate_year) if source_year and candidate_year else "",
            "title_normalized_equal": str(self._normalize(source.get("title_en") or source.get("title_es")) == self._normalize(title)).lower(),
            "director_normalized_match": str(bool(self._normalize(source.get("director")) and self._normalize(source.get("director")) in {self._normalize(p) for p in people.split('; ') if p})).lower(),
            "imdb_exact_match": str(bool((source.get("imdb_id") or "").strip() and candidate_imdb and (source.get("imdb_id") or "").strip() == candidate_imdb)).lower(),
        })
        return row

    def _base_output(self, source, database_id, rank, method, error, search_query=""):
        return {"database_id": database_id, "source_type": source.get("type", ""), "source_title_en": source.get("title_en") or source.get("title_english", ""), "source_title_es": source.get("title_es") or source.get("title_spanish", ""), "source_year": source.get("year") or source.get("release_year", ""), "source_director": source.get("director", ""), "source_imdb_id": source.get("imdb_id", ""), "source_cast": source.get("cast") or source.get("cast_members", ""), "candidate_rank": rank, "match_method": method, "search_query": search_query, "review_decision": "", "approved_tmdb_id": "", "review_notes": "", "error_message": error}

    def _movie_directors(self, details):
        return "; ".join(dict.fromkeys(p.get("name", "") for p in (details.get("credits") or {}).get("crew", []) if p.get("job") == "Director" and p.get("name")))

    def _tv_creators_or_directors(self, details):
        creators = [p.get("name", "") for p in details.get("created_by", []) if p.get("name")]
        if creators:
            return "; ".join(dict.fromkeys(creators))
        crew = (details.get("aggregate_credits") or {}).get("crew", [])
        return "; ".join(dict.fromkeys(p.get("name", "") for p in crew if any(j.get("job") == "Director" for j in p.get("jobs", [])) and p.get("name")))

    def _movie_cast(self, details):
        return "; ".join(p.get("name", "") for p in (details.get("credits") or {}).get("cast", [])[:10] if p.get("name"))

    def _tv_cast(self, details):
        return "; ".join(p.get("name", "") for p in (details.get("aggregate_credits") or {}).get("cast", [])[:10] if p.get("name"))

    def _map_type(self, value):
        normalized = self._normalize(value).replace(" ", "_")
        if normalized in self.MOVIE_ALIASES:
            return Movie.MOVIE
        if normalized in self.SERIES_ALIASES:
            return Movie.SERIES
        return None

    def _normalize(self, value):
        value = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii").lower()
        value = value.translate(str.maketrans("", "", string.punctuation))
        return re.sub(r"\s+", " ", value).strip()

    def _parse_positive_int(self, value):
        try:
            number = int(str(value or "").strip())
        except (TypeError, ValueError):
            return None
        return number if number >= 0 else None

    def _year_from_date(self, value):
        if not value:
            return None
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").year
        except ValueError:
            return None

    def _in_range(self, database_id, options):
        return not ((options["min_database_id"] is not None and database_id < options["min_database_id"]) or (options["max_database_id"] is not None and database_id > options["max_database_id"]))

    def _read_processed_ids(self, output_path):
        ids = set()
        with output_path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                database_id = self._parse_positive_int(row.get("database_id"))
                if database_id is not None:
                    ids.add(database_id)
        return ids

    def _clean_cell(self, value):
        if value is None:
            return ""
        return re.sub(r"[\r\n]+", " ", str(value)).strip()

    def _print_start(self, options, input_path, output_path):
        self.stdout.write("Starting TMDb review candidate export")
        for label, value in [("input", input_path), ("output", output_path), ("range", f"{options['min_database_id']}..{options['max_database_id']}"), ("start_row", options["start_row"]), ("limit", options["limit"]), ("max_candidates", options["max_candidates"]), ("language", options["language"]), ("resume", options["resume"]), ("overwrite", options["overwrite"])]:
            self.stdout.write(f"{label}: {value}")

    def _print_finish(self, stats, requests_made, first_id, last_id, started, output_path):
        self.stdout.write(self.style.SUCCESS("TMDb review candidate export finished"))
        self.stdout.write(f"rows_read: {stats['rows_read']}")
        self.stdout.write(f"skipped_before_start_row: {stats['skipped_before_start_row']}")
        self.stdout.write(f"out_of_range: {stats['out_of_range']}")
        self.stdout.write(f"skipped_by_resume: {stats['skipped_by_resume']}")
        self.stdout.write(f"processed_sources: {stats['processed_sources']}")
        self.stdout.write(f"candidates_generated: {stats['candidates']}")
        self.stdout.write(f"no_match_records: {stats['no_match']}")
        self.stdout.write(f"errors: {stats['errors']}")
        self.stdout.write(f"approx_requests_made: {requests_made}")
        self.stdout.write(f"first_database_id_processed: {first_id}")
        self.stdout.write(f"last_database_id_processed: {last_id}")
        self.stdout.write(f"duration_seconds: {time.monotonic() - started:.2f}")
        self.stdout.write(f"csv_absolute_path: {output_path.resolve()}")
