import gzip
import json
import re
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone

from core.models import Movie
from core.tmdb import TMDbServiceError, get_tmdb_json


class Command(BaseCommand):
    help = "Enriquece Movie con tmdb_id, image y sinopsis (EN/ES) usando imdb_id y TMDb."

    IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"
    EXPORT_FILENAME_PATTERN = re.compile(r"^(movie_ids|tv_series_ids)_\d{2}_\d{2}_\d{4}\.json\.gz$")

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--start-id", type=int, default=None)
        parser.add_argument("--movie-id", type=int, default=None)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--sleep", type=float, default=0.25)
        parser.add_argument("--workers", type=int, default=1)
        parser.add_argument("--overwrite-image", action="store_true")
        parser.add_argument("--overwrite-synopsis", action="store_true")
        parser.add_argument("--only-missing-image", action="store_true")
        parser.add_argument("--only-missing-synopsis", action="store_true")
        parser.add_argument("--only-missing-tmdb-id", action="store_true")
        parser.add_argument("--use-existing-tmdb-id-only", action="store_true")
        parser.add_argument("--retry-not-found", action="store_true")
        parser.add_argument("--retry-errors", action="store_true")
        parser.add_argument("--quiet-warnings", action="store_true")
        parser.add_argument("--use-local-exports", action="store_true")
        parser.add_argument("--exports-dir", type=str, default="tmdb_exports")
        parser.add_argument("--local-only", action="store_true")
        parser.add_argument("--verify-persistence", action="store_true")

    def handle(self, *args, **options):
        sleep_seconds = max(0.0, options["sleep"])
        started_at = timezone.now()
        stats = defaultdict(int)

        if max(1, options["workers"]) > 1:
            stats["workers_forced_to_main_thread"] = 1
            if not options["quiet_warnings"]:
                self.stdout.write(
                    self.style.WARNING(
                        "--workers > 1 deshabilitado temporalmente: se procesa y persiste en el thread principal."
                    )
                )

        local_index = None
        if options["use_local_exports"]:
            local_index = self._build_local_export_index(options["exports_dir"])

        movies = list(self._get_movies_queryset(options))
        stats["eligible_with_tmdb_id"] = len([m for m in movies if m.tmdb_id])
        updates = []
        updated_images = 0
        updated_synopsis = 0
        updated_synopsis_es = 0
        updated_tmdb_ids = 0

        for movie in movies:
            result = self._enrich_movie(movie, options, stats, sleep_seconds, local_index=local_index)
            if result["error"]:
                movie.tmdb_lookup_status = "error"
                movie.tmdb_lookup_error = result["error"][:255]
                movie.tmdb_lookup_checked_at = timezone.now()
                updates.append(movie)
                continue
            if result["updates"]:
                for field, value in result["updates"].items():
                    setattr(movie, field, value)
                if "image" in result["updates"] and movie.image:
                    updated_images += 1
                if "synopsis" in result["updates"] and movie.synopsis:
                    updated_synopsis += 1
                if "synopsis_es" in result["updates"] and movie.synopsis_es:
                    updated_synopsis_es += 1
                if "tmdb_id" in result["updates"] and movie.tmdb_id:
                    updated_tmdb_ids += 1
                updates.append(movie)

        update_fields = [
            "tmdb_id",
            "image",
            "synopsis",
            "synopsis_es",
            "tmdb_lookup_status",
            "tmdb_lookup_error",
            "tmdb_lookup_checked_at",
        ]
        if not options["dry_run"] and updates:
            Movie.objects.bulk_update(updates, update_fields, batch_size=500)
            if options["verify_persistence"]:
                for movie in updates:
                    persisted = Movie.objects.only(*update_fields).get(pk=movie.pk)
                    for field in update_fields:
                        if getattr(persisted, field) != getattr(movie, field):
                            stats["persistence_mismatches"] += 1
                            break

        elapsed_seconds = max(1e-6, (timezone.now() - started_at).total_seconds())
        total_requests = stats["requests_realizadas"]
        first_processed_id = movies[0].id if movies else None
        last_processed_id = movies[-1].id if movies else None
        next_start_id = (last_processed_id + 1) if last_processed_id is not None else None

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Procesadas: {len(movies)}")
        self.stdout.write(f"eligible_with_tmdb_id: {stats['eligible_with_tmdb_id']}")
        self.stdout.write(f"skipped_missing_tmdb_id: {stats['skipped_missing_tmdb_id']}")
        self.stdout.write(f"tmdb_ids_updated: {updated_tmdb_ids}")
        self.stdout.write(f"Imágenes actualizadas: {updated_images}")
        self.stdout.write(f"images_updated: {updated_images}")
        self.stdout.write(f"Synopsis actualizadas: {updated_synopsis}")
        self.stdout.write(f"synopsis_updated: {updated_synopsis}")
        self.stdout.write(f"Synopsis_es actualizadas: {updated_synopsis_es}")
        self.stdout.write(f"synopsis_es_updated: {updated_synopsis_es}")
        self.stdout.write(f"detail_requests_en: {stats['detail_requests_en']}")
        self.stdout.write(f"detail_requests_es: {stats['detail_requests_es']}")
        self.stdout.write(f"requests_realizadas: {total_requests}")
        self.stdout.write(f"total_requests: {total_requests}")
        self.stdout.write(f"registros_por_minuto: {len(movies)*60/elapsed_seconds:.2f}")
        self.stdout.write(f"requests_por_minuto: {total_requests*60/elapsed_seconds:.2f}")
        self.stdout.write(f"first_processed_id: {first_processed_id}")
        self.stdout.write(f"last_processed_id: {last_processed_id}")
        self.stdout.write(f"next_start_id sugerido: {next_start_id}")
        self.stdout.write(f"local_candidates_found: {stats['local_candidates_found']}")
        self.stdout.write(f"api_requests_avoided: {stats['api_requests_avoided']}")
        self.stdout.write(f"local_only_skipped: {stats['local_only_skipped']}")
        self.stdout.write(f"persistence_mismatches: {stats['persistence_mismatches']}")
        self.stdout.write(f"workers_forced_to_main_thread: {stats['workers_forced_to_main_thread']}")
        self.stdout.write(f"errores: {stats['errors']}")

    def _get_movies_queryset(self, options):
        qs = Movie.objects.all().only(
            "id",
            "imdb_id",
            "tmdb_id",
            "title_english",
            "title_spanish",
            "release_year",
            "type",
            "image",
            "synopsis",
            "synopsis_es",
            "tmdb_lookup_status",
            "tmdb_lookup_error",
            "tmdb_lookup_checked_at",
        ).order_by("id")
        if options["start_id"] is not None:
            qs = qs.filter(id__gte=options["start_id"])
        if options["movie_id"] is not None:
            qs = qs.filter(id=options["movie_id"])
        if options["use_existing_tmdb_id_only"]:
            qs = qs.filter(tmdb_id__isnull=False)
        elif options["only_missing_tmdb_id"]:
            qs = qs.filter(tmdb_id__isnull=True).exclude(imdb_id__isnull=True).exclude(imdb_id="")
            if not options["retry_not_found"]:
                qs = qs.exclude(tmdb_lookup_status="not_found")
            if not options["retry_errors"]:
                qs = qs.exclude(tmdb_lookup_status="error")
        else:
            qs = qs.filter(Q(tmdb_id__isnull=False) | (Q(imdb_id__isnull=False) & ~Q(imdb_id="")))

        missing_filters = Q()
        if options["only_missing_image"]:
            missing_filters |= Q(image__isnull=True) | Q(image="")
        if options["only_missing_synopsis"]:
            missing_filters |= Q(synopsis__isnull=True) | Q(synopsis="") | Q(synopsis_es__isnull=True) | Q(synopsis_es="")
        if missing_filters:
            qs = qs.filter(missing_filters)
        if options["limit"]:
            qs = qs[: options["limit"]]
        return qs

    def _enrich_movie(self, movie, options, stats, sleep_seconds, local_index=None):
        result = {"updates": {}, "error": None}
        try:
            content_kind = self._resolve_content_kind(movie.type)
            if not content_kind:
                stats["skipped"] += 1
                return result

            needs_tmdb_id = not movie.tmdb_id and not options["use_existing_tmdb_id_only"]
            if needs_tmdb_id and not self._can_retry_lookup(movie, options):
                stats["skipped"] += 1
                return result

            wants_metadata = not options["only_missing_tmdb_id"]
            wants_synopsis = wants_metadata and (
                options["overwrite_synopsis"]
                or options["only_missing_synopsis"]
                or not options["only_missing_image"]
            )
            needs_image = wants_metadata and (options["overwrite_image"] or not movie.image)
            needs_synopsis = wants_synopsis and (options["overwrite_synopsis"] or not movie.synopsis)
            needs_synopsis_es = wants_synopsis and (options["overwrite_synopsis"] or not movie.synopsis_es)
            if options["only_missing_image"]:
                needs_image = not movie.image
            if options["only_missing_synopsis"]:
                needs_synopsis = not movie.synopsis
                needs_synopsis_es = not movie.synopsis_es

            if options["only_missing_tmdb_id"]:
                needs_image = needs_synopsis = needs_synopsis_es = False

            tmdb_id = movie.tmdb_id
            find_candidate = None
            if needs_tmdb_id:
                find_candidate = self._resolve_tmdb_candidate(movie, content_kind, options, stats, local_index)
                if not find_candidate:
                    if not options["local_only"]:
                        result["updates"].update(
                            {
                                "tmdb_lookup_status": "not_found",
                                "tmdb_lookup_error": "",
                                "tmdb_lookup_checked_at": timezone.now(),
                            }
                        )
                    stats["skipped_missing_tmdb_id"] += 1
                    stats["skipped"] += 1
                    return result
                tmdb_id = find_candidate.get("tmdb_id") or find_candidate.get("id")
                if tmdb_id:
                    result["updates"].update(
                        {
                            "tmdb_id": tmdb_id,
                            "tmdb_lookup_status": "found",
                            "tmdb_lookup_error": "",
                            "tmdb_lookup_checked_at": timezone.now(),
                        }
                    )

            if not tmdb_id:
                stats["skipped_missing_tmdb_id"] += 1
                stats["skipped"] += 1
                return result

            if not (needs_tmdb_id or needs_image or needs_synopsis or needs_synopsis_es):
                stats["skipped"] += 1
                return result

            if needs_image and find_candidate and find_candidate.get("poster_path"):
                result["updates"]["image"] = self._poster_url(find_candidate["poster_path"])

            detail_en = None
            detail_es = None
            if needs_image or needs_synopsis:
                detail_en = self._get_tmdb_json_with_retries(
                    stats, f"/{content_kind}/{tmdb_id}", params={"language": "en-US"}
                )
                stats["detail_requests_en"] += 1
                if sleep_seconds:
                    time.sleep(sleep_seconds)
            if needs_synopsis_es:
                detail_es = self._get_tmdb_json_with_retries(
                    stats, f"/{content_kind}/{tmdb_id}", params={"language": "es-ES"}
                )
                stats["detail_requests_es"] += 1
                if sleep_seconds:
                    time.sleep(sleep_seconds)
            if needs_image and detail_en and detail_en.get("poster_path"):
                result["updates"]["image"] = self._poster_url(detail_en["poster_path"])
            if needs_synopsis and detail_en:
                overview = (detail_en.get("overview") or "").strip()
                if overview:
                    result["updates"]["synopsis"] = overview
            if needs_synopsis_es and detail_es:
                overview = (detail_es.get("overview") or "").strip()
                if overview:
                    result["updates"]["synopsis_es"] = overview
            if not result["updates"]:
                stats["skipped"] += 1
        except Exception as exc:
            result["error"] = str(exc)
            stats["errors"] += 1
        return result

    def _can_retry_lookup(self, movie, options):
        if movie.tmdb_lookup_status == "not_found" and not options["retry_not_found"]:
            return False
        if movie.tmdb_lookup_status == "error" and not options["retry_errors"]:
            return False
        return True

    def _resolve_tmdb_candidate(self, movie, content_kind, options, stats, local_index):
        local_candidate = self._find_local_candidate(movie, content_kind, local_index) if local_index is not None else None
        if local_candidate:
            stats["local_candidates_found"] += 1
            stats["api_requests_avoided"] += 1
            return local_candidate
        if options["local_only"]:
            stats["local_only_skipped"] += 1
            stats["api_requests_avoided"] += 1
            return None
        if not movie.imdb_id:
            return None
        data = self._get_tmdb_json_with_retries(
            stats,
            f"/find/{movie.imdb_id}",
            params={"external_source": "imdb_id"},
        )
        results_key = "movie_results" if content_kind == "movie" else "tv_results"
        candidates = data.get(results_key) or []
        if not candidates:
            return None
        candidate = candidates[0]
        return {"tmdb_id": candidate.get("id"), "poster_path": candidate.get("poster_path")}

    def _find_local_candidate(self, movie, content_kind, local_index):
        if local_index is None:
            return None
        titles = [movie.title_english, movie.title_spanish]
        for title in titles:
            normalized = self._normalize_title(title)
            if not normalized:
                continue
            candidates = local_index.get(content_kind, {}).get(normalized) or []
            if candidates:
                return candidates[0]
        return None

    def _poster_url(self, poster_path):
        return f"{self.IMAGE_BASE_URL}/{poster_path.lstrip('/')}"

    def _resolve_content_kind(self, movie_type):
        if movie_type == Movie.MOVIE:
            return "movie"
        if movie_type == Movie.SERIES:
            return "tv"
        return None

    def _get_tmdb_json_with_retries(self, stats, path, params=None, retries=3, backoff_seconds=0.4):
        last_error = None
        for attempt in range(1, retries + 1):
            try:
                stats["requests_realizadas"] += 1
                return get_tmdb_json(path, params=params)
            except TMDbServiceError as exc:
                last_error = exc
                if attempt >= retries:
                    break
                time.sleep(backoff_seconds * attempt)
        raise last_error

    def _build_local_export_index(self, exports_dir):
        export_dir = Path(exports_dir)
        index = {"movie": defaultdict(list), "tv": defaultdict(list)}
        files = sorted(export_dir.glob("*.json.gz"))
        for file_path in files:
            if not self.EXPORT_FILENAME_PATTERN.match(file_path.name):
                continue
            media_type = "movie" if file_path.name.startswith("movie_ids_") else "tv"
            with gzip.open(file_path, "rt", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    row = json.loads(line)
                    title = (row.get("original_title") or row.get("name") or "").strip()
                    if not title:
                        continue
                    normalized = self._normalize_title(title)
                    index[media_type][normalized].append(
                        {
                            "tmdb_id": row.get("id"),
                            "title": title,
                            "popularity": row.get("popularity", 0.0),
                            "media_type": media_type,
                        }
                    )
        for media_type in ("movie", "tv"):
            for normalized_title in index[media_type]:
                index[media_type][normalized_title].sort(
                    key=lambda item: item.get("popularity") or 0.0, reverse=True
                )
        return index

    def _normalize_title(self, title):
        value = unicodedata.normalize("NFKD", title or "").encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
