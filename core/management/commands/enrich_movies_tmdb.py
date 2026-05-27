import time
import gzip
import json
import re
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def handle(self, *args, **options):
        workers = max(1, options["workers"])
        sleep_seconds = max(0.0, options["sleep"])
        started_at = timezone.now()

        qs = Movie.objects.filter(imdb_id__isnull=False).exclude(imdb_id="").only(
            "id", "imdb_id", "tmdb_id", "title_english", "title_spanish", "release_year", "type", "image", "synopsis", "synopsis_es", "tmdb_lookup_status", "tmdb_lookup_error", "tmdb_lookup_checked_at",
        ).order_by("id")
        if options["start_id"] is not None:
            qs = qs.filter(id__gte=options["start_id"])
        if options["movie_id"] is not None:
            qs = qs.filter(id=options["movie_id"])
        if options["use_existing_tmdb_id_only"]:
            qs = qs.filter(tmdb_id__isnull=False)
        if options["only_missing_image"]:
            qs = qs.filter(Q(image__isnull=True) | Q(image=""))
        if options["only_missing_synopsis"]:
            qs = qs.filter(Q(synopsis__isnull=True) | Q(synopsis="") | Q(synopsis_es__isnull=True) | Q(synopsis_es=""))
        if options["only_missing_tmdb_id"]:
            qs = qs.filter(tmdb_id__isnull=True)
        if options["limit"]:
            qs = qs[: options["limit"]]

        movies = list(qs)
        stats = defaultdict(int)
        stats["eligible_with_tmdb_id"] = len([m for m in movies if m.tmdb_id])
        updates = []
        updated_images = 0
        updated_synopsis = 0
        updated_synopsis_es = 0

        def process(movie):
            result = {"movie": movie, "updates": {}, "stats": defaultdict(int), "error": None, "warning": None}
            try:
                content_kind = self._resolve_content_kind(movie.type)
                if not content_kind:
                    result["stats"]["skipped"] += 1
                    return result
                has_image = self._has_value(movie.image)
                has_synopsis_en = self._has_value(movie.synopsis)
                has_synopsis_es = self._has_value(movie.synopsis_es)

                wants_image = not options["only_missing_tmdb_id"]
                wants_synopsis = not options["only_missing_tmdb_id"]

                if options["only_missing_image"]:
                    wants_synopsis = False
                if options["only_missing_synopsis"]:
                    wants_image = False

                needs_image = wants_image and (options["overwrite_image"] or not has_image)
                needs_synopsis = wants_synopsis and (options["overwrite_synopsis"] or not has_synopsis_en)
                needs_synopsis_es = wants_synopsis and (options["overwrite_synopsis"] or not has_synopsis_es)

                if options["only_missing_image"] and has_image and not options["overwrite_image"]:
                    needs_image = False
                    result["stats"]["requests_skipped_image"] += 1
                if options["only_missing_synopsis"] and has_synopsis_en and not options["overwrite_synopsis"]:
                    needs_synopsis = False
                    result["stats"]["requests_skipped_synopsis_en"] += 1
                if options["only_missing_synopsis"] and has_synopsis_es and not options["overwrite_synopsis"]:
                    needs_synopsis_es = False
                    result["stats"]["requests_skipped_synopsis_es"] += 1

                if not (needs_image or needs_synopsis or needs_synopsis_es):
                    if has_image and has_synopsis_en and has_synopsis_es:
                        result["stats"]["skipped_already_complete"] += 1
                    result["stats"]["skipped"] += 1
                    result["stats"]["requests_saved"] += 2
                    return result
                tmdb_id = movie.tmdb_id
                if not tmdb_id:
                    result["stats"]["skipped_missing_tmdb_id"] += 1
                    result["stats"]["skipped"] += 1
                    return result
                detail_en = None
                detail_es = None
                if needs_image or needs_synopsis:
                    detail_en = self._get_tmdb_json_with_retries(result["stats"], f"/{content_kind}/{tmdb_id}", params={"language": "en-US"})
                    result["stats"]["detail_requests_en"] += 1
                    if sleep_seconds:
                        time.sleep(sleep_seconds)
                if needs_synopsis_es:
                    detail_es = self._get_tmdb_json_with_retries(result["stats"], f"/{content_kind}/{tmdb_id}", params={"language": "es-ES"})
                    result["stats"]["detail_requests_es"] += 1
                    if sleep_seconds:
                        time.sleep(sleep_seconds)
                else:
                    result["stats"]["requests_saved"] += 1
                if needs_image and detail_en:
                    poster_path = detail_en.get("poster_path")
                    if poster_path:
                        result["updates"]["image"] = f"{self.IMAGE_BASE_URL}/{poster_path.lstrip('/')}"
                if needs_synopsis and detail_en:
                    ov = (detail_en.get("overview") or "").strip()
                    if ov:
                        result["updates"]["synopsis"] = ov
                if needs_synopsis_es and detail_es:
                    ov = (detail_es.get("overview") or "").strip()
                    if ov:
                        result["updates"]["synopsis_es"] = ov
                if not (needs_image or needs_synopsis):
                    result["stats"]["requests_saved"] += 1
                if not result["updates"]:
                    result["stats"]["skipped"] += 1
            except Exception as exc:
                result["error"] = str(exc)
                result["stats"]["errors"] += 1
            return result

        if workers == 1:
            results = [process(m) for m in movies]
        else:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(process, m) for m in movies]
                results = [f.result() for f in as_completed(futures)]

        for r in results:
            for k, v in r["stats"].items():
                stats[k] += v
            movie = r["movie"]
            if r["error"]:
                movie.tmdb_lookup_status = "error"
                movie.tmdb_lookup_error = r["error"][:255]
                movie.tmdb_lookup_checked_at = timezone.now()
                updates.append(movie)
                continue
            if r["updates"]:
                if "image" in r["updates"]:
                    updated_images += 1
                if "synopsis" in r["updates"]:
                    updated_synopsis += 1
                if "synopsis_es" in r["updates"]:
                    updated_synopsis_es += 1
                for f, val in r["updates"].items():
                    setattr(movie, f, val)
                updates.append(movie)

        if not options["dry_run"] and updates:
            Movie.objects.bulk_update(updates, ["image", "synopsis", "synopsis_es", "tmdb_lookup_status", "tmdb_lookup_error", "tmdb_lookup_checked_at"], batch_size=500)

        elapsed_seconds = max(1e-6, (timezone.now() - started_at).total_seconds())
        total_requests = stats["requests_realizadas"]
        first_processed_id = movies[0].id if movies else None
        last_processed_id = movies[-1].id if movies else None
        next_start_id = (last_processed_id + 1) if last_processed_id is not None else None

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Procesadas: {len(movies)}")
        self.stdout.write(f"eligible_with_tmdb_id: {stats['eligible_with_tmdb_id']}")
        self.stdout.write(f"skipped_missing_tmdb_id: {stats['skipped_missing_tmdb_id']}")
        self.stdout.write(f"Imágenes actualizadas: {updated_images}")
        self.stdout.write(f"images_updated: {updated_images}")
        self.stdout.write(f"Synopsis actualizadas: {updated_synopsis}")
        self.stdout.write(f"synopsis_updated: {updated_synopsis}")
        self.stdout.write(f"Synopsis_es actualizadas: {updated_synopsis_es}")
        self.stdout.write(f"synopsis_es_updated: {updated_synopsis_es}")
        self.stdout.write(f"detail_requests_en: {stats['detail_requests_en']}")
        self.stdout.write(f"detail_requests_es: {stats['detail_requests_es']}")
        self.stdout.write(f"skipped_already_complete: {stats['skipped_already_complete']}")
        self.stdout.write(f"requests_saved: {stats['requests_saved']}")
        self.stdout.write(f"requests_skipped_image: {stats['requests_skipped_image']}")
        self.stdout.write(f"requests_skipped_synopsis_en: {stats['requests_skipped_synopsis_en']}")
        self.stdout.write(f"requests_skipped_synopsis_es: {stats['requests_skipped_synopsis_es']}")
        self.stdout.write(f"total_requests: {total_requests}")
        self.stdout.write(f"registros_por_minuto: {len(movies)*60/elapsed_seconds:.2f}")
        self.stdout.write(f"requests_por_minuto: {total_requests*60/elapsed_seconds:.2f}")
        self.stdout.write(f"first_processed_id: {first_processed_id}")
        self.stdout.write(f"last_processed_id: {last_processed_id}")
        self.stdout.write(f"next_start_id sugerido: {next_start_id}")
        self.stdout.write(f"errores: {stats['errors']}")

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
                        {"tmdb_id": row.get("id"), "title": title, "popularity": row.get("popularity", 0.0), "media_type": media_type}
                    )
        for media_type in ("movie", "tv"):
            for normalized_title in index[media_type]:
                index[media_type][normalized_title].sort(key=lambda item: item.get("popularity") or 0.0, reverse=True)
        return index

    def _normalize_title(self, title):
        value = unicodedata.normalize("NFKD", title or "").encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()

    def _has_value(self, value):
        return bool((value or "").strip())
