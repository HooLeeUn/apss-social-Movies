import time
import gzip
import json
import re
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
        limit = options["limit"]
        start_id = options["start_id"]
        movie_id = options["movie_id"]
        dry_run = options["dry_run"]
        sleep_seconds = max(0.0, options["sleep"])
        overwrite_image = options["overwrite_image"]
        overwrite_synopsis = options["overwrite_synopsis"]
        only_missing_image = options["only_missing_image"]
        only_missing_synopsis = options["only_missing_synopsis"]
        only_missing_tmdb_id = options["only_missing_tmdb_id"]
        use_existing_tmdb_id_only = options["use_existing_tmdb_id_only"]
        retry_not_found = options["retry_not_found"]
        retry_errors = options["retry_errors"]
        quiet_warnings = options["quiet_warnings"]
        use_local_exports = options["use_local_exports"]
        exports_dir = options["exports_dir"]
        local_only = options["local_only"]
        started_at = timezone.now()

        qs = (
            Movie.objects.filter(imdb_id__isnull=False)
            .exclude(imdb_id="")
            .only(
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
            )
            .order_by("id")
        )
        if start_id is not None:
            qs = qs.filter(id__gte=start_id)
        if movie_id is not None:
            qs = qs.filter(id=movie_id)
        if only_missing_image:
            qs = qs.filter(Q(image__isnull=True) | Q(image=""))
        if only_missing_synopsis:
            qs = qs.filter(
                Q(synopsis__isnull=True)
                | Q(synopsis="")
                | Q(synopsis_es__isnull=True)
                | Q(synopsis_es="")
            )
        if only_missing_tmdb_id:
            qs = qs.filter(tmdb_id__isnull=True)
            if not retry_not_found:
                qs = qs.exclude(tmdb_lookup_status="not_found")
            if not retry_errors:
                qs = qs.exclude(tmdb_lookup_status="error")
        if limit:
            qs = qs[:limit]

        stats = {
            "processed": 0,
            "tmdb_id_updated": 0,
            "image_updated": 0,
            "synopsis_updated": 0,
            "synopsis_es_updated": 0,
            "skipped": 0,
            "errors": 0,
            "warnings": 0,
            "found": 0,
            "not_found": 0,
            "skipped_not_found": 0,
            "requests_realizadas": 0,
            "details_requests_realizadas": 0,
            "skipped_missing_tmdb_id": 0,
            "local_candidates_found": 0,
            "local_matches_saved": 0,
            "local_only_skipped": 0,
            "api_requests_avoided": 0,
            "first_processed_id": None,
            "last_processed_id": None,
        }
        local_index = None
        if use_local_exports:
            local_index = self._build_local_export_index(exports_dir)

        for movie in qs.iterator(chunk_size=500):
            stats["processed"] += 1
            if stats["first_processed_id"] is None:
                stats["first_processed_id"] = movie.id
            stats["last_processed_id"] = movie.id
            updates = []

            try:
                content_kind = self._resolve_content_kind(movie.type)
                if content_kind is None:
                    stats["skipped"] += 1
                    self._warn(
                        quiet_warnings,
                        stats,
                        f"Movie(id={movie.id}) type no soportado: {movie.type}",
                    )
                    continue

                needs_tmdb_id = (not use_existing_tmdb_id_only) and (not movie.tmdb_id)
                needs_image = (not only_missing_tmdb_id) and (overwrite_image or not movie.image)
                needs_synopsis = (not only_missing_tmdb_id) and (overwrite_synopsis or not movie.synopsis)
                needs_synopsis_es = (not only_missing_tmdb_id) and (overwrite_synopsis or not movie.synopsis_es)

                if only_missing_image and movie.image:
                    needs_image = False
                if only_missing_synopsis and movie.synopsis and movie.synopsis_es:
                    needs_synopsis = False
                    needs_synopsis_es = False

                if not (needs_tmdb_id or needs_image or needs_synopsis or needs_synopsis_es):
                    stats["skipped"] += 1
                    continue

                tmdb_id = movie.tmdb_id
                find_result = None
                if not tmdb_id:
                    if use_existing_tmdb_id_only:
                        stats["skipped_missing_tmdb_id"] += 1
                        stats["skipped"] += 1
                        continue
                    # Flujo optimizado: primero usar exports locales para estimar candidatos por título/año.
                    local_candidate = self._get_local_candidate(local_index, movie, content_kind)
                    if local_candidate:
                        stats["local_candidates_found"] += 1
                        tmdb_id = local_candidate["tmdb_id"]
                        stats["api_requests_avoided"] += 1
                        # Validación final: se consulta detalle real del candidato para confirmar y extraer metadata.
                    else:
                        if local_only and use_local_exports:
                            stats["local_only_skipped"] += 1
                            stats["api_requests_avoided"] += 1
                            stats["skipped"] += 1
                            self._warn(
                                quiet_warnings,
                                stats,
                                f"Movie(id={movie.id}) sin candidato local confiable (modo local-only).",
                            )
                            continue
                        # Fallback API tradicional (lógica existente): lookup por imdb_id en /find.
                        find_result = self._get_tmdb_json_with_retries(
                            stats,
                            f"/find/{movie.imdb_id}",
                            params={"external_source": "imdb_id"},
                        )
                        time.sleep(sleep_seconds)
                        match = self._extract_match(find_result, content_kind)
                        if not match:
                            movie.tmdb_lookup_status = "not_found"
                            movie.tmdb_lookup_checked_at = timezone.now()
                            updates.extend(["tmdb_lookup_status", "tmdb_lookup_checked_at"])
                            stats["not_found"] += 1
                            if only_missing_tmdb_id:
                                stats["skipped_not_found"] += 1
                            stats["skipped"] += 1
                            self._warn(
                                quiet_warnings,
                                stats,
                                f"Movie(id={movie.id}) sin resultado TMDb compatible para imdb_id={movie.imdb_id}",
                            )
                            continue
                        tmdb_id = match.get("id")
                        if tmdb_id:
                            stats["found"] += 1
                    if tmdb_id and not movie.tmdb_id:
                        movie.tmdb_id = tmdb_id
                        movie.tmdb_lookup_status = "found"
                        movie.tmdb_lookup_error = ""
                        movie.tmdb_lookup_checked_at = timezone.now()
                        updates.append("tmdb_id")
                        updates.extend(["tmdb_lookup_status", "tmdb_lookup_error", "tmdb_lookup_checked_at"])
                        stats["tmdb_id_updated"] += 1
                        if use_local_exports and local_candidate:
                            stats["local_matches_saved"] += 1
                        stats["found"] += 1

                if not tmdb_id:
                    stats["skipped"] += 1
                    continue

                if needs_image:
                    source = self._extract_match(find_result, content_kind) if find_result else None
                    poster_path = source.get("poster_path") if source else None
                    if not poster_path:
                        detail = self._get_tmdb_json_with_retries(
                            stats, f"/{content_kind}/{tmdb_id}", params={"language": "en-US"}
                        )
                        stats["details_requests_realizadas"] += 1
                        time.sleep(sleep_seconds)
                        poster_path = detail.get("poster_path")
                    if poster_path:
                        movie.image = f"{self.IMAGE_BASE_URL}/{poster_path.lstrip('/')}"
                        if "image" not in updates:
                            updates.append("image")
                            stats["image_updated"] += 1

                if needs_synopsis or needs_synopsis_es:
                    if needs_synopsis:
                        detail_en = self._get_tmdb_json_with_retries(
                            stats, f"/{content_kind}/{tmdb_id}", params={"language": "en-US"}
                        )
                        stats["details_requests_realizadas"] += 1
                        time.sleep(sleep_seconds)
                        overview_en = (detail_en.get("overview") or "").strip()
                        if overview_en:
                            movie.synopsis = overview_en
                            updates.append("synopsis") if "synopsis" not in updates else None
                            stats["synopsis_updated"] += 1

                    if needs_synopsis_es:
                        detail_es = self._get_tmdb_json_with_retries(
                            stats, f"/{content_kind}/{tmdb_id}", params={"language": "es-ES"}
                        )
                        stats["details_requests_realizadas"] += 1
                        time.sleep(sleep_seconds)
                        overview_es = (detail_es.get("overview") or "").strip()
                        if overview_es:
                            movie.synopsis_es = overview_es
                            updates.append("synopsis_es") if "synopsis_es" not in updates else None
                            stats["synopsis_es_updated"] += 1

                if not updates:
                    stats["skipped"] += 1
                    continue

                if not dry_run:
                    movie.save(update_fields=sorted(set(updates)))
            except TMDbServiceError as exc:
                stats["errors"] += 1
                if not dry_run:
                    movie.tmdb_lookup_status = "error"
                    movie.tmdb_lookup_error = str(exc)[:255]
                    movie.tmdb_lookup_checked_at = timezone.now()
                    movie.save(update_fields=["tmdb_lookup_status", "tmdb_lookup_error", "tmdb_lookup_checked_at"])
                self.stdout.write(self.style.ERROR(f"Movie(id={movie.id}) error TMDb: {exc}"))
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                if not dry_run:
                    movie.tmdb_lookup_status = "error"
                    movie.tmdb_lookup_error = str(exc)[:255]
                    movie.tmdb_lookup_checked_at = timezone.now()
                    movie.save(update_fields=["tmdb_lookup_status", "tmdb_lookup_error", "tmdb_lookup_checked_at"])
                self.stdout.write(self.style.ERROR(f"Movie(id={movie.id}) error inesperado: {exc}"))

        elapsed_seconds = max(1e-6, (timezone.now() - started_at).total_seconds())
        avg_per_minute = stats["processed"] * 60 / elapsed_seconds
        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Procesadas: {stats['processed']}")
        self.stdout.write(f"tmdb_id actualizados: {stats['tmdb_id_updated']}")
        self.stdout.write(f"Imágenes actualizadas: {stats['image_updated']}")
        self.stdout.write(f"images_updated: {stats['image_updated']}")
        self.stdout.write(f"Synopsis actualizadas: {stats['synopsis_updated']}")
        self.stdout.write(f"synopsis_updated: {stats['synopsis_updated']}")
        self.stdout.write(f"Synopsis_es actualizadas: {stats['synopsis_es_updated']}")
        self.stdout.write(f"synopsis_es_updated: {stats['synopsis_es_updated']}")
        self.stdout.write(f"Omitidas: {stats['skipped']}")
        self.stdout.write(f"Warnings: {stats['warnings']}")
        self.stdout.write(f"Errores: {stats['errors']}")
        self.stdout.write(f"found: {stats['found']}")
        self.stdout.write(f"not_found: {stats['not_found']}")
        self.stdout.write(f"errors: {stats['errors']}")
        self.stdout.write(f"skipped_not_found: {stats['skipped_not_found']}")
        self.stdout.write(f"local_candidates_found: {stats['local_candidates_found']}")
        self.stdout.write(f"local_matches_saved: {stats['local_matches_saved']}")
        self.stdout.write(f"local_only_skipped: {stats['local_only_skipped']}")
        self.stdout.write(f"api_requests_avoided: {stats['api_requests_avoided']}")
        self.stdout.write(f"requests_realizadas: {stats['requests_realizadas']}")
        self.stdout.write(f"details_requests_realizadas: {stats['details_requests_realizadas']}")
        self.stdout.write(f"skipped_missing_tmdb_id: {stats['skipped_missing_tmdb_id']}")
        self.stdout.write(f"tiempo_total: {elapsed_seconds:.2f}s")
        self.stdout.write(f"registros_por_minuto: {avg_per_minute:.2f}")
        self.stdout.write(f"promedio registros/minuto: {avg_per_minute:.2f}")
        self.stdout.write(f"first_processed_id: {stats['first_processed_id']}")
        self.stdout.write(f"last_processed_id: {stats['last_processed_id']}")
        next_start_id = (stats["last_processed_id"] + 1) if stats["last_processed_id"] is not None else None
        self.stdout.write(f"next_start_id sugerido: {next_start_id}")
        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run activo: no se guardaron cambios."))

    def _warn(self, quiet_warnings, stats, message):
        stats["warnings"] += 1
        if quiet_warnings:
            return
        self.stdout.write(self.style.WARNING(message))

    def _resolve_content_kind(self, movie_type):
        if movie_type == Movie.MOVIE:
            return "movie"
        if movie_type == Movie.SERIES:
            return "tv"
        return None

    def _extract_match(self, find_result, content_kind):
        if not isinstance(find_result, dict):
            return None
        result_key = "movie_results" if content_kind == "movie" else "tv_results"
        results = find_result.get(result_key) or []
        if not results:
            return None
        return results[0]

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
                index[media_type][normalized_title].sort(key=lambda item: item.get("popularity") or 0.0, reverse=True)
        return index

    def _normalize_title(self, title):
        value = unicodedata.normalize("NFKD", title or "").encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()

    def _get_local_candidate(self, local_index, movie, content_kind):
        if not local_index:
            return None
        title = (movie.title_english or movie.title_spanish or "").strip()
        if not title:
            return None
        normalized = self._normalize_title(title)
        candidates = local_index[content_kind].get(normalized) or []
        return candidates[0] if candidates else None
        if use_existing_tmdb_id_only:
            qs = qs.filter(tmdb_id__isnull=False)
