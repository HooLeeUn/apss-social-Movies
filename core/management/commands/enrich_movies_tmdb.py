import time

from django.core.management.base import BaseCommand
from django.db.models import Q

from core.models import Movie
from core.tmdb import TMDbServiceError, get_tmdb_json


class Command(BaseCommand):
    help = "Enriquece Movie con tmdb_id, image y sinopsis (EN/ES) usando imdb_id y TMDb."

    IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"

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
        parser.add_argument("--quiet-warnings", action="store_true")

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
        quiet_warnings = options["quiet_warnings"]

        qs = Movie.objects.filter(imdb_id__isnull=False).exclude(imdb_id="").order_by("id")
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
            qs = qs.filter(Q(tmdb_id__isnull=True) | Q(tmdb_id=""))
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
            "first_processed_id": None,
            "last_processed_id": None,
        }

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

                needs_tmdb_id = not movie.tmdb_id
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
                    find_result = get_tmdb_json(
                        f"/find/{movie.imdb_id}",
                        params={"external_source": "imdb_id"},
                    )
                    time.sleep(sleep_seconds)
                    match = self._extract_match(find_result, content_kind)
                    if not match:
                        stats["skipped"] += 1
                        self._warn(
                            quiet_warnings,
                            stats,
                            f"Movie(id={movie.id}) sin resultado TMDb compatible para imdb_id={movie.imdb_id}",
                        )
                        continue
                    tmdb_id = match.get("id")
                    if tmdb_id and not movie.tmdb_id:
                        movie.tmdb_id = tmdb_id
                        updates.append("tmdb_id")
                        stats["tmdb_id_updated"] += 1

                if not tmdb_id:
                    stats["skipped"] += 1
                    continue

                if needs_image:
                    source = self._extract_match(find_result, content_kind) if find_result else None
                    poster_path = source.get("poster_path") if source else None
                    if not poster_path:
                        detail = get_tmdb_json(f"/{content_kind}/{tmdb_id}", params={"language": "en-US"})
                        time.sleep(sleep_seconds)
                        poster_path = detail.get("poster_path")
                    if poster_path:
                        movie.image = f"{self.IMAGE_BASE_URL}/{poster_path.lstrip('/')}"
                        if "image" not in updates:
                            updates.append("image")
                            stats["image_updated"] += 1

                if needs_synopsis or needs_synopsis_es:
                    if needs_synopsis:
                        detail_en = get_tmdb_json(f"/{content_kind}/{tmdb_id}", params={"language": "en-US"})
                        time.sleep(sleep_seconds)
                        overview_en = (detail_en.get("overview") or "").strip()
                        if overview_en:
                            movie.synopsis = overview_en
                            updates.append("synopsis") if "synopsis" not in updates else None
                            stats["synopsis_updated"] += 1

                    if needs_synopsis_es:
                        detail_es = get_tmdb_json(f"/{content_kind}/{tmdb_id}", params={"language": "es-ES"})
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
                self.stdout.write(self.style.ERROR(f"Movie(id={movie.id}) error TMDb: {exc}"))
            except Exception as exc:  # noqa: BLE001
                stats["errors"] += 1
                self.stdout.write(self.style.ERROR(f"Movie(id={movie.id}) error inesperado: {exc}"))

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Procesadas: {stats['processed']}")
        self.stdout.write(f"tmdb_id actualizados: {stats['tmdb_id_updated']}")
        self.stdout.write(f"Imágenes actualizadas: {stats['image_updated']}")
        self.stdout.write(f"Synopsis actualizadas: {stats['synopsis_updated']}")
        self.stdout.write(f"Synopsis_es actualizadas: {stats['synopsis_es_updated']}")
        self.stdout.write(f"Omitidas: {stats['skipped']}")
        self.stdout.write(f"Warnings: {stats['warnings']}")
        self.stdout.write(f"Errores: {stats['errors']}")
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
