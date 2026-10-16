import csv
import time
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, transaction
from django.db.models import Q
from django.utils import timezone

from core.models import Movie
from core.tmdb import TMDbServiceError, get_tmdb_json
from ._csv_utils import delimiter_label, open_csv_dict_reader


class Command(BaseCommand):
    help = "Enriquece image y sinopsis (EN/ES) usando exclusivamente tmdb_id existente."

    IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500"
    ERROR_CSV_FIELDS = (
        "movie_id", "tmdb_id", "type", "title_english", "release_year",
        "error_type", "status_code", "error_message",
    )

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=None)
        parser.add_argument("--start-id", type=int, default=None)
        parser.add_argument("--movie-id", type=int, default=None)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--errors-csv", type=str, default=None,
            help="CSV acumulativo de errores de --sync-rating-votes (tambien en dry-run).",
        )
        parser.add_argument(
            "--sync-rating-votes", action="store_true",
            help="Sincroniza solo external_rating y external_votes para registros con tmdb_id.",
        )
        parser.add_argument("--sleep", type=float, default=0.25)
        parser.add_argument("--workers", type=int, default=1)
        parser.add_argument("--overwrite-image", action="store_true")
        parser.add_argument("--overwrite-synopsis", action="store_true")
        parser.add_argument("--only-missing-image", action="store_true")
        parser.add_argument("--only-missing-synopsis", action="store_true")
        parser.add_argument("--backfill-missing-images", action="store_true")
        parser.add_argument("--use-existing-tmdb-id-only", action="store_true",
                            help="Compatibilidad: usar TMDB existente es ahora el único modo.")
        parser.add_argument("--quiet-warnings", action="store_true")
        parser.add_argument("--verify-persistence", action="store_true")
        parser.add_argument(
            "--affected-csv",
            type=str,
            default=None,
            help="CSV con movie_id/status/new_tmdb_id para enriquecer únicamente registros reparados.",
        )
        parser.add_argument(
            "--resume-from-csv",
            action="store_true",
            help="En modo --affected-csv, respeta --start-id como punto de reanudación del CSV.",
        )

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

        sync_rating_votes = options["sync_rating_votes"]
        self._errors_csv_path = None
        self._seen_csv_errors = set()
        if options["errors_csv"]:
            if not sync_rating_votes:
                raise CommandError("--errors-csv requiere --sync-rating-votes.")
            self._prepare_errors_csv(options["errors_csv"])
        if not sync_rating_votes and options["affected_csv"] and not options["backfill_missing_images"]:
            return self._handle_affected_csv(options, stats, sleep_seconds, started_at)

        movies = list(self._get_movies_queryset(options))
        if not sync_rating_votes and options["backfill_missing_images"]:
            stats["backfill_missing_image_candidates"] = len(movies)
        stats["eligible_with_tmdb_id"] = len([m for m in movies if m.tmdb_id])
        updates = []
        updated_images = 0
        updated_synopsis = 0
        updated_synopsis_es = 0
        updated_tmdb_ids = 0

        for movie in movies:
            if sync_rating_votes:
                result = self._sync_rating_votes(movie, options, stats, sleep_seconds)
            else:
                result = self._enrich_movie(movie, options, stats, sleep_seconds)
            if result["error"]:
                if sync_rating_votes:
                    continue
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
            "image",
            "synopsis",
            "synopsis_es",
            "tmdb_lookup_status",
            "tmdb_lookup_error",
            "tmdb_lookup_checked_at",
        ]
        if sync_rating_votes:
            update_fields = ["external_rating", "external_votes"]
        if not options["dry_run"] and updates:
            if sync_rating_votes and options["errors_csv"]:
                self._persist_rating_votes_with_errors(updates, update_fields, options, stats)
            else:
                Movie.objects.bulk_update(updates, update_fields, batch_size=500)
            if options["verify_persistence"] and not (sync_rating_votes and options["errors_csv"]):
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
        self.stdout.write(
            f"backfill_missing_image_candidates: {stats['backfill_missing_image_candidates']}"
        )
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
        self.stdout.write(f"persistence_mismatches: {stats['persistence_mismatches']}")
        self.stdout.write(f"workers_forced_to_main_thread: {stats['workers_forced_to_main_thread']}")
        self.stdout.write(f"errores: {stats['errors']}")
        if sync_rating_votes:
            self.stdout.write(f"ratings_updated: {stats['ratings_updated']}")
            self.stdout.write(f"votes_updated: {stats['votes_updated']}")
            self.stdout.write(f"unchanged: {stats['unchanged']}")
            self.stdout.write(f"skipped: {stats['skipped']}")
            self.stdout.write(f"dry_run: {options['dry_run']}")

    def _sync_rating_votes(self, movie, options, stats, sleep_seconds):
        result = {"updates": {}, "error": None}
        content_kind = self._resolve_content_kind(movie.type)
        if not content_kind:
            stats["skipped"] += 1
            return result
        try:
            stats["detail_requests_en"] += 1
            detail = self._get_tmdb_json_with_retries(
                stats, f"/{content_kind}/{movie.tmdb_id}", params={"language": "en-US"}
            )
            rating = detail.get("vote_average")
            if rating is not None:
                rating = Decimal(str(rating)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            votes = detail.get("vote_count")
            votes = 0 if votes is None else int(votes)
            for field, value, counter in (
                ("external_rating", rating, "ratings_updated"),
                ("external_votes", votes, "votes_updated"),
            ):
                if getattr(movie, field) != value:
                    result["updates"][field] = value
                    stats[counter] += 1
            if not result["updates"]:
                stats["unchanged"] += 1
            prefix = "DRY-RUN " if options["dry_run"] else ""
            self.stdout.write(
                f"{prefix}Movie(id={movie.id}) tmdb_id={movie.tmdb_id}: "
                f"rating {movie.external_rating} -> {rating}; "
                f"votes {movie.external_votes} -> {votes}"
            )
        except TMDbServiceError as exc:
            stats["errors"] += 1
            result["error"] = str(exc)
            self._record_sync_error(movie, "tmdb", exc)
            self.stderr.write(
                f"Movie(id={movie.id}) tmdb_id={movie.tmdb_id}: TMDB error: {exc}"
            )
        finally:
            if sleep_seconds:
                time.sleep(sleep_seconds)
        return result

    def _prepare_errors_csv(self, filename):
        path = Path(filename)
        try:
            if path.exists() and path.stat().st_size:
                with path.open("r", newline="", encoding="utf-8-sig") as stream:
                    reader = csv.DictReader(stream)
                    if reader.fieldnames != list(self.ERROR_CSV_FIELDS):
                        raise CommandError("Cabecera incompatible en --errors-csv.")
                    for row in reader:
                        self._seen_csv_errors.add(self._csv_error_key(row))
            else:
                with path.open("a", newline="", encoding="utf-8") as stream:
                    csv.DictWriter(stream, fieldnames=self.ERROR_CSV_FIELDS).writeheader()
        except OSError as exc:
            raise CommandError(f"No se puede abrir --errors-csv: {exc}") from exc
        self._errors_csv_path = path

    def _csv_error_key(self, row):
        return tuple(str(row.get(field, "")) for field in (
            "movie_id", "tmdb_id", "error_type", "status_code", "error_message"
        ))

    def _record_sync_error(self, movie, error_type, exc):
        if self._errors_csv_path is None:
            return
        row = {
            "movie_id": movie.id, "tmdb_id": movie.tmdb_id, "type": movie.type,
            "title_english": movie.title_english, "release_year": movie.release_year,
            "error_type": error_type, "status_code": getattr(exc, "status_code", None) or "",
            "error_message": str(exc),
        }
        key = self._csv_error_key(row)
        if key in self._seen_csv_errors:
            return
        try:
            with self._errors_csv_path.open("a", newline="", encoding="utf-8") as stream:
                csv.DictWriter(stream, fieldnames=self.ERROR_CSV_FIELDS).writerow(row)
        except OSError as error:
            raise CommandError(f"No se puede agregar el error a --errors-csv: {error}") from error
        self._seen_csv_errors.add(key)

    def _persist_rating_votes_with_errors(self, updates, update_fields, options, stats):
        for offset in range(0, len(updates), 500):
            chunk = updates[offset:offset + 500]
            try:
                with transaction.atomic():
                    Movie.objects.bulk_update(chunk, update_fields, batch_size=500)
                persisted_movies = chunk
            except DatabaseError:
                # The failed chunk was rolled back. Retry each row to isolate its error.
                persisted_movies = []
                for movie in chunk:
                    try:
                        with transaction.atomic():
                            Movie.objects.bulk_update([movie], update_fields, batch_size=1)
                        persisted_movies.append(movie)
                    except DatabaseError as exc:
                        stats["errors"] += 1
                        self._record_sync_error(movie, "persistence", exc)
                        self.stderr.write(f"Movie(id={movie.id}) tmdb_id={movie.tmdb_id}: persistence error: {exc}")
            if options["verify_persistence"]:
                for movie in persisted_movies:
                    try:
                        with transaction.atomic():
                            persisted = Movie.objects.only(*update_fields).get(pk=movie.pk)
                        if any(getattr(persisted, field) != getattr(movie, field) for field in update_fields):
                            stats["persistence_mismatches"] += 1
                            raise ValueError("Persisted rating/votes differ from requested values")
                    except (DatabaseError, Movie.DoesNotExist, ValueError) as exc:
                        stats["errors"] += 1
                        self._record_sync_error(movie, "verification", exc)
                        self.stderr.write(f"Movie(id={movie.id}) tmdb_id={movie.tmdb_id}: verification error: {exc}")

    def _handle_affected_csv(self, options, stats, sleep_seconds, started_at):
        affected_ids = self._read_affected_csv_movie_ids(options)
        stats["csv_valid_rows"] = len(affected_ids)
        stats["eligible_with_tmdb_id"] = Movie.objects.filter(id__in=affected_ids, tmdb_id__isnull=False).count()

        csv_options = options.copy()

        updates = []
        processed = 0
        updated_images = 0
        updated_synopsis = 0
        updated_synopsis_es = 0
        found_ids = set()
        first_processed_id = None
        last_processed_id = None
        chunk_size = 500

        for index in range(0, len(affected_ids), chunk_size):
            chunk_ids = affected_ids[index : index + chunk_size]
            movies_by_id = {
                movie.id: movie
                for movie in self._get_affected_csv_movies_queryset(chunk_ids)
            }
            found_ids.update(movies_by_id.keys())
            stats["movie_id_not_found"] += len(set(chunk_ids) - set(movies_by_id.keys()))
            for movie_id in chunk_ids:
                movie = movies_by_id.get(movie_id)
                if movie is None:
                    continue
                processed += 1
                if first_processed_id is None:
                    first_processed_id = movie.id
                last_processed_id = movie.id
                result = self._enrich_movie(movie, csv_options, stats, sleep_seconds)
                if result["error"]:
                    movie.tmdb_lookup_status = "error"
                    movie.tmdb_lookup_error = result["error"][:255]
                    movie.tmdb_lookup_checked_at = timezone.now()
                    updates.append(movie)
                    if processed % 1000 == 0:
                        self._write_affected_csv_progress(
                            stats, processed, updated_images, updated_synopsis, updated_synopsis_es
                        )
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
                    updates.append(movie)
                if processed % 1000 == 0:
                    self._write_affected_csv_progress(stats, processed, updated_images, updated_synopsis, updated_synopsis_es)

            if not options["dry_run"] and updates:
                Movie.objects.bulk_update(
                    updates,
                    [
                        "image",
                        "synopsis",
                        "synopsis_es",
                        "tmdb_lookup_status",
                        "tmdb_lookup_error",
                        "tmdb_lookup_checked_at",
                    ],
                    batch_size=chunk_size,
                )
                updates = []

        elapsed_seconds = max(1e-6, (timezone.now() - started_at).total_seconds())
        total_requests = stats["requests_realizadas"]
        next_start_id = (last_processed_id + 1) if last_processed_id is not None else None

        self.stdout.write(self.style.SUCCESS("Proceso CSV finalizado."))
        self.stdout.write(f"csv_valid_rows: {stats['csv_valid_rows']}")
        self.stdout.write(f"Procesadas: {processed}")
        self.stdout.write(f"eligible_with_tmdb_id: {stats['eligible_with_tmdb_id']}")
        self.stdout.write(f"skipped_missing_tmdb_id: {stats['skipped_missing_tmdb_id']}")
        self.stdout.write("tmdb_ids_updated: 0")
        self.stdout.write(f"Imágenes actualizadas: {updated_images}")
        self.stdout.write(f"images_updated: {updated_images}")
        self.stdout.write(f"Synopsis actualizadas: {updated_synopsis}")
        self.stdout.write(f"synopsis_updated: {updated_synopsis}")
        self.stdout.write(f"Synopsis_es actualizadas: {updated_synopsis_es}")
        self.stdout.write(f"synopsis_es_updated: {updated_synopsis_es}")
        self.stdout.write(f"movie_id_not_found: {stats['movie_id_not_found']}")
        self.stdout.write(f"detail_requests_en: {stats['detail_requests_en']}")
        self.stdout.write(f"detail_requests_es: {stats['detail_requests_es']}")
        self.stdout.write(f"requests_realizadas: {total_requests}")
        self.stdout.write(f"total_requests: {total_requests}")
        self.stdout.write(f"registros_por_minuto: {processed*60/elapsed_seconds:.2f}")
        self.stdout.write(f"requests_por_minuto: {total_requests*60/elapsed_seconds:.2f}")
        self.stdout.write(f"first_processed_id: {first_processed_id}")
        self.stdout.write(f"last_processed_id: {last_processed_id}")
        self.stdout.write(f"next_start_id sugerido: {next_start_id}")
        self.stdout.write(f"errores: {stats['errors']}")

    def _read_affected_csv_movie_ids(self, options):
        affected_ids = []
        seen = set()
        with open_csv_dict_reader(options["affected_csv"]) as (reader, delimiter):
            self.stdout.write(f"Detected CSV delimiter: {delimiter_label(delimiter)} ({delimiter})")
            for row in reader:
                if (row.get("status") or "").strip() != "repaired":
                    continue
                if not self._valid_csv_tmdb_id(row.get("new_tmdb_id")):
                    continue
                movie_id = self._valid_csv_movie_id(row.get("movie_id"))
                if movie_id is None:
                    continue
                if options["start_id"] is not None and movie_id < options["start_id"]:
                    continue
                if movie_id in seen:
                    continue
                seen.add(movie_id)
                affected_ids.append(movie_id)
                if options["limit"] and len(affected_ids) >= options["limit"]:
                    break
        return affected_ids

    def _get_affected_csv_movies_queryset(self, movie_ids):
        return Movie.objects.filter(id__in=movie_ids).only(
            "id",
            "tmdb_id",
            "title_english",
            "title_spanish",
            "release_year",
            "director",
            "cast_members",
            "type",
            "image",
            "synopsis",
            "synopsis_es",
            "tmdb_lookup_status",
            "tmdb_lookup_error",
            "tmdb_lookup_checked_at",
        ).order_by("id")

    def _valid_csv_movie_id(self, value):
        try:
            movie_id = int((value or "").strip())
        except (TypeError, ValueError):
            return None
        return movie_id if movie_id > 0 else None

    def _valid_csv_tmdb_id(self, value):
        try:
            tmdb_id = int(str(value or "").strip())
        except (TypeError, ValueError):
            return False
        return tmdb_id > 0

    def _write_affected_csv_progress(self, stats, processed, updated_images, updated_synopsis, updated_synopsis_es):
        self.stdout.write(
            "Progreso CSV: "
            f"filas_validas={stats['csv_valid_rows']} "
            f"procesados={processed} "
            f"imagenes_actualizadas={updated_images} "
            f"synopsis_en_actualizadas={updated_synopsis} "
            f"synopsis_es_actualizadas={updated_synopsis_es} "
            f"omitidos_sin_tmdb_id={stats['skipped_missing_tmdb_id']} "
            f"movie_id_no_encontrados={stats['movie_id_not_found']} "
            f"errores_api={stats['errors']}"
        )

    def _get_movies_queryset(self, options):
        if options["sync_rating_votes"]:
            qs = Movie.objects.filter(tmdb_id__gt=0).only(
                "id", "tmdb_id", "type", "external_rating", "external_votes"
            ).order_by("id")
            if options["errors_csv"]:
                qs = qs.only("id", "tmdb_id", "type", "external_rating", "external_votes",
                             "title_english", "release_year")
            if options["start_id"] is not None:
                qs = qs.filter(id__gte=options["start_id"])
            if options["movie_id"] is not None:
                qs = qs.filter(id=options["movie_id"])
            if options["limit"] is not None:
                qs = qs[:options["limit"]]
            return qs
        qs = Movie.objects.filter(tmdb_id__gt=0).only(
            "id",
            "tmdb_id",
            "title_english",
            "title_spanish",
            "release_year",
            "director",
            "cast_members",
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
        if options["backfill_missing_images"]:
            qs = qs.filter(tmdb_id__gt=0).filter(Q(image__isnull=True) | Q(image=""))

        if not options["backfill_missing_images"]:
            missing_filters = Q()
            if options["only_missing_image"]:
                missing_filters |= Q(image__isnull=True) | Q(image="")
            if options["only_missing_synopsis"]:
                missing_filters |= (
                    Q(synopsis__isnull=True)
                    | Q(synopsis="")
                    | Q(synopsis_es__isnull=True)
                    | Q(synopsis_es="")
                )
            if missing_filters:
                qs = qs.filter(missing_filters)
        if options["limit"]:
            qs = qs[: options["limit"]]
        return qs

    def _enrich_movie(self, movie, options, stats, sleep_seconds):
        result = {"updates": {}, "error": None}
        try:
            content_kind = self._resolve_content_kind(movie.type)
            if not content_kind:
                stats["skipped"] += 1
                return result

            backfill_missing_images = options["backfill_missing_images"]
            wants_synopsis = (
                options["overwrite_synopsis"]
                or options["only_missing_synopsis"]
                or not options["only_missing_image"]
            )
            needs_image = (options["overwrite_image"] or not movie.image)
            needs_synopsis = wants_synopsis and (options["overwrite_synopsis"] or not movie.synopsis)
            needs_synopsis_es = wants_synopsis and (options["overwrite_synopsis"] or not movie.synopsis_es)
            if options["only_missing_image"]:
                needs_image = not movie.image
            if options["only_missing_synopsis"]:
                needs_synopsis = not movie.synopsis
                needs_synopsis_es = not movie.synopsis_es

            if backfill_missing_images:
                needs_image = not movie.image
                needs_synopsis = not movie.synopsis
                needs_synopsis_es = not movie.synopsis_es

            tmdb_id = movie.tmdb_id
            if not tmdb_id:
                stats["skipped_missing_tmdb_id"] += 1
                stats["skipped"] += 1
                return result

            if not (needs_image or needs_synopsis or needs_synopsis_es):
                stats["skipped"] += 1
                return result

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
