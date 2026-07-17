from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Movie
from ._csv_utils import delimiter_label, open_csv_dict_reader


class Command(BaseCommand):
    help = "Limpia sólo contenido derivado de TMDb para películas indicadas en un CSV, sin tocar tmdb_id ni imdb_id."

    CHUNK_SIZE = 1000
    FIELDS_TO_CLEAR = (
        "image",
        "synopsis",
        "synopsis_es",
        "trailer_es_key",
        "trailer_en_key",
        "trailer_checked_at",
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--affected-csv",
            required=True,
            help="CSV con columna movie_id o database_id que identifica las películas a limpiar.",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Guarda cambios reales. Por defecto sólo muestra un dry-run.",
        )

    def handle(self, *args, **options):
        movie_ids = self._load_movie_ids(options["affected_csv"])
        if not movie_ids:
            raise CommandError("El CSV no contiene movie_id/database_id válidos.")

        queryset = Movie.objects.filter(id__in=movie_ids).only("id", *self.FIELDS_TO_CLEAR).order_by("id")

        if options["apply"]:
            self._apply(queryset)
        else:
            self._dry_run(queryset)

    def _load_movie_ids(self, csv_path):
        path = Path(csv_path)
        if not path.exists():
            raise CommandError(f"No se encontró el CSV: {path}")

        movie_ids = []
        seen = set()
        with open_csv_dict_reader(path) as (reader, delimiter):
            self.stdout.write(f"Detected CSV delimiter: {delimiter_label(delimiter)} ({delimiter})")
            fieldnames = set(reader.fieldnames or [])
            if "movie_id" in fieldnames:
                id_column = "movie_id"
            elif "database_id" in fieldnames:
                id_column = "database_id"
            else:
                raise CommandError("El CSV debe contener una columna movie_id o database_id.")

            for row_number, row in enumerate(reader, start=2):
                movie_id = self._parse_movie_id(row.get(id_column), row_number)
                if movie_id is None or movie_id in seen:
                    continue
                seen.add(movie_id)
                movie_ids.append(movie_id)
        return movie_ids

    def _parse_movie_id(self, value, row_number):
        raw_value = (value or "").strip()
        if not raw_value:
            return None
        try:
            movie_id = int(raw_value)
        except ValueError:
            raise CommandError(f"ID inválido en fila {row_number}: {raw_value!r}")
        if movie_id <= 0:
            raise CommandError(f"ID inválido en fila {row_number}: {raw_value!r}")
        return movie_id

    def _dry_run(self, queryset):
        processed = 0
        for movie in queryset.iterator(chunk_size=self.CHUNK_SIZE):
            processed += 1
            self.stdout.write(f"Movie {movie.id}")
            for field in self.FIELDS_TO_CLEAR:
                self.stdout.write(field)
            self.stdout.write("WOULD BE CLEARED")
            self.stdout.write("")
        self.stdout.write("Dry run only. Add --apply to persist changes.")
        self.stdout.write("Movies processed:")
        self.stdout.write(str(processed))
        self.stdout.write("Movies updated:")
        self.stdout.write("0")
        self.stdout.write("Errors:")
        self.stdout.write("0")

    def _apply(self, queryset):
        processed = 0
        updated = 0
        errors = 0
        batch = []

        for movie in queryset.iterator(chunk_size=self.CHUNK_SIZE):
            processed += 1
            try:
                movie.image = None
                movie.synopsis = ""
                movie.synopsis_es = None
                movie.trailer_es_key = None
                movie.trailer_en_key = None
                movie.trailer_checked_at = None
                batch.append(movie)
            except Exception as exc:  # Defensive per-row accounting; database errors are handled per batch.
                errors += 1
                self.stderr.write(self.style.ERROR(f"Movie {movie.id}: {exc}"))

            if len(batch) >= self.CHUNK_SIZE:
                batch_updated, batch_errors = self._flush_batch(batch)
                updated += batch_updated
                errors += batch_errors
                batch = []

            if processed % 100 == 0:
                self.stdout.write(f"Processed {processed}...")

        if batch:
            batch_updated, batch_errors = self._flush_batch(batch)
            updated += batch_updated
            errors += batch_errors

        self.stdout.write("Movies processed:")
        self.stdout.write(str(processed))
        self.stdout.write("Movies updated:")
        self.stdout.write(str(updated))
        self.stdout.write("Errors:")
        self.stdout.write(str(errors))

    def _flush_batch(self, batch):
        try:
            with transaction.atomic():
                Movie.objects.bulk_update(batch, self.FIELDS_TO_CLEAR, batch_size=self.CHUNK_SIZE)
        except Exception as exc:
            self.stderr.write(self.style.ERROR(f"Error actualizando lote: {exc}"))
            return 0, len(batch)
        return len(batch), 0
