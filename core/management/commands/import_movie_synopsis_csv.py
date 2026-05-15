import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Movie


DEFAULT_BATCH_SIZE = 1000
REQUIRED_COLUMNS = {"imdb_id", "synopsis"}


class Command(BaseCommand):
    help = "Importa sinopsis desde un CSV local a Movie.synopsis usando Movie.imdb_id."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Ruta al archivo synopsis.csv a importar.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Muestra qué haría sin guardar cambios.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help=f"Cantidad de filas a procesar por lote (default: {DEFAULT_BATCH_SIZE}).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Procesa como máximo N filas de datos del CSV.",
        )
        parser.add_argument(
            "--start-row",
            type=int,
            default=1,
            help="Fila de datos desde la que empezar (1 = primera fila después del encabezado).",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Permite reemplazar synopsis existente. Por defecto no se sobrescribe.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        dry_run = options["dry_run"]
        batch_size = max(1, options["batch_size"])
        limit = options["limit"]
        start_row = max(1, options["start_row"])
        overwrite = options["overwrite"]

        if limit is not None and limit < 0:
            raise CommandError("--limit debe ser un número mayor o igual a 0.")

        if not csv_path.exists():
            raise CommandError(f"No existe el archivo CSV: {csv_path}")

        stats = {
            "total_rows": 0,
            "updated": 0,
            "already_had_synopsis": 0,
            "not_found": 0,
            "empty_synopsis": 0,
            "read_errors": 0,
        }
        batch = []
        dry_run_updated_movie_ids = set()

        self.stdout.write(
            self.style.NOTICE(
                f"Iniciando importación CSV de sinopsis desde: {csv_path} "
                f"(batch-size={batch_size}, start-row={start_row}, limit={limit}, "
                f"dry-run={dry_run}, overwrite={overwrite})."
            )
        )

        with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file, delimiter=";")
            header = set(reader.fieldnames or [])
            missing = REQUIRED_COLUMNS - header
            if missing:
                raise CommandError(
                    "El CSV no contiene todas las columnas requeridas. "
                    f"Faltan: {', '.join(sorted(missing))}"
                )

            for data_row_number, row in enumerate(reader, start=1):
                if data_row_number < start_row:
                    continue
                if limit is not None and stats["total_rows"] >= limit:
                    break

                stats["total_rows"] += 1
                try:
                    imdb_id = self._clean_text(row.get("imdb_id"))
                    synopsis = self._clean_text(row.get("synopsis"))
                    if not synopsis:
                        stats["empty_synopsis"] += 1
                        continue
                    batch.append({"imdb_id": imdb_id, "synopsis": synopsis})
                except (AttributeError, csv.Error, TypeError, ValueError):
                    stats["read_errors"] += 1
                    continue

                if len(batch) >= batch_size:
                    self._flush_batch(batch, stats, dry_run, overwrite, dry_run_updated_movie_ids)
                    batch.clear()

        if batch:
            self._flush_batch(batch, stats, dry_run, overwrite, dry_run_updated_movie_ids)

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run activo: no se guardaron cambios."))

        self.stdout.write(self.style.SUCCESS(f"Total filas leídas: {stats['total_rows']}"))
        self.stdout.write(self.style.SUCCESS(f"Actualizadas: {stats['updated']}"))
        self.stdout.write(
            self.style.SUCCESS(f"Saltadas porque ya tenían synopsis: {stats['already_had_synopsis']}")
        )
        self.stdout.write(self.style.SUCCESS(f"No encontradas por imdb_id: {stats['not_found']}"))
        self.stdout.write(self.style.SUCCESS(f"Saltadas por synopsis vacía: {stats['empty_synopsis']}"))
        self.stdout.write(self.style.SUCCESS(f"Errores de lectura: {stats['read_errors']}"))

    def _flush_batch(self, rows, stats, dry_run, overwrite, dry_run_updated_movie_ids):
        imdb_ids = {row["imdb_id"] for row in rows if row["imdb_id"]}
        movies_by_imdb_id = {
            movie.imdb_id: movie
            for movie in Movie.objects.filter(imdb_id__in=imdb_ids).only("id", "imdb_id", "synopsis")
        }
        movies_to_update = []
        queued_movie_ids = set()

        for row in rows:
            imdb_id = row["imdb_id"]
            movie = movies_by_imdb_id.get(imdb_id)
            if movie is None:
                stats["not_found"] += 1
                continue

            already_queued = movie.id in queued_movie_ids or movie.id in dry_run_updated_movie_ids
            if already_queued:
                if overwrite:
                    movie.synopsis = row["synopsis"]
                continue
            if not overwrite and self._clean_text(movie.synopsis):
                stats["already_had_synopsis"] += 1
                continue

            movie.synopsis = row["synopsis"]
            movies_to_update.append(movie)
            queued_movie_ids.add(movie.id)
            stats["updated"] += 1
            if dry_run:
                dry_run_updated_movie_ids.add(movie.id)

        if dry_run or not movies_to_update:
            return

        with transaction.atomic():
            Movie.objects.bulk_update(movies_to_update, ["synopsis"], batch_size=len(movies_to_update))

    @staticmethod
    def _clean_text(value):
        if value is None:
            return ""
        return str(value).strip()
