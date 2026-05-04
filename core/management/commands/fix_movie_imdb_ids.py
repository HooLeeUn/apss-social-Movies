import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Movie


class Command(BaseCommand):
    help = (
        "Corrige Movie.imdb_id usando un CSV maestro y matching por campos de identidad "
        "(sin usar imdb_id actual)."
    )

    REQUIRED_COLUMNS = {
        "imdb_id",
        "title_english",
        "title_spanish",
        "type",
        "genre",
        "release_year",
        "director",
        "cast_members",
        "external_rating",
        "external_votes",
        "synopsis",
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--csv",
            required=True,
            help="Ruta al CSV maestro.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula los cambios sin persistir en base de datos.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"])
        dry_run = options["dry_run"]

        if not csv_path.exists() or not csv_path.is_file():
            raise CommandError(f"El archivo CSV no existe o no es válido: {csv_path}")

        counters = {
            "rows_read": 0,
            "updated": 0,
            "already_correct": 0,
            "no_match": 0,
            "multiple_matches": 0,
            "ignored_incomplete": 0,
        }

        self.stdout.write(self.style.NOTICE(f"Procesando CSV: {csv_path}"))
        if dry_run:
            self.stdout.write(self.style.WARNING("Modo dry-run activado: no se guardarán cambios."))

        with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            header = set(reader.fieldnames or [])
            missing = self.REQUIRED_COLUMNS - header
            if missing:
                raise CommandError(
                    "El CSV no contiene todas las columnas esperadas. "
                    f"Faltan: {', '.join(sorted(missing))}"
                )

            with transaction.atomic():
                for row_number, row in enumerate(reader, start=2):
                    counters["rows_read"] += 1

                    csv_imdb_id = self._clean_text(row.get("imdb_id"))
                    title_english = self._clean_text(row.get("title_english"))
                    title_spanish = self._clean_text(row.get("title_spanish"))
                    movie_type = self._clean_text(row.get("type"))
                    release_year = self._parse_year(row.get("release_year"))
                    director = self._clean_text(row.get("director"))

                    if (
                        not csv_imdb_id
                        or not title_english
                        or not movie_type
                        or release_year is None
                        or director is None
                    ):
                        counters["ignored_incomplete"] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                "Fila "
                                f"{row_number}: ignorada por datos incompletos "
                                "(imdb_id/title_english/type/release_year/director)."
                            )
                        )
                        continue

                    matches = Movie.objects.filter(
                        title_english=title_english,
                        title_spanish=title_spanish,
                        type=movie_type,
                        release_year=release_year,
                        director=director,
                    )

                    match_count = matches.count()
                    if match_count == 0:
                        counters["no_match"] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: sin coincidencia para "
                                f"'{title_english}' ({release_year}) type='{movie_type}'."
                            )
                        )
                        continue

                    if match_count > 1:
                        counters["multiple_matches"] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: {match_count} coincidencias para "
                                f"'{title_english}' ({release_year}) type='{movie_type}'."
                            )
                        )
                        continue

                    movie = matches.first()
                    if movie.imdb_id == csv_imdb_id:
                        counters["already_correct"] += 1
                        continue

                    old_imdb_id = movie.imdb_id
                    movie.imdb_id = csv_imdb_id

                    if dry_run:
                        counters["updated"] += 1
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"Fila {row_number}: DRY-RUN actualizaría Movie(id={movie.id}) "
                                f"imdb_id '{old_imdb_id}' -> '{csv_imdb_id}'."
                            )
                        )
                        continue

                    movie.save(update_fields=["imdb_id", "updated_at"])
                    counters["updated"] += 1
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"Fila {row_number}: actualizado Movie(id={movie.id}) "
                            f"imdb_id '{old_imdb_id}' -> '{csv_imdb_id}'."
                        )
                    )

                if dry_run:
                    transaction.set_rollback(True)

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Filas leídas: {counters['rows_read']}")
        self.stdout.write(f"Actualizadas: {counters['updated']}")
        self.stdout.write(f"Ya tenían el imdb_id correcto: {counters['already_correct']}")
        self.stdout.write(f"Sin coincidencia: {counters['no_match']}")
        self.stdout.write(f"Coincidencias múltiples: {counters['multiple_matches']}")
        self.stdout.write(f"Filas ignoradas por datos incompletos: {counters['ignored_incomplete']}")

    @staticmethod
    def _clean_text(value):
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    @staticmethod
    def _parse_year(raw_year):
        value = Command._clean_text(raw_year)
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
