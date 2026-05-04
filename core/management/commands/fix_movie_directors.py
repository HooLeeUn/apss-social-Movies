import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Movie


class Command(BaseCommand):
    help = (
        "Corrige Movie.director usando un CSV maestro y matching exacto por campos "
        "de identidad (sin usar imdb_id ni director para buscar)."
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

    MATCH_FIELDS = (
        "title_english",
        "title_spanish",
        "type",
        "genre",
        "release_year",
    )

    def add_arguments(self, parser):
        parser.add_argument("--csv", required=True, help="Ruta al CSV maestro.")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Simula los cambios sin persistir en base de datos.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Imprime el detalle fila por fila (comportamiento anterior).",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"])
        dry_run = options["dry_run"]
        verbose = options["verbose"]
        max_examples = 20

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

        examples = {
            "updated": [],
            "no_match": [],
            "multiple_matches": [],
            "ignored_incomplete": [],
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

                    title_english = self._clean_text(row.get("title_english"))
                    title_spanish = self._clean_text(row.get("title_spanish"))
                    movie_type = self._clean_text(row.get("type"))
                    genre = self._clean_text(row.get("genre"))
                    release_year = self._parse_year(row.get("release_year"))
                    csv_director = self._clean_text(row.get("director"))

                    missing_fields = []
                    if not title_english:
                        missing_fields.append("title_english")
                    if not title_spanish:
                        missing_fields.append("title_spanish")
                    if not movie_type:
                        missing_fields.append("type")
                    if not genre:
                        missing_fields.append("genre")
                    if release_year is None:
                        missing_fields.append("release_year")
                    if not csv_director:
                        missing_fields.append("director")

                    if missing_fields:
                        counters["ignored_incomplete"] += 1
                        message = (
                            f"Fila {row_number}: ignorada por datos incompletos. "
                            f"Campos faltantes: {missing_fields}. "
                            f"Valores match: title_english='{title_english}', "
                            f"title_spanish='{title_spanish}', type='{movie_type}', "
                            f"genre='{genre}', release_year='{release_year}'. "
                            f"director_csv='{csv_director}'"
                        )
                        if verbose:
                            self.stdout.write(self.style.WARNING(message))
                        elif len(examples["ignored_incomplete"]) < max_examples:
                            examples["ignored_incomplete"].append(message)
                        continue

                    matches = Movie.objects.filter(
                        title_english=title_english,
                        title_spanish=title_spanish,
                        type=movie_type,
                        genre=genre,
                        release_year=release_year,
                    )

                    match_count = matches.count()
                    lookup_values = (
                        f"title_english='{title_english}', title_spanish='{title_spanish}', "
                        f"type='{movie_type}', genre='{genre}', release_year='{release_year}'"
                    )

                    if match_count == 0:
                        counters["no_match"] += 1
                        message = f"Fila {row_number}: sin coincidencia. Búsqueda: {lookup_values}."
                        if verbose:
                            self.stdout.write(self.style.WARNING(message))
                        elif len(examples["no_match"]) < max_examples:
                            examples["no_match"].append(message)
                        continue

                    if match_count > 1:
                        counters["multiple_matches"] += 1
                        message = (
                            f"Fila {row_number}: {match_count} coincidencias múltiples. "
                            f"Búsqueda: {lookup_values}."
                        )
                        if verbose:
                            self.stdout.write(self.style.WARNING(message))
                        elif len(examples["multiple_matches"]) < max_examples:
                            examples["multiple_matches"].append(message)
                        continue

                    movie = matches.first()
                    old_director = movie.director

                    if old_director == csv_director:
                        counters["already_correct"] += 1
                        if verbose:
                            self.stdout.write(
                                self.style.NOTICE(
                                    f"Fila {row_number}: director ya correcto para Movie(id={movie.id}). "
                                    f"Búsqueda: {lookup_values}. director='{old_director}'."
                                )
                            )
                        continue

                    movie.director = csv_director

                    if dry_run:
                        counters["updated"] += 1
                        message = (
                            f"Fila {row_number}: DRY-RUN actualizaría Movie(id={movie.id}). "
                            f"Búsqueda: {lookup_values}. "
                            f"director anterior='{old_director}' -> nuevo='{csv_director}'."
                        )
                        if verbose:
                            self.stdout.write(self.style.SUCCESS(message))
                        elif len(examples["updated"]) < max_examples:
                            examples["updated"].append(message)
                        continue

                    movie.save(update_fields=["director", "updated_at"])
                    counters["updated"] += 1
                    message = (
                        f"Fila {row_number}: actualizado Movie(id={movie.id}). "
                        f"Búsqueda: {lookup_values}. "
                        f"director anterior='{old_director}' -> nuevo='{csv_director}'."
                    )
                    if verbose:
                        self.stdout.write(self.style.SUCCESS(message))
                    elif len(examples["updated"]) < max_examples:
                        examples["updated"].append(message)

                if dry_run:
                    transaction.set_rollback(True)

        if not verbose:
            self._print_examples("Ejemplos de actualizaciones propuestas", examples["updated"], max_examples)
            self._print_examples("Ejemplos de filas sin coincidencia", examples["no_match"], max_examples)
            self._print_examples("Ejemplos de filas con múltiples coincidencias", examples["multiple_matches"], max_examples)
            self._print_examples("Ejemplos de filas ignoradas por datos incompletos", examples["ignored_incomplete"], max_examples)

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Filas leídas: {counters['rows_read']}")
        self.stdout.write(f"Actualizadas: {counters['updated']}")
        self.stdout.write(f"Ya tenían director correcto: {counters['already_correct']}")
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

    def _print_examples(self, title, messages, max_examples):
        if not messages:
            return
        self.stdout.write(self.style.NOTICE(f"{title} (máximo {max_examples}):"))
        for message in messages:
            self.stdout.write(f"- {message}")
