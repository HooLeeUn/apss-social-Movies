import csv
from collections import defaultdict
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

    EMPTY_EQUIVALENTS = {"n/a", "na", "none", "null", ""}
    LOG_SAMPLE_LIMIT = 20
    BULK_UPDATE_BATCH_SIZE = 5000

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
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Muestra logs detallados por fila.",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv"])
        dry_run = options["dry_run"]
        verbose = options["verbose"]

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

        movie_index = self._build_movie_index(verbose=verbose)
        movies_to_update = []

        warning_samples = {
            "ignored_incomplete": 0,
            "no_match": 0,
            "multiple_matches": 0,
        }

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

                    csv_imdb_id = self._normalize_text(row.get("imdb_id"))
                    title_english = self._normalize_text(row.get("title_english"))
                    title_spanish = self._normalize_text(row.get("title_spanish"))
                    movie_type = self._normalize_text(row.get("type"))
                    release_year = self._normalize_year(row.get("release_year"))
                    first_director = self._normalize_first_director(row.get("director"))

                    if not csv_imdb_id or not title_english or not title_spanish or not movie_type:
                        counters["ignored_incomplete"] += 1
                        if verbose or warning_samples["ignored_incomplete"] < self.LOG_SAMPLE_LIMIT:
                            warning_samples["ignored_incomplete"] += 1
                            missing_fields = []
                            if not csv_imdb_id:
                                missing_fields.append("imdb_id")
                            if not title_english:
                                missing_fields.append("title_english")
                            if not title_spanish:
                                missing_fields.append("title_spanish")
                            if not movie_type:
                                missing_fields.append("type")

                            self.stdout.write(
                                self.style.WARNING(
                                    f"Fila {row_number}: ignorada por datos incompletos. "
                                    f"Campos faltantes: {missing_fields}. "
                                    f"Valores: title_english='{title_english}', "
                                    f"title_spanish='{title_spanish}', type='{movie_type}', "
                                    f"release_year='{release_year}', director='{first_director}', "
                                    f"imdb_id='{csv_imdb_id}'"
                                )
                            )
                        continue

                    key = (title_english, title_spanish, movie_type, release_year, first_director)
                    matches = movie_index.get(key, [])

                    match_count = len(matches)
                    if match_count == 0:
                        counters["no_match"] += 1
                        if verbose or warning_samples["no_match"] < self.LOG_SAMPLE_LIMIT:
                            warning_samples["no_match"] += 1
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Fila {row_number}: sin coincidencia para "
                                    f"'{title_english}' / '{title_spanish}' ({release_year}) "
                                    f"type='{movie_type}' director='{first_director}'."
                                )
                            )
                        continue

                    if match_count > 1:
                        counters["multiple_matches"] += 1
                        if verbose or warning_samples["multiple_matches"] < self.LOG_SAMPLE_LIMIT:
                            warning_samples["multiple_matches"] += 1
                            self.stdout.write(
                                self.style.WARNING(
                                    f"Fila {row_number}: {match_count} coincidencias para "
                                    f"'{title_english}' / '{title_spanish}' ({release_year}) "
                                    f"type='{movie_type}' director='{first_director}'."
                                )
                            )
                        continue

                    movie = matches[0]
                    if movie.imdb_id == csv_imdb_id:
                        counters["already_correct"] += 1
                        continue

                    old_imdb_id = movie.imdb_id
                    movie.imdb_id = csv_imdb_id

                    if dry_run:
                        counters["updated"] += 1
                        if verbose:
                            self.stdout.write(
                                self.style.SUCCESS(
                                    f"Fila {row_number}: DRY-RUN actualizaría Movie(id={movie.id}) "
                                    f"imdb_id '{old_imdb_id}' -> '{csv_imdb_id}'."
                                )
                            )
                        continue

                    movies_to_update.append(movie)
                    counters["updated"] += 1
                    if verbose:
                        self.stdout.write(
                            self.style.SUCCESS(
                                f"Fila {row_number}: preparado Movie(id={movie.id}) "
                                f"imdb_id '{old_imdb_id}' -> '{csv_imdb_id}'."
                            )
                        )

                if dry_run:
                    transaction.set_rollback(True)
                elif movies_to_update:
                    Movie.objects.bulk_update(
                        movies_to_update,
                        ["imdb_id"],
                        batch_size=self.BULK_UPDATE_BATCH_SIZE,
                    )

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Filas leídas: {counters['rows_read']}")
        self.stdout.write(f"Actualizadas: {counters['updated']}")
        self.stdout.write(f"Ya tenían el imdb_id correcto: {counters['already_correct']}")
        self.stdout.write(f"Sin coincidencia: {counters['no_match']}")
        self.stdout.write(f"Coincidencias múltiples: {counters['multiple_matches']}")
        self.stdout.write(f"Filas ignoradas por datos incompletos: {counters['ignored_incomplete']}")

    def _build_movie_index(self, verbose=False):
        if verbose:
            self.stdout.write(self.style.NOTICE("Cargando películas e indexando en memoria..."))

        index = defaultdict(list)
        queryset = Movie.objects.only(
            "id",
            "imdb_id",
            "title_english",
            "title_spanish",
            "type",
            "release_year",
            "director",
        ).iterator(chunk_size=10000)

        count = 0
        for movie in queryset:
            key = (
                self._normalize_text(movie.title_english),
                self._normalize_text(movie.title_spanish),
                self._normalize_text(movie.type),
                self._normalize_year(movie.release_year),
                self._normalize_first_director(movie.director),
            )
            index[key].append(movie)
            count += 1

        self.stdout.write(
            self.style.NOTICE(
                f"Índice en memoria construido: {count} películas en {len(index)} claves."
            )
        )
        return index

    @classmethod
    def _normalize_text(cls, value):
        if value is None:
            return None
        cleaned = str(value).strip()
        if cleaned.lower() in cls.EMPTY_EQUIVALENTS:
            return None
        return cleaned or None

    @classmethod
    def _normalize_year(cls, value):
        normalized = cls._normalize_text(value)
        if normalized is None:
            return None
        try:
            return int(normalized)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _normalize_first_director(cls, director):
        normalized = cls._normalize_text(director)
        if normalized is None:
            return None
        first_director = normalized.split(",")[0].strip()
        return cls._normalize_text(first_director)
