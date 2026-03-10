import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError

from core.models import Movie


class Command(BaseCommand):
    help = "Importa películas desde un CSV consolidado al modelo Movie."

    TYPE_MOVIE_ALIASES = {
        "movie",
        "film",
        "feature",
        "featurefilm",
        "tvmovie",
    }
    TYPE_SERIES_ALIASES = {
        "series",
        "tvseries",
        "tvminiseries",
        "miniseries",
        "show",
    }

    REQUIRED_COLUMNS = {
        "title_english",
        "title_spanish",
        "type",
        "genre",
        "release_year",
        "director",
        "cast_members",
        "external_rating",
    }

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Ruta al archivo CSV a importar.")
        parser.add_argument(
            "--author",
            default="admin",
            help="Username del autor que se asignará a todas las películas (default: admin).",
        )

    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"])
        author_username = options["author"]

        if not csv_path.exists() or not csv_path.is_file():
            raise CommandError(f"El archivo CSV no existe o no es válido: {csv_path}")

        user_model = get_user_model()
        try:
            author = user_model.objects.get(username=author_username)
        except user_model.DoesNotExist as exc:
            raise CommandError(
                f"No existe un usuario con username '{author_username}'. "
                "Crea el usuario o usa --author con uno existente."
            ) from exc

        total_rows = 0
        created_count = 0
        duplicate_count = 0
        error_count = 0
        seen_keys = set()

        self.stdout.write(self.style.NOTICE(f"Iniciando importación desde: {csv_path}"))
        self.stdout.write(self.style.NOTICE(f"Autor asignado: {author.username}"))

        with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            header = set(reader.fieldnames or [])
            missing = self.REQUIRED_COLUMNS - header
            if missing:
                raise CommandError(
                    "El CSV no contiene todas las columnas requeridas. "
                    f"Faltan: {', '.join(sorted(missing))}"
                )

            for row_number, row in enumerate(reader, start=2):
                total_rows += 1
                try:
                    movie_payload = self._build_movie_payload(row)
                    key = (
                        movie_payload["title_english"].strip().lower(),
                        movie_payload["release_year"],
                        movie_payload["type"],
                    )

                    if key in seen_keys:
                        duplicate_count += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: duplicado dentro del CSV para "
                                f"'{movie_payload['title_english']}'. Se omite."
                            )
                        )
                        continue

                    exists_in_db = Movie.objects.filter(
                        title_english=movie_payload["title_english"],
                        release_year=movie_payload["release_year"],
                        type=movie_payload["type"],
                    ).exists()
                    if exists_in_db:
                        duplicate_count += 1
                        seen_keys.add(key)
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: ya existe en BD "
                                f"'{movie_payload['title_english']}' ({movie_payload['release_year']}). Se omite."
                            )
                        )
                        continue

                    Movie.objects.create(author=author, image=None, **movie_payload)
                    seen_keys.add(key)
                    created_count += 1
                except Exception as exc:  # noqa: BLE001
                    error_count += 1
                    self.stdout.write(
                        self.style.ERROR(f"Fila {row_number}: error al importar -> {exc}")
                    )

        self.stdout.write(self.style.SUCCESS("Importación finalizada."))
        self.stdout.write(f"Total filas leídas: {total_rows}")
        self.stdout.write(f"Creadas: {created_count}")
        self.stdout.write(f"Omitidas por duplicado: {duplicate_count}")
        self.stdout.write(f"Omitidas por error: {error_count}")

    def _build_movie_payload(self, row):
        title_english = self._clean_text(row.get("title_english"))
        if not title_english:
            raise ValueError("title_english es obligatorio para crear Movie")

        return {
            "title_english": title_english,
            "title_spanish": self._clean_text(row.get("title_spanish")),
            "type": self._normalize_type(row.get("type")),
            "genre": self._clean_text(row.get("genre")),
            "release_year": self._parse_year(row.get("release_year")),
            "director": self._clean_text(row.get("director")),
            "cast_members": self._clean_text(row.get("cast_members")),
            "external_rating": self._parse_rating(row.get("external_rating")),
        }

    @staticmethod
    def _clean_text(value):
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned or None

    def _normalize_type(self, raw_type):
        value = self._clean_text(raw_type)
        if not value:
            return None

        normalized = "".join(ch for ch in value.lower() if ch.isalnum())

        if normalized in self.TYPE_MOVIE_ALIASES:
            return Movie.MOVIE
        if normalized in self.TYPE_SERIES_ALIASES:
            return Movie.SERIES

        if "series" in normalized:
            return Movie.SERIES
        if "movie" in normalized or "film" in normalized:
            return Movie.MOVIE

        return None

    @staticmethod
    def _parse_year(raw_year):
        value = Command._clean_text(raw_year)
        if not value:
            return None

        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) != 4:
            return None

        year_int = int(digits)
        if year_int <= 0:
            return None
        return year_int

    @staticmethod
    def _parse_rating(raw_rating):
        value = Command._clean_text(raw_rating)
        if not value:
            return None

        normalized = value.replace(",", ".")
        try:
            decimal_value = Decimal(normalized)
        except InvalidOperation:
            return None

        if decimal_value < 0:
            return None

        return decimal_value.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
