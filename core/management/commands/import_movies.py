import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Movie, build_genre_key


class Command(BaseCommand):
    help = (
        "Importa películas desde un CSV consolidado al modelo Movie. "
        "Columnas esperadas: title_english,title_spanish,type,genre,release_year,"
        "director,cast_members,external_rating\n"
        "Columnas opcionales soportadas: imdb_id, external_votes"
    )

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
    # `imdb_id` se mantiene opcional para no romper CSV históricos.

    BATCH_SIZE = 2000
    PROGRESS_EVERY = 10000

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Ruta al archivo CSV a importar.")
        parser.add_argument(
            "--author",
            default="admin",
            help="Username del autor que se asignará a todas las películas (default: admin).",
        )
        parser.epilog = (
            "Ejemplo de encabezado CSV: "
            "title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating"
            "[,imdb_id][,external_votes]"
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
        updated_existing_count = 0
        error_count = 0

        self.stdout.write(self.style.NOTICE(f"Iniciando importación desde: {csv_path}"))
        self.stdout.write(self.style.NOTICE(f"Autor asignado: {author.username}"))
        self.stdout.write(
            self.style.NOTICE(
                "Precargando películas existentes (clave: title_english, title_spanish, type, release_year)..."
            )
        )

        existing_movies = {}
        existing_rows = Movie.objects.values_list(
            "id",
            "title_english",
            "title_spanish",
            "type",
            "release_year",
            "imdb_id",
            "external_votes",
        ).iterator(chunk_size=10000)
        for (
            movie_id,
            title_english,
            title_spanish,
            movie_type,
            release_year,
            imdb_id,
            external_votes,
        ) in existing_rows:
            key = self._build_key(title_english, title_spanish, movie_type, release_year)
            existing_movies[key] = {
                "id": movie_id,
                "imdb_id": self._clean_text(imdb_id),
                "external_votes": external_votes,
                "pending_create": None,
            }

        self.stdout.write(
            self.style.NOTICE(
                f"Claves existentes cargadas: {len(existing_movies)}. "
                f"Iniciando importación por lotes de {self.BATCH_SIZE}."
            )
        )

        to_create = []
        to_update = []

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
                    key = self._build_key(
                        movie_payload["title_english"],
                        movie_payload["title_spanish"],
                        movie_payload["type"],
                        movie_payload["release_year"],
                    )

                    existing_movie = existing_movies.get(key)
                    if existing_movie is not None:
                        duplicate_count += 1
                        csv_imdb_id = movie_payload["imdb_id"]
                        csv_external_votes = movie_payload["external_votes"]
                        pending_create = existing_movie.get("pending_create")

                        if pending_create is not None:
                            if csv_imdb_id and not pending_create.imdb_id:
                                pending_create.imdb_id = csv_imdb_id
                                existing_movie["imdb_id"] = csv_imdb_id

                            if (
                                movie_payload["external_votes_provided"]
                                and csv_external_votes != pending_create.external_votes
                            ):
                                pending_create.external_votes = csv_external_votes
                                existing_movie["external_votes"] = csv_external_votes
                            continue

                        if csv_imdb_id and not existing_movie["imdb_id"]:
                            Movie.objects.filter(pk=existing_movie["id"], imdb_id__isnull=True).update(
                                imdb_id=csv_imdb_id
                            )
                            existing_movie["imdb_id"] = csv_imdb_id

                        if (
                            movie_payload["external_votes_provided"]
                            and csv_external_votes != existing_movie["external_votes"]
                        ):
                            existing_movie["external_votes"] = csv_external_votes
                            to_update.append(
                                Movie(
                                    id=existing_movie["id"],
                                    external_votes=existing_movie["external_votes"],
                                )
                            )

                            if len(to_update) >= self.BATCH_SIZE:
                                updated_existing_count += self._flush_existing_updates(to_update)
                                to_update.clear()
                        continue

                    create_payload = {
                        key: value
                        for key, value in movie_payload.items()
                        if key != "external_votes_provided"
                    }
                    pending_movie = Movie(author=author, image=None, **create_payload)
                    to_create.append(pending_movie)
                    existing_movies[key] = {
                        "id": None,
                        "imdb_id": movie_payload["imdb_id"],
                        "external_votes": movie_payload["external_votes"],
                        "pending_create": pending_movie,
                    }

                    if len(to_create) >= self.BATCH_SIZE:
                        created_count += self._flush_batch(to_create)
                        to_create.clear()
                except Exception as exc:  # noqa: BLE001
                    error_count += 1
                    self.stdout.write(
                        self.style.ERROR(f"Fila {row_number}: error al importar -> {exc}")
                    )

                if total_rows % self.PROGRESS_EVERY == 0:
                    self.stdout.write(
                        self.style.NOTICE(
                            f"Progreso: {total_rows} filas leídas | "
                            f"creadas={created_count} | "
                            f"actualizadas_existentes={updated_existing_count} | "
                            f"duplicadas={duplicate_count} | "
                            f"errores={error_count}"
                        )
                    )

        if to_create:
            created_count += self._flush_batch(to_create)
        if to_update:
            updated_existing_count += self._flush_existing_updates(to_update)

        self.stdout.write(self.style.SUCCESS("Importación finalizada."))
        self.stdout.write(f"Total filas leídas: {total_rows}")
        self.stdout.write(f"Creadas: {created_count}")
        self.stdout.write(f"Registros existentes actualizados: {updated_existing_count}")
        self.stdout.write(f"Omitidas por duplicado: {duplicate_count}")
        self.stdout.write(f"Omitidas por error: {error_count}")

    def _flush_batch(self, items):
        with transaction.atomic():
            created = Movie.objects.bulk_create(items, batch_size=self.BATCH_SIZE)
        return len(created)

    def _flush_existing_updates(self, items):
        persisted_items = [item for item in items if item.pk]
        if not persisted_items:
            return 0

        with transaction.atomic():
            Movie.objects.bulk_update(persisted_items, ["external_votes"], batch_size=self.BATCH_SIZE)
        return len(persisted_items)

    def _build_movie_payload(self, row):
        title_english = self._clean_text(row.get("title_english"))
        if not title_english:
            raise ValueError("title_english es obligatorio para crear Movie")

        external_votes = self._parse_external_votes(row.get("external_votes"))

        return {
            "title_english": title_english,
            "title_spanish": self._clean_text(row.get("title_spanish")),
            "type": self._normalize_type(row.get("type")),
            "genre": self._clean_text(row.get("genre")),
            "genre_key": build_genre_key(row.get("genre")),
            "release_year": self._parse_year(row.get("release_year")),
            "director": self._clean_text(row.get("director")),
            "cast_members": self._clean_text(row.get("cast_members")),
            "external_rating": self._parse_rating(row.get("external_rating")),
            "external_votes": external_votes if external_votes is not None else 0,
            "external_votes_provided": external_votes is not None,
            "imdb_id": self._clean_text(row.get("imdb_id")),
        }

    @staticmethod
    def _build_key(title_english, title_spanish, movie_type, release_year):
        return (
            str(title_english).strip().lower(),
            str(title_spanish).strip().lower() if title_spanish else None,
            movie_type,
            release_year,
        )

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

    @staticmethod
    def _parse_external_votes(raw_votes):
        value = Command._clean_text(raw_votes)
        if not value:
            return None

        digits = "".join(ch for ch in value if ch.isdigit())
        if not digits:
            return None

        votes = int(digits)
        if votes < 0:
            return None

        return votes
