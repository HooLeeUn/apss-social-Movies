import csv
import re
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlparse

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import models, transaction

from core.models import Movie, build_genre_key, build_movie_search_fields


class RowValidationError(ValueError):
    pass


class Command(BaseCommand):
    help = "Importa un catálogo portable, sin efectuar consultas externas."

    TYPE_MOVIE_ALIASES = {"movie", "film", "feature", "featurefilm", "tvmovie"}
    TYPE_SERIES_ALIASES = {"series", "tvseries", "tvminiseries", "miniseries", "show"}
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
    OPTIONAL_COLUMNS = {
        "imdb_id",
        "external_votes",
        "tmdb_id",
        "image",
        "synopsis",
        "synopsis_es",
    }
    DESCRIPTIVE_FIELDS = (
        "title_english",
        "title_spanish",
        "type",
        "genre",
        "genre_key",
        "release_year",
        "director",
        "cast_members",
        "external_rating",
        "synopsis",
        "synopsis_es",
        "image",
    )
    SEARCH_FIELDS = (
        "title_english_search",
        "title_spanish_search",
        "director_search",
        "cast_members_search",
    )
    BATCH_SIZE = 2000
    PROGRESS_EVERY = 10000
    IMDB_RE = re.compile(r"^tt\d+$")

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str)
        parser.add_argument(
            "--author", default="admin", help="Username del autor (default: admin)."
        )
        parser.add_argument(
            "--update-existing",
            action="store_true",
            help="Reemplaza campos descriptivos con valores no vacíos del CSV.",
        )

    def handle(self, *args, **options):
        path = Path(options["csv_path"])
        if not path.is_file():
            raise CommandError(f"El archivo CSV no existe o no es válido: {path}")
        user_model = get_user_model()
        try:
            author = user_model.objects.get(username=options["author"])
        except user_model.DoesNotExist as exc:
            raise CommandError(
                f"No existe un usuario con username '{options['author']}'. "
                "Crea el usuario o usa --author con uno existente."
            ) from exc

        counters = defaultdict(int)
        completion_fields = ("tmdb_id", "image", "synopsis", "synopsis_es")
        self.director_reduced_count = self.director_truncated_count = 0
        self.charfield_truncated_counts = {}

        # One streaming query, rather than one lookup for every CSV row. Values are
        # deliberately kept as model instances because updates need the old values.
        imdb_index, fallback_index = defaultdict(list), defaultdict(list)
        for movie in Movie.objects.all().iterator(chunk_size=10000):
            imdb = self._normalize_stored_imdb(movie.imdb_id)
            if imdb:
                imdb_index[imdb].append(movie)
            fallback_index[self._key_for(movie)].append(movie)

        creates, updates, update_ids = [], [], set()

        def flush():
            nonlocal creates, updates, update_ids
            if creates:
                with transaction.atomic():
                    Movie.objects.bulk_create(creates, batch_size=self.BATCH_SIZE)
                counters["created"] += len(creates)
                creates = []
            if updates:
                with transaction.atomic():
                    Movie.objects.bulk_update(
                        updates,
                        list(self.DESCRIPTIVE_FIELDS)
                        + list(self.SEARCH_FIELDS)
                        + ["imdb_id", "tmdb_id", "external_votes"],
                        batch_size=self.BATCH_SIZE,
                    )
                counters["updated"] += len(updates)
                updates, update_ids = [], set()

        self.stdout.write(self.style.NOTICE(f"Iniciando importación desde: {path}"))
        self.stdout.write(self.style.NOTICE(f"Autor asignado: {author.username}"))
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            missing = self.REQUIRED_COLUMNS - set(reader.fieldnames or ())
            if missing:
                raise CommandError(
                    "El CSV no contiene todas las columnas requeridas. Faltan: "
                    + ", ".join(sorted(missing))
                )

            for row_number, row in enumerate(reader, 2):
                counters["total"] += 1
                try:
                    payload = self._build_movie_payload(row)
                    imdb = payload["imdb_id"]
                    fallback = self._build_key(
                        payload["title_english"],
                        payload["title_spanish"],
                        payload["release_year"],
                        payload["type"],
                        payload["director"],
                    )
                    matches = (
                        imdb_index.get(imdb, [])
                        if imdb
                        else fallback_index.get(fallback, [])
                    )
                    # IMDb is authoritative when it resolves.  If it does not,
                    # the historical composite key remains a migration bridge.
                    if imdb and not matches:
                        matches = fallback_index.get(fallback, [])
                    if len(matches) > 1:
                        counters["conflicts"] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: conflicto; hay {len(matches)} registros con "
                                + (
                                    f"IMDb {imdb}"
                                    if imdb
                                    else "la misma clave fallback"
                                )
                            )
                        )
                        continue
                    existing = matches[0] if matches else None
                    if (
                        existing
                        and imdb
                        and existing.imdb_id
                        and self._normalize_stored_imdb(existing.imdb_id) != imdb
                    ):
                        counters["conflicts"] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: conflicto de imdb_id"
                            )
                        )
                        continue
                    if (
                        existing
                        and payload["tmdb_id"]
                        and existing.tmdb_id
                        and existing.tmdb_id != payload["tmdb_id"]
                    ):
                        counters["conflicts"] += 1
                        self.stdout.write(
                            self.style.WARNING(
                                f"Fila {row_number}: conflicto de tmdb_id"
                            )
                        )
                        continue

                    if not existing:
                        clean = {
                            k: v
                            for k, v in payload.items()
                            if k != "external_votes_provided"
                        }
                        # synopsis is the sole enriched text column whose model
                        # does not allow SQL NULL; an absent legacy column maps
                        # to its established empty-string representation.
                        clean["synopsis"] = clean["synopsis"] or ""
                        existing = Movie(author=author, **clean)
                        creates.append(existing)
                        imdb_index[imdb].append(existing) if imdb else None
                        fallback_index[fallback].append(existing)
                        for field in completion_fields:
                            if clean.get(field):
                                counters[f"completed_{field}"] += 1
                        continue

                    counters["duplicates"] += 1
                    changed = False
                    before = {
                        field: getattr(existing, field) for field in completion_fields
                    }
                    if imdb and not existing.imdb_id:
                        existing.imdb_id, changed = imdb, True
                        imdb_index[imdb].append(existing)
                    if payload["tmdb_id"] and not existing.tmdb_id:
                        existing.tmdb_id, changed = payload["tmdb_id"], True
                    for field in self.DESCRIPTIVE_FIELDS:
                        incoming = payload.get(field)
                        current = getattr(existing, field)
                        if self._has_value(incoming) and (
                            options["update_existing"] or not self._has_value(current)
                        ):
                            if current != incoming:
                                setattr(existing, field, incoming)
                                changed = True
                    if (
                        payload["external_votes_provided"]
                        and existing.external_votes != payload["external_votes"]
                    ):
                        existing.external_votes, changed = (
                            payload["external_votes"],
                            True,
                        )
                    if changed:
                        for field, value in build_movie_search_fields(existing).items():
                            setattr(existing, field, value)
                        if existing.pk and existing.pk not in update_ids:
                            updates.append(existing)
                            update_ids.add(existing.pk)
                        for field in completion_fields:
                            if not self._has_value(before[field]) and self._has_value(
                                getattr(existing, field)
                            ):
                                counters[f"completed_{field}"] += 1
                    else:
                        counters["unchanged"] += 1
                except RowValidationError as exc:
                    counters["invalid"] += 1
                    self.stdout.write(
                        self.style.ERROR(f"Fila {row_number}: inválida -> {exc}")
                    )
                except Exception as exc:  # noqa: BLE001 - isolate malformed rows
                    counters["errors"] += 1
                    self.stdout.write(
                        self.style.ERROR(
                            f"Fila {row_number}: error al importar -> {exc}"
                        )
                    )

                if len(creates) + len(updates) >= self.BATCH_SIZE:
                    flush()
                if counters["total"] % self.PROGRESS_EVERY == 0:
                    self.stdout.write(
                        self.style.NOTICE(f"Progreso: {counters['total']} filas leídas")
                    )
        flush()
        self.stdout.write(self.style.SUCCESS("Importación finalizada."))
        labels = (
            ("total", "Total filas leídas"),
            ("created", "Creadas"),
            ("updated", "Registros existentes actualizados"),
            ("unchanged", "Sin cambios/omitidas"),
            ("duplicates", "Omitidas por duplicado"),
            ("conflicts", "Conflictos"),
            ("invalid", "Inválidas"),
            ("errors", "Errores"),
        )
        for key, label in labels:
            self.stdout.write(f"{label}: {counters[key]}")
        for field in completion_fields:
            self.stdout.write(f"Completados {field}: {counters[f'completed_{field}']}")
        self.stdout.write(
            f"Directores reducidos a primer director: {self.director_reduced_count}"
        )
        self.stdout.write(
            f"Directores truncados por max_length: {self.director_truncated_count}"
        )
        summary = ", ".join(
            f"{k}={v}" for k, v in sorted(self.charfield_truncated_counts.items())
        )
        self.stdout.write(f"CharFields truncados por max_length: {summary or 0}")

    @staticmethod
    def _has_value(value):
        return value is not None and value != ""

    @staticmethod
    def _normalize_stored_imdb(value):
        value = Command._clean_text(value)
        return (
            value.lower()
            if value and Command.IMDB_RE.fullmatch(value.lower())
            else None
        )

    def _build_movie_payload(self, row):
        title = self._clean_text(row.get("title_english"))
        if not title:
            raise RowValidationError("title_english es obligatorio")
        movie_type = self._normalize_type(row.get("type"))
        if movie_type not in (Movie.MOVIE, Movie.SERIES):
            raise RowValidationError("type debe ser movie o series")
        raw_imdb = self._clean_text(row.get("imdb_id"))
        if raw_imdb and not self.IMDB_RE.fullmatch(raw_imdb.lower()):
            raise RowValidationError("imdb_id debe tener formato tt seguido de dígitos")
        tmdb_id = self._parse_positive_integer(row.get("tmdb_id"), "tmdb_id")
        image = self._clean_text(row.get("image"))
        if image and (
            urlparse(image).scheme not in {"http", "https"}
            or not urlparse(image).netloc
        ):
            raise RowValidationError("image debe ser una URL HTTP/HTTPS")
        raw_votes = self._clean_text(row.get("external_votes"))
        votes = self._parse_votes(raw_votes)
        raw_director = self._clean_text(row.get("director"))
        director = self._first_director_for_storage(raw_director)
        if raw_director and director != raw_director:
            self.director_reduced_count += 1
        payload = {
            "title_english": self._truncate_char_field("title_english", title),
            "title_spanish": self._truncate_char_field(
                "title_spanish", row.get("title_spanish")
            ),
            "type": movie_type,
            "genre": self._truncate_char_field("genre", row.get("genre")),
            "genre_key": self._truncate_char_field(
                "genre_key", build_genre_key(row.get("genre"))
            ),
            "release_year": self._parse_year(row.get("release_year")),
            "director": self._truncate_char_field("director", director),
            "cast_members": self._clean_text(row.get("cast_members")),
            "external_rating": self._parse_rating(row.get("external_rating")),
            "external_votes": votes if votes is not None else 0,
            "external_votes_provided": raw_votes is not None,
            "imdb_id": self._truncate_char_field(
                "imdb_id", raw_imdb.lower() if raw_imdb else None
            ),
            "tmdb_id": tmdb_id,
            "image": image,
            "synopsis": self._clean_text(row.get("synopsis")),
            "synopsis_es": self._clean_text(row.get("synopsis_es")),
        }
        payload.update(
            build_movie_search_fields(
                Movie(
                    **{
                        k: v
                        for k, v in payload.items()
                        if k != "external_votes_provided"
                    }
                )
            )
        )
        return payload

    @staticmethod
    def _parse_positive_integer(raw, name):
        value = Command._clean_text(raw)
        if not value:
            return None
        try:
            number = int(value)
        except ValueError as exc:
            raise RowValidationError(f"{name} debe ser un entero positivo") from exc
        if number <= 0:
            raise RowValidationError(f"{name} debe ser un entero positivo")
        return number

    @staticmethod
    def _parse_votes(raw):
        value = Command._clean_text(raw)
        if not value:
            return None
        normalized = value.replace(",", "").replace(".", "")
        if not normalized.isdigit():
            raise RowValidationError("external_votes debe ser un entero no negativo")
        return int(normalized)

    @staticmethod
    def _key_for(movie):
        return Command._build_key(
            movie.title_english,
            movie.title_spanish,
            movie.release_year,
            movie.type,
            movie.director,
        )

    @staticmethod
    def _build_key(title_english, title_spanish, release_year, movie_type, director):
        return (
            Command._normalize_key_text(title_english),
            Command._normalize_key_text(title_spanish),
            movie_type,
            Command._normalize_key_year(release_year),
            Command._normalize_first_director(director),
        )

    @staticmethod
    def _normalize_key_text(value):
        value = Command._clean_text(value)
        return value.lower() if value else None

    @staticmethod
    def _normalize_key_year(value):
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_first_director(value):
        value = Command._first_director_for_storage(value)
        return value.lower() if value else None

    @staticmethod
    def _first_director_for_storage(value):
        if value is None:
            return None
        return str(value).split(",", 1)[0].strip() or None

    def _truncate_char_field(self, field_name, value):
        value = self._clean_text(value)
        if value is None:
            return None
        field = Movie._meta.get_field(field_name)
        if (
            not isinstance(field, models.CharField)
            or not field.max_length
            or len(value) <= field.max_length
        ):
            return value
        self.charfield_truncated_counts[field_name] = (
            self.charfield_truncated_counts.get(field_name, 0) + 1
        )
        if field_name == "director":
            self.director_truncated_count += 1
        return value[: field.max_length].rstrip() or None

    @staticmethod
    def _clean_text(value):
        if value is None:
            return None
        return str(value).strip() or None

    def _normalize_type(self, raw):
        value = self._clean_text(raw)
        normalized = "".join(ch for ch in (value or "").lower() if ch.isalnum())
        if (
            normalized in self.TYPE_MOVIE_ALIASES
            or "movie" in normalized
            or "film" in normalized
        ):
            return Movie.MOVIE
        if normalized in self.TYPE_SERIES_ALIASES or "series" in normalized:
            return Movie.SERIES
        return None

    @staticmethod
    def _parse_year(raw):
        value = Command._clean_text(raw)
        if not value:
            return None
        digits = "".join(ch for ch in value if ch.isdigit())
        return int(digits) if len(digits) == 4 and int(digits) > 0 else None

    @staticmethod
    def _parse_rating(raw):
        value = Command._clean_text(raw)
        if not value:
            return None
        try:
            number = Decimal(value.replace(",", "."))
        except InvalidOperation:
            return None
        return (
            number.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            if number >= 0
            else None
        )
