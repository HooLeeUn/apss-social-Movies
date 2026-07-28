import json
from collections import defaultdict, deque
from datetime import timezone as datetime_timezone
from pathlib import Path

from django.core import serializers
from django.core.management.base import BaseCommand, CommandError
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone

from core.models import Movie


class Command(BaseCommand):
    help = "Export a deterministic, representative 400 movie/100 series staging catalog."

    DEFAULT_OUTPUT = "staging_data/staging_catalog_500.json"
    MOVIE_COUNT = 400
    SERIES_COUNT = 100
    SCHEMA_VERSION = 1

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            default=self.DEFAULT_OUTPUT,
            help=f"UTF-8 JSON output path (default: {self.DEFAULT_OUTPUT}).",
        )

    def handle(self, *args, **options):
        selected_movies = self._select_type(Movie.MOVIE, self.MOVIE_COUNT)
        selected_series = self._select_type(Movie.SERIES, self.SERIES_COUNT)
        selected = selected_movies + selected_series
        self._validate(selected, selected_movies, selected_series)

        # Django's serializer follows model metadata, so newly added concrete fields
        # cannot silently be omitted. It also preserves each object's original PK.
        objects = json.loads(serializers.serialize("json", selected))
        exported_at = timezone.now().astimezone(datetime_timezone.utc)
        document = {
            "schema_version": self.SCHEMA_VERSION,
            "exported_at": exported_at.isoformat().replace("+00:00", "Z"),
            "total": len(selected),
            "movies_count": len(selected_movies),
            "series_count": len(selected_series),
            "model_label": Movie._meta.label_lower,
            "objects": objects,
        }

        output_path = Path(options["output"]).expanduser()
        if not output_path.is_absolute():
            output_path = Path.cwd() / output_path
        output_path = output_path.resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as output_file:
            json.dump(document, output_file, cls=DjangoJSONEncoder, ensure_ascii=False, indent=2)
            output_file.write("\n")

        years = [movie.release_year for movie in selected if movie.release_year is not None]
        self.stdout.write(self.style.SUCCESS(f"Archivo: {output_path}"))
        self.stdout.write(f"Total exportado: {len(selected)}")
        self.stdout.write(f"Películas: {len(selected_movies)}")
        self.stdout.write(f"Series: {len(selected_series)}")
        self.stdout.write(f"Con image: {sum(bool(item.image) for item in selected)}")
        self.stdout.write(f"Con synopsis: {sum(bool(item.synopsis) for item in selected)}")
        self.stdout.write(f"Con synopsis_es: {sum(bool(item.synopsis_es) for item in selected)}")
        self.stdout.write(f"Con algún trailer: {sum(self._has_trailer(item) for item in selected)}")
        year_range = f"{min(years)}–{max(years)}" if years else "sin años disponibles"
        self.stdout.write(f"Rango de años: {year_range}")

    def _select_type(self, content_type, required_count):
        candidates = list(
            Movie.objects.filter(type=content_type, tmdb_id__isnull=False, tmdb_id__gt=0)
        )
        if len(candidates) < required_count:
            label = "películas" if content_type == Movie.MOVIE else "series"
            raise CommandError(
                f"No hay suficientes {label} con tmdb_id válido: "
                f"se necesitan {required_count} y se encontraron {len(candidates)}."
            )

        buckets = defaultdict(list)
        for movie in candidates:
            buckets[self._diversity_bucket(movie)].append(movie)

        queues = {}
        for bucket, rows in buckets.items():
            rows.sort(key=lambda item: (-self._completeness(item), self._stable_key(item), item.pk))
            # Four complete-first rows followed by one incomplete-first row ensures
            # placeholders get exercised whenever a stratum contains such examples.
            mixed = []
            while rows:
                mixed.extend(rows[:4])
                del rows[:4]
                if rows:
                    mixed.append(rows.pop())
            queues[bucket] = deque(mixed)

        ordered_buckets = sorted(queues, key=lambda value: (self._text_key(value), value))
        selected = []
        while len(selected) < required_count:
            for bucket in ordered_buckets:
                if queues[bucket]:
                    selected.append(queues[bucket].popleft())
                    if len(selected) == required_count:
                        break
        return selected

    @staticmethod
    def _diversity_bucket(movie):
        decade = (movie.release_year // 10) * 10 if movie.release_year else 0
        primary_genre = (movie.genre or "sin género").split(",", 1)[0].strip().casefold()
        return decade, primary_genre

    @classmethod
    def _completeness(cls, movie):
        values = (
            movie.image,
            movie.synopsis,
            movie.synopsis_es,
            movie.director,
            movie.cast_members,
            movie.imdb_id,
            movie.external_rating,
            movie.release_year,
        )
        return sum(value is not None and value != "" for value in values) + cls._has_trailer(movie)

    @staticmethod
    def _has_trailer(movie):
        return bool(movie.trailer_es_key or movie.trailer_en_key)

    @staticmethod
    def _stable_key(movie):
        return (movie.pk * 2654435761) % 4294967296

    @staticmethod
    def _text_key(value):
        decade, genre = value
        return ((decade * 2654435761) + sum((index + 1) * ord(char) for index, char in enumerate(genre))) % 4294967296

    def _validate(self, selected, movies, series):
        if len(selected) != self.MOVIE_COUNT + self.SERIES_COUNT:
            raise CommandError(f"La selección contiene {len(selected)} registros; se esperaban 500.")
        if len(movies) != self.MOVIE_COUNT or len(series) != self.SERIES_COUNT:
            raise CommandError("La selección no contiene exactamente 400 películas y 100 series.")
        if any(not item.tmdb_id or item.tmdb_id <= 0 for item in selected):
            raise CommandError("La selección contiene al menos un tmdb_id no válido.")
        ids = [item.pk for item in selected]
        if len(ids) != len(set(ids)):
            raise CommandError("La selección contiene IDs duplicados.")
