import argparse
import csv
import json
import time
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from core.models import Movie


class Command(BaseCommand):
    help = (
        "Export movies/series without a valid TMDb ID to a UTF-8 BOM CSV. "
        "Allowed --type values: all, movie, series."
    )

    DEFAULT_OUTPUT = "movies_without_tmdb_id.csv"
    CHUNK_SIZE = 2000
    PROGRESS_EVERY = 10000

    TYPE_ALIASES = {
        "all": None,
        "movie": Movie.MOVIE,
        "series": Movie.SERIES,
    }

    FIELD_MAP = {
        "database_id": "id",
        "type": "type",
        "title_en": "title_english",
        "title_es": "title_spanish",
        "year": "release_year",
        "director": "director",
        "imdb_id": "imdb_id",
        "cast": "cast_members",
        "current_tmdb_id": "tmdb_id",
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--output",
            default=self.DEFAULT_OUTPUT,
            help=f"CSV output path. Defaults to {self.DEFAULT_OUTPUT!r}.",
        )
        parser.add_argument(
            "--type",
            choices=sorted(self.TYPE_ALIASES),
            default="all",
            help=(
                "Content type to export. Use 'all' for every type. "
                f"Real project values: movie={Movie.MOVIE!r}, series={Movie.SERIES!r}."
            ),
        )
        parser.add_argument(
            "--limit",
            type=self._positive_int,
            help="Export only the first N matching rows, useful for tests.",
        )
        parser.add_argument(
            "--overwrite",
            action="store_true",
            help="Replace the output file if it already exists.",
        )

    def handle(self, *args, **options):
        started_at = time.monotonic()
        output_path = Path(options["output"]).expanduser()
        if not output_path.is_absolute():
            output_path = Path.cwd() / output_path
        output_path = output_path.resolve()

        if output_path.exists() and not options["overwrite"]:
            raise CommandError(
                f"Output file already exists: {output_path}. Use --overwrite to replace it."
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)

        export_type = options["type"]
        limit = options.get("limit")
        real_type_value = self.TYPE_ALIASES[export_type]

        queryset = self._build_queryset(real_type_value, limit)
        exported_count = self._write_csv(output_path, queryset)
        elapsed = time.monotonic() - started_at

        type_label = "all" if real_type_value is None else f"{export_type} ({real_type_value})"
        limit_label = "none" if limit is None else str(limit)

        self.stdout.write(self.style.SUCCESS(f"Exported rows: {exported_count}"))
        self.stdout.write(f"Output file: {output_path}")
        self.stdout.write(f"Type filter: {type_label}")
        self.stdout.write(f"Limit: {limit_label}")
        self.stdout.write(f"Elapsed time: {elapsed:.2f}s")

    @classmethod
    def _positive_int(cls, value):
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise argparse.ArgumentTypeError("--limit must be a positive integer.") from exc
        if parsed <= 0:
            raise argparse.ArgumentTypeError("--limit must be a positive integer.")
        return parsed

    def _build_queryset(self, real_type_value, limit):
        tmdb_id_field = Movie._meta.get_field(self.FIELD_MAP["current_tmdb_id"])
        tmdb_filter = Q(tmdb_id__isnull=True)

        if tmdb_id_field.get_internal_type() in {"CharField", "TextField"}:
            tmdb_filter |= Q(tmdb_id="") | Q(tmdb_id__regex=r"^\s*$")

        queryset = Movie.objects.filter(tmdb_filter)
        if real_type_value is not None:
            queryset = queryset.filter(type=real_type_value)

        queryset = queryset.order_by("type", "title_english", "release_year", "id")
        queryset = queryset.values(*self.FIELD_MAP.values())
        if limit is not None:
            queryset = queryset[:limit]
        return queryset

    def _write_csv(self, output_path, queryset):
        fieldnames = list(self.FIELD_MAP.keys())
        exported_count = 0

        with output_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()

            for row in queryset.iterator(chunk_size=self.CHUNK_SIZE):
                writer.writerow(self._format_row(row))
                exported_count += 1
                if exported_count % self.PROGRESS_EVERY == 0:
                    self.stdout.write(f"Exported {exported_count} rows...")

        return exported_count

    def _format_row(self, row):
        formatted = {}
        for output_name, model_field in self.FIELD_MAP.items():
            formatted[output_name] = self._serialize_value(row.get(model_field))
        return formatted

    @staticmethod
    def _serialize_value(value):
        if value is None:
            return ""
        if isinstance(value, (dict, list, tuple, set)):
            try:
                return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
            except TypeError:
                return json.dumps(str(value), ensure_ascii=False)
        return value
