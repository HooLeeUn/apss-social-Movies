import csv
from collections import Counter
from decimal import Decimal, InvalidOperation
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.management.commands._csv_utils import open_csv_dict_reader
from core.management.commands.export_tmdb_rating_votes import FIELDS
from core.models import Movie


REPORT_FIELDS = ["database_id", "csv_tmdb_id", "db_tmdb_id", "csv_type", "db_type",
                 "old_rating", "new_rating", "old_votes", "new_votes", "status"]


def integer(raw, minimum=1, maximum=9223372036854775807):
    value = int(raw)
    if str(value) != raw or not minimum <= value <= maximum:
        raise ValueError
    return value


class Command(BaseCommand):
    help = "Validate identity and bulk update only Movie external_rating/external_votes. No TMDB calls."

    def add_arguments(self, parser):
        parser.add_argument("--input", required=True)
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--dry-run", action="store_true")
        mode.add_argument("--apply", action="store_true")
        parser.add_argument("--batch-size", type=int, default=5000)
        parser.add_argument("--report-csv", default="import_tmdb_rating_votes_report.csv")

    def handle(self, *args, **options):
        size = options["batch_size"]
        if size <= 0:
            raise CommandError("--batch-size debe ser positivo.")
        source, report = Path(options["input"]), Path(options["report_csv"])
        if source.resolve() == report.resolve():
            raise CommandError("El reporte no puede ser el archivo de entrada.")
        apply = options["apply"]
        counts = Counter()
        seen = set()
        report.parent.mkdir(parents=True, exist_ok=True)
        with open_csv_dict_reader(source) as (reader, _):
            if not reader.fieldnames or not set(FIELDS).issubset(reader.fieldnames):
                raise CommandError(f"Columnas obligatorias: {', '.join(FIELDS)}")
            with report.open("x", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=REPORT_FIELDS)
                writer.writeheader()
                batch = []
                for row in reader:
                    counts["filas leídas"] += 1
                    result = dict.fromkeys(REPORT_FIELDS, "")
                    result.update(database_id=row.get("database_id"), csv_tmdb_id=row.get("tmdb_id"),
                                  csv_type=row.get("type"), new_rating=row.get("external_rating"),
                                  new_votes=row.get("external_votes"))
                    try:
                        pk = integer((row.get("database_id") or "").strip())
                        tmdb = integer((row.get("tmdb_id") or "").strip(), maximum=2147483647)
                        kind = row.get("type") or ""
                        raw_rating = (row.get("external_rating") or "").strip()
                        rating = Decimal(raw_rating) if raw_rating else None
                        votes = integer((row.get("external_votes") or "").strip(), 0, 2147483647)
                        if (None in row or any(row.get(field) is None for field in FIELDS)
                                or kind not in (Movie.MOVIE, Movie.SERIES) or pk in seen):
                            raise ValueError
                        if rating is not None and (not rating.is_finite() or not 0 <= rating <= Decimal("10")
                                                   or rating != rating.quantize(Decimal("0.1"))):
                            raise ValueError
                    except (ValueError, TypeError, InvalidOperation):
                        result["status"] = "invalid_row"
                        counts["invalid_row"] += 1
                        writer.writerow(result)
                        continue
                    seen.add(pk)
                    counts["válidas"] += 1
                    batch.append((pk, tmdb, kind, rating, votes, result))
                    if len(batch) >= size:
                        self.process_batch(batch, apply, writer, counts, size)
                        batch = []
                if batch:
                    self.process_batch(batch, apply, writer, counts, size)
        for label in ("filas leídas", "válidas", "encontradas", "missing", "identity_mismatch",
                      "updated" if apply else "would_update", "unchanged", "invalid_row", "error"):
            self.stdout.write(f"{label if label != 'error' else 'errores'}: {counts[label]}")
        self.stdout.write(f"modo: {'apply' if apply else 'dry-run'}")
        self.stdout.write(f"Reporte CSV: {report}")

    def process_batch(self, batch, apply, writer, counts, size):
        changed = []
        # Locks keep identity and old values stable until the batch commits.
        with transaction.atomic():
            queryset = Movie.objects.only("id", "tmdb_id", "type", "external_rating", "external_votes")
            if apply:
                queryset = queryset.select_for_update()
            movies = queryset.in_bulk([row[0] for row in batch])
            for pk, tmdb, kind, rating, votes, result in batch:
                movie = movies.get(pk)
                if movie is None:
                    result["status"] = "missing"
                    continue
                counts["encontradas"] += 1
                result.update(db_tmdb_id=movie.tmdb_id, db_type=movie.type,
                              old_rating=movie.external_rating, old_votes=movie.external_votes)
                if movie.tmdb_id != tmdb or movie.type != kind:
                    result["status"] = "identity_mismatch"
                elif movie.external_rating == rating and movie.external_votes == votes:
                    result["status"] = "unchanged"
                else:
                    result["status"] = "updated" if apply else "would_update"
                    movie.external_rating, movie.external_votes = rating, votes
                    changed.append(movie)
            if apply and changed:
                try:
                    with transaction.atomic():
                        Movie.objects.bulk_update(changed, ["external_rating", "external_votes"], batch_size=size)
                except Exception as exc:
                    self.stderr.write(f"Error de actualización del lote (rollback): {exc}")
                    for *_, result in batch:
                        if result["status"] == "updated":
                            result["status"] = "error"
        for *_, result in batch:
            counts[result["status"]] += 1
            writer.writerow(result)
