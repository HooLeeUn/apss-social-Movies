import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from core.management.commands._csv_utils import open_csv_dict_reader


class Command(BaseCommand):
    help = "Combine deletion CSVs without accessing the database. First reason wins for duplicate IDs."
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument("--missing-tmdb-csv", required=True)
        parser.add_argument("--tmdb-404-csv", required=True)
        parser.add_argument("--output", default="movies_to_delete_render.csv")

    def handle(self, *args, **options):
        ids = {}
        counts = dict(missing_tmdb_id=0, tmdb_404=0, duplicados=0, inválidos=0)
        for option, reason in (("missing_tmdb_csv", "missing_tmdb_id"), ("tmdb_404_csv", "tmdb_404")):
            with open_csv_dict_reader(options[option]) as (reader, _):
                if not reader.fieldnames or "database_id" not in reader.fieldnames:
                    raise CommandError(f"Falta database_id: {options[option]}")
                for row in reader:
                    raw = (row.get("database_id") or "").strip()
                    try:
                        value = int(raw)
                        if value <= 0 or str(value) != raw:
                            raise ValueError
                    except (ValueError, TypeError):
                        counts["inválidos"] += 1
                        continue
                    if value in ids:
                        counts["duplicados"] += 1
                        continue
                    ids[value] = reason
                    counts[reason] += 1
        path = Path(options["output"])
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(["database_id", "reason"])
            writer.writerows(sorted(ids.items()))
        for name, count in counts.items():
            self.stdout.write(f"{name}: {count}")
        self.stdout.write(f"total único: {len(ids)}")
        self.stdout.write(f"archivo generado: {path}")
