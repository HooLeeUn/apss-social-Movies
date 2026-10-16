import csv
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from core.models import Movie


FIELDS = ["database_id", "tmdb_id", "type", "external_rating", "external_votes"]


class Command(BaseCommand):
    help = "Export existing Movie ratings/votes without contacting TMDB."

    def add_arguments(self, parser):
        parser.add_argument("--output", default="tmdb_rating_votes_final.csv")

    def handle(self, *args, **options):
        path = Path(options["output"])
        missing = Movie.objects.filter(tmdb_id__isnull=True).count()
        self.stdout.write(f"filas sin tmdb_id: {missing}")
        if missing or Movie.objects.filter(tmdb_id__lte=0).exists():
            raise CommandError("Todos los Movie deben tener un tmdb_id positivo. No se generó el archivo.")
        count = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8", newline="") as output:
            writer = csv.writer(output)
            writer.writerow(FIELDS)
            for row in Movie.objects.order_by("id").values_list(
                "id", "tmdb_id", "type", "external_rating", "external_votes"
            ).iterator(chunk_size=5000):
                if not row[1]:
                    raise CommandError("Movie sin tmdb_id durante la exportación; descartar archivo incompleto.")
                writer.writerow(row)
                count += 1
        self.stdout.write(f"filas exportadas: {count}")
        self.stdout.write(f"archivo generado: {path}")
