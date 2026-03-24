from datetime import datetime

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from core.weekly_recommendations import refresh_weekly_recommendation_snapshot


class Command(BaseCommand):
    help = "Genera o refresca el snapshot semanal de recomendaciones para la semana cerrada anterior."

    def add_arguments(self, parser):
        parser.add_argument(
            "--reference-datetime",
            help="Datetime ISO-8601 opcional usado como referencia para calcular la semana cerrada anterior.",
        )

    def handle(self, *args, **options):
        reference_datetime_raw = options.get("reference_datetime")
        reference_datetime = None

        if reference_datetime_raw:
            try:
                reference_datetime = datetime.fromisoformat(reference_datetime_raw)
            except ValueError as exc:
                raise CommandError(
                    "--reference-datetime debe ser un datetime ISO-8601 válido."
                ) from exc

            if timezone.is_naive(reference_datetime):
                reference_datetime = timezone.make_aware(
                    reference_datetime, timezone.get_current_timezone()
                )

        snapshot = refresh_weekly_recommendation_snapshot(
            reference_datetime=reference_datetime
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"Snapshot semanal generado: {snapshot.week_start} -> {snapshot.week_end} ({snapshot.items.count()} items)"
            )
        )
