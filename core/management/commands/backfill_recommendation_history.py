from django.core.management.base import BaseCommand
from core.recommendation_history import backfill_history

class Command(BaseCommand):
    help = "Initialize missing open recommendation intervals without guessing deleted history."
    def handle(self, **options):
        self.stdout.write(f"Intervalos creados: {backfill_history()}")
