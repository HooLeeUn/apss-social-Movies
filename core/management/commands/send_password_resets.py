from django.core.management.base import BaseCommand

from core.password_reset import deliver_password_resets


class Command(BaseCommand):
    help = "Process up to 100 queued password reset requests. Schedule every minute."

    def handle(self, *args, **options):
        count = deliver_password_resets()
        self.stdout.write(f"Processed {count} password reset requests.")
