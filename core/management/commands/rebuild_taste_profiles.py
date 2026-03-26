from django.core.management.base import BaseCommand, CommandError

from core.services import rebuild_taste_profiles


class Command(BaseCommand):
    help = "Rebuilds UserTasteProfile and preference tables from MovieRating records."

    def add_arguments(self, parser):
        parser.add_argument(
            "--user-id",
            type=int,
            dest="user_id",
            help="Rebuild only for a specific user id.",
        )

    def handle(self, *args, **options):
        user_id = options.get("user_id")
        try:
            processed_users = rebuild_taste_profiles(user_id=user_id)
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        if user_id is not None:
            self.stdout.write(self.style.SUCCESS(f"Taste profile rebuilt for user_id={user_id}."))
            return

        self.stdout.write(self.style.SUCCESS(f"Taste profiles rebuilt for {processed_users} users."))
