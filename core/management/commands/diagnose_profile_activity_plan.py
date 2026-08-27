import json
import re

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction

from core.social_feed import SocialActivityFeedService


class Command(BaseCommand):
    help = "Run an explicit, read-only EXPLAIN ANALYZE for the Candidate ratings family."

    def add_arguments(self, parser):
        parser.add_argument("--family", required=True, choices=("ratings",))
        parser.add_argument("--user-id", required=True, type=int)
        parser.add_argument("--limit", type=int, default=11)

    def handle(self, *args, **options):
        if not 1 <= options["limit"] <= 101:
            raise CommandError("--limit must be between 1 and 101")
        viewer_id = options["user_id"]
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
            try:
                viewer = get_user_model().objects.get(pk=viewer_id)
            except get_user_model().DoesNotExist as exc:
                raise CommandError("User does not exist") from exc
            queryset = SocialActivityFeedService.rating_candidates_queryset(
                actor_ids=[viewer_id], viewer=viewer
            ).order_by("-candidate_activity_at", "-id")[:options["limit"]]
            plan = queryset.explain(analyze=True, buffers=True, format="json")

        # Plans can echo predicates. Preserve costs, times, rows and buffer
        # metrics while replacing the explicitly supplied viewer identifier.
        def redact(value):
            if isinstance(value, str):
                return re.sub(
                    rf"(?<!\d){re.escape(str(viewer_id))}(?!\d)",
                    "<viewer_id>", value,
                )
            if isinstance(value, list):
                return [redact(item) for item in value]
            if isinstance(value, dict):
                return {key: redact(item) for key, item in value.items()}
            return value

        self.stdout.write(json.dumps(redact(json.loads(plan)), indent=2, sort_keys=True))
