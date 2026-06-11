from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import StreamingProviderLink
from core.streaming_provider_links_seed import STREAMING_PROVIDER_LINK_SEEDS


class Command(BaseCommand):
    help = "Seed general streaming provider landing URLs by provider and country."

    @transaction.atomic
    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0

        for seed in STREAMING_PROVIDER_LINK_SEEDS:
            link = (
                StreamingProviderLink.objects.filter(
                    provider_id=seed.provider_id,
                    country_code=seed.country_code,
                    movie__isnull=True,
                    tmdb_id__isnull=True,
                    imdb_id__isnull=True,
                )
                .order_by("id")
                .first()
            )

            if link is None:
                StreamingProviderLink.objects.create(
                    provider_id=seed.provider_id,
                    provider_name=seed.provider_name,
                    country_code=seed.country_code,
                    landing_url=seed.landing_url,
                    is_active=True,
                    monetization_type=StreamingProviderLink.MonetizationType.NONE,
                )
                created_count += 1
                continue

            update_fields = []
            if not link.landing_url:
                link.landing_url = seed.landing_url
                update_fields.append("landing_url")
            if not link.provider_name:
                link.provider_name = seed.provider_name
                update_fields.append("provider_name")

            if update_fields:
                update_fields.append("updated_at")
                link.save(update_fields=update_fields)
                updated_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Seeded streaming provider links: {created_count} created, {updated_count} updated."
            )
        )
