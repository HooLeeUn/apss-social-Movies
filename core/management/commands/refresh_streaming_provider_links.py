from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from core.models import StreamingProviderLink
from core.streaming_provider_links_seed import STREAMING_PROVIDER_LINK_SEEDS


class Command(BaseCommand):
    help = "Refresh general streaming provider landing URLs without overwriting manual affiliate settings."

    @transaction.atomic
    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0
        verified_at = timezone.now()

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
                    last_verified_at=verified_at,
                    monetization_type=StreamingProviderLink.MonetizationType.NONE,
                )
                created_count += 1
                continue

            update_fields = []
            if link.provider_name != seed.provider_name:
                link.provider_name = seed.provider_name
                update_fields.append("provider_name")
            if link.landing_url != seed.landing_url:
                link.landing_url = seed.landing_url
                update_fields.append("landing_url")
            if link.last_verified_at != verified_at:
                link.last_verified_at = verified_at
                update_fields.append("last_verified_at")

            if update_fields:
                update_fields.append("updated_at")
                link.save(update_fields=update_fields)
                updated_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Refreshed streaming provider links: {created_count} created, {updated_count} updated."
            )
        )
