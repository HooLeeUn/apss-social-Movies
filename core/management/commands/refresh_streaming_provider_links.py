from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone

from core.models import StreamingProviderLink
from core.streaming_provider_links_seed import (
    build_streaming_provider_link_seeds,
    get_general_provider_link,
    should_add_notes_from_seed,
    should_update_landing_url_from_seed,
)


class Command(BaseCommand):
    help = "Refresh general streaming provider landing URLs without overwriting manual affiliate settings."

    @transaction.atomic
    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0
        verified_at = timezone.now()

        for seed in build_streaming_provider_link_seeds():
            link = get_general_provider_link(seed)

            if link is None:
                StreamingProviderLink.objects.create(
                    provider_id=seed.provider_id,
                    provider_name=seed.provider_name,
                    country_code=seed.country_code,
                    landing_url=seed.landing_url,
                    is_active=True,
                    last_verified_at=verified_at,
                    monetization_type=StreamingProviderLink.MonetizationType.NONE,
                    notes=seed.notes,
                )
                created_count += 1
                continue

            update_fields = []
            if link.provider_id != seed.provider_id:
                link.provider_id = seed.provider_id
                update_fields.append("provider_id")
            if link.provider_name != seed.provider_name:
                link.provider_name = seed.provider_name
                update_fields.append("provider_name")
            should_update_landing_url = should_update_landing_url_from_seed(link, seed)
            if should_update_landing_url:
                link.landing_url = seed.landing_url
                update_fields.append("landing_url")
            if not link.is_active:
                link.is_active = True
                update_fields.append("is_active")
            if should_add_notes_from_seed(link, seed):
                link.notes = f"{link.notes}\n{seed.notes}".strip()
                update_fields.append("notes")
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
