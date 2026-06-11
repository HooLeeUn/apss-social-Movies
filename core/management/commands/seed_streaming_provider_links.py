from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db import transaction

from core.models import StreamingProviderLink
from core.streaming_provider_links_seed import (
    build_streaming_provider_link_seeds,
    get_general_provider_link,
    should_add_notes_from_seed,
    should_update_landing_url_from_seed,
)


class Command(BaseCommand):
    help = "Seed general streaming provider landing URLs by provider and country."

    @transaction.atomic
    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0

        for seed in build_streaming_provider_link_seeds():
            link = get_general_provider_link(seed)

            if link is None:
                StreamingProviderLink.objects.create(
                    provider_id=seed.provider_id,
                    provider_name=seed.provider_name,
                    country_code=seed.country_code,
                    landing_url=seed.landing_url,
                    is_active=True,
                    monetization_type=StreamingProviderLink.MonetizationType.NONE,
                    notes=seed.notes,
                )
                created_count += 1
                continue

            update_fields = []
            if link.provider_id != seed.provider_id:
                link.provider_id = seed.provider_id
                update_fields.append("provider_id")
            should_update_landing_url = should_update_landing_url_from_seed(
                link,
                seed,
                update_static_provider=False,
            )
            if should_update_landing_url:
                link.landing_url = seed.landing_url
                update_fields.append("landing_url")
            if not link.provider_name:
                link.provider_name = seed.provider_name
                update_fields.append("provider_name")
            if not link.is_active:
                link.is_active = True
                update_fields.append("is_active")
            if should_add_notes_from_seed(link, seed):
                link.notes = f"{link.notes}\n{seed.notes}".strip()
                update_fields.append("notes")

            if update_fields:
                update_fields.append("updated_at")
                link.save(update_fields=update_fields)
                updated_count += 1

        self.stdout.write(
            self.style.SUCCESS(
                f"Seeded streaming provider links: {created_count} created, {updated_count} updated."
            )
        )
