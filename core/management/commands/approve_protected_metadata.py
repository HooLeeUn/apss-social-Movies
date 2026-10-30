"""Create an explicit sidecar; never alter selection, journal or database."""
import json
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from core.adult_cleanup import digest_file, target_identity
from core.models import Movie
from core.protected_metadata_approval import (IDENTITY, METADATA_FIELDS, journal_binding,
                                            recover_known_metadata, validate_approval, validate_protected_rows)


class Command(BaseCommand):
    help = "Pin one reviewed metadata hash transition for protected Render Movie 525617"

    def add_arguments(self, parser):
        parser.add_argument("--selection", required=True, type=Path)
        parser.add_argument("--journal", required=True, type=Path)
        parser.add_argument("--expected-database", required=True)
        parser.add_argument("--expected-host", required=True)
        parser.add_argument("--batch-size", type=int, default=25)  # Existing renderArgs2 compatibility.
        parser.add_argument("--expected-metadata-sha256", required=True)
        parser.add_argument("--approved-metadata-sha256", required=True)
        parser.add_argument("--reason", required=True)
        parser.add_argument("--output", required=True, type=Path)

    def handle(self, *args, **options):
        selection, journal, output = options["selection"], options["journal"], options["output"]
        if output.exists():
            raise CommandError("Output exists; refusing to overwrite any authorization or audit file")
        if options["expected_database"] != "reccool_postgres_production":
            raise CommandError("This approval is exclusively for reccool_postgres_production")
        if not options["reason"].strip():
            raise CommandError("Explicit review reason required")
        try:
            selection_hash, journal_hash = digest_file(selection), digest_file(journal)
            proof = json.loads(selection.read_text(encoding="utf-8"))
            identity = target_identity(options["expected_database"], options["expected_host"])
            if identity != proof["database"]:
                raise CommandError("Selection belongs to another database")
            present = {m.pk: m for m in Movie.objects.filter(pk__in=[r["database_id"] for r in proof["protected_movies"]])}
            movie = present.get(525617)
            if movie is None:
                raise CommandError("Protected 525617 is missing")
            document = {
                "version": 1, "database": identity, "identity": IDENTITY,
                "selection_sha256": selection_hash, "manifest_sha256": proof["manifest_sha256"],
                "journal_binding_sha256": journal_binding(journal, proof, selection),
                "expected_metadata_sha256": options["expected_metadata_sha256"],
                "approved_metadata_sha256": options["approved_metadata_sha256"],
                "approved_metadata": {key: getattr(movie, key) for key in METADATA_FIELDS},
                "expected_metadata": recover_known_metadata(options["expected_metadata_sha256"]),
                "reason": options["reason"], "created_at": datetime.now(timezone.utc).isoformat(),
            }
            validate_approval(document, proof, selection, journal, identity)
            validate_protected_rows(proof, present, document)
            if digest_file(selection) != selection_hash or digest_file(journal) != journal_hash:
                raise CommandError("Selection or journal changed concurrently; stop the other writer and review")
            # Exclusive creation only. Never replace the source selection/journal or an existing sidecar.
            with output.open("x", encoding="utf-8") as file:
                json.dump(document, file, ensure_ascii=False, indent=2)
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps({"created": str(output), "approval_sha256": digest_file(output),
                                      "database_id": 525617, "protected_checked": len(present),
                                      "changed_metadata_fields": [key for key in METADATA_FIELDS
                                          if document["expected_metadata"] is not None
                                          and document["expected_metadata"][key] != document["approved_metadata"][key]],
                                      "selection_and_journal_unchanged": True}, ensure_ascii=True))
