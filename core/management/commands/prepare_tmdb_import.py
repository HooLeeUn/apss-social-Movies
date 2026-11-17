"""Prepare import_ready without opening any Django database connection."""
from pathlib import Path
import sqlite3
import zlib

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from core.tmdb_import_ready import prepare_run, verify_ready_run


class Command(BaseCommand):
    help = "Prepare or verify a unique import_ready.jsonl.gz; no PostgreSQL or HTTP."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        action = parser.add_mutually_exclusive_group(required=True)
        action.add_argument("--run-dir", help="Enrichment run source")
        action.add_argument("--verify", help="Prepared run directory to verify read-only")
        parser.add_argument("--output-dir")
        parser.add_argument("--allow-partial", action="store_true")
        parser.add_argument("--allow-offline-input", action="store_true")
        parser.add_argument("--convert-legacy", action="store_true")
        parser.add_argument("--resume")
        parser.add_argument("--batch-size", type=int, default=100)

    def handle(self, *args, **options):
        try:
            if options["verify"]:
                summary = verify_ready_run(options["verify"])
                self.stdout.write(self.style.SUCCESS(f"Verified import_ready: {summary['counts']}"))
                return
            if not options["output_dir"]:
                raise CommandError("--output-dir is required for preparation.")
            output = Path(options["output_dir"]).expanduser().resolve()
            repo = Path(settings.BASE_DIR).resolve()
            if output == repo or repo in output.parents or any((path / ".git").exists() for path in (output, *output.parents)):
                raise CommandError("Prepared outputs must be outside repositories.")
            summary = prepare_run(options["run_dir"], output, allow_offline=options["allow_offline_input"],
                                  allow_partial=options["allow_partial"], convert_legacy=options["convert_legacy"],
                                  batch_size=options["batch_size"], resume=options["resume"])
        except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
            raise CommandError("Preparation/verification rejected; inspect the unique preparation manifest. No database writes or HTTP requests were made.") from exc
        self.stdout.write(self.style.SUCCESS(f"Prepared {summary['counts']['included']} records: {summary['run_directory']}"))
