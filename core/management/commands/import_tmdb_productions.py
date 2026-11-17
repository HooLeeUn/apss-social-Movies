"""Controlled import planning. --apply is deliberately unavailable in phase 5's first delivery."""
from pathlib import Path
import sqlite3
import zlib

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from django.utils.connection import ConnectionDoesNotExist

from core.management.commands.diagnose_local_movie_duplicates import require_local, local_readonly_snapshot
from core.tmdb_importer import plan_import


class Command(BaseCommand):
    help = "Verify TMDb enrichment and plan new Movie records, local PostgreSQL READ ONLY. Apply is disabled."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument("--run-dir", required=True, help="Verified enrichment run directory, not an individual JSONL file")
        parser.add_argument("--output-dir", required=True, help="Parent for a unique import report directory")
        parser.add_argument("--database", default="default")
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--dry-run", action="store_true")
        mode.add_argument("--apply", action="store_true")
        parser.add_argument("--confirm-database", help="Reserved for future reviewed apply; does not unlock writes")
        parser.add_argument("--limit", type=int, help="Terminal records examined per invocation")
        parser.add_argument("--batch-size", type=int, default=100)
        parser.add_argument("--allow-partial", action="store_true", help="Explicitly accept a verified limited enrichment run")
        parser.add_argument("--allow-offline-input", action="store_true", help="Explicit synthetic-input opt-in for dry-run only")
        parser.add_argument("--resume", help="Previous import planning run; copied read-only into a new report run")

    def handle(self, *args, **options):
        if options["apply"]:
            raise CommandError("Apply is disabled: dry-run, backups and a concurrency integrity strategy must be reviewed first. No connection opened.")
        if not 1 <= options["batch_size"] <= 500 or (options["limit"] is not None and options["limit"] <= 0):
            raise CommandError("Use --batch-size 1..500 and an optional positive --limit.")
        parent = Path(options["output_dir"]).expanduser().resolve()
        repo = Path(settings.BASE_DIR).resolve()
        if parent == repo or repo in parent.parents or any((item / ".git").exists() for item in (parent, *parent.parents)):
            raise CommandError("Import reports must be outside repositories.")
        try:
            connection = connections[options["database"]]
            require_local(connection)  # configuration check only, before any connection opens
            target = {"alias": options["database"], **{key.lower(): connection.settings_dict[key] for key in ("HOST", "PORT", "NAME")}}
            summary = plan_import(options["run_dir"], parent, target,
                                  lambda: local_readonly_snapshot(options["database"]),
                                  batch_size=options["batch_size"], limit=options["limit"],
                                  allow_offline=options["allow_offline_input"], allow_partial=options["allow_partial"], resume=options["resume"])
        except ConnectionDoesNotExist as exc:
            raise CommandError("Unknown database alias.") from exc
        except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError, sqlite3.Error, DatabaseError) as exc:
            raise CommandError("Import planning rejected: source, contract, model or checkpoint validation failed. Check the unique run report; no target writes were made.") from exc
        self.stdout.write(self.style.SUCCESS(f"Dry-run {summary['state']}: {summary['counts']}; inserted=0; reports={summary['run_directory']}"))
