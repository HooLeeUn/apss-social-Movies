"""Plan or apply a verified TMDb import on PostgreSQL."""
from contextlib import contextmanager, nullcontext
from pathlib import Path
import sqlite3
import zlib

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from django.utils.connection import ConnectionDoesNotExist

from core.management.commands.diagnose_local_movie_duplicates import require_local, local_readonly_snapshot
from core.tmdb_importer import plan_import
from core.tmdb_enrichment_client import strict_json
from core.tmdb_enrichment_store import atomic_bytes
from core.models import Movie
from core.tmdb_import_apply import apply_import, check_schema, resolve_author
from core.tmdb_import_render import RENDER_ALIAS, RENDER_AUTHOR, render_connection, render_readonly_snapshot


class Command(BaseCommand):
    help = "Verify TMDb input on local or explicitly confirmed Render PostgreSQL. Dry-run is READ ONLY."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument("--run-dir", required=True, help="Prepared run directory (enrichment also accepted for dry-run)")
        parser.add_argument("--output-dir", required=True, help="Parent for a unique import report directory")
        parser.add_argument("--database", default="default")
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--dry-run", action="store_true")
        mode.add_argument("--apply", action="store_true")
        parser.add_argument("--confirm-database", help="Exact database name for apply or any Render operation")
        parser.add_argument("--confirm-host", help="Exact external Render hostname")
        parser.add_argument("--confirm-port", type=int, help="Exact Render PostgreSQL port")
        parser.add_argument("--confirm-user", help="Exact Render PostgreSQL login role")
        parser.add_argument("--tls-ca-file", help="Trusted CA bundle for Render PostgreSQL")
        parser.add_argument("--confirm-write", action="store_true", help="Explicitly authorize inserts with --apply")
        parser.add_argument("--author-username", help="Exact username in the destination database, required with --apply")
        parser.add_argument("--limit", type=int, help="Maximum inserts with apply, or maximum examined records with dry-run")
        parser.add_argument("--batch-size", type=int, default=100)
        parser.add_argument("--allow-partial", action="store_true", help="Explicitly accept a verified limited enrichment run")
        parser.add_argument("--allow-offline-input", action="store_true", help="Explicit synthetic-input opt-in for dry-run only")
        parser.add_argument("--resume", help="Previous report for this mode; writes a new report directory")

    def handle(self, *args, **options):
        apply = options["apply"]
        remote = options["database"] == RENDER_ALIAS
        if remote:
            if options["author_username"] not in (None, RENDER_AUTHOR):
                raise CommandError("The Render import author must be catalog_import.")
            options["author_username"] = RENDER_AUTHOR
            if options["allow_partial"] or options["allow_offline_input"]:
                raise CommandError("Render requires a verified live import_ready preparation.")
        elif any(options[key] is not None for key in ("confirm_host", "confirm_port", "confirm_user", "tls_ca_file")):
            raise CommandError("Render confirmations require the dedicated reccool_render_import alias.")
        if apply:
            if not options["confirm_write"] or not options["confirm_database"] or options["limit"] is None:
                raise CommandError("Apply requires --confirm-write, --confirm-database NAME and a positive --limit.")
            if not options["author_username"] or not options["author_username"].strip():
                raise CommandError("Apply requires an explicit --author-username.")
            if options["allow_partial"] or options["allow_offline_input"]:
                raise CommandError("Apply requires verified import_ready from live input and rejects dry-run overrides.")
        elif options["confirm_write"] or (options["confirm_database"] and not remote):
            raise CommandError("Write confirmations require --apply.")
        if not 1 <= options["batch_size"] <= 500 or (options["limit"] is not None and options["limit"] <= 0):
            raise CommandError("Use --batch-size 1..500 and an optional positive --limit.")
        parent = Path(options["output_dir"]).expanduser().resolve()
        repo = Path(settings.BASE_DIR).resolve()
        if parent == repo or repo in parent.parents or any((item / ".git").exists() for item in (parent, *parent.parents)):
            raise CommandError("Import reports must be outside repositories.")
        try:
            if remote:
                if not (Path(options["run_dir"]) / "preparation_manifest.json").is_file():
                    raise CommandError("Render requires a prepared run with preparation_manifest.json.")
                scope = render_connection(options)
            else:
                connection = connections[options["database"]]
                require_local(connection)
                target = {"alias": options["database"], **{key.lower(): connection.settings_dict[key] for key in ("HOST", "PORT", "NAME")}}
                scope = nullcontext((connection, target))
            with scope as (connection, target):
                if apply:
                    if options["confirm_database"] != target["name"]:
                        raise CommandError("The confirmed database name does not match the configured target.")
                    self.stdout.write(f"Apply target: {target['name']} at {target['host']}:{target['port']} (alias {target['alias']})")
                    summary = apply_import(options["run_dir"], parent, connection, target,
                                           batch_size=options["batch_size"], limit=options["limit"],
                                           author_username=options["author_username"], resume=options["resume"])
                else:
                    summary = self.plan_with_author(parent, target, options, connection=connection if remote else None)
        except ConnectionDoesNotExist as exc:
            raise CommandError("Unknown database alias.") from exc
        except CommandError as exc:
            if remote:
                raise CommandError(str(exc)) from None
            raise
        except (OSError, EOFError, zlib.error, ValueError, KeyError, TypeError, sqlite3.Error, DatabaseError) as exc:
            cause = None if remote else exc
            if apply:
                raise CommandError("Apply stopped. Check the run report; earlier batches may have committed. Reverify and rerun the same source to reconcile identities.") from cause
            raise CommandError("Import planning rejected: source, contract, model or checkpoint validation failed. Check the unique run report; no target writes were made.") from cause
        mode = "Apply" if apply else "Dry-run"
        self.stdout.write(self.style.SUCCESS(f"{mode} {summary['state']}: {summary['counts']}; inserted={summary['inserted']}; reports={summary['run_directory']}"))
        self.stdout.write(f"Destination author {strict_json(summary['author'])}")

    def plan_with_author(self, parent, target, options, *, connection=None):
        username = options["author_username"]
        author = {}

        @contextmanager
        def snapshot():
            extra = {"extra_read_tables": (get_user_model()._meta.db_table,)} if username else {}
            scope = (render_readonly_snapshot(connection, target) if connection is not None
                     else local_readonly_snapshot(target["alias"], **extra))
            with scope:
                if connection is not None:
                    with connection.cursor() as cursor:
                        check_schema(cursor, connection.ops.quote_name(Movie._meta.db_table))
                author.update(resolve_author(target, username))
                yield

        summary = plan_import(options["run_dir"], parent, target, snapshot,
                              batch_size=options["batch_size"], limit=options["limit"],
                              allow_offline=options["allow_offline_input"], allow_partial=options["allow_partial"],
                              resume=options["resume"])
        summary.update(author=author, author_blocks_apply=author["status"] != "resolved")
        path = Path(summary["run_directory"]) / "import_manifest.json"
        atomic_bytes(path, (strict_json(summary) + "\n").encode("utf-8"))
        return summary
