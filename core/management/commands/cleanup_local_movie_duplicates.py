"""Validate or execute an explicitly reviewed local Movie cleanup plan."""
import uuid
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections

from core.management.commands.diagnose_local_movie_duplicates import require_local
from core.movie_duplicate_cleanup import cleanup_session, execute_cleanup, load_plan, publish
from core.movie_duplicate_resume import failure_report, load_review
from core.tmdb_enrichment_store import StorageError, run_lock
from core.tmdb_importer import read_json


class Command(BaseCommand):
    help = "Dry-run by default. Apply a reviewed duplicate cleanup plan on local PostgreSQL."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument("--plan-dir", required=True)
        parser.add_argument("--output-dir", required=True)
        parser.add_argument("--confirm-database", required=True)
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--dry-run", action="store_true")
        mode.add_argument("--apply", action="store_true")
        parser.add_argument("--confirm-maintenance", action="store_true")
        parser.add_argument("--confirm-plan-sha256")
        parser.add_argument("--review-dir", help="Output of the successful dry-run reviewed for apply")
        parser.add_argument("--limit-groups", type=int)
        parser.add_argument("--resume", action="store_true", help="Resume apply in its existing output directory")

    def handle(self, *args, **options):
        apply = options["apply"]
        limit = options["limit_groups"]
        if apply and (not options["confirm_maintenance"] or not options["confirm_plan_sha256"]
                      or not options["review_dir"] or not limit or limit < 1):
            raise CommandError("Apply requires --confirm-maintenance, --confirm-plan-sha256, --review-dir and a positive --limit-groups.")
        if not apply and (options["resume"] or options["confirm_maintenance"] or options["confirm_plan_sha256"]
                          or options["review_dir"] or limit is not None):
            raise CommandError("Maintenance, approval, limit and resume options require --apply.")
        source = Path(options["plan_dir"]).resolve()
        output = Path(options["output_dir"]).resolve()
        repo = Path(settings.BASE_DIR).resolve()
        if (output == source or source in output.parents or output in source.parents or output == repo
                or repo in output.parents or any((path / ".git").exists() for path in (output, *output.parents))):
            raise CommandError("Output must be outside repositories and separate from the source plan.")
        connection = connections["default"]
        require_local(connection)
        config = connection.settings_dict
        target = {"alias": "default", "name": str(config["NAME"]), "host": str(config["HOST"]), "port": int(config["PORT"])}
        if options["confirm_database"] != target["name"]:
            raise CommandError("Confirmed database differs from the configured target.")
        resume = options["resume"]
        try:
            manifest, groups, schema, manifest_digest = load_plan(source, target, options["confirm_plan_sha256"])
            config_path = output / "execution_config.json"
            execution = {"target": target, "plan_sha256": manifest["plan_sha256"],
                         "source_manifest_sha256": manifest_digest,
                         "mode": "apply" if apply else "dry-run"}
            reviewed = None
            if apply:
                review_dir = Path(options["review_dir"]).resolve(strict=True)
                if review_dir == output or output in review_dir.parents or review_dir in output.parents:
                    raise CommandError("Review and execution directories must be separate.")
                reviewed, review_digest = load_review(review_dir, execution)
                execution["review_sha256"] = review_digest
            if not resume:
                output.mkdir(parents=True, exist_ok=False)
            elif not output.is_dir():
                raise CommandError("Resume requires the original apply output directory.")
            with run_lock(output):
                if resume:
                    if read_json(config_path) != execution:
                        raise CommandError("Execution output belongs to another source, target or mode.")
                else:
                    publish(config_path, execution)
                # Only during an explicitly requested execution: preserve the
                # previous report verbatim before the next summary replaces it.
                attempt_path = output / ("attempt-" + uuid.uuid4().hex + ".json")
                previous = read_json(output / "execution_summary.json") if (output / "execution_summary.json").exists() else None
                publish(attempt_path, {**execution, "state": "started", "previous_summary": previous})
                progress = {}
                try:
                    with cleanup_session(connection, target, apply):
                        summary = execute_cleanup(connection, target, schema, groups, output,
                            apply=apply, limit=limit, resume=resume, reviewed=reviewed, progress=progress)
                except BaseException as exc:
                    failure = failure_report(execution, progress, exc)
                    publish(attempt_path, {**failure, "previous_summary": previous})
                    publish(output / "execution_summary.json", failure)
                    raise
                publish(attempt_path, {**execution, **summary, "previous_summary": previous})
                publish(output / "execution_summary.json", {**execution, **summary})
        except (OSError, DatabaseError, StorageError, ValueError, TypeError, KeyError) as exc:
            raise CommandError("Cleanup stopped. Inspect the execution reports before continuing.") from exc
        self.stdout.write(f"Cleanup {summary['state']}. Groups={summary['groups']}, committed={summary['committed_groups']}. Reports={output}")
