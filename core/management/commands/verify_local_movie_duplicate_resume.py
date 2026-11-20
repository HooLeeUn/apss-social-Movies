"""Strict read-only recovery validation; never writes inside the reviewed run."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections

from core.management.commands.diagnose_local_movie_duplicates import require_local
from core.movie_duplicate_cleanup import (cleanup_session, dependency_paths, django_dependencies,
    load_plan, load_receipts, preflight, publish)
from core.movie_duplicate_resume import failure_report, load_review, update_progress
from core.tmdb_importer import read_json


def artifact_hashes(directories):
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for directory in directories for path in sorted(directory.rglob("*")) if path.is_file()}


class Command(BaseCommand):
    help = "Verify an existing cleanup execution in READ ONLY; no apply, migrations or checkpoint edits."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        for name in ("plan-dir", "review-dir", "execution-dir", "confirm-database", "confirm-plan-sha256", "report-dir"):
            parser.add_argument("--" + name, required=True)

    def handle(self, *args, **options):
        source, review_dir, execution_dir, report_dir = (Path(options[key]).resolve() for key in
            ("plan_dir", "review_dir", "execution_dir", "report_dir"))
        repo = Path(settings.BASE_DIR).resolve()
        if any(report_dir == path or path in report_dir.parents or report_dir in path.parents
               for path in (source, review_dir, execution_dir, repo)) or report_dir.exists() or any(
                   (path / ".git").exists() for path in (report_dir, *report_dir.parents)):
            raise CommandError("Recovery report requires a new directory outside the source/review/execution/repository.")
        connection = connections["default"]
        require_local(connection)
        cfg = connection.settings_dict
        target = {"alias": "default", "name": str(cfg["NAME"]), "host": str(cfg["HOST"]), "port": int(cfg["PORT"])}
        if options["confirm_database"] != target["name"]:
            raise CommandError("Confirmed database differs from the configured target.")
        manifest, groups, schema, manifest_sha = load_plan(source, target, options["confirm_plan_sha256"])
        execution = {"target": target, "plan_sha256": manifest["plan_sha256"],
                     "source_manifest_sha256": manifest_sha, "mode": "apply"}
        reviewed, review_sha = load_review(review_dir, execution)
        execution["review_sha256"] = review_sha
        if read_json(execution_dir / "execution_config.json") != execution:
            raise CommandError("Execution output belongs to another source, target or mode.")
        before = artifact_hashes((source, review_dir, execution_dir))
        previous_state = read_json(execution_dir / "execution_summary.json").get("state")
        report_dir.mkdir(parents=True, exist_ok=False)
        progress = {"groups": len(groups), "remove_movies": sum(len(group["remove_ids"]) for group in groups),
            "committed_groups": None, "pending_groups": None, "last_committed_group": None,
            "progress_verification_complete": False, "removed_movies": 0, "active_group": None}
        old_options = deepcopy(cfg.get("OPTIONS", {}))
        cfg["OPTIONS"] = {**old_options, "options": old_options.get("options", "") + " -c default_transaction_read_only=on"}
        statements = {"select": 0, "control": 0, "mutations": 0}

        def readonly(execute, sql, params, many, context):
            text = sql.lstrip().upper()
            if (many or not text.startswith(("SELECT", "SET ", "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE"))
                    or "READ WRITE" in text):
                statements["mutations"] += 1
                raise CommandError("SQL mutation blocked during recovery verification.")
            statements["select" if text.startswith("SELECT") else "control"] += 1
            return execute(sql, params, many, context)

        try:
            with connection.execute_wrapper(readonly), cleanup_session(connection, target, False):
                receipts = load_receipts(execution_dir, groups)
                applied = {i for i, receipt in receipts.items() if receipt["state"] == "committed"}
                pending = next((i for i in receipts if i not in applied), None)
                from core.models import Movie
                if pending is not None and not Movie.objects.filter(pk__in=groups[pending]["remove_ids"]).exists():
                    applied.add(pending)
                update_progress(progress, groups, receipts, set(), complete=False)
                edges = django_dependencies()
                # The normal preflight recomputation stays ephemeral. It never
                # becomes a replacement plan or touches the three input folders.
                with tempfile.TemporaryDirectory(prefix="reccool-resume-check-") as temporary:
                    preflight(connection, target, schema, edges, dependency_paths(edges), groups,
                        Path(temporary), receipts, applied, reviewed, progress=progress)
            if artifact_hashes((source, review_dir, execution_dir)) != before:
                raise CommandError("Original cleanup artifacts changed during recovery verification.")
            report = {**execution, **progress, "mode": "verify-resume", "state": "recoverable",
                "original_execution_state": previous_state, "read_only": True,
                "original_artifacts_unchanged": True, "sql": statements,
                "limit_groups_semantics": "additional_groups_per_invocation"}
            publish(report_dir / "recovery_summary.json", report)
        except BaseException as error:
            publish(report_dir / "recovery_summary.json", {**failure_report(execution, progress, error),
                "mode": "verify-resume", "read_only": True, "sql": statements,
                "original_artifacts_unchanged": artifact_hashes((source, review_dir, execution_dir)) == before})
            if isinstance(error, (OSError, ValueError, TypeError, KeyError, DatabaseError)):
                raise CommandError("Recovery verification blocked; inspect the separate recovery report.") from error
            raise
        finally:
            connection.close()
            cfg["OPTIONS"] = old_options
        self.stdout.write(json.dumps(report, ensure_ascii=False, sort_keys=True))
