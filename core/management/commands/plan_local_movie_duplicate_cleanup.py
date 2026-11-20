"""Generate a reviewable cleanup plan without mutating PostgreSQL."""
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections
from django.db.models import Count, Q
from django.utils.connection import ConnectionDoesNotExist

from core.management.commands.diagnose_local_movie_duplicates import json_text, local_readonly_snapshot, require_local
from core.models import Movie
from core.movie_duplicate_cleanup_plan import POLICY, dependency_paths, django_dependencies, read_schema, write_plan
from core.tmdb_enrichment_store import atomic_bytes, file_digest
from core.tmdb_importer import read_json


class Command(BaseCommand):
    help = "Build a deterministic duplicate cleanup plan on local PostgreSQL, READ ONLY."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument("--database", default="default")
        parser.add_argument("--confirm-database", required=True)
        parser.add_argument("--output-dir", required=True)
        parser.add_argument("--resume", help="Verify and compare a prior complete plan in a fresh snapshot")

    def handle(self, *args, **options):
        alias = options["database"]
        output = Path(options["output_dir"]).expanduser().resolve()
        repo = Path(settings.BASE_DIR).resolve()
        if output == repo or repo in output.parents or any((path / ".git").exists() for path in (output, *output.parents)):
            raise CommandError("Reports must be outside repositories.")
        try:
            connection = connections[alias]
        except ConnectionDoesNotExist as exc:
            raise CommandError("Unknown database alias.") from exc
        require_local(connection)
        config = connection.settings_dict
        target = {"alias": alias, "name": str(config["NAME"]), "host": str(config["HOST"]),
                  "port": int(config["PORT"])}
        if target["name"] != options["confirm_database"]:
            raise CommandError("Confirmed database differs from the configured target.")
        previous = None
        try:
            if options["resume"]:
                old = Path(options["resume"]).expanduser().resolve(strict=True)
                previous = read_json(old / "cleanup_manifest.json")
                if (previous.get("status") != "complete" or previous.get("target") != target
                        or previous.get("policy") != POLICY
                        or previous.get("plan_sha256") != file_digest(old / "cleanup_plan.jsonl")
                        or previous.get("dependencies_sha256") != file_digest(old / "cleanup_dependencies.json")):
                    raise CommandError("Previous plan is incomplete, has changed, or belongs to a different target/policy.")
            output.mkdir(parents=True, exist_ok=False)
            edges = django_dependencies()
            extra_tables = sorted({"pg_constraint", "pg_index", "pg_trigger", *(edge["table"] for edge in edges)})
            with local_readonly_snapshot(alias, extra_read_tables=extra_tables):
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT current_database(), inet_server_port(),
                               to_regclass('core_movie') = to_regclass('public.core_movie')
                        FROM pg_catalog."pg_constraint" LIMIT 1
                    """)
                    if cursor.fetchone() != (target["name"], target["port"], True):
                        raise CommandError("PostgreSQL did not confirm the selected database, port and Movie table.")
                    schema = read_schema(cursor, edges)
                types = list(Movie.objects.using(alias).order_by().values("type").annotate(
                    count=Count("pk"), null_tmdb=Count("pk", filter=Q(tmdb_id__isnull=True)),
                    invalid_tmdb=Count("pk", filter=Q(tmdb_id__lte=0) | Q(tmdb_id__gt=2147483647)),
                ).order_by("type"))
                excluded_duplicates = (Movie.objects.using(alias)
                    .filter(type__isnull=False, tmdb_id__isnull=False)
                    .exclude(type__in=(Movie.MOVIE, Movie.SERIES), tmdb_id__gt=0, tmdb_id__lte=2147483647)
                    .order_by().values("type", "tmdb_id").annotate(row_count=Count("pk"))
                    .filter(row_count__gt=1).count())
                plan = write_plan(alias, output, dependency_paths(edges),
                                  estimate_dependencies=schema["fk_graph_matches"])
            summary = {"status": "complete", "policy": POLICY, "target": target, "apply_enabled": False,
                       "types": types, "total_movies": sum(row["count"] for row in types),
                       "null_tmdb": sum(row["null_tmdb"] for row in types),
                       "invalid_tmdb": sum(row["invalid_tmdb"] for row in types),
                       "unrecognized_type_rows": sum(row["count"] for row in types if row["type"] not in ("movie", "series")),
                       "excluded_nonnull_duplicate_groups": excluded_duplicates,
                       "dependency_estimate_complete": schema["fk_graph_matches"], **plan}
            schema_path = output / "cleanup_dependencies.json"
            atomic_bytes(schema_path, (json_text({"django": edges, "postgresql": schema}) + "\n").encode("utf-8"))
            summary.update(plan_sha256=file_digest(output / "cleanup_plan.jsonl"),
                           dependencies_sha256=file_digest(schema_path), schema_blockers=schema["blockers"])
            if previous is not None:
                summary["previous_plan_matches"] = all(summary.get(key) == previous.get(key) for key in
                                                       ("plan_sha256", "dependencies_sha256", "types", "excluded_nonnull_duplicate_groups"))
            atomic_bytes(output / "cleanup_manifest.json", (json_text(summary) + "\n").encode("utf-8"))
        except (OSError, DatabaseError, ValueError, TypeError) as exc:
            raise CommandError("Cleanup planning failed. A directory without a complete manifest cannot be used.") from exc
        self.stdout.write(f"Planned {plan['groups']} groups and {plan['remove_movies']} candidate removals in {output}")
