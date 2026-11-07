"""Diagnose Movie duplicates on an explicitly local PostgreSQL, without mutations."""
import csv
import ipaddress
import json
import os
import re
import socket
import time
from collections import Counter
from contextlib import ExitStack, contextmanager
from itertools import groupby, islice
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DatabaseError, connections, transaction
from django.db.models import Count, Q
from django.utils.connection import ConnectionDoesNotExist

from core.management.commands.compare_tmdb_exports import positive
from core.models import Movie
from core.movie_duplicate_diagnosis import analyze_group, selected_fields, technical_dependencies


BATCH_SIZE = 100
SESSION_CHECK = "SELECT host(inet_server_addr()), current_setting('transaction_read_only')"


def is_loopback(value):
    try:
        address = ipaddress.ip_address(value)
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        return address.is_loopback
    except ValueError:
        return False


def require_local(connection):
    """Validate configuration before get_autocommit/ensure_connection can connect."""
    if connection.vendor != "postgresql":
        raise CommandError("Only local PostgreSQL is authorized.")
    config = connection.settings_dict
    host = str(config.get("HOST", "")).lower()
    port = str(config.get("PORT", ""))
    name = str(config.get("NAME", ""))
    if host != "localhost" and not is_loopback(host):
        raise CommandError("Database HOST must be localhost or a literal loopback address.")
    if not port.isdecimal() or not 1 <= int(port) <= 65535:
        raise CommandError("Set an explicit local database PORT (1-65535).")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise CommandError("A plain database NAME is required; connection strings are forbidden.")
    forbidden = {"host", "hostaddr", "port", "dbname", "database", "dsn", "conninfo", "service", "servicefile", "pool"}
    if forbidden.intersection(key.lower() for key in config.get("OPTIONS", {})):
        raise CommandError("Routing overrides in database OPTIONS are forbidden.")
    if any(os.environ.get(key) for key in ("PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE")):
        raise CommandError("Remove libpq hostaddr/service environment overrides before diagnosis.")
    if host == "localhost":
        try:
            addresses = socket.getaddrinfo(host, int(port), type=socket.SOCK_STREAM)
        except OSError as exc:
            raise CommandError("Cannot establish that localhost resolves to loopback.") from exc
        if not addresses or any(not is_loopback(item[4][0]) for item in addresses):
            raise CommandError("localhost must resolve exclusively to loopback addresses.")
    if connection.connection is not None:
        # Reused connections may have been opened with configuration different from current settings.
        raise CommandError("An unopened dedicated connection is required; start a fresh management process.")


@contextmanager
def local_readonly_snapshot(alias):
    try:
        connection = connections[alias]
    except ConnectionDoesNotExist as exc:
        raise CommandError("Unknown database alias.") from exc
    require_local(connection)
    if connection.in_atomic_block:
        raise CommandError("Existing transactions are forbidden.")
    try:
        if not connection.get_autocommit():
            raise CommandError("Autocommit must be enabled before starting the diagnostic transaction.")
        with transaction.atomic(using=alias):
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(SESSION_CHECK)
                address, readonly = cursor.fetchone()
                if not address or not is_loopback(address) or readonly != "on":
                    raise CommandError("Server did not confirm loopback and READ ONLY; diagnosis rejected.")

            table = connection.ops.quote_name(Movie._meta.db_table)

            def guard(execute, sql, params, many, context):
                if many or not sql.lstrip().upper().startswith("SELECT ") or table not in sql:
                    raise CommandError("Unexpected SQL blocked: only Movie SELECTs are allowed.")
                return execute(sql, params, many, context)

            with connection.execute_wrapper(guard):
                yield
    finally:
        # Never leave an opened session or a long-lived snapshot behind.
        connection.close()


def valid_catalog(alias):
    return Movie.objects.using(alias).filter(
        type__in=(Movie.MOVIE, Movie.SERIES), tmdb_id__gt=0, tmdb_id__lte=2147483647)


def catalog_totals(alias):
    valid = Q(type__in=(Movie.MOVIE, Movie.SERIES), tmdb_id__gt=0, tmdb_id__lte=2147483647)
    return Movie.objects.using(alias).aggregate(
        total_rows=Count("id"), total_movies=Count("id", filter=Q(type=Movie.MOVIE)),
        total_series=Count("id", filter=Q(type=Movie.SERIES)), valid_rows=Count("id", filter=valid))


def duplicate_groups(alias):
    return (valid_catalog(alias).order_by().values("type", "tmdb_id")
            .annotate(record_count=Count("id")).filter(record_count__gt=1)
            .order_by("type", "tmdb_id").iterator(chunk_size=1000))


def duplicate_records(alias, groups):
    keys = Q()
    for group in groups:
        keys |= Q(type=group["type"], tmdb_id=group["tmdb_id"])
    return (Movie.objects.using(alias).filter(keys).order_by("type", "tmdb_id", "id")
            .values(*selected_fields()).iterator(chunk_size=1000))


def json_text(value):
    return json.dumps(value, ensure_ascii=False, default=str, sort_keys=True)


def csv_safe(value):
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
        return "'" + value
    return value


class Command(BaseCommand):
    help = "Read-only diagnosis of duplicate Movie keys on an authorized local PostgreSQL only."
    requires_system_checks = []
    requires_migrations_checks = False

    def add_arguments(self, parser):
        parser.add_argument("--database", default="default")
        parser.add_argument("--output-dir", required=True)
        parser.add_argument("--sample-size", type=int, default=20)
        parser.add_argument("--limit-groups", type=positive)

    def handle(self, *args, **options):
        started = time.monotonic()
        if options["sample_size"] < 0:
            raise CommandError("--sample-size must be nonnegative.")
        output = Path(options["output_dir"]).expanduser().resolve()
        repo = Path(settings.BASE_DIR).resolve()
        if output == repo or repo in output.parents or any((parent / ".git").exists() for parent in (output, *output.parents)):
            raise CommandError("Reports must be outside the repository.")
        if output.exists() and (not output.is_dir() or any(output.iterdir())):
            raise CommandError("Output directory must be new or empty.")
        # Validate the destination configuration before even creating output files.
        try:
            require_local(connections[options["database"]])
        except ConnectionDoesNotExist as exc:
            raise CommandError("Unknown database alias.") from exc
        try:
            with local_readonly_snapshot(options["database"]):
                totals = catalog_totals(options["database"])
                summary = {"status": "complete", "database_alias": options["database"], **totals,
                           "invalid_key_rows_excluded": totals["total_rows"] - totals["valid_rows"],
                           "duplicate_group_count": 0, "duplicate_extra_rows": 0,
                           "group_size_distribution": Counter(), "by_type": {},
                           "analyzed_groups": 0, "analyzed_extra_rows": 0,
                           "manual_review_required_groups": 0, "samples": [],
                           "limit_groups": options["limit_groups"], "record_batch_queries": 0}
                output.mkdir(parents=True, exist_ok=True)
                with ExitStack() as stack:
                    writers = {}
                    columns = {
                        "duplicate_groups": ["type", "tmdb_id", "record_count", "database_ids", "recommended_id",
                                             "reason", "manual_review_required", "different_fields", "complementary_fields"],
                        "duplicate_records": [*selected_fields(), "field_states", "default_matches", "coherence_issues", "completeness_score", "recommended"],
                        "duplicate_conflicts": ["type", "tmdb_id", "recommended_id", "conflicts", "complementary_fields", "coherence_issues"],
                    }
                    for name, fields in columns.items():
                        handle = stack.enter_context((output / f"{name}.csv").open("w", encoding="utf-8-sig", newline=""))
                        writers[name] = csv.DictWriter(handle, fieldnames=fields)
                        writers[name].writeheader()
                    groups = iter(duplicate_groups(options["database"]))
                    while batch := list(islice(groups, BATCH_SIZE)):
                        for group in batch:
                            kind = group["type"]
                            counts = summary["by_type"].setdefault(kind, {"groups": 0, "extra_rows": 0})
                            counts["groups"] += 1
                            counts["extra_rows"] += group["record_count"] - 1
                            summary["duplicate_group_count"] += 1
                            summary["duplicate_extra_rows"] += group["record_count"] - 1
                            summary["group_size_distribution"][str(group["record_count"])] += 1
                        remaining = None if options["limit_groups"] is None else max(0, options["limit_groups"] - summary["analyzed_groups"])
                        selected = batch if remaining is None else batch[:remaining]
                        if not selected:
                            continue
                        summary["record_batch_queries"] += 1
                        rows = duplicate_records(options["database"], selected)
                        for (kind, tmdb_id), members in groupby(rows, key=lambda row: (row["type"], row["tmdb_id"])):
                            records = list(members)
                            analysis = analyze_group(records)
                            summary["analyzed_groups"] += 1
                            summary["analyzed_extra_rows"] += len(records) - 1
                            summary["manual_review_required_groups"] += int(analysis["manual_review_required"])
                            group_row = {"type": kind, "tmdb_id": tmdb_id, "record_count": len(records),
                                         "database_ids": json_text([row["id"] for row in records]),
                                         **{key: analysis[key] for key in ("recommended_id", "reason", "manual_review_required")},
                                         "different_fields": json_text(analysis["different_fields"]),
                                         "complementary_fields": json_text(analysis["complementary_fields"])}
                            writers["duplicate_groups"].writerow(group_row)
                            if len(summary["samples"]) < options["sample_size"]:
                                summary["samples"].append(group_row)
                            issues = {}
                            for row in records:
                                details = analysis["records"][row["id"]]
                                issues[str(row["id"])] = details["issues"]
                                record_row = {**row, "field_states": json_text(details["states"]),
                                              "default_matches": json_text(details["default_matches"]),
                                              "coherence_issues": json_text(details["issues"]),
                                              "completeness_score": details["completeness"],
                                              "recommended": row["id"] == analysis["recommended_id"]}
                                writers["duplicate_records"].writerow({key: csv_safe(value) for key, value in record_row.items()})
                            if analysis["manual_review_required"]:
                                writers["duplicate_conflicts"].writerow({"type": kind, "tmdb_id": tmdb_id,
                                    "recommended_id": analysis["recommended_id"], "conflicts": json_text(analysis["conflicts"]),
                                    "complementary_fields": json_text(analysis["complementary_fields"]),
                                    "coherence_issues": json_text(issues)})
                    summary["partial_analysis"] = summary["analyzed_groups"] < summary["duplicate_group_count"]
                    summary["manual_review_scope"] = "analyzed_groups_only"
                    summary["select_query_count"] = 3 + summary["record_batch_queries"]
                    (output / "technical_dependencies.json").write_text(json_text(technical_dependencies()), encoding="utf-8")
            # Complete marker is written only after transaction exit, connection close and CSV flush.
            summary["elapsed_seconds"] = round(time.monotonic() - started, 3)
            (output / "duplicate_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        except OSError as exc:
            raise CommandError("Report I/O failed; files without duplicate_summary.json are incomplete.") from exc
        except DatabaseError as exc:
            raise CommandError("Database diagnosis failed; files without duplicate_summary.json are incomplete.") from exc
        self.stdout.write(self.style.SUCCESS(
            f"Duplicate groups: {summary['duplicate_group_count']}; extra rows: {summary['duplicate_extra_rows']}; "
            f"analyzed: {summary['analyzed_groups']}; manual review: {summary['manual_review_required_groups']}; "
            f"elapsed: {summary['elapsed_seconds']}s; reports: {output}"))
