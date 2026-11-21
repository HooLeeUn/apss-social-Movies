"""Insert verified prepared productions into an explicitly confirmed database."""
from collections import Counter
from datetime import datetime, timezone
import gzip
from itertools import islice
import json
import os
from pathlib import Path
import uuid

from django.core.management.base import CommandError
from django.contrib.auth import get_user_model
from django.db import connections, transaction

from core.management.commands.diagnose_local_movie_duplicates import is_loopback, require_local
from core.models import Movie
from core.tmdb_enrichment_client import reject_constant, strict_json
from core.tmdb_enrichment_store import atomic_bytes, file_digest
from core.tmdb_import_ready import snapshot_ready_input
from core.tmdb_importer import DERIVED_FIELDS, evaluate_record, existing_identities, read_json
from core.tmdb_import_render import RENDER_ALIAS, RENDER_AUTHOR, check_render_session


def resolve_author(target, username, *, lock=False):
    result = {"username": username, "id": None, "status": "not_provided"}
    if not username:
        return result
    if target["alias"] == RENDER_ALIAS and username != RENDER_AUTHOR:
        return {**result, "status": "expected_render_catalog_import"}
    if target["alias"] != RENDER_ALIAS and target["name"] == "socialapp" and username != "Admin":
        return {**result, "status": "expected_local_admin"}
    user_model = get_user_model()
    username_field = user_model.USERNAME_FIELD
    users = user_model._default_manager.using(target["alias"])
    if lock:
        users = users.select_for_update(no_key=True)
    try:
        pk, actual = users.values_list("pk", username_field).get(**{username_field: username})
    except user_model.DoesNotExist:
        return {**result, "status": "not_found"}
    except user_model.MultipleObjectsReturned:
        return {**result, "status": "ambiguous"}
    if actual != username:
        return {**result, "status": "not_found"}
    return {**result, "id": pk, "status": "resolved"}


def check_session(cursor, target, read_only):
    if target.get("alias") == RENDER_ALIAS:
        check_render_session(cursor, target, read_only)
        return
    cursor.execute("""
        SELECT host(inet_server_addr()), inet_server_port(), current_database(),
               current_setting('transaction_read_only'), pg_is_in_recovery()
    """)
    address, port, name, readonly, recovery = cursor.fetchone()
    if not is_loopback(address) or port != int(target["port"]) or name != target["name"]:
        raise CommandError("PostgreSQL did not confirm the selected loopback address, port and database.")
    if readonly != ("on" if read_only else "off") or recovery:
        raise CommandError("PostgreSQL did not confirm the required transaction mode on a primary.")


def check_schema(cursor, table):
    # Require uniqueness for every non-null pair, independent of other writers.
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_index i
            JOIN pg_attribute first ON first.attrelid = i.indrelid AND first.attnum = i.indkey[0]
            JOIN pg_attribute second ON second.attrelid = i.indrelid AND second.attnum = i.indkey[1]
            WHERE i.indrelid = %s::regclass AND i.indrelid = to_regclass('public.core_movie')
              AND i.indisunique AND i.indisvalid
              AND i.indisready AND i.indislive AND i.indimmediate
              AND i.indnkeyatts = 2 AND i.indpred IS NULL AND i.indexprs IS NULL
              AND first.attname IN ('type', 'tmdb_id')
              AND second.attname IN ('type', 'tmdb_id') AND first.attnum <> second.attnum
        )
    """, [table])
    if not cursor.fetchone()[0]:
        raise CommandError(
            "Apply blocked: public.core_movie requires a valid, immediate, non-partial unique index "
            "on (type, tmdb_id). Review historical duplicates and approve schema changes first."
        )
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid
            WHERE t.tgrelid = %s::regclass AND NOT t.tgisinternal
              AND t.tgname = 'core_movie_search_vector_update_trigger'
              AND t.tgenabled IN ('O', 'A') AND (t.tgtype::integer & 7) = 7
              AND t.tgqual IS NULL AND p.proname = 'core_movie_search_vector_update'
              AND current_setting('session_replication_role') IN ('origin', 'local')
        )
    """, [table])
    if not cursor.fetchone()[0]:
        raise CommandError("Apply blocked: the expected Movie search-vector insert trigger is inactive or missing.")
    author_field = Movie._meta.get_field("author")
    user_model = get_user_model()
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_constraint c
            JOIN pg_attribute movie_column ON movie_column.attrelid = c.conrelid
                AND movie_column.attnum = c.conkey[1]
            JOIN pg_attribute user_column ON user_column.attrelid = c.confrelid
                AND user_column.attnum = c.confkey[1]
            WHERE c.conrelid = %s::regclass AND c.confrelid = %s::regclass
                AND c.contype = 'f' AND c.convalidated AND cardinality(c.conkey) = 1
                AND movie_column.attname = %s AND user_column.attname = %s
        )
    """, [table, user_model._meta.db_table, author_field.column, user_model._meta.pk.column])
    if not cursor.fetchone()[0]:
        raise CommandError("Apply blocked: Movie.author lacks a validated foreign key to the destination user model.")


def apply_batch(connection, target, table, batch, remaining, author):
    alias = target["alias"]
    with transaction.atomic(using=alias, durable=True):
        with connection.cursor() as cursor:
            if alias == RENDER_ALIAS:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE")
            else:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            check_session(cursor, target, read_only=False)
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute("SET LOCAL synchronous_commit = 'on'")
            # Serializes Movie writes and holds schema protection through COMMIT.
            cursor.execute(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE")
            check_schema(cursor, table)
        if resolve_author(target, author["username"], lock=True) != author:
            raise CommandError("Apply blocked: the destination author changed or disappeared.")
        existing = existing_identities(alias, batch)
        decisions, proposals, movies = [], [], []
        for record in batch:
            key = (record["type"], record["tmdb_id"])
            decision = evaluate_record(record, existing.get(key))
            if decision["decision"] == "proposed_new":
                if decision["adult"]["resolved"] is not False or decision["adult"]["conflict"]:
                    raise CommandError("Apply blocked: inconsistent adult admission in prepared input.")
                proposals.append(decision)
                movies.append(Movie(author_id=author["id"], **decision["proposed_fields"]))
            decisions.append(decision)
            if len(movies) == remaining:
                break
        if movies:
            catalog = Movie.objects.using(alias)
            catalog.bulk_create(movies)
            saved = catalog.in_bulk([movie.pk for movie in movies])
            for movie, decision in zip(movies, proposals, strict=True):
                row = saved.get(movie.pk)
                fields = ("type", "tmdb_id", *DERIVED_FIELDS)
                if (row is None or not row.search_vector or row.author_id != author["id"]
                        or any(getattr(row, field) != getattr(movie, field) for field in fields)):
                    raise CommandError("Movie identity, author or derived fields failed verification. Batch rolled back.")
                decision.update(decision="inserted", inserted=True, movie_id=movie.pk, author_id=row.author_id)
    return decisions


def apply_import(source, output_parent, connection, target, *, batch_size, limit, author_username, resume=None):
    if type(limit) is not int or limit <= 0:
        raise CommandError("Apply requires a positive insertion limit.")
    if connection.alias != target["alias"]:
        raise CommandError("Apply target and connection aliases must match.")
    if connection.alias == RENDER_ALIAS:
        if (connection is not connections[RENDER_ALIAS] or connection.vendor != "postgresql"
                or connection.connection is not None or author_username != RENDER_AUTHOR):
            raise CommandError("Render apply requires a fresh dedicated PostgreSQL session and catalog_import.")
    else:
        require_local(connection)
        if not is_loopback(target["host"]):
            raise CommandError("Apply requires a literal loopback HOST, such as 127.0.0.1.")
    source = Path(source).resolve(strict=True)
    if output_parent == source or source in output_parent.parents:
        raise CommandError("Reports must be outside the prepared source directory.")
    directory = output_parent / (datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ-") + uuid.uuid4().hex[:12])
    directory.mkdir(parents=True, exist_ok=False)
    counts = Counter()
    summary = {"schema_version": 1, "mode": "apply", "state": "verifying", "complete": False,
               "target": target, "run_directory": str(directory), "inserted": 0,
               "batch_size": batch_size, "limit": limit, "counts": {}}

    def publish():
        atomic_bytes(directory / "import_manifest.json", (strict_json(summary) + "\n").encode("utf-8"))

    publish()
    try:
        manifest, snapshot = snapshot_ready_input(source, directory)
        source_hash = file_digest(snapshot / "preparation_manifest.json")
        summary.update(source_directory=str(source), source_manifest_sha256=source_hash,
                       verified_records=manifest["prepared_manifest"]["counts"]["included"])
        old = None
        if resume:
            previous = Path(resume).resolve(strict=True)
            old = read_json(previous / "import_manifest.json")
            if (old.get("mode") != "apply" or old.get("target") != target
                    or old.get("source_manifest_sha256") != source_hash
                    or (old.get("author") or {}).get("username") != author_username):
                raise CommandError("Resume requires an apply report for the same verified source and target and explicit author.")
            summary["resumed_from"] = str(previous)

        table = connection.ops.quote_name(Movie._meta.db_table)
        if connection.in_atomic_block or not connection.get_autocommit():
            raise CommandError("Apply requires a fresh connection with autocommit enabled.")
        with transaction.atomic(using=target["alias"], durable=True):
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                check_session(cursor, target, read_only=True)
                author = resolve_author(target, author_username)
                summary["author"] = author
                if author["status"] != "resolved":
                    raise CommandError(f"Apply blocked: destination author status is {author['status']}.")
                if old is not None and old["author"] != author:
                    raise CommandError("Resume blocked: the destination author ID no longer matches the previous report.")
                check_schema(cursor, table)
        summary["state"] = "running"
        publish()
        with gzip.open(snapshot / "import_ready.jsonl.gz", "rt", encoding="utf-8") as stream, \
                (directory / "decisions.jsonl").open("x", encoding="utf-8", newline="\n") as report:
            records = (json.loads(line, parse_constant=reject_constant)["candidate"] for line in stream)
            while summary["inserted"] < limit:
                batch = list(islice(records, batch_size))
                if not batch:
                    break
                for record in batch:
                    record["_ready_contract"] = 1
                decisions = apply_batch(connection, target, table, batch, limit - summary["inserted"], author)
                counts.update(item["decision"] for item in decisions)
                summary.update(inserted=counts["inserted"], examined=counts.total(), counts=dict(counts),
                               last_reported_position=decisions[-1]["position"])
                for item in decisions:
                    report.write(strict_json(item) + "\n")
                report.flush()
                os.fsync(report.fileno())
                publish()
        complete = counts.total() == summary["verified_records"]
        summary.update(state="completed" if complete else "limited", complete=complete)
        publish()
        return summary
    except BaseException as exc:
        summary.update(state="interrupted" if isinstance(exc, KeyboardInterrupt) else "failed",
                       error_type=type(exc).__name__)
        if isinstance(exc, CommandError):
            summary["reason"] = str(exc)
        publish()
        raise
    finally:
        connection.close()
