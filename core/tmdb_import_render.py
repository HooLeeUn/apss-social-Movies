"""Dedicated Render connection for the TMDb import command."""
from contextlib import contextmanager
from hashlib import sha256
import os
import re

from django.contrib.auth import get_user_model
from django.core.management.base import CommandError
from django.db import connections, transaction
from django.db.utils import ConnectionHandler

from core.remote_movie_duplicate_audit import connection_config, select_only


RENDER_ALIAS = "reccool_render_import"
RENDER_AUTHOR = "catalog_import"


@contextmanager
def render_connection(options):
    host, port, name, user, ca = (options[key] for key in (
        "confirm_host", "confirm_port", "confirm_database", "confirm_user", "tls_ca_file",
    ))
    if not all((host, port, name, user, ca)) or not re.fullmatch(r"[A-Za-z0-9_-]+", user):
        raise CommandError("Render requires explicit host, port, database, PostgreSQL user and trusted TLS CA.")
    if any(value for key, value in os.environ.items() if key.upper().startswith("PG")):
        raise CommandError("Remove ambient PG environment variables before opening the Render import session.")
    config = connection_config(host, port, name, ca, host, name, user)
    config["OPTIONS"]["application_name"] = "reccool-tmdb-import"
    target = {"alias": RENDER_ALIAS, "host": host, "port": port, "name": name,
              "environment": "render-production", "user_sha256": sha256(user.encode()).hexdigest()}
    if RENDER_ALIAS in connections.databases:
        raise CommandError("The reserved Render import alias is already configured.")
    connections.databases[RENDER_ALIAS] = ConnectionHandler({"default": config}).databases["default"]
    try:
        connection = connections[RENDER_ALIAS]
        try:
            yield connection, target
        finally:
            try:
                connection.close()
            finally:
                del connections[RENDER_ALIAS]
    finally:
        connections.databases.pop(RENDER_ALIAS, None)


def check_render_session(cursor, target, read_only):
    connection = cursor.db.connection
    info = connection.info
    if ((info.host, int(info.port), info.dbname) != (target["host"], target["port"], target["name"])
            or sha256(info.user.encode()).hexdigest() != target["user_sha256"]
            or info.get_parameters().get("sslmode") != "verify-full"):
        raise CommandError("Render connection identity or TLS verification mode could not be confirmed.")
    cursor.execute("""
        SELECT current_database(), inet_server_port(), current_user, session_user,
               current_setting('transaction_read_only'), current_setting('default_transaction_read_only'),
               current_setting('search_path'), pg_is_in_recovery(),
               EXISTS (SELECT 1 FROM pg_stat_ssl WHERE pid = pg_backend_pid() AND ssl)
    """)
    if cursor.fetchone() != (target["name"], target["port"], info.user, info.user,
                             "on" if read_only else "off", "on", "public,pg_catalog", False, True):
        raise CommandError("Render server identity, TLS or transaction mode could not be confirmed.")
    cursor.execute("""
        SELECT count(*) = 2 AND NOT bool_or(relrowsecurity)
        FROM pg_class WHERE oid IN ('core_movie'::regclass, %s::regclass)
          AND relnamespace = 'public'::regnamespace
    """, [get_user_model()._meta.db_table])
    if cursor.fetchone() != (True,):
        raise CommandError("Render Movie and author tables must resolve to public without row-level security.")


@contextmanager
def render_readonly_snapshot(connection, target):
    with transaction.atomic(using=connection.alias, durable=True):
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with connection.execute_wrapper(select_only), connection.cursor() as cursor:
                check_render_session(cursor, target, read_only=True)
                yield
                check_render_session(cursor, target, read_only=True)
        finally:
            transaction.set_rollback(True, using=connection.alias)
