"""Render-only diagnostics and planning. No remote write executor exists here."""
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import uuid
from unittest.mock import patch
from urllib.parse import urlsplit

import psycopg
from psycopg.conninfo import conninfo_to_dict
from django.db.backends.base.base import BaseDatabaseWrapper

from django.conf import settings
from django.core.management.base import CommandError
from django.db import connections, transaction
from django.db.models import Count, Q

from core.models import Movie
from core.movie_duplicate_cleanup_plan import (
    POLICY, dependency_paths, django_dependencies, read_schema, write_plan,
)
from core.management.commands.diagnose_local_movie_duplicates import json_text
from core.tmdb_enrichment_store import atomic_bytes, file_digest

ALIAS = 'reccool_production_audit'
CONTRACT = 'reccool.remote-duplicate-audit.v1'


def verify_report(directory):
    directory = Path(directory).resolve(strict=True)
    path = directory / 'remote_manifest.json'
    if (directory / 'remote_manifest.sha256').read_text().strip() != file_digest(path):
        raise ValueError('Remote manifest digest mismatch')
    manifest = json.loads(path.read_text(encoding='utf-8'))
    if (manifest.get('contract') != CONTRACT or manifest.get('status') != 'complete'
            or manifest.get('policy') != POLICY or manifest.get('read_only') is not True
            or manifest.get('apply_enabled') is not False
            or set(manifest.get('outputs', {})) != {'remote_candidates.jsonl', 'remote_dependencies.json'}):
        raise ValueError('Unsupported or incomplete remote report')
    for name, digest in manifest['outputs'].items():
        if file_digest(directory / name) != digest:
            raise ValueError('Remote report output digest mismatch')
    return manifest


def connection_config(host, port, name, ca_file, confirm_host, confirm_database, confirm_user):
    if (host != confirm_host or name != confirm_database or not 1 <= port <= 65535
            or not re.fullmatch(r'dpg-[a-z0-9-]+(?:\.[a-z0-9-]+)*\.render\.com', host)
            or not re.fullmatch(r'[A-Za-z0-9_-]+', name)):
        raise CommandError('Explicit matching Render host, database and port are required.')
    if any(os.environ.get(key) for key in ('PGHOST', 'PGHOSTADDR', 'PGPORT', 'PGDATABASE',
            'PGUSER', 'PGPASSWORD', 'PGSERVICE', 'PGSERVICEFILE', 'PGOPTIONS', 'PGSSLMODE', 'PGSSLROOTCERT')):
        raise CommandError('Remove ambient libpq routing, credential and TLS overrides.')
    raw = os.environ.get('RECCOOL_RENDER_DATABASE_URL', '')
    try:
        if not raw or urlsplit(raw).scheme not in ('postgres', 'postgresql'):
            raise ValueError()
        parsed = conninfo_to_dict(raw)
        if set(parsed) - {'host', 'port', 'dbname', 'user', 'password', 'sslmode', 'sslrootcert'}:
            raise ValueError()
        if (parsed.get('host') != host or int(parsed.get('port', '5432')) != port
                or parsed.get('dbname') != name or parsed.get('user') != confirm_user
                or not parsed.get('password') or not confirm_user
                or parsed.get('sslmode', 'verify-full') != 'verify-full'):
            raise ValueError()
        ca = Path(ca_file).expanduser().resolve(strict=True)
        if not ca.is_file() or (parsed.get('sslrootcert') and Path(parsed['sslrootcert']).resolve() != ca):
            raise ValueError()
    except Exception:
        raise CommandError('Missing or invalid RECCOOL_RENDER_DATABASE_URL, confirmations or trusted TLS CA; verify-full is required.') from None
    user, password = parsed['user'], parsed['password']
    return {'ENGINE': 'django.db.backends.postgresql', 'HOST': host, 'PORT': str(port),
            'NAME': name, 'USER': user, 'PASSWORD': password, 'CONN_MAX_AGE': 0,
            'DISABLE_SERVER_SIDE_CURSORS': True,
            'OPTIONS': {'sslmode': 'verify-full', 'sslrootcert': str(ca), 'connect_timeout': 10,
                'application_name': 'reccool-readonly-duplicate-audit',
                'options': '-c default_transaction_read_only=on -c search_path=public,pg_catalog '
                           '-c statement_timeout=60000 -c lock_timeout=1000 '
                           '-c idle_in_transaction_session_timeout=60000'}}


def select_only(execute, sql, params, many, context):
    text = sql.strip()
    if many or not text.upper().startswith('SELECT ') or ';' in text:
        raise CommandError('Remote audit permits single SELECT statements only.')
    # READ ONLY also rejects writes performed inside SELECT-called functions.
    if re.search(r'\b(set_config|nextval|setval|dblink\w*|pg_advisory\w*|lo_\w*)\s*\(', text, re.I):
        raise CommandError('State-changing SELECT functions are forbidden.')
    return execute(sql, params, many, context)


def verify_session(cursor, target):
    cursor.execute("SELECT current_database(), inet_server_port(), current_setting('transaction_read_only'), "
                   "current_setting('default_transaction_read_only'), current_setting('search_path'), "
                   "EXISTS(SELECT 1 FROM pg_stat_ssl WHERE pid=pg_backend_pid() AND ssl)")
    if cursor.fetchone() != (target['name'], target['port'], 'on', 'on', 'public,pg_catalog', True):
        raise CommandError('Remote identity, TLS or read-only session could not be verified.')
    cursor.execute('SELECT current_user, session_user')
    if cursor.fetchone() != (target['user'], target['user']):
        raise CommandError('Connected PostgreSQL user differs from the operator confirmation.')
    cursor.execute("SELECT to_regclass('core_movie')=to_regclass('public.core_movie'), "
                   "EXISTS(SELECT 1 FROM pg_class WHERE oid='public.core_movie'::regclass AND relrowsecurity)")
    if cursor.fetchone() != (True, False):
        raise CommandError('Unexpected Movie table or row security; complete counts cannot be guaranteed.')


@contextmanager
def remote_snapshot(config, target):
    if ALIAS in connections.databases:
        raise CommandError('Reserved audit alias already exists; refusing to reuse a connection.')
    # A separate handler normalizes defaults without touching the default connection.
    from django.db.utils import ConnectionHandler
    normalized = ConnectionHandler({'default': config}).databases['default']
    connections.databases[ALIAS] = normalized
    connection = connections[ALIAS]
    original_ensure = BaseDatabaseWrapper.ensure_connection
    opened = False
    opening = False

    def dedicated_only(wrapper):
        nonlocal opening
        if wrapper is not connection or (opened and wrapper.connection is None):
            raise CommandError('Secondary connections and reconnects are forbidden during the remote snapshot.')
        opening = wrapper.connection is None
        try:
            return original_ensure(wrapper)
        finally:
            opening = False

    native_connect = psycopg.connect
    native_class_connect = psycopg.Connection.connect

    def protected_connect(*args, **kwargs):
        if not opening or opened:
            raise CommandError('Additional Psycopg connections are forbidden during the remote snapshot.')
        return native_connect(*args, **kwargs)

    def protected_class_connect(*args, **kwargs):
        if not opening or opened:
            raise CommandError('Additional Psycopg connections are forbidden during the remote snapshot.')
        return native_class_connect(*args, **kwargs)

    try:
        with patch.object(BaseDatabaseWrapper, 'ensure_connection', dedicated_only), \
                patch.object(psycopg, 'connect', protected_connect), \
                patch.object(psycopg.Connection, 'connect', protected_class_connect):
            with transaction.atomic(using=ALIAS):
                try:
                    with connection.cursor() as cursor:
                        cursor.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
                    opened = True
                    info = connection.connection.info
                    if (info.host, int(info.port), info.dbname, info.user) != (
                            target['host'], target['port'], target['name'], config['USER']):
                        raise CommandError('Actual Psycopg connection differs from the confirmed destination.')
                    session_target = {**target, 'user': config['USER']}
                    with connection.execute_wrapper(select_only):
                        with connection.cursor() as cursor:
                            verify_session(cursor, session_target)
                        yield connection
                        with connection.cursor() as cursor:
                            verify_session(cursor, session_target)
                finally:
                    transaction.set_rollback(True, using=ALIAS)
    finally:
        connection.close()
        del connections[ALIAS]
        connections.databases.pop(ALIAS, None)


def collect_snapshot(connection, output, target, *, plan):
    edges = django_dependencies()
    with connection.cursor() as cursor:
        schema = read_schema(cursor, edges)
        # RLS on a dependency could silently hide related rows from this role.
        cursor.execute('SELECT relname FROM pg_class WHERE relrowsecurity AND oid IN '
                       '(SELECT to_regclass(name) FROM unnest(%s::text[]) name)',
                       [['public.core_movie', *['public.' + edge['table'] for edge in edges]]])
        if cursor.fetchall():
            raise CommandError('Row security on audited tables prevents complete dependency counts.')
        cursor.execute("SELECT to_regclass('public.django_migrations')")
        if cursor.fetchone()[0] is None:
            migration = {'status': 'table_absent'}
        else:
            cursor.execute("SELECT has_table_privilege(current_user,'public.django_migrations','SELECT')")
            if cursor.fetchone()[0]:
                cursor.execute("SELECT EXISTS(SELECT 1 FROM public.django_migrations WHERE app='core' "
                               "AND name='0079_movie_type_tmdb_id_uniq')")
                migration = {'status': 'readable', 'applied': cursor.fetchone()[0]}
            else:
                migration = {'status': 'permission_unavailable'}
    types = list(Movie.objects.using(ALIAS).order_by().values('type').annotate(
        count=Count('pk'), null_tmdb=Count('pk', filter=Q(tmdb_id__isnull=True)),
        invalid_tmdb=Count('pk', filter=Q(tmdb_id__lte=0) | Q(tmdb_id__gt=2147483647))).order_by('type'))
    groups = Movie.objects.using(ALIAS).order_by().values('type', 'tmdb_id').annotate(n=Count('pk')).filter(n__gt=1)
    all_groups = list(groups)
    valid = [g for g in all_groups if g['type'] in ('movie', 'series')
             and g['tmdb_id'] is not None and 0 < g['tmdb_id'] <= 2147483647]
    # Reuse the unchanged planner also for diagnostics, to include dependency impact.
    result = write_plan(ALIAS, output, dependency_paths(edges), estimate_dependencies=schema['fk_graph_matches'])
    dependencies = output / 'remote_dependencies.json'
    atomic_bytes(dependencies, (json_text({'django': edges, 'postgresql': schema}) + '\n').encode())
    plan_path = output / 'cleanup_plan.jsonl'
    plan_path.rename(output / 'remote_candidates.jsonl')
    return {'contract': CONTRACT, 'status': 'complete', 'mode': 'plan' if plan else 'diagnose',
        'target': target, 'read_only': True, 'apply_enabled': False, 'policy': POLICY,
        'types': types, 'total_movies': sum(t['count'] for t in types),
        'null_tmdb': sum(t['null_tmdb'] for t in types), 'invalid_tmdb': sum(t['invalid_tmdb'] for t in types),
        'all_duplicate_groups': len(all_groups), 'extra_duplicate_rows': sum(g['n'] - 1 for g in all_groups),
        'unplannable_duplicate_groups': len(all_groups) - len(valid),
        'migration_0079': migration, 'dependency_estimate_complete': schema['fk_graph_matches'],
        'schema_blockers': schema['blockers'], **result,
        'outputs': {'remote_candidates.jsonl': file_digest(output / 'remote_candidates.jsonl'),
                    'remote_dependencies.json': file_digest(dependencies)}}


def run_audit(config, target, output_parent, *, plan=False):
    parent = Path(output_parent).expanduser().resolve()
    repo = Path(settings.BASE_DIR).resolve()
    if parent == repo or repo in parent.parents or any((p / '.git').exists() for p in (parent, *parent.parents)):
        raise CommandError('Reports must be outside repositories.')
    output = parent / ('remote-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-') + uuid.uuid4().hex[:12])
    output.mkdir(parents=True, exist_ok=False)
    manifest = output / 'remote_manifest.json'
    summary = {'contract': CONTRACT, 'status': 'running', 'target': target, 'apply_enabled': False}

    def publish():
        atomic_bytes(manifest, (json_text(summary) + '\n').encode())

    publish()
    try:
        with remote_snapshot(config, target) as connection:
            summary = collect_snapshot(connection, output, target, plan=plan)
        summary['rules_sha256'] = {name: file_digest(Path(__file__).with_name(name)) for name in
            ('remote_movie_duplicate_audit.py', 'movie_duplicate_cleanup_plan.py', 'movie_duplicate_diagnosis.py', 'models.py')}
        summary['run_directory'] = str(output)
        publish()
        atomic_bytes(output / 'remote_manifest.sha256', (file_digest(manifest) + '\n').encode())
        return summary
    except BaseException as exc:
        # Do not serialize driver exceptions: they can contain connection secrets.
        summary.update(status='interrupted' if isinstance(exc, KeyboardInterrupt) else 'failed',
                       error_type=type(exc).__name__)
        publish()
        raise CommandError('Remote read-only audit stopped; inspect the incomplete report. No apply is available.') from None
