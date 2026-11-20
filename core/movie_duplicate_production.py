"""Remote-plan validation and connection policy for the shared cleanup engine."""
from collections import Counter
from contextlib import contextmanager
import json
from pathlib import Path
from unittest.mock import patch

from django.core.management.base import CommandError
from django.db import connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.utils import ConnectionHandler

from core import movie_duplicate_cleanup as engine
from core.movie_duplicate_cleanup_plan import POLICY
from core.movie_duplicate_execution_scope import execution_scope
from core.remote_movie_duplicate_audit import ALIAS, verify_report, select_only
from core.tmdb_enrichment_store import file_digest


def strict_json(text):
    def pairs(values):
        result = dict(values)
        if len(result) != len(values):
            raise ValueError('Duplicate JSON keys')
        return result

    def invalid(value):
        raise ValueError('Non-finite JSON constant')

    return json.loads(text, object_pairs_hook=pairs, parse_constant=invalid)


def load_remote_plan(directory, target, approved_digest=None):
    directory = Path(directory).resolve(strict=True)
    manifest = verify_report(directory)
    manifest = strict_json((directory / 'remote_manifest.json').read_text(encoding='utf-8'))
    counters = ('groups', 'all_duplicate_groups', 'remove_movies', 'extra_duplicate_rows',
                'groups_with_dependencies', 'protected_groups', 'unplannable_duplicate_groups', 'null_tmdb', 'invalid_tmdb')
    if any(type(manifest.get(name)) is not int or manifest[name] < 0 for name in counters):
        raise CommandError('Invalid remote counter types.')
    if (manifest['target'] != target or manifest.get('mode') != 'plan'
            or manifest.get('policy') != POLICY or manifest.get('schema_blockers')
            or manifest.get('protected_groups') != 0 or manifest.get('unplannable_duplicate_groups') != 0
            or not manifest.get('dependency_estimate_complete')
            or manifest.get('null_tmdb') != 0 or manifest.get('invalid_tmdb') != 0):
        raise CommandError('Remote plan is blocked, incomplete or belongs to another destination.')
    expected_rules = {name: file_digest(Path(__file__).with_name(name)) for name in
        ('remote_movie_duplicate_audit.py', 'movie_duplicate_cleanup_plan.py', 'movie_duplicate_diagnosis.py', 'models.py')}
    if manifest.get('rules_sha256') != expected_rules:
        raise CommandError('Remote plan rules differ; obtain a new read-only plan without overwriting this one.')
    digest = file_digest(directory / 'remote_candidates.jsonl')
    if approved_digest is not None and approved_digest != digest:
        raise CommandError('Explicit plan digest differs from the remote plan.')
    groups = [strict_json(line) for line in (directory / 'remote_candidates.jsonl').read_text(encoding='utf-8').splitlines() if line.strip()]
    ids, keys, impact = set(), set(), Counter()
    for group in groups:
        key = (group['type'], group['tmdb_id'])
        members = [group['keep_id'], *group['remove_ids']]
        if (group['status'] != 'planned' or group['type'] not in ('movie', 'series')
                or type(group['tmdb_id']) is not int or not 0 < group['tmdb_id'] <= 2147483647
                or not group['remove_ids'] or any(type(pk) is not int or pk <= 0 for pk in members)
                or len(set(members)) != len(members) or ids.intersection(members) or key in keys
                or set(group['scores']) != {str(pk) for pk in members}
                or len(group['metadata_sha256']) != 64):
            raise CommandError('Invalid or repeated group members/identity in the remote plan.')
        ids.update(members)
        keys.add(key)
        for dependency in group['dependencies']:
            if (dependency['movie_id'] not in group['remove_ids'] or dependency['on_delete'] != 'CASCADE'
                    or dependency['model'] not in {'core.MovieRating', 'core.UserDailyFeedCandidate', 'core.WeeklyRecommendationItem'}
                    or type(dependency['rows']) is not int or dependency['rows'] <= 0):
                raise CommandError('Unsupported or invalid planned dependency.')
            impact[dependency['model'], dependency['path'], dependency['on_delete']] += dependency['rows']
    expected_impact = [{'model': model, 'path': path, 'on_delete': behavior, 'rows': count}
                      for (model, path, behavior), count in sorted(impact.items())]
    if (len(groups) != manifest['groups'] or len(groups) != manifest['all_duplicate_groups']
            or sum(len(g['remove_ids']) for g in groups) != manifest['remove_movies']
            or manifest['extra_duplicate_rows'] != manifest['remove_movies']
            or sum(bool(g['dependencies']) for g in groups) != manifest['groups_with_dependencies']
            or expected_impact != manifest['impact_by_fk_path']):
        raise CommandError('Remote plan counters/dependencies do not reconcile.')
    schema = strict_json((directory / 'remote_dependencies.json').read_text(encoding='utf-8'))
    if schema['postgresql']['blockers'] or not schema['postgresql']['fk_graph_matches']:
        raise CommandError('Remote dependency audit is blocked.')
    return manifest, groups, schema, file_digest(directory / 'remote_manifest.json')


@contextmanager
def production_session(config, target, apply):
    if ALIAS in connections.databases:
        raise CommandError('Remote alias already exists.')
    # Only this separately authorized session can open in write mode.
    config = {**config, 'OPTIONS': dict(config['OPTIONS'])}
    if apply:
        config['OPTIONS']['options'] = config['OPTIONS']['options'].replace('default_transaction_read_only=on', 'default_transaction_read_only=off')
    connections.databases[ALIAS] = ConnectionHandler({'default': config}).databases['default']
    connection = connections[ALIAS]
    ensure = BaseDatabaseWrapper.ensure_connection
    opened = False

    def guard(wrapper):
        if wrapper is not connection or (opened and wrapper.connection is None):
            raise CommandError('Secondary/default connections and reconnects are forbidden during maintenance.')
        return ensure(wrapper)

    def validate(cursor, destination, read_only):
        info = connection.connection.info
        if (info.host, int(info.port), info.dbname, info.user) != (target['host'], target['port'], target['name'], config['USER']):
            raise CommandError('Actual maintenance connection differs from the confirmed destination.')
        cursor.execute("SELECT current_database(),inet_server_port(),current_user,session_user, "
                       "current_setting('transaction_read_only'),current_setting('search_path'), "
                       "EXISTS(SELECT 1 FROM pg_stat_ssl WHERE pid=pg_backend_pid() AND ssl)")
        if cursor.fetchone() != (target['name'], target['port'], config['USER'], config['USER'],
                                 'on' if read_only else 'off', 'public,pg_catalog', True):
            raise CommandError('Maintenance destination, TLS, user or transaction mode mismatch.')
        cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_class WHERE relnamespace='public'::regnamespace "
                       "AND relname LIKE 'core_%' AND relrowsecurity), "
                       "EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='public.core_weeklyrecommendationsnapshot'::regclass "
                       "AND NOT tgisinternal AND tgenabled <> 'D'), "
                       "EXISTS(SELECT 1 FROM pg_rewrite WHERE ev_class='public.core_weeklyrecommendationsnapshot'::regclass)")
        if any(cursor.fetchone()):
            raise CommandError('RLS or weekly-parent triggers/rules require a separate review.')

    def readonly_guard(execute, sql, params, many, context):
        upper = sql.strip().upper()
        if upper in ('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY', "SET LOCAL LOCK_TIMEOUT = '5S'"):
            return execute(sql, params, many, context)
        return select_only(execute, sql, params, many, context)

    try:
        with patch.object(BaseDatabaseWrapper, 'ensure_connection', guard), execution_scope(ALIAS, validate):
            connection.ensure_connection()
            opened = True
            with connection.cursor() as cursor:
                validate(cursor, target, read_only=not apply)
                if apply:
                    cursor.execute('SELECT pg_try_advisory_lock(%s)', [engine.LOCK_ID])
                    if not cursor.fetchone()[0]:
                        raise CommandError('Another cleanup holds the maintenance lock.')
            if apply:
                yield connection
            else:
                with connection.execute_wrapper(readonly_guard):
                    yield connection
    finally:
        connection.close()
        del connections[ALIAS]
        connections.databases.pop(ALIAS, None)
