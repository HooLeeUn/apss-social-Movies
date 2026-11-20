"""Real read-only enforcement on a disposable loopback cluster; never Render/.env."""
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.test_postgres_movie_duplicate_cleanup import isolated_server, create_fixture_schema_without_migrations


def exercise(root, credentials):
    import django
    from django.conf import settings
    settings.configure(SECRET_KEY='disposable-remote-audit', BASE_DIR=Path(__file__).resolve().parent.parent,
        INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes', 'django.contrib.postgres', 'core'],
        DATABASES={'default': {'ENGINE': 'django.db.backends.postgresql', 'NAME': credentials['dbname'],
            **{k.upper(): v for k, v in credentials.items() if k not in ('dbname', 'connect_timeout')}}},
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField', USE_TZ=True)
    from django.db.backends.base.base import BaseDatabaseWrapper
    original = BaseDatabaseWrapper.ensure_connection

    def guard(conn):
        cfg = conn.settings_dict
        if (cfg['NAME'], cfg['HOST'], int(cfg['PORT'])) != (credentials['dbname'], '127.0.0.1', credentials['port']):
            raise AssertionError('Only the disposable cluster is allowed')
        return original(conn)

    with patch.object(BaseDatabaseWrapper, 'ensure_connection', guard):
        django.setup()
        from django.core.management.base import CommandError
        from django.db import connection, connections
        from core.models import Movie
        from core import remote_movie_duplicate_audit as audit
        import psycopg

        create_fixture_schema_without_migrations()
        first = Movie.objects.create(type='movie', tmdb_id=12, title_english='Complete', director='Director')
        Movie.objects.create(type='movie', tmdb_id=12, title_english='Partial')
        before = list(Movie.objects.order_by('id').values())
        with connection.cursor() as cursor:
            cursor.execute("CREATE ROLE audit_reader LOGIN PASSWORD 'disposable-reader-password'")
            cursor.execute('GRANT USAGE ON SCHEMA public TO audit_reader')
            cursor.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO audit_reader')
        config = {**settings.DATABASES['default'], 'USER': 'audit_reader',
                  'PASSWORD': 'disposable-reader-password', 'DISABLE_SERVER_SIDE_CURSORS': True,
                  'OPTIONS': {'options': '-c default_transaction_read_only=on -c search_path=public,pg_catalog'}}
        target = {'environment': 'disposable-fixture', 'name': credentials['dbname'],
                  'host': '127.0.0.1', 'port': credentials['port']}

        # The disposable server uses loopback without TLS. Only its SSL observation is
        # substituted; all role permissions, SQL guards and READ ONLY enforcement are real.
        verify = audit.verify_session

        def local_verify(cursor, destination):
            class Cursor:
                def execute(self, sql, params=None):
                    sql = sql.replace('EXISTS(SELECT 1 FROM pg_stat_ssl WHERE pid=pg_backend_pid() AND ssl)', 'TRUE')
                    return cursor.execute(sql, params)

                def fetchone(self):
                    return cursor.fetchone()
            return verify(Cursor(), destination)

        with patch.object(audit, 'verify_session', local_verify):
            first_report = audit.run_audit(config, target, root / 'reports', plan=True)
            assert (first_report['groups'], first_report['remove_movies'], first_report['total_movies']) == (1, 1, 2)
            audit.verify_report(first_report['run_directory'])
            second = audit.run_audit(config, target, root / 'reports')
            assert first_report['outputs'] == second['outputs']
            assert audit.ALIAS not in connections.databases
            with connection.cursor() as cursor:
                cursor.execute('GRANT INSERT, UPDATE, DELETE, TRUNCATE ON core_movie TO audit_reader')
                cursor.execute('GRANT CREATE ON SCHEMA public TO audit_reader')
                cursor.execute('ALTER ROLE audit_reader CREATEROLE')
            writer = audit.run_audit(config, target, root / 'writable-role')
            assert writer['outputs'] == first_report['outputs']
            rollbacks = []
            real_rollback = BaseDatabaseWrapper._rollback

            def tracked_rollback(wrapper):
                if wrapper.alias == audit.ALIAS:
                    rollbacks.append(wrapper.alias)
                return real_rollback(wrapper)

            with patch.object(BaseDatabaseWrapper, '_rollback', tracked_rollback):
                with audit.remote_snapshot(config, target) as remote:
                    for connect in (psycopg.connect, psycopg.Connection.connect):
                        try:
                            connect()
                        except CommandError:
                            pass
                        else:
                            raise AssertionError('An additional Psycopg connection was allowed')
                    try:
                        connection.cursor()
                    except CommandError:
                        pass
                    else:
                        raise AssertionError('Unprotected secondary/default connection was allowed')
                    for sql in ('DELETE FROM core_movie', 'INSERT INTO core_movie(id) VALUES(42)',
                                'UPDATE core_movie SET title_english=title_english', 'CREATE ROLE forbidden_role'):
                        try:
                            with remote.cursor() as cursor:
                                cursor.execute(sql)
                        except CommandError:
                            pass
                        else:
                            raise AssertionError('SQL guard allowed a write')
                assert len(rollbacks) == 1, 'Successful snapshots must end in ROLLBACK'
                for sql in ('UPDATE core_movie SET title_english=title_english',
                            'DELETE FROM core_movie', 'INSERT INTO core_movie(id) VALUES(42)',
                            'CREATE TABLE forbidden_table(id int)', 'CREATE ROLE forbidden_role'):
                    try:
                        with audit.remote_snapshot(config, target) as remote:
                            remote.connection.execute(sql)
                    except psycopg.errors.ReadOnlySqlTransaction:
                        pass
                    else:
                        raise AssertionError('PostgreSQL READ ONLY did not reject a write')
                assert len(rollbacks) == 6, 'Failures must also ROLLBACK'
                try:
                    with audit.remote_snapshot(config, target):
                        raise RuntimeError('synthetic diagnostic failure')
                except RuntimeError:
                    pass
                assert len(rollbacks) == 7
            # Failure to establish/verify READ ONLY must precede all diagnostics.
            from django.db.backends.utils import CursorWrapper
            execute = CursorWrapper.execute

            def reject_readonly(cursor, sql, params=None):
                if cursor.db.alias == audit.ALIAS and sql.startswith('SET TRANSACTION'):
                    raise CommandError('Synthetic READ ONLY activation failure')
                return execute(cursor, sql, params)

            with patch.object(CursorWrapper, 'execute', reject_readonly), \
                    patch.object(audit, 'collect_snapshot') as collect:
                try:
                    audit.run_audit(config, target, root / 'readonly-activation-failure')
                except CommandError:
                    pass
                else:
                    raise AssertionError('Failure to establish READ ONLY was accepted')
                collect.assert_not_called()
            unsafe_default = {**config, 'OPTIONS': {'options': '-c default_transaction_read_only=off -c search_path=public,pg_catalog'}}
            with patch.object(audit, 'collect_snapshot') as collect:
                try:
                    audit.run_audit(unsafe_default, target, root / 'readonly-verification-failure')
                except CommandError:
                    pass
                else:
                    raise AssertionError('Unverified session protection was accepted')
                collect.assert_not_called()
        assert list(Movie.objects.order_by('id').values()) == before
        assert Movie.objects.get(pk=first.pk).director == 'Director'
        assert audit.ALIAS not in connections.databases
        connection.close()
        print('PASS: real read-only role; planning; dependencies/schema; deterministic reports; manifest; '
              'SQL write guard; PostgreSQL READ ONLY; writer and CREATEROLE accepted under READ ONLY; rollback on success/error; secondary connections blocked; unchanged rows; alias cleanup. '
              'TLS handshake is not tested by this loopback fixture. No migrations or production connections.')


def main():
    with tempfile.TemporaryDirectory(prefix='reccool-remote-audit-postgres-') as temp:
        root = Path(temp)
        with isolated_server(Path(r'C:\Program Files\PostgreSQL\18\bin'), root) as credentials:
            exercise(root, credentials)


if __name__ == '__main__':
    main()
