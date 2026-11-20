from contextlib import contextmanager
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock, patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection, connections

from core import remote_movie_duplicate_audit as audit
from core import test_local_movie_duplicates as fixtures
from core import test_movie_duplicate_cleanup_plan as plans

COLLECT_SNAPSHOT = audit.collect_snapshot


class SafetyTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.ca = self.root / 'ca.pem'
        self.ca.write_text('synthetic-ca')
        self.target = {'name': 'production_fixture', 'host': 'dpg-example.oregon-postgres.render.com', 'port': 5432}
        self.url = 'postgresql://reader:secret-sentinel@dpg-example.oregon-postgres.render.com:5432/production_fixture'
        self.env = {'RECCOOL_RENDER_DATABASE_URL': self.url}

    def config(self, **overrides):
        options = {'host': self.target['host'], 'port': 5432, 'name': self.target['name'],
                   'confirm_host': self.target['host'], 'confirm_database': self.target['name'], 'ca_file': self.ca, 'confirm_user': 'reader'}
        options.update(overrides)
        with patch.dict(os.environ, self.env, clear=True):
            return audit.connection_config(**options)

    def test_dedicated_credentials_tls_and_startup_readonly(self):
        original = dict(connections.databases['default'])
        config = self.config()
        self.assertEqual(config['OPTIONS']['sslmode'], 'verify-full')
        self.assertIn('default_transaction_read_only=on', config['OPTIONS']['options'])
        self.assertEqual(config['PASSWORD'], 'secret-sentinel')
        self.assertEqual(connections.databases['default'], original)

    def test_default_database_url_untouched_and_encoded_password_supported(self):
        env = {**self.env, 'DATABASE_URL': 'local-sentinel',
               'RECCOOL_RENDER_DATABASE_URL': self.url.replace('secret-sentinel', 'secret%2Dsentinel')}
        with patch.dict(os.environ, env, clear=True):
            config = audit.connection_config(self.target['host'], 5432, self.target['name'], self.ca,
                                            self.target['host'], self.target['name'], 'reader')
            self.assertEqual(os.environ['DATABASE_URL'], 'local-sentinel')
            self.assertEqual(config['PASSWORD'], 'secret-sentinel')

    def test_missing_ca_or_incompatible_url_ca_rejected(self):
        for ca, url in ((self.root / 'missing.pem', self.url),
                        (self.ca, self.url + '?sslrootcert=other-ca.pem')):
            with patch.dict(os.environ, {'RECCOOL_RENDER_DATABASE_URL': url}, clear=True):
                with self.assertRaises(CommandError):
                    audit.connection_config(self.target['host'], 5432, self.target['name'], ca,
                                            self.target['host'], self.target['name'], 'reader')

    def test_ambiguous_targets_rejected_before_connect(self):
        for changes in ({'host': '127.0.0.1'}, {'host': 'example.com'}, {'port': 5433},
                        {'confirm_host': 'other'}, {'confirm_database': 'other'}, {'name': 'postgres://secret'}):
            with self.subTest(changes=changes), self.assertRaises(CommandError):
                self.config(**changes)

    def test_ambient_routes_and_missing_credentials_rejected(self):
        for extra in ({'PGHOSTADDR': '127.0.0.1'}, {'PGOPTIONS': '-c default_transaction_read_only=off'},
                      {'RECCOOL_RENDER_DATABASE_URL': ''}):
            with patch.dict(os.environ, {**self.env, **extra}, clear=True), self.assertRaises(CommandError):
                audit.connection_config(self.target['host'], 5432, self.target['name'], self.ca,
                                        self.target['host'], self.target['name'], 'reader')

    def test_sql_guard_rejects_writes_and_migrations(self):
        execute = Mock()
        for sql in ('DELETE FROM core_movie', 'INSERT INTO core_movie VALUES(1)', 'UPDATE core_movie SET id=2',
                    'CREATE TABLE bad(id int)', 'ALTER TABLE core_movie ADD x int', 'DROP TABLE core_movie',
                    'TRUNCATE core_movie', 'CREATE ROLE forbidden', 'GRANT SELECT ON core_movie TO public',
                    'REVOKE SELECT ON core_movie FROM public', 'INSERT INTO django_migrations VALUES(1)',
                    'SET TRANSACTION READ WRITE', 'SELECT 1; DELETE FROM core_movie',
                    "SELECT set_config('transaction_read_only','off',true)", "SELECT nextval('seq')"):
            with self.subTest(sql=sql), self.assertRaises(CommandError):
                audit.select_only(execute, sql, None, False, {})
        execute.assert_not_called()
        audit.select_only(execute, 'SELECT count(*) FROM core_movie', None, False, {})
        execute.assert_called_once()

    def test_verified_readonly_role(self):
        cursor = Mock()
        cursor.fetchone.side_effect = [(self.target['name'], 5432, 'on', 'on', 'public,pg_catalog', True),
                                      ('reader', 'reader'), (True, False)]
        audit.verify_session(cursor, {**self.target, 'user': 'reader'})

    def test_unverified_tls_session_or_database_rejected(self):
        for row in [('other', 5432, 'on', 'on', 'public,pg_catalog', True),
                    (self.target['name'], 5432, 'off', 'on', 'public,pg_catalog', True),
                    (self.target['name'], 5432, 'on', 'on', 'public,pg_catalog', False)]:
            cursor = Mock()
            cursor.fetchone.return_value = row
            with self.assertRaises(CommandError):
                audit.verify_session(cursor, {**self.target, 'user': 'reader'})

    def test_existing_writer_and_createrole_no_longer_vetoed(self):
        cursor = Mock()
        cursor.fetchone.side_effect = [(self.target['name'], 5432, 'on', 'on', 'public,pg_catalog', True),
                                      ('reader', 'reader'), (True, False)]
        audit.verify_session(cursor, {**self.target, 'user': 'reader'})
        sql = ' '.join(call.args[0] for call in cursor.execute.call_args_list)
        self.assertNotIn('rolcreaterole', sql)
        self.assertNotIn('has_table_privilege', sql)

    def test_wrong_connected_user_rejected(self):
        cursor = Mock()
        cursor.fetchone.side_effect = [(self.target['name'], 5432, 'on', 'on', 'public,pg_catalog', True),
                                      ('unexpected', 'unexpected')]
        with self.assertRaises(CommandError):
            audit.verify_session(cursor, {**self.target, 'user': 'reader'})

    def test_invalid_urls_passwords_tls_and_user_are_redacted(self):
        for url in ('', 'bad secret-sentinel', self.url.replace(':secret-sentinel', ''),
                    self.url + '?sslmode=require', self.url + '?options=-c%20transaction_read_only=off',
                    self.url.replace('reader:', 'unexpected:'), self.url.replace('render.com', 'example.com')):
            with patch.dict(os.environ, {'RECCOOL_RENDER_DATABASE_URL': url}, clear=True):
                with self.assertRaises(CommandError) as raised:
                    audit.connection_config(self.target['host'], 5432, self.target['name'], self.ca,
                                            self.target['host'], self.target['name'], 'reader')
                self.assertNotIn('secret-sentinel', str(raised.exception))
                self.assertNotIn(url, str(raised.exception)) if url else None

    def test_alias_collision_rejected_without_replacing_default(self):
        with patch.dict(connections.databases, {audit.ALIAS: {}}), self.assertRaises(CommandError):
            with audit.remote_snapshot({}, self.target):
                self.fail('must not connect')

    def test_connection_errors_and_interruptions_redacted(self):
        for exc, state in [(RuntimeError('password=secret-sentinel'), 'failed'), (KeyboardInterrupt(), 'interrupted')]:
            with patch.object(audit, 'remote_snapshot', side_effect=exc), self.assertRaises(CommandError) as raised:
                audit.run_audit({}, self.target, self.root / state)
            text = next((self.root / state).glob('*/remote_manifest.json')).read_text()
            self.assertEqual(json.loads(text)['status'], state)
            self.assertNotIn('secret-sentinel', text + str(raised.exception))

    def test_no_apply_or_write_confirmation_arguments(self):
        for flag in ('--apply', '--confirm-write'):
            with self.assertRaises(CommandError):
                call_command('audit_render_movie_duplicates', '--host', self.target['host'], '--name', self.target['name'],
                    '--confirm-user', 'reader', '--confirm-host', self.target['host'], '--confirm-database', self.target['name'],
                    '--tls-ca-file', str(self.ca), '--output-dir', str(self.root), flag, stderr=io.StringIO())


class QueryTests(unittest.TestCase):
    add_rows = fixtures.DiagnosticQueryTests.add_rows
    cleanup_database = fixtures.DiagnosticQueryTests.cleanup_database
    cleanup_list = plans.PlanTests.cleanup_list
    setUp = plans.PlanTests.setUp

    def collect(self, plan=True):
        self.output.mkdir(parents=True, exist_ok=True)
        fake = MagicMock()
        cursor = fake.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = (None,)
        edges = [{'parent': 'core.Movie', 'dependent': 'core.MovieListItem', 'table': 'core_movielistitem',
                  'field': 'movie', 'on_delete': 'CASCADE'}]
        with connection.cursor() as real:
            real.execute('PRAGMA query_only=ON')
        try:
            with patch.object(audit, 'ALIAS', 'default'), patch.object(audit, 'django_dependencies', return_value=edges), \
                    patch.object(audit, 'read_schema', return_value={'fk_graph_matches': True, 'blockers': []}):
                return COLLECT_SNAPSHOT(fake, self.output, {'name': 'synthetic'}, plan=plan)
        finally:
            with connection.cursor() as real:
                real.execute('PRAGMA query_only=OFF')

    def test_remote_counts_dependencies_and_deterministic_survivor(self):
        self.add_rows(fixtures.movie(1, director='Complete'), fixtures.movie(2),
                      fixtures.movie(3, None), fixtures.movie(4, None), fixtures.movie(5, 0), fixtures.movie(6, 0))
        result = self.collect()
        self.assertEqual((result['total_movies'], result['all_duplicate_groups'], result['extra_duplicate_rows']), (6, 3, 3))
        self.assertEqual(result['unplannable_duplicate_groups'], 2)
        self.assertEqual(result['groups'], 1)
        row = json.loads((self.output / 'remote_candidates.jsonl').read_text())
        self.assertEqual(row['keep_id'], 1)
        self.assertEqual(row['remove_ids'], [2])
        self.assertEqual(row['dependencies'][0]['rows'], 2)
        self.assertEqual(result['migration_0079']['status'], 'table_absent')

    def test_repeat_consistency_and_complete_manifest_validation(self):
        self.add_rows(fixtures.movie(1), fixtures.movie(2))

        @contextmanager
        def snapshot(*args):
            yield Mock()

        real_collect = self.collect

        def synthetic_collect(conn, output, target, **kwargs):
            self.output = output
            return real_collect(kwargs['plan'])

        with patch.object(audit, 'remote_snapshot', snapshot), patch.object(audit, 'collect_snapshot', synthetic_collect):
            first = audit.run_audit({'PASSWORD': 'secret-sentinel'}, {'name': 'synthetic'}, Path(self.temp.name) / 'one')
            second = audit.run_audit({}, {'name': 'synthetic'}, Path(self.temp.name) / 'two', plan=True)
        self.assertEqual(first['outputs'], second['outputs'])
        folder = Path(first['run_directory'])
        self.assertEqual(audit.verify_report(folder)['status'], 'complete')
        self.assertNotIn('secret-sentinel', (folder / 'remote_manifest.json').read_text())
        (folder / 'remote_candidates.jsonl').write_text('tampered')
        with self.assertRaisesRegex(ValueError, 'digest'):
            audit.verify_report(folder)

