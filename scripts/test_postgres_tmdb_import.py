"""Importer integration on a fresh disposable PostgreSQL; no .env or migrations."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from psycopg import sql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.test_postgres_movie_duplicate_cleanup import isolated_server, create_fixture_schema_without_migrations

RENDER_HOST = 'dpg-tmdb-fixture.oregon-postgres.render.com'


def make_certificate(openssl, root, host, prefix):
    cert, key = root / f'{prefix}.crt', root / f'{prefix}.key'
    subprocess.run([openssl, 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '2',
                    '-subj', f'/CN={host}', '-addext', f'subjectAltName=DNS:{host}',
                    '-out', str(cert), '-keyout', str(key)], check=True, capture_output=True)
    key.chmod(0o600)
    return cert, key


def exercise(root, credentials, historical=None, prepared=None, cert=None, other_ca=None):
    import django
    from django.conf import settings
    settings.configure(SECRET_KEY='disposable-import-only', BASE_DIR=Path(__file__).resolve().parent.parent,
        INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes', 'django.contrib.postgres', 'core'],
        DATABASES={'default': {'ENGINE': 'django.db.backends.postgresql', 'NAME': credentials['dbname'],
            **{k.upper(): v for k, v in credentials.items() if k not in ('dbname', 'connect_timeout')}}},
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField', USE_TZ=True)
    from django.db.backends.base.base import BaseDatabaseWrapper
    from django.db.backends.postgresql.base import DatabaseWrapper
    original = BaseDatabaseWrapper.ensure_connection
    original_params = DatabaseWrapper.get_connection_params

    def guard(conn):
        cfg = conn.settings_dict
        host = RENDER_HOST if conn.alias == RENDER_ALIAS and cert else '127.0.0.1'
        if (cfg['NAME'], cfg['HOST'], int(cfg['PORT'])) != (credentials['dbname'], host, credentials['port']):
            raise AssertionError('Only this disposable cluster is permitted')
        return original(conn)

    def loopback_params(conn):
        params = original_params(conn)
        if conn.alias == RENDER_ALIAS:
            params['hostaddr'] = '127.0.0.1'  # Keep the hostname for TLS while bypassing DNS.
        return params

    with patch.object(BaseDatabaseWrapper, 'ensure_connection', guard), \
            patch.object(DatabaseWrapper, 'get_connection_params', loopback_params):
        django.setup()
        from django.contrib.auth import get_user_model
        from django.core.management import call_command
        from django.core.management.base import CommandError
        from django.db import connection, connections, IntegrityError, transaction
        from core.models import Movie
        from core.test_tmdb_importer import PlanningTests, record
        from core.tmdb_import_apply import apply_import, check_schema
        from core.tmdb_import_ready import prepare_run, verify_ready_run
        from core.tmdb_importer import read_json, evaluate_record
        from core.tmdb_enrichment_store import file_digest
        from core.tmdb_import_render import RENDER_ALIAS, render_connection, render_readonly_snapshot

        create_fixture_schema_without_migrations()
        constraint = next(c for c in Movie._meta.constraints if c.name == 'movie_type_tmdb_id_uniq')
        with connection.schema_editor() as editor:
            editor.add_constraint(Movie, constraint)
        admin = get_user_model().objects.create_user(username='Admin')
        catalog_import = get_user_model().objects.create_user(username='catalog_import')
        target = {'alias': 'default', 'host': '127.0.0.1', 'port': str(credentials['port']), 'name': credentials['dbname']}

        class Tests(unittest.TestCase):
            source_run = PlanningTests.source_run

            def setUp(self):
                # Fixtures, including deletes, are guarded by the disposable endpoint check.
                Movie.objects.all().delete()
                self.folder = root / self._testMethodName
                self.folder.mkdir()
                self.source = self.folder / 'enrichment'
                self.source.mkdir()
                rows = [record(1, optional=False), record(2, kind='series'), record(3)]
                rows[2]['normalization']['data']['genre'] = 'Action, Animation, Comedy, Documentary, Drama, Horror, Musical, Sci-Fi'
                self.source_run(rows)
                self.prepared = Path(prepare_run(self.source, self.folder / 'prepared')['run_directory'])

            def apply(self, limit=10, resume=None, source=None, batch_size=2):
                connection.close()
                return apply_import(source or self.prepared, self.folder / 'reports', connection, target,
                                    batch_size=batch_size, limit=limit, author_username='Admin', resume=resume)

            def test_real_unique_constraint_and_model_0079(self):
                with connection.cursor() as cursor:
                    check_schema(cursor, '"core_movie"')
                    physical = connection.introspection.get_constraints(cursor, 'core_movie')
                self.assertTrue(physical[constraint.name]['unique'])
                self.assertEqual(physical[constraint.name]['columns'], ['type', 'tmdb_id'])
                Movie.objects.create(type='movie', tmdb_id=1, title_english='First')
                with self.assertRaises(IntegrityError), transaction.atomic():
                    Movie.objects.create(type='movie', tmdb_id=1, title_english='Duplicate')
                Movie.objects.create(type='series', tmdb_id=1, title_english='Separate type')

            def test_metadata_author_genres_defaults_unicode_and_trigger(self):
                result = self.apply()
                self.assertEqual(result['inserted'], 3)
                with gzip.open(self.prepared / 'import_ready.jsonl.gz', 'rt', encoding='utf-8') as stream:
                    rows = [json.loads(line) for line in stream]
                for row in rows:
                    movie = Movie.objects.get(type=row['type'], tmdb_id=row['tmdb_id'])
                    candidate = {**row['candidate'], '_ready_contract': 1}
                    fields = evaluate_record(candidate, None)['proposed_fields']
                    for name, value in fields.items():
                        actual = getattr(movie, name)
                        self.assertEqual(str(actual) if name == 'external_rating' and actual is not None else actual, value)
                    self.assertEqual(movie.author_id, admin.pk)
                    self.assertTrue(movie.search_vector)
                self.assertEqual(len(Movie.objects.get(tmdb_id=3).genre.split(', ')), 8)
                self.assertIsNone(Movie.objects.get(tmdb_id=1).title_spanish)
                self.assertIsNone(Movie.objects.get(tmdb_id=1).director)

            def test_limit_resume_and_idempotence_preserve_existing(self):
                first = self.apply(limit=1)
                original = Movie.objects.values().get()
                second = self.apply(resume=first['run_directory'])
                self.assertEqual(second['counts'], {'existing': 1, 'inserted': 2})
                self.assertEqual(Movie.objects.filter(pk=original['id']).values().get(), original)
                third = self.apply()
                self.assertEqual(third['counts'], {'existing': 3})
                self.assertEqual(Movie.objects.count(), 3)

            def test_real_trigger_failure_rolls_back_then_resumes_failed(self):
                with connection.cursor() as cursor:
                    cursor.execute("CREATE FUNCTION fixture_bad_vector() RETURNS trigger AS $$ BEGIN NEW.search_vector := NULL; RETURN NEW; END; $$ LANGUAGE plpgsql")
                    cursor.execute('CREATE TRIGGER z_fixture_bad_vector BEFORE INSERT ON core_movie FOR EACH ROW EXECUTE FUNCTION fixture_bad_vector()')
                try:
                    with self.assertRaisesRegex(CommandError, 'rolled back'):
                        self.apply()
                    self.assertEqual(Movie.objects.count(), 0)
                finally:
                    with connection.cursor() as cursor:
                        cursor.execute('DROP TRIGGER z_fixture_bad_vector ON core_movie')
                        cursor.execute('DROP FUNCTION fixture_bad_vector()')
                prior = next((self.folder / 'reports').glob('*/import_manifest.json'))
                self.assertEqual(read_json(prior)['state'], 'failed')
                self.assertEqual(self.apply(resume=prior.parent)['inserted'], 3)

            def test_unique_constraint_missing_blocks_apply(self):
                with connection.schema_editor() as editor:
                    editor.remove_constraint(Movie, constraint)
                try:
                    with self.assertRaisesRegex(CommandError, 'unique index'):
                        self.apply()
                    self.assertEqual(Movie.objects.count(), 0)
                finally:
                    with connection.schema_editor() as editor:
                        editor.add_constraint(Movie, constraint)

            def test_committed_batch_report_interruption_recovers_from_database(self):
                from core import tmdb_import_apply as module
                real = module.apply_batch

                def interrupted(*args, **kwargs):
                    real(*args, **kwargs)
                    raise KeyboardInterrupt()

                with patch.object(module, 'apply_batch', interrupted), self.assertRaises(KeyboardInterrupt):
                    self.apply()
                self.assertEqual(Movie.objects.count(), 2)
                before = dict(Movie.objects.values_list('tmdb_id', 'pk'))
                prior = next((self.folder / 'reports').glob('*/import_manifest.json'))
                self.assertEqual(read_json(prior)['state'], 'interrupted')
                result = self.apply(resume=prior.parent)
                self.assertEqual(result['counts'], {'existing': 2, 'inserted': 1})
                self.assertEqual(dict(Movie.objects.filter(tmdb_id__in=before).values_list('tmdb_id', 'pk')), before)

            def test_concurrent_imports_serialize_and_do_not_duplicate(self):
                connection.close()

                def worker(number):
                    conn = connections['default']
                    try:
                        return apply_import(self.prepared, self.folder / f'worker-{number}', conn, target,
                                            batch_size=2, limit=10, author_username='Admin')
                    finally:
                        conn.close()

                with ThreadPoolExecutor(max_workers=2) as pool:
                    results = list(pool.map(worker, range(2)))
                self.assertEqual(sum(r['inserted'] for r in results), 3)
                self.assertEqual(Movie.objects.count(), 3)

            def test_changed_manifest_rejected_without_insertions(self):
                path = self.prepared / 'import_ready.jsonl.gz'
                path.write_bytes(path.read_bytes() + b'tampered')
                with self.assertRaisesRegex(ValueError, 'integrity'):
                    self.apply()
                self.assertEqual(Movie.objects.count(), 0)

            def render_options(self):
                return dict(database=RENDER_ALIAS, confirm_host=RENDER_HOST,
                            confirm_port=credentials['port'], confirm_user=credentials['user'],
                            confirm_database=credentials['dbname'], tls_ca_file=str(cert),
                            run_dir=str(self.prepared), output_dir=str(self.folder / 'render-reports'))

            def render_environment(self, user=None):
                login = credentials if user is None else credentials | {'user': user}
                url = ('postgresql://{user}:{password}@' + RENDER_HOST + ':{port}/{dbname}').format(**login)
                return patch.dict(os.environ, RECCOOL_RENDER_DATABASE_URL=url)

            def render(self, **options):
                output = io.StringIO()
                known = set((self.folder / 'render-reports').glob('*/import_manifest.json'))
                with self.render_environment():
                    call_command('import_tmdb_productions', stdout=output, **(self.render_options() | options))
                report, = set((self.folder / 'render-reports').glob('*/import_manifest.json')) - known
                self.assertNotIn(credentials['password'], output.getvalue() + report.read_text())
                self.assertNotIn(RENDER_ALIAS, connections.databases)
                return read_json(report)

            @unittest.skipUnless(cert, 'Pass --openssl to exercise a real verified TLS connection')
            def test_render_dry_run_apply_resume_and_idempotence(self):
                original_default = connection.settings_dict.copy()
                existing = Movie.objects.create(type='movie', tmdb_id=3, author=admin)
                dry = self.render(dry_run=True)
                self.assertEqual((dry['counts']['existing'], dry['counts']['proposed_new'], dry['inserted']), (1, 2, 0))
                self.assertEqual(dry['author']['id'], catalog_import.pk)
                self.assertEqual(Movie.objects.count(), 1)
                first = self.render(apply=True, confirm_write=True, limit=1)
                self.assertEqual(first['inserted'], 1)
                second = self.render(apply=True, confirm_write=True, limit=10, resume=first['run_directory'])
                self.assertEqual(second['counts'], {'existing': 2, 'inserted': 1})
                self.assertEqual(self.render(apply=True, confirm_write=True, limit=10)['counts'], {'existing': 3})
                self.assertEqual(Movie.objects.get(pk=existing.pk).author_id, admin.pk)
                self.assertEqual(Movie.objects.exclude(pk=existing.pk).filter(author=catalog_import).count(), 2)
                self.assertEqual(connection.settings_dict, original_default)

            @unittest.skipUnless(cert, 'Pass --openssl to exercise a real verified TLS connection')
            def test_render_readonly_tls_and_write_authorization(self):
                with self.render_environment(), render_connection(self.render_options()) as (remote, destination):
                    with render_readonly_snapshot(remote, destination), remote.cursor() as cursor:
                        cursor.execute("SELECT current_setting('transaction_read_only')")
                        self.assertEqual(cursor.fetchone(), ('on',))
                        with self.assertRaisesRegex(CommandError, 'SELECT'):
                            cursor.execute('DELETE FROM core_movie')
                for options in ({'tls_ca_file': str(other_ca)}, {'confirm_host': 'dpg-wrong.render.com'},
                                {'confirm_database': 'wrong_database'}, {'confirm_port': 1},
                                {'confirm_user': 'wrong_role'}, {'apply': True, 'limit': 1},
                                {'apply': True, 'confirm_write': True, 'limit': 1, 'author_username': 'Admin'}):
                    with self.subTest(options=options), self.render_environment():
                        with self.assertRaises(CommandError) as failure:
                            call_command('import_tmdb_productions', **(self.render_options() | options))
                        self.assertNotIn(credentials['password'], str(failure.exception))
                        self.assertIsNone(failure.exception.__cause__)
                self.assertFalse(Movie.objects.exists())

            @unittest.skipUnless(cert, 'Pass --openssl to exercise a real verified TLS connection')
            def test_render_postgresql_rejects_apply_for_readonly_login(self):
                reader = 'tmdb_fixture_reader'
                with connection.cursor() as cursor:
                    cursor.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(
                        sql.Identifier(reader), sql.Literal(credentials['password'])))
                    cursor.execute('GRANT SELECT ON core_movie, auth_user TO tmdb_fixture_reader')
                options = self.render_options() | {'confirm_user': reader}
                with self.render_environment(reader):
                    call_command('import_tmdb_productions', stdout=io.StringIO(), dry_run=True, **options)
                    with self.assertRaisesRegex(CommandError, 'Apply stopped') as failure:
                        call_command('import_tmdb_productions', stdout=io.StringIO(), apply=True,
                                     confirm_write=True, limit=1, **options)
                    self.assertIsNone(failure.exception.__cause__)
                self.assertFalse(Movie.objects.exists())

            @unittest.skipUnless(cert, 'Pass --openssl to exercise a real verified TLS connection')
            def test_render_schema_and_rollback(self):
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT conname FROM pg_constraint WHERE conrelid = 'core_movie'::regclass
                         AND confrelid = 'auth_user'::regclass AND contype = 'f'
                    """)
                    fk, = cursor.fetchone()
                    cursor.execute(sql.SQL('ALTER TABLE core_movie DROP CONSTRAINT {}').format(sql.Identifier(fk)))
                try:
                    with self.assertRaisesRegex(CommandError, 'foreign key'):
                        self.render(dry_run=True)
                finally:
                    with connection.cursor() as cursor:
                        cursor.execute(sql.SQL(
                            'ALTER TABLE core_movie ADD CONSTRAINT {} FOREIGN KEY (author_id) REFERENCES auth_user(id)'
                            ' DEFERRABLE INITIALLY DEFERRED'
                        ).format(sql.Identifier(fk)))
                with connection.cursor() as cursor:
                    cursor.execute('ALTER TABLE core_movie DISABLE TRIGGER core_movie_search_vector_update_trigger')
                try:
                    with self.assertRaisesRegex(CommandError, 'trigger'):
                        self.render(dry_run=True)
                finally:
                    with connection.cursor() as cursor:
                        cursor.execute('ALTER TABLE core_movie ENABLE TRIGGER core_movie_search_vector_update_trigger')
                with connection.cursor() as cursor:
                    cursor.execute('ALTER TABLE core_movie ADD CONSTRAINT reject_fixture_id CHECK (tmdb_id <> 2) NOT VALID')
                try:
                    with self.assertRaisesRegex(CommandError, 'Apply stopped'):
                        self.render(apply=True, confirm_write=True, limit=3, batch_size=3)
                    self.assertFalse(Movie.objects.exists())
                finally:
                    with connection.cursor() as cursor:
                        cursor.execute('ALTER TABLE core_movie DROP CONSTRAINT reject_fixture_id')

            @unittest.skipUnless(historical and prepared, 'Pass historical and regenerated pilot directories')
            def test_historical_rejected_regenerated_real_pilot_imports(self):
                original = Path(historical)
                hashes = {p: file_digest(p) for p in original.rglob('*') if p.is_file()}
                with self.assertRaisesRegex(ValueError, 'rules'):
                    verify_ready_run(original)
                verified = verify_ready_run(prepared)
                self.assertEqual(verified['counts']['included'], 10)
                def rows(folder):
                    with gzip.open(Path(folder) / 'import_ready.jsonl.gz', 'rt', encoding='utf-8') as stream:
                        return [json.loads(line) for line in stream]
                self.assertEqual(rows(original), rows(prepared))
                result = self.apply(source=Path(prepared), batch_size=10)
                self.assertEqual(result['inserted'], 10)
                self.assertEqual(Movie.objects.count(), 10)
                self.assertEqual(set(Movie.objects.values_list('author_id', flat=True)), {admin.pk})
                self.assertEqual(self.apply(source=Path(prepared))['counts'], {'existing': 10})
                self.assertTrue(all(file_digest(p) == digest for p, digest in hashes.items()))

        result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Tests))
        connection.close()
        return result.wasSuccessful()


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument('--pg-bin', type=Path, default=Path(r'C:\Program Files\PostgreSQL\18\bin'))
    parser.add_argument('--historical')
    parser.add_argument('--prepared')
    parser.add_argument('--openssl', help='OpenSSL executable for disposable verify-full TLS tests')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='reccool-import-postgres-') as temp:
        root = Path(temp)
        cert = other_ca = None
        server_options = ()
        if args.openssl:
            cert, key = make_certificate(args.openssl, root, RENDER_HOST, 'server')
            other_ca, _ = make_certificate(args.openssl, root, RENDER_HOST, 'wrong-ca')
            server_options = ('-c', 'ssl=on', '-c', f'ssl_cert_file={cert}', '-c', f'ssl_key_file={key}')
        with isolated_server(args.pg_bin, root, server_options=server_options) as credentials:
            success = exercise(root, credentials, args.historical, args.prepared, cert, other_ca)
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
