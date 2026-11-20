"""Importer integration on a fresh disposable PostgreSQL; no .env or migrations."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.test_postgres_movie_duplicate_cleanup import isolated_server, create_fixture_schema_without_migrations


def exercise(root, credentials, historical=None, prepared=None):
    import django
    from django.conf import settings
    settings.configure(SECRET_KEY='disposable-import-only', BASE_DIR=Path(__file__).resolve().parent.parent,
        INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes', 'django.contrib.postgres', 'core'],
        DATABASES={'default': {'ENGINE': 'django.db.backends.postgresql', 'NAME': credentials['dbname'],
            **{k.upper(): v for k, v in credentials.items() if k not in ('dbname', 'connect_timeout')}}},
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField', USE_TZ=True)
    from django.db.backends.base.base import BaseDatabaseWrapper
    original = BaseDatabaseWrapper.ensure_connection

    def guard(conn):
        cfg = conn.settings_dict
        if (cfg['NAME'], cfg['HOST'], int(cfg['PORT'])) != (credentials['dbname'], '127.0.0.1', credentials['port']):
            raise AssertionError('Only this disposable cluster is permitted')
        return original(conn)

    with patch.object(BaseDatabaseWrapper, 'ensure_connection', guard):
        django.setup()
        from django.contrib.auth import get_user_model
        from django.core.management.base import CommandError
        from django.db import connection, connections, IntegrityError, transaction
        from core.models import Movie
        from core.test_tmdb_importer import PlanningTests, record
        from core.tmdb_import_apply import apply_import, check_schema
        from core.tmdb_import_ready import prepare_run, verify_ready_run
        from core.tmdb_importer import read_json, evaluate_record
        from core.tmdb_enrichment_store import file_digest

        create_fixture_schema_without_migrations()
        constraint = next(c for c in Movie._meta.constraints if c.name == 'movie_type_tmdb_id_uniq')
        with connection.schema_editor() as editor:
            editor.add_constraint(Movie, constraint)
        admin = get_user_model().objects.create_user(username='Admin')
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
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='reccool-import-postgres-') as temp:
        root = Path(temp)
        with isolated_server(args.pg_bin, root) as credentials:
            success = exercise(root, credentials, args.historical, args.prepared)
    return 0 if success else 1


if __name__ == '__main__':
    raise SystemExit(main())
