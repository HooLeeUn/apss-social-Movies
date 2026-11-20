"""End-to-end production command on disposable PostgreSQL only, without migrations."""
from datetime import timedelta
from decimal import Decimal
import io
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.test_postgres_movie_duplicate_cleanup import isolated_server, create_fixture_schema_without_migrations


def exercise(root, credentials):
    import django
    from django.conf import settings
    settings.configure(SECRET_KEY='disposable-production-cleanup', BASE_DIR=Path(__file__).resolve().parent.parent,
        INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes', 'django.contrib.postgres', 'core'],
        DATABASES={'default': {'ENGINE': 'django.db.backends.postgresql', 'NAME': credentials['dbname'],
            **{k.upper(): v for k, v in credentials.items() if k not in ('dbname', 'connect_timeout')}}},
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField', USE_TZ=True)
    from django.db.backends.base.base import BaseDatabaseWrapper
    native = BaseDatabaseWrapper.ensure_connection

    def isolated_only(conn):
        cfg = conn.settings_dict
        if (cfg['HOST'], int(cfg['PORT']), cfg['NAME']) != ('127.0.0.1', credentials['port'], credentials['dbname']):
            raise AssertionError('Only the disposable endpoint is permitted')
        return native(conn)

    with patch.object(BaseDatabaseWrapper, 'ensure_connection', isolated_only):
        django.setup()
        from django.contrib.auth import get_user_model
        from django.core.management import call_command
        from django.core.management.base import CommandError
        from django.db import connection, transaction, IntegrityError
        from django.db.backends.utils import CursorWrapper
        from django.utils import timezone
        from core import movie_duplicate_cleanup as engine
        from core.models import (Movie, MovieRating, Profile, UserTasteProfile, UserDirectorPreference,
            UserDailyFeedPool, UserDailyFeedCandidate, WeeklyRecommendationSnapshot, WeeklyRecommendationItem)
        from core.remote_movie_duplicate_audit import run_audit, ALIAS
        from core.tmdb_importer import read_json
        import psycopg

        create_fixture_schema_without_migrations()
        user = get_user_model().objects.create_user(username='synthetic')
        keep = Movie.objects.create(type='movie', tmdb_id=80, title_english='Complete', director='Keep', genre='Comedy, Drama', synopsis='Full')
        discard = Movie.objects.create(type='movie', tmdb_id=80, title_english='Partial', director='Discard', genre='Drama')
        third = Movie.objects.create(type='movie', tmdb_id=80, title_english='Partial')
        unrelated = Movie.objects.create(type='movie', tmdb_id=81, title_english='Unrelated', director='Unrelated', genre='Drama')
        keep2 = Movie.objects.create(type='series', tmdb_id=90, title_english='Complete', director='Keep2', genre='Drama', synopsis='Full')
        discard2 = Movie.objects.create(type='series', tmdb_id=90, title_english='Partial', director='Discard2', genre='Drama')
        for movie, score in ((discard, 4), (discard2, 5), (unrelated, 8)):
            MovieRating.objects.create(user=user, movie=movie, score=score)
        history = UserDirectorPreference.objects.create(user=user, director='Historical', count_7=1, ratings_count=1, score=Decimal('7'))
        history_before = UserDirectorPreference.objects.filter(pk=history.pk).values().get()
        today = timezone.localdate()
        pool = UserDailyFeedPool.objects.create(user=user, pool_date=today, expires_at=timezone.now() + timedelta(days=1))
        for movie in (keep, discard, third, discard2, unrelated):
            UserDailyFeedCandidate.objects.create(pool=pool, movie=movie)
        weekly = WeeklyRecommendationSnapshot.objects.create(week_start=today, week_end=today + timedelta(days=6), items_count=2)
        WeeklyRecommendationItem.objects.create(snapshot=weekly, movie=keep, position=1, weekly_score=5)
        WeeklyRecommendationItem.objects.create(snapshot=weekly, movie=discard, position=2, weekly_score=4)
        original = {m.pk: Movie.objects.filter(pk=m.pk).values().get() for m in (keep, unrelated, keep2)}
        config = {**settings.DATABASES['default'], 'DISABLE_SERVER_SIDE_CURSORS': True,
                  'OPTIONS': {'options': '-c default_transaction_read_only=on -c search_path=public,pg_catalog'}}
        target = {'environment': 'render-production', 'alias': ALIAS, 'host': '127.0.0.1',
                  'port': credentials['port'], 'name': credentials['dbname']}
        execute = CursorWrapper.execute

        def fixture_tls(cursor, sql, params=None):
            # The sole TLS observation is substituted for this loopback-only fixture.
            sql = sql.replace('EXISTS(SELECT 1 FROM pg_stat_ssl WHERE pid=pg_backend_pid() AND ssl)', 'TRUE')
            return execute(cursor, sql, params)

        with patch.object(CursorWrapper, 'execute', fixture_tls):
            plan = run_audit(config, target, root / 'plans', plan=True)
            assert plan['groups'] == 2 and plan['remove_movies'] == 3
            command = 'core.management.commands.cleanup_render_movie_duplicates'
            base = {'plan_dir': plan['run_directory'], 'host': '127.0.0.1', 'port': credentials['port'],
                    'name': credentials['dbname'], 'confirm_host': '127.0.0.1', 'confirm_database': credentials['dbname'],
                    'confirm_user': credentials['user'], 'tls_ca_file': 'disposable-TLS-observation-only', 'stdout': io.StringIO()}
            with patch(command + '.connection_config', return_value=config):
                WeeklyRecommendationSnapshot.objects.filter(pk=weekly.pk).update(items_count=3)
                try:
                    call_command('cleanup_render_movie_duplicates', **base, output_dir=str(root / 'historical-weekly-counter'), dry_run=True)
                except CommandError:
                    pass
                else:
                    raise AssertionError('A historical weekly counter was silently repaired')
                assert WeeklyRecommendationSnapshot.objects.get(pk=weekly.pk).items_count == 3
                WeeklyRecommendationSnapshot.objects.filter(pk=weekly.pk).update(items_count=2)
                review = root / 'review'
                call_command('cleanup_render_movie_duplicates', **base, output_dir=str(review), dry_run=True)
                summary = read_json(review / 'execution_summary.json')
                assert summary['state'] == 'validated' and summary['dependencies']['core.MovieRating'] == 2
                assert summary['dependencies']['core.WeeklyRecommendationItem'] == 1
                assert summary['preference_validation']['preexisting_unrelated_director_discrepancies'] == 1
                assert Movie.objects.count() == 6 and MovieRating.objects.count() == 3
                options = {**base, 'apply': True, 'confirm_maintenance': True,
                    'confirm_plan_sha256': plan['outputs']['remote_candidates.jsonl'],
                    'confirm_review_sha256': summary['review_sha256'], 'review_dir': str(review),
                    'confirm_backup_id': 'restorable-disposable-fixture', 'limit_groups': 1}

                # A full-row change absent from original selection fields still fails the review.
                Movie.objects.filter(pk=keep.pk).update(author_id=user.pk)
                try:
                    call_command('cleanup_render_movie_duplicates', **options, output_dir=str(root / 'external'))
                except CommandError:
                    pass
                else:
                    raise AssertionError('Unreviewed survivor change was accepted')
                Movie.objects.filter(pk=keep.pk).update(author_id=None)
                with connection.cursor() as cursor:
                    cursor.execute('CREATE TABLE fixture_external_fk(id integer PRIMARY KEY, movie_id bigint REFERENCES core_movie(id))')
                    cursor.execute('INSERT INTO fixture_external_fk VALUES(1,%s)', [discard.pk])
                try:
                    call_command('cleanup_render_movie_duplicates', **options, output_dir=str(root / 'foreign-key-change'))
                except CommandError:
                    pass
                else:
                    raise AssertionError('An unreviewed incoming foreign key was accepted')
                with connection.cursor() as cursor:
                    cursor.execute('DROP TABLE fixture_external_fk')
                with psycopg.connect(**credentials, autocommit=True) as other:
                    other.execute('SELECT pg_advisory_lock(%s)', [engine.LOCK_ID])
                    try:
                        call_command('cleanup_render_movie_duplicates', **options, output_dir=str(root / 'concurrent'))
                    except CommandError:
                        pass
                    else:
                        raise AssertionError('Concurrent cleanup accepted')

                native_assert = engine.assert_preferences

                def fail_after_delete(users, **kwargs):
                    native_assert(users, **kwargs)
                    if not Movie.objects.filter(pk=discard.pk).exists():
                        raise CommandError('Synthetic failure after cascades and weekly counter update')

                apply_dir = root / 'apply'
                with patch.object(engine, 'assert_preferences', fail_after_delete):
                    try:
                        call_command('cleanup_render_movie_duplicates', **options, output_dir=str(apply_dir))
                    except CommandError:
                        pass
                    else:
                        raise AssertionError('Rollback injection did not fail')
                assert Movie.objects.count() == 6 and MovieRating.objects.count() == 3
                assert WeeklyRecommendationSnapshot.objects.get(pk=weekly.pk).items_count == 2
                assert UserDailyFeedCandidate.objects.count() == 5
                call_command('cleanup_render_movie_duplicates', **options, output_dir=str(apply_dir), resume=True)
                limited = read_json(apply_dir / 'execution_summary.json')
                assert limited['state'] == 'limited' and limited['committed_groups'] == 1
                assert Movie.objects.count() == 4
                assert WeeklyRecommendationSnapshot.objects.get(pk=weekly.pk).items_count == 1
                checkpoint = apply_dir / 'group-000000.json'
                checkpoint_bytes = checkpoint.read_bytes()
                forged = read_json(checkpoint)
                forged['survivor_sha256'] = '0' * 64
                engine.publish(checkpoint, forged)
                try:
                    call_command('cleanup_render_movie_duplicates', **options, output_dir=str(apply_dir), resume=True)
                except CommandError:
                    pass
                else:
                    raise AssertionError('Checkpoint inconsistent with the reviewed survivor was accepted')
                # Fixture-only restoration; real recovery must never edit receipts manually.
                checkpoint.write_bytes(checkpoint_bytes)
                # Failed publication after the second COMMIT must recover using its pending receipt.
                native_publish = engine.publish

                def fail_receipt(path, payload):
                    if path.name == 'group-000001.json' and payload.get('state') == 'committed':
                        raise CommandError('Synthetic interruption after durable COMMIT')
                    return native_publish(path, payload)

                with patch.object(engine, 'publish', fail_receipt):
                    try:
                        call_command('cleanup_render_movie_duplicates', **options, output_dir=str(apply_dir), resume=True)
                    except CommandError:
                        pass
                    else:
                        raise AssertionError('Commit gap not exercised')
                assert read_json(apply_dir / 'group-000001.json')['state'] == 'pending_commit'
                assert Movie.objects.count() == 3
                call_command('cleanup_render_movie_duplicates', **options, output_dir=str(apply_dir), resume=True)
                done = read_json(apply_dir / 'execution_summary.json')
                assert done['state'] == 'complete' and done['committed_groups'] == 2 and done['removed_movies'] == 3
                call_command('cleanup_render_movie_duplicates', **options, output_dir=str(apply_dir), resume=True)
                assert Movie.objects.count() == 3 and MovieRating.objects.count() == 1
                assert UserDailyFeedCandidate.objects.count() == 2 and WeeklyRecommendationItem.objects.count() == 1
                assert get_user_model().objects.count() == 1 and Profile.objects.filter(user=user).exists()
                assert UserTasteProfile.objects.get(user=user).ratings_count == 1
                assert UserDirectorPreference.objects.filter(pk=history.pk).values().get() == history_before
                for pk, expected in original.items():
                    assert Movie.objects.filter(pk=pk).values().get() == expected
                # Compatibility with 0079: add its exact constraint directly, without migrations.
                constraint = next(c for c in Movie._meta.constraints if c.name == 'movie_type_tmdb_id_uniq')
                with connection.schema_editor() as schema:
                    schema.add_constraint(Movie, constraint)
                try:
                    with transaction.atomic():
                        Movie.objects.create(type='movie', tmdb_id=80, title_english='Forbidden duplicate')
                except IntegrityError:
                    pass
                else:
                    raise AssertionError('Identity constraint did not prevent a duplicate')
                assert done['remaining_duplicate_groups'] == 0
        connection.close()
        print('PASS: shared remote engine, reviewed plans, read-only dry-run, two rating cascades, feed, weekly counter, '
              'historical preference preservation, full rollback, limited/failed resume, commit-gap recovery, '
              'idempotence, concurrency, external changes, untouched survivors/users and exact 0079 constraint. '
              'Disposable PostgreSQL only; no migrations. Real TLS handshake not exercised.')


def main():
    with tempfile.TemporaryDirectory(prefix='reccool-production-cleanup-test-') as temp:
        root = Path(temp)
        with isolated_server(Path(r'C:\Program Files\PostgreSQL\18\bin'), root) as credentials:
            exercise(root, credentials)


if __name__ == '__main__':
    main()
