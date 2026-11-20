"""Synthetic suite: no .env, no network, SQLite in memory only."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import django
from django.conf import settings
from django.db.backends.base.base import BaseDatabaseWrapper


def main():
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    settings.configure(SECRET_KEY='synthetic-remote-audit', BASE_DIR=root,
        INSTALLED_APPS=['django.contrib.auth', 'django.contrib.contenttypes', 'core'],
        DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}},
        DEFAULT_AUTO_FIELD='django.db.models.BigAutoField', USE_TZ=True)
    original = BaseDatabaseWrapper.ensure_connection

    def guard(conn):
        if conn.vendor != 'sqlite' or conn.settings_dict['NAME'] != ':memory:':
            raise AssertionError('Only synthetic SQLite is allowed')
        return original(conn)

    with patch.object(BaseDatabaseWrapper, 'ensure_connection', guard), \
            patch('socket.socket.connect', side_effect=AssertionError('Network forbidden')):
        django.setup()
        suite = unittest.defaultTestLoader.loadTestsFromNames(['core.test_remote_movie_duplicate_audit',
            'core.test_local_movie_duplicates', 'core.test_movie_duplicate_cleanup_plan',
            'core.test_movie_duplicate_preferences', 'core.test_movie_duplicate_resume'])
        result = unittest.TextTestRunner(verbosity=1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
