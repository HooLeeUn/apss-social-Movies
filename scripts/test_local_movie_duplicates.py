"""Synthetic diagnostics tests; never load .env or connect to PostgreSQL."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import django
from django.conf import settings
from django.db.backends.base.base import BaseDatabaseWrapper


def main():
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    settings.configure(
        SECRET_KEY="synthetic-local-duplicate-tests",
        BASE_DIR=root,
        INSTALLED_APPS=["django.contrib.auth", "django.contrib.contenttypes", "core"],
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
        USE_TZ=True,
    )
    original = BaseDatabaseWrapper.ensure_connection

    def isolated_only(connection):
        if connection.vendor != "sqlite" or connection.settings_dict["NAME"] != ":memory:":
            raise AssertionError("Only synthetic in-memory SQLite connections are allowed")
        return original(connection)

    with patch.object(BaseDatabaseWrapper, "ensure_connection", isolated_only):
        django.setup()
        suite = unittest.defaultTestLoader.loadTestsFromName("core.test_local_movie_duplicates")
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
