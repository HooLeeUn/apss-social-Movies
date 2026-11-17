"""Run phase-5 tests with only a synthetic in-memory SQLite target, never .env/PostgreSQL."""
import sys
from pathlib import Path
import unittest
from unittest.mock import patch

import django
from django.conf import settings
from django.db.backends.base.base import BaseDatabaseWrapper


def main():
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    settings.configure(SECRET_KEY="synthetic-import-planner-tests", BASE_DIR=root,
        INSTALLED_APPS=["django.contrib.auth", "django.contrib.contenttypes", "core"],
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField", USE_TZ=True)
    original = BaseDatabaseWrapper.ensure_connection

    def isolated_only(connection):
        if connection.vendor != "sqlite" or connection.settings_dict["NAME"] != ":memory:":
            raise AssertionError("PostgreSQL and non-synthetic databases forbidden")
        return original(connection)

    with patch.object(BaseDatabaseWrapper, "ensure_connection", isolated_only), \
            patch("socket.socket.connect", side_effect=AssertionError("Network forbidden")):
        django.setup()
        suite = unittest.defaultTestLoader.loadTestsFromNames([
            "core.test_tmdb_importer", "core.test_tmdb_import_ready", "core.test_tmdb_enrichment", "core.test_tmdb_normalizer",
            "core.test_tmdb_eligibility", "core.test_local_movie_duplicates.LocalSafetyTests"])
        result = unittest.TextTestRunner(verbosity=1).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
