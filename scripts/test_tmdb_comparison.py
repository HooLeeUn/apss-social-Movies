"""Run synthetic comparison tests without .env, migrations or database connections."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import django
from django.conf import settings


def main():
    root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root))
    settings.configure(
        SECRET_KEY="synthetic-comparison-tests-only",
        BASE_DIR=root,
        INSTALLED_APPS=["django.contrib.auth", "django.contrib.contenttypes", "core"],
        DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}},
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
    )
    with patch("django.db.backends.base.base.BaseDatabaseWrapper.ensure_connection",
               side_effect=AssertionError("Database connections forbidden in synthetic tests")):
        django.setup()
        suite = unittest.defaultTestLoader.loadTestsFromName("core.test_compare_tmdb_exports")
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
