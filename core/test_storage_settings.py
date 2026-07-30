import importlib.util
import os
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch


SETTINGS_PATH = Path(__file__).resolve().parents[1] / "config" / "settings.py"
R2_ENVIRONMENT = {
    "R2_ACCESS_KEY_ID": "test-access-key",
    "R2_SECRET_ACCESS_KEY": "test-secret-key",
    "R2_BUCKET_NAME": "test-bucket",
    "R2_ENDPOINT_URL": "https://example.r2.cloudflarestorage.com",
    "R2_PUBLIC_BASE_URL": "https://media.example.com/assets/",
}


def load_settings(environment):
    effective_environment = {name: "" for name in R2_ENVIRONMENT}
    effective_environment.update(environment)
    with patch.dict(os.environ, effective_environment):
        spec = importlib.util.spec_from_file_location("storage_test_settings", SETTINGS_PATH)
        settings = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(settings)
    return settings


class MediaStorageSettingsTests(TestCase):
    def test_local_storage_is_used_when_r2_configuration_is_incomplete(self):
        settings = load_settings({"R2_BUCKET_NAME": "test-bucket"})

        self.assertFalse(settings.R2_ENABLED)
        self.assertEqual(
            settings.STORAGES["default"]["BACKEND"],
            "django.core.files.storage.FileSystemStorage",
        )
        self.assertEqual(settings.MEDIA_URL, "/media/")
        self.assertEqual(settings.MEDIA_ROOT, settings.BASE_DIR / "media")

    def test_s3_storage_is_used_when_all_r2_configuration_is_present(self):
        settings = load_settings(R2_ENVIRONMENT)

        self.assertTrue(settings.R2_ENABLED)
        self.assertEqual(
            settings.STORAGES["default"]["BACKEND"],
            "storages.backends.s3.S3Storage",
        )
        self.assertEqual(settings.MEDIA_URL, "https://media.example.com/assets/")
        self.assertEqual(
            settings.STORAGES["default"]["OPTIONS"]["custom_domain"],
            "media.example.com/assets",
        )

    def test_staticfiles_storage_remains_whitenoise(self):
        settings = load_settings(R2_ENVIRONMENT)

        self.assertEqual(
            settings.STORAGES["staticfiles"]["BACKEND"],
            "whitenoise.storage.CompressedManifestStaticFilesStorage",
        )
