import json
from pathlib import Path
from tempfile import TemporaryDirectory

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.models import Movie


class ExportStagingCatalogTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.author = get_user_model().objects.create_user("catalog-owner")

    def _movies(self, content_type, count, tmdb_start=1):
        return Movie.objects.bulk_create(
            [
                Movie(
                    author=self.author,
                    title_english=f"Title {content_type} {index}",
                    title_spanish=f"Título {index}" if index % 2 else None,
                    type=content_type,
                    genre=("Drama", "Comedy", "Documentary")[index % 3],
                    release_year=1940 + (index % 86),
                    synopsis="Synopsis" if index % 4 else "",
                    synopsis_es="Sinopsis" if index % 5 else None,
                    image="https://example.com/poster.jpg" if index % 3 else None,
                    trailer_en_key="trailer" if index % 7 else None,
                    external_rating=(index % 10),
                    tmdb_id=tmdb_start + index,
                )
                for index in range(count)
            ]
        )

    def test_exports_exact_catalog_as_valid_json_and_preserves_ids(self):
        movies = self._movies(Movie.MOVIE, 400)
        series = self._movies(Movie.SERIES, 100, tmdb_start=1000)
        invalid = Movie.objects.create(
            author=self.author, title_english="Invalid TMDb", type=Movie.MOVIE, tmdb_id=None
        )

        with TemporaryDirectory() as directory:
            output = Path(directory) / "nested" / "catalog.json"
            call_command("export_staging_catalog", output=str(output))
            payload = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(payload["total"], 500)
        self.assertEqual(payload["movies_count"], 400)
        self.assertEqual(payload["series_count"], 100)
        self.assertEqual(payload["model_label"], Movie._meta.label_lower)
        exported_ids = {item["pk"] for item in payload["objects"]}
        self.assertEqual(exported_ids, {item.pk for item in movies + series})
        self.assertNotIn(invalid.pk, exported_ids)
        self.assertTrue(all(item["fields"]["tmdb_id"] for item in payload["objects"]))
        concrete_names = {field.name for field in Movie._meta.concrete_fields if not field.primary_key}
        self.assertEqual(set(payload["objects"][0]["fields"]), concrete_names)

    def test_fails_clearly_when_a_type_has_too_few_valid_records(self):
        self._movies(Movie.MOVIE, 399)
        self._movies(Movie.SERIES, 100, tmdb_start=1000)

        with TemporaryDirectory() as directory:
            with self.assertRaisesMessage(
                CommandError,
                "se necesitan 400 y se encontraron 399",
            ):
                call_command("export_staging_catalog", output=str(Path(directory) / "catalog.json"))
