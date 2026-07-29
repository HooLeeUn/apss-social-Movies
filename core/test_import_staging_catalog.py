import copy
import json
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core import serializers
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test import TestCase

from core.models import Movie


class ImportStagingCatalogTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.author = get_user_model().objects.create_user("Julian")
        rows = []
        for index in range(500):
            rows.append(
                Movie(
                    id=1001 + index,
                    author=cls.author,
                    title_english=f"Catalog title {index}",
                    title_spanish=f"Título {index}",
                    type=Movie.MOVIE if index < 400 else Movie.SERIES,
                    genre="Drama",
                    release_year=2000 + index % 25,
                    synopsis="Synopsis",
                    synopsis_es="Sinopsis",
                    image="https://example.com/poster.jpg",
                    trailer_en_key="trailer" if index % 2 else None,
                    tmdb_id=5001 + index,
                )
            )
        Movie.objects.bulk_create(rows)
        objects = json.loads(serializers.serialize("json", Movie.objects.order_by("pk")))
        cls.payload = {
            "schema_version": 1,
            "exported_at": "2026-07-29T00:00:00Z",
            "total": 500,
            "movies_count": 400,
            "series_count": 100,
            "model_label": Movie._meta.label_lower,
            "objects": objects,
        }
        for item in cls.payload["objects"]:
            item["fields"]["author"] = 6

    def _write(self, directory, payload=None):
        path = Path(directory) / "catalog.json"
        path.write_text(json.dumps(payload or self.payload), encoding="utf-8")
        return path

    def test_dry_run_validates_but_does_not_modify_database(self):
        Movie.objects.all().delete()
        with TemporaryDirectory() as directory:
            output = StringIO()
            call_command(
                "import_staging_catalog",
                self._write(directory),
                author_username="Julian",
                dry_run=True,
                stdout=output,
            )
        self.assertEqual(Movie.objects.count(), 0)
        self.assertIn("Creados: 500", output.getvalue())
        self.assertIn("Autor asignado: Julian", output.getvalue())
        self.assertNotIn("Secuencia de Movie ajustada", output.getvalue())

    def test_requires_author_username(self):
        with TemporaryDirectory() as directory:
            with self.assertRaisesMessage(CommandError, "--author-username"):
                call_command("import_staging_catalog", self._write(directory))

    def test_rejects_missing_author_without_creating_it_or_modifying_movies(self):
        fallback_author = get_user_model().objects.create_user("fallback-owner")
        Movie.objects.update(author=fallback_author)
        get_user_model().objects.filter(username="Julian").delete()
        original_movies = list(
            Movie.objects.order_by("pk").values_list("pk", "author_id", "title_english")
        )
        with TemporaryDirectory() as directory:
            with self.assertRaisesMessage(CommandError, 'usuario "Julian" no existe'):
                call_command(
                    "import_staging_catalog",
                    self._write(directory),
                    author_username="Julian",
                )
        self.assertFalse(get_user_model().objects.filter(username="Julian").exists())
        self.assertEqual(
            list(
                Movie.objects.order_by("pk").values_list(
                    "pk", "author_id", "title_english"
                )
            ),
            original_movies,
        )

    def test_import_creates_records_and_preserves_ids(self):
        Movie.objects.all().delete()
        with TemporaryDirectory() as directory:
            call_command(
                "import_staging_catalog",
                self._write(directory),
                author_username="Julian",
                stdout=StringIO(),
            )
        self.assertEqual(Movie.objects.count(), 500)
        self.assertEqual(set(Movie.objects.values_list("pk", flat=True)), set(range(1001, 1501)))
        self.assertEqual(
            set(Movie.objects.values_list("author__username", flat=True)), {"Julian"}
        )

    def test_repeat_updates_by_id_without_duplicates(self):
        Movie.objects.all().delete()
        payload = copy.deepcopy(self.payload)
        with TemporaryDirectory() as directory:
            path = self._write(directory, payload)
            call_command(
                "import_staging_catalog", path, author_username="Julian", stdout=StringIO()
            )
            other_author = get_user_model().objects.create_user("other-owner")
            Movie.objects.filter(pk=1001).update(author=other_author)
            payload["objects"][0]["fields"]["title_english"] = "Updated by repeat"
            path = self._write(directory, payload)
            output = StringIO()
            call_command(
                "import_staging_catalog", path, author_username="Julian", stdout=output
            )
        self.assertEqual(Movie.objects.count(), 500)
        self.assertEqual(Movie.objects.get(pk=1001).title_english, "Updated by repeat")
        self.assertEqual(Movie.objects.get(pk=1001).author.username, "Julian")
        self.assertIn("Creados: 0", output.getvalue())
        self.assertIn("Actualizados: 500", output.getvalue())

    def test_rejects_invalid_json(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "bad.json"
            path.write_text("{not json", encoding="utf-8")
            with self.assertRaisesMessage(CommandError, "JSON inválido"):
                call_command("import_staging_catalog", path, author_username="Julian")

    def test_rejects_incorrect_counts_before_writing(self):
        payload = copy.deepcopy(self.payload)
        payload["objects"].pop()
        payload["total"] = 499
        with TemporaryDirectory() as directory:
            with self.assertRaisesMessage(CommandError, "exactamente 500"):
                call_command(
                    "import_staging_catalog",
                    self._write(directory, payload),
                    author_username="Julian",
                )
        self.assertEqual(Movie.objects.count(), 500)

    def test_rejects_record_without_tmdb_id(self):
        payload = copy.deepcopy(self.payload)
        payload["objects"][0]["fields"]["tmdb_id"] = None
        with TemporaryDirectory() as directory:
            with self.assertRaisesMessage(CommandError, "no tiene un tmdb_id válido"):
                call_command(
                    "import_staging_catalog",
                    self._write(directory, payload),
                    author_username="Julian",
                )

    def test_sequence_reset_uses_database_backend_operation(self):
        Movie.objects.all().delete()
        with TemporaryDirectory() as directory:
            with patch.object(
                connection.ops,
                "sequence_reset_sql",
                wraps=connection.ops.sequence_reset_sql,
            ) as reset_sql:
                call_command(
                    "import_staging_catalog",
                    self._write(directory),
                    author_username="Julian",
                    stdout=StringIO(),
                )
        reset_sql.assert_called_once()
        self.assertEqual(reset_sql.call_args.args[1], [Movie])
