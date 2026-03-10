import io
from pathlib import Path
from tempfile import TemporaryDirectory

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase

from core.models import Movie


class ImportMoviesCommandTests(TestCase):
    def setUp(self):
        self.author = get_user_model().objects.create_user(
            username="admin", email="admin@example.com", password="test1234"
        )

    def _write_csv(self, csv_content):
        temp_dir = TemporaryDirectory()
        csv_path = Path(temp_dir.name) / "movies.csv"
        csv_path.write_text(csv_content, encoding="utf-8")
        self.addCleanup(temp_dir.cleanup)
        return csv_path

    def test_import_movies_creates_records_and_skips_duplicates(self):
        csv_content = """title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8
Planet Earth,,tvSeries,Documentary,2006,,David Attenborough,9.4
"""
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        self.assertEqual(Movie.objects.count(), 2)

        inception = Movie.objects.get(title_english="Inception")
        self.assertEqual(inception.type, Movie.MOVIE)
        self.assertEqual(inception.release_year, 2010)
        self.assertEqual(float(inception.external_rating), 8.8)
        self.assertEqual(inception.author, self.author)
        self.assertIsNone(inception.image)

        planet = Movie.objects.get(title_english="Planet Earth")
        self.assertEqual(planet.type, Movie.SERIES)
        self.assertIsNone(planet.director)

        output = out.getvalue()
        self.assertIn("Total filas leídas: 3", output)
        self.assertIn("Creadas: 2", output)
        self.assertIn("Omitidas por duplicado: 1", output)

    def test_import_movies_uses_given_author(self):
        alt_user = get_user_model().objects.create_user(
            username="catalog_admin", email="catalog@example.com", password="test1234"
        )
        csv_content = """title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating
Arrival,La llegada,film,Sci-Fi,2016,Denis Villeneuve,Amy Adams,7.9
"""
        csv_path = self._write_csv(csv_content)

        call_command("import_movies", str(csv_path), author="catalog_admin")

        movie = Movie.objects.get(title_english="Arrival")
        self.assertEqual(movie.author, alt_user)

    def test_import_movies_fails_when_author_does_not_exist(self):
        csv_content = """title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating
Arrival,La llegada,film,Sci-Fi,2016,Denis Villeneuve,Amy Adams,7.9
"""
        csv_path = self._write_csv(csv_content)

        with self.assertRaisesMessage(CommandError, "No existe un usuario con username 'missing_user'"):
            call_command("import_movies", str(csv_path), author="missing_user")
