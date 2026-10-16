import io
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib import admin
from django.core.exceptions import FieldDoesNotExist
from django.core.management import call_command, get_commands
from django.db import connection
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import Movie, StreamingProviderLink
from core.management.commands.fix_movie_directors import Command as FixDirectorsCommand
from core.serializers import MovieSearchResultSerializer
from core.watch_providers import get_active_provider_links


class MovieImdbRemovalTests(TestCase):
    def setUp(self):
        self.movie = Movie.objects.create(title_english="Unique Removal Film", type=Movie.MOVIE,
                                          tmdb_id=123, image="", synopsis="", synopsis_es="")
        self.payload = {"flatrate": [{"provider_id": 8}]}

    def link(self, **changes):
        fields = dict(provider_id=8, provider_name="Platform", country_code="CO", content_type="movie",
                      landing_url="https://example.com/general", monetization_type="none")
        fields.update(changes)
        return StreamingProviderLink.objects.create(**fields)

    def selected(self):
        return get_active_provider_links(self.movie, self.payload, "CO")

    def test_model_and_admin_remove_only_movie_field(self):
        with self.assertRaises(FieldDoesNotExist):
            Movie._meta.get_field("imdb_id")
        self.assertIsNotNone(StreamingProviderLink._meta.get_field("imdb_id"))
        movie_admin = admin.site._registry[Movie]
        self.assertNotIn("imdb_id", movie_admin.list_display)
        self.assertNotIn("imdb_id", movie_admin.search_fields)
        self.assertNotIn("imdb_id", str(movie_admin.fieldsets))
        self.assertIn("imdb_id", admin.site._registry[StreamingProviderLink].search_fields)

    def test_search_serializer_preserves_other_fields(self):
        fields = MovieSearchResultSerializer().fields
        self.assertNotIn("imdb_id", fields)
        for name in ("external_rating", "external_votes", "search_rank", "created_at", "updated_at"):
            self.assertIn(name, fields)

    def test_migrated_database_keeps_only_provider_imdb_column(self):
        with connection.cursor() as cursor:
            movie_columns = {c.name for c in connection.introspection.get_table_description(cursor, Movie._meta.db_table)}
            provider_columns = {c.name for c in connection.introspection.get_table_description(cursor, StreamingProviderLink._meta.db_table)}
        self.assertNotIn("imdb_id", movie_columns)
        self.assertIn("imdb_id", provider_columns)

    def test_normal_and_social_search_and_detail_without_imdb(self):
        client = APIClient()
        for social in (False, True):
            response = client.get(reverse("movie-search"), {"q": "Unique Removal Film",
                                                            "include_social": str(social).lower()})
            self.assertEqual(response.status_code, 200)
            rows = response.data.get("results", []) if isinstance(response.data, dict) else response.data
            self.assertEqual(rows[0]["id"], self.movie.pk)
            self.assertNotIn("imdb_id", rows[0])
        detail = client.get(reverse("movie-detail", kwargs={"pk": self.movie.pk}))
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.data["tmdb_id"], 123)
        self.assertNotIn("imdb_id", detail.data)

    def test_general_link_still_selected(self):
        general = self.link()
        self.assertEqual(self.selected()[8].pk, general.pk)

    def test_movie_specific_overrides_newer_general(self):
        specific = self.link(movie=self.movie, affiliate_url="https://example.com/affiliate",
                             monetization_type="affiliate")
        StreamingProviderLink.objects.filter(pk=specific.pk).update(updated_at=timezone.now() - timedelta(days=1))
        self.link()
        chosen = self.selected()[8]
        self.assertEqual(chosen.pk, specific.pk)
        self.assertEqual(chosen.affiliate_url, "https://example.com/affiliate")
        self.assertEqual(chosen.monetization_type, "affiliate")

    def test_tmdb_specific_overrides_general(self):
        self.link()
        specific = self.link(tmdb_id=123)
        self.assertEqual(self.selected()[8].pk, specific.pk)

    def test_specific_order_remains_updated_at_then_id(self):
        first = self.link(movie=self.movie)
        second = self.link(tmdb_id=123)
        StreamingProviderLink.objects.filter(pk__in=[first.pk, second.pk]).update(updated_at=timezone.now())
        self.assertEqual(self.selected()[8].pk, second.pk)
        StreamingProviderLink.objects.filter(pk=first.pk).update(updated_at=timezone.now() + timedelta(days=1))
        self.assertEqual(self.selected()[8].pk, first.pk)

    def test_imdb_only_link_does_not_become_general(self):
        self.link(imdb_id="tt123456")
        self.assertEqual(self.selected(), {})

    def test_country_active_provider_and_content_type_are_preserved(self):
        for fields in ({"country_code": "US"}, {"is_active": False}, {"provider_id": 99},
                       {"tmdb_id": 123, "content_type": "tv"}):
            self.link(**fields)
        self.assertEqual(self.selected(), {})

    def test_general_content_type_priority_and_fallback(self):
        fallback = self.link(content_type="tv")
        self.assertEqual(self.selected()[8].pk, fallback.pk)
        matching = self.link(content_type="movie")
        self.assertEqual(self.selected()[8].pk, matching.pk)

    @patch("core.management.commands.enrich_movies_tmdb.get_tmdb_json")
    def test_enrichment_skips_missing_tmdb_without_resolving_identity(self, tmdb):
        missing = Movie.objects.create(title_english="No TMDB", type=Movie.MOVIE)
        call_command("enrich_movies_tmdb", movie_id=missing.pk, sleep=0, stdout=io.StringIO())
        tmdb.assert_not_called()
        missing.refresh_from_db()
        self.assertIsNone(missing.tmdb_id)

    def test_legacy_commands_are_not_registered(self):
        for name in ("fix_movie_imdb_ids", "fix_tmdb_ids", "import_movie_posters", "import_movies",
                     "import_movie_posters_csv", "import_movie_synopsis_csv", "import_movie_synopsis_es_csv"):
            self.assertNotIn(name, get_commands())

    def test_diagnostic_export_does_not_require_imdb(self):
        missing = Movie.objects.create(title_english="No TMDB", type=Movie.MOVIE)
        with TemporaryDirectory() as directory:
            output = Path(directory) / "export.csv"
            call_command("export_movies_without_tmdb", output=str(output), stdout=io.StringIO())
            contents = output.read_text(encoding="utf-8-sig")
            self.assertNotIn("imdb_id", contents.splitlines()[0])
            self.assertIn(str(missing.pk), contents)

    def test_director_command_does_not_require_imdb_column(self):
        self.assertNotIn("imdb_id", FixDirectorsCommand.REQUIRED_COLUMNS)
