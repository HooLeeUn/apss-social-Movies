from django.contrib import admin
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from core.models import Movie, MovieRating


class MovieAdminTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_superuser(
            username="movie-admin", password="test-password", email="admin@example.com"
        )
        cls.movie = Movie.objects.create(
            title_english="Admin fixture", title_spanish="Prueba admin",
            director="Fixture director", cast_members="Fixture actor", genre="Drama",
            release_year=2024, author=cls.user, external_rating=8, tmdb_id=123456,
        )
        cls.other = Movie.objects.create(title_english="Other movie", release_year=2020)
        MovieRating.objects.create(movie=cls.movie, user=cls.user, score=6)

    def setUp(self):
        self.client.force_login(self.user)
        self.url = reverse("admin:core_movie_changelist")

    def test_list_sql_has_no_rating_aggregates_or_filter_enumeration(self):
        with CaptureQueriesContext(connection) as queries:
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        sql = [q["sql"].upper() for q in queries]
        self.assertFalse(any("CORE_MOVIERATING" in q for q in sql))
        movie_sql = [q for q in sql if '"CORE_MOVIE"' in q]
        self.assertFalse(any("GROUP BY" in q or "SELECT DISTINCT" in q for q in movie_sql))
        counts = [q for q in movie_sql if "COUNT(" in q]
        self.assertEqual(len(counts), 1)  # Exact paginator count remains.
        self.assertNotIn("JOIN", counts[0])
        changelist = response.context["cl"]
        self.assertEqual(changelist.result_count, 2)
        self.assertIsNone(changelist.full_result_count)
        self.assertEqual([m.pk for m in changelist.result_list], [self.other.pk, self.movie.pk])
        self.assertIn("author", changelist.result_list.query.select_related)
        self.assertContains(response, 'name="genre__exact"')

    def test_search_preserves_all_existing_fields(self):
        for term in (str(self.movie.pk), "Admin fixture", "Prueba admin", "Fixture director", "Fixture actor", "Drama", "123456"):
            with self.subTest(term=term):
                response = self.client.get(self.url, {"q": term})
                self.assertEqual(response.status_code, 200)
                self.assertIn(self.movie.pk, [m.pk for m in response.context["cl"].result_list])

    def test_filters_and_hidden_parameters(self):
        params = {"genre__exact": "Drama", "release_year__exact": "2024",
                  "author__id__exact": str(self.user.pk), "q": "Admin"}
        response = self.client.get(self.url, params)
        self.assertEqual(response.status_code, 200)
        self.assertEqual([m.pk for m in response.context["cl"].result_list], [self.movie.pk])
        self.assertContains(response, 'name="q" value="Admin"')
        response = self.client.get(self.url, {"created_at__isnull": "True"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["cl"].result_count, 0)
        response = self.client.get(self.url, {"release_year__exact": "invalid"})
        self.assertEqual(response.status_code, 302)

    def test_detail_rating_and_edit(self):
        url = reverse("admin:core_movie_change", args=[self.movie.pk])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        obj = response.context["original"]
        expected = Movie.objects.with_display_rating().get(pk=self.movie.pk)
        for name in ("real_ratings_avg", "real_ratings_count", "display_rating"):
            self.assertEqual(getattr(obj, name), getattr(expected, name))
        form = response.context["adminform"].form
        data = {name: form[name].value() for name in form.fields
                if form[name].value() is not None}
        data.update(title_english="Edited movie", _save="Save")
        response = self.client.post(url, data)
        self.assertEqual(response.status_code, 302)
        self.movie.refresh_from_db()
        self.assertEqual(self.movie.title_english, "Edited movie")

    def test_movie_autocomplete_keeps_order_search_and_exact_count(self):
        for model in ("comment", "movierating"):
            with self.subTest(model=model), CaptureQueriesContext(connection) as queries:
                response = self.client.get(reverse("admin:autocomplete"), {
                    "app_label": "core", "model_name": model,
                    "field_name": "movie", "term": "Admin fixture",
                })
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["results"], [
                {"id": str(self.movie.pk), "text": str(self.movie)}
            ])
            self.assertFalse(response.json()["pagination"]["more"])
            self.assertFalse(any("core_movierating" in q["sql"] for q in queries))
            selects = [q["sql"] for q in queries if 'FROM "core_movie"' in q["sql"] and "ORDER BY" in q["sql"]]
            self.assertTrue(any('"core_movie"."created_at" DESC' in q for q in selects))

    def test_invalid_or_missing_detail(self):
        movie_admin = admin.site._registry[Movie]
        request = self.client.request().wsgi_request
        self.assertIsNone(movie_admin.get_object(request, "invalid"))
        self.assertIsNone(movie_admin.get_object(request, "999999999"))

    def test_pagination_keeps_exact_totals_and_second_page(self):
        Movie.objects.bulk_create([Movie(title_english=f"Page fixture {i}") for i in range(105)])
        first = self.client.get(self.url)
        second = self.client.get(self.url, {"p": 2})
        self.assertEqual(second.status_code, 200)
        self.assertEqual(first.context["cl"].paginator.count, 107)
        first_ids = {m.pk for m in first.context["cl"].result_list}
        second_ids = {m.pk for m in second.context["cl"].result_list}
        self.assertEqual(len(first_ids), 100)
        self.assertEqual(len(second_ids), 7)
        self.assertFalse(first_ids & second_ids)
