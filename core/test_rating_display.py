from decimal import Decimal

from django.contrib import admin
from django.contrib.auth import get_user_model
from django.db import connection
from django.test import RequestFactory, TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from rest_framework.test import APIClient

from core.models import Movie, MovieRating


class RatingDisplayTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.users = get_user_model().objects.bulk_create([
            get_user_model()(username=f"rating-display-{i}") for i in range(101)
        ])
        cls.movies = {}
        for count in (0, 1, 50, 99, 100, 101):
            movie = Movie.objects.create(title_english=f"RatingDisplayFixture {count}", type=Movie.MOVIE,
                                          external_rating=Decimal("8.0"), external_votes=123456)
            MovieRating.objects.bulk_create([
                MovieRating(user=user, movie=movie, score=6) for user in cls.users[:count]
            ])
            cls.movies[count] = movie

    @staticmethod
    def rows(response):
        return response.data["results"] if isinstance(response.data, dict) else response.data

    def test_normal_search_uses_central_formula_at_every_boundary(self):
        client = APIClient()
        before = list(Movie.objects.values_list("id", "external_rating", "external_votes"))
        for count, expected in ((0, 8), (1, 7.98), (50, 7), (99, 6.02), (100, 6), (101, 6)):
            with self.subTest(count=count):
                response = client.get(reverse("movie-search"), {"q": "RatingDisplayFixture", "release_year": ""})
                self.assertEqual(response.status_code, 200)
                row = next(row for row in self.rows(response) if row["id"] == self.movies[count].pk)
                self.assertAlmostEqual(row["display_rating"], expected)
                self.assertNotIn("real_ratings_count", row)  # light serializer stays light
                self.assertNotIn("my_rating", row)
                central = Movie.objects.with_display_rating().get(pk=row["id"])
                self.assertAlmostEqual(row["display_rating"], central.display_rating)
        self.assertEqual(list(Movie.objects.values_list("id", "external_rating", "external_votes")), before)

    def test_detail_exposes_internal_metrics_and_matches_both_search_modes(self):
        client = APIClient()
        normal = self.rows(client.get(reverse("movie-search"), {"q": "RatingDisplayFixture"}))
        social = self.rows(client.get(reverse("movie-search"), {"q": "RatingDisplayFixture", "include_social": "true"}))
        for count, movie in self.movies.items():
            with self.subTest(count=count):
                detail = client.get(reverse("movie-detail", kwargs={"pk": movie.pk}))
                self.assertEqual(detail.status_code, 200)
                self.assertEqual(detail.data["real_ratings_count"], count)
                self.assertEqual(detail.data["real_ratings_avg"], 6 if count else None)
                for rows in (normal, social):
                    row = next(row for row in rows if row["id"] == movie.pk)
                    self.assertAlmostEqual(row["display_rating"], detail.data["display_rating"])
                self.assertEqual(detail.data["general_rating"], detail.data["display_rating"])

    def test_external_votes_do_not_affect_display_and_types_share_formula(self):
        movie = self.movies[50]
        before = Movie.objects.with_display_rating().get(pk=movie.pk).display_rating
        Movie.objects.filter(pk=movie.pk).update(external_votes=0, type=Movie.SERIES)
        after = Movie.objects.with_display_rating().get(pk=movie.pk).display_rating
        self.assertEqual(before, after)

    def test_admin_metrics_are_readonly_and_loaded_only_for_individual_movies(self):
        movie_admin = admin.site._registry[Movie]
        request = RequestFactory().get("/admin/core/movie/")
        rendered = {}
        for movie in self.movies.values():
            row = movie_admin.get_object(request, str(movie.pk))
            rendered[row.pk] = (movie_admin.reccool_average(row), movie_admin.reccool_ratings_count(row),
                                movie_admin.overall_rating(row))
        self.assertEqual(rendered[self.movies[0].pk], (None, 0, "8.0"))
        self.assertEqual(rendered[self.movies[1].pk], ("6.0", 1, "8.0"))
        self.assertEqual(rendered[self.movies[50].pk], ("6.0", 50, "7.0"))
        self.assertEqual(rendered[self.movies[100].pk], ("6.0", 100, "6.0"))
        self.assertIn("Calificaciones de RecCool", dict(movie_admin.fieldsets))
        for name in ("reccool_average", "reccool_ratings_count", "overall_rating"):
            self.assertIn(name, movie_admin.readonly_fields)
        self.assertEqual(movie_admin.reccool_average.empty_value_display, "—")

    def test_admin_handles_missing_external_and_internal_average(self):
        movie = Movie.objects.create(title_english="No ratings")
        movie_admin = admin.site._registry[Movie]
        row = movie_admin.get_object(RequestFactory().get("/"), str(movie.pk))
        self.assertIsNone(movie_admin.reccool_average(row))
        self.assertEqual(movie_admin.reccool_ratings_count(row), 0)
        self.assertIsNone(movie_admin.overall_rating(row))

    def test_normal_search_keeps_relevance_order_and_uses_one_rating_join(self):
        from core.views import MovieSearchView
        from rest_framework.test import APIRequestFactory
        view = MovieSearchView()
        view.request = view.initialize_request(APIRequestFactory().get("/", {"q": "RatingDisplayFixture"}))
        queryset = view.get_queryset()
        self.assertEqual(queryset.query.order_by[:5], (
            "-exact_title_match", "-complete_word_title_match", "-prefix_title_match", "-year_match", "-search_rank",
        ))
        with CaptureQueriesContext(connection) as queries:
            list(queryset)
        self.assertEqual(len(queries), 1)
        self.assertIn("core_movierating", queries[0]["sql"])
