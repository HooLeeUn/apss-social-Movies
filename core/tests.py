import io
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.core.cache import cache
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import IntegrityError, connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils.dateparse import parse_datetime
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from core.feed_pool import DailyFeedPoolService
from core.models import (
    Comment,
    CommentReaction,
    Follow,
    Friendship,
    Movie,
    Profile,
    ProfileFavoriteMovie,
    MovieRating,
    UserVisibilityBlock,
    WeeklyRecommendationItem,
    WeeklyRecommendationSnapshot,
    build_genre_key,
    normalize_movie_search_text,
    UserDirectorPreference,
    UserGenrePreference,
    UserTasteProfile,
    UserTypePreference,
    UserDailyFeedPool,
    UserNotification,
)
from core.serializers import CommentSerializer, MovieAutocompleteSerializer, MovieSearchLightSerializer, MovieSearchResultSerializer
from core.services import (
    remove_user_preferences_for_movie_rating,
    update_user_preferences_for_movie_rating,
)
from core.views import (
    MovieListView,
    MovieSearchView,
    apply_movie_autocomplete_search,
    build_movie_autocomplete_fast_queryset,
)
from core.weekly_recommendations import (
    get_previous_closed_week_window,
    get_weekly_recommendation_candidates,
)


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
        csv_content = """title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating,external_votes
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8,2500000
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8,2500000
Planet Earth,,tvSeries,Documentary,2006,,David Attenborough,9.4,
"""
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        self.assertEqual(Movie.objects.count(), 2)

        inception = Movie.objects.get(title_english="Inception")
        self.assertEqual(inception.type, Movie.MOVIE)
        self.assertEqual(inception.release_year, 2010)
        self.assertEqual(float(inception.external_rating), 8.8)
        self.assertEqual(inception.external_votes, 2500000)
        self.assertEqual(inception.genre_key, "Sci-Fi")
        self.assertEqual(inception.author, self.author)
        self.assertIsNone(inception.image)

        planet = Movie.objects.get(title_english="Planet Earth")
        self.assertEqual(planet.type, Movie.SERIES)
        self.assertIsNone(planet.director)

        output = out.getvalue()
        self.assertIn("Total filas leídas: 3", output)
        self.assertIn("Creadas: 2", output)
        self.assertIn("Omitidas por duplicado: 1", output)

    def test_import_movies_uses_first_director_to_detect_csv_duplicates(self):
        csv_content = '''title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating,external_votes
The Omega Man,El último hombre vivo,Movie,Sci-Fi,1971,"Boris Sagal, Robert Gist, Alan Crosland Jr.",Charlton Heston,6.4,100
The Omega Man,El último hombre vivo,Movie,Sci-Fi,1971," Boris Sagal , Different Director",Charlton Heston,6.4,200
The Omega Man,El último hombre vivo,Movie,Sci-Fi,1971,Alan Crosland Jr.,Charlton Heston,6.4,300
'''
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        self.assertEqual(Movie.objects.count(), 2)
        self.assertTrue(Movie.objects.filter(director="Boris Sagal").exists())
        self.assertTrue(Movie.objects.filter(director="Alan Crosland Jr.").exists())

        output = out.getvalue()
        self.assertIn("Creadas: 2", output)
        self.assertIn("Omitidas por duplicado: 1", output)
        self.assertIn("Directores reducidos a primer director: 2", output)

    def test_import_movies_uses_first_director_to_match_existing_movies(self):
        existing_movie = Movie.objects.create(
            author=self.author,
            title_english="The Omega Man",
            title_spanish="El último hombre vivo",
            type=Movie.MOVIE,
            genre="Sci-Fi",
            release_year=1971,
            director="Boris Sagal, Robert Gist",
            cast_members="Charlton Heston",
            external_rating=6.4,
        )
        csv_content = '''title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating,imdb_id,external_votes
The Omega Man,El último hombre vivo,Movie,Sci-Fi,1971," boris sagal , Alan Crosland Jr.",Charlton Heston,6.4,tt0067525,500
The Omega Man,El último hombre vivo,Movie,Sci-Fi,1971,Alan Crosland Jr.,Charlton Heston,6.4,tt0067525,700
'''
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        existing_movie.refresh_from_db()

        self.assertEqual(Movie.objects.count(), 2)
        self.assertEqual(existing_movie.imdb_id, "tt0067525")
        self.assertEqual(existing_movie.external_votes, 500)
        self.assertTrue(
            Movie.objects.filter(director="Alan Crosland Jr.", external_votes=700).exists()
        )

        output = out.getvalue()
        self.assertIn("Creadas: 1", output)
        self.assertIn("Registros existentes actualizados: 1", output)
        self.assertIn("Omitidas por duplicado: 1", output)

    def test_import_movies_updates_external_votes_and_missing_imdb_for_existing_movies(self):
        existing_movie = Movie.objects.create(
            author=self.author,
            title_english="Inception",
            title_spanish="El origen",
            type=Movie.MOVIE,
            genre="Sci-Fi",
            release_year=2010,
            director="Christopher Nolan",
            cast_members="Leonardo DiCaprio",
            external_rating=8.8,
        )
        csv_content = """title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating,imdb_id,external_votes
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8,tt1375666,2500000
"""
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        existing_movie.refresh_from_db()

        self.assertEqual(Movie.objects.count(), 1)
        self.assertEqual(existing_movie.imdb_id, "tt1375666")
        self.assertEqual(existing_movie.external_votes, 2500000)

        output = out.getvalue()
        self.assertIn("Creadas: 0", output)
        self.assertIn("Registros existentes actualizados: 1", output)
        self.assertIn("Omitidas por duplicado: 1", output)

    def test_import_movies_does_not_queue_unsaved_movies_for_bulk_update(self):
        csv_content = """title_english,title_spanish,type,genre,release_year,director,cast_members,external_rating,imdb_id,external_votes
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8,,100
Inception,El origen,Movie,Sci-Fi,2010,Christopher Nolan,Leonardo DiCaprio,8.8,tt1375666,200
"""
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        movie = Movie.objects.get(title_english="Inception")

        self.assertEqual(Movie.objects.count(), 1)
        self.assertEqual(movie.imdb_id, "tt1375666")
        self.assertEqual(movie.external_votes, 200)

        output = out.getvalue()
        self.assertIn("Creadas: 1", output)
        self.assertIn("Registros existentes actualizados: 0", output)
        self.assertIn("Omitidas por duplicado: 1", output)

    def test_import_movies_truncates_charfields_but_not_textfields(self):
        long_title = "T" * 300
        long_spanish_title = "S" * 300
        long_genre = "G" * 150
        long_director = "D" * 300
        long_cast = "Actor " * 120
        csv_content = (
            "title_english,title_spanish,type,genre,release_year,director,"
            "cast_members,external_rating,imdb_id\n"
            f"{long_title},{long_spanish_title},Movie,{long_genre},2020,"
            f"{long_director},{long_cast},7.0,tt12345678901234567890\n"
        )
        csv_path = self._write_csv(csv_content)
        out = io.StringIO()

        call_command("import_movies", str(csv_path), stdout=out)

        movie = Movie.objects.get()
        self.assertEqual(movie.title_english, "T" * 255)
        self.assertEqual(movie.title_spanish, "S" * 255)
        self.assertEqual(movie.genre, "G" * 100)
        self.assertEqual(movie.director, "D" * 255)
        self.assertEqual(movie.cast_members, long_cast.strip())
        self.assertEqual(movie.imdb_id, "tt123456789012345678")

        output = out.getvalue()
        self.assertIn("Directores truncados por max_length: 1", output)
        self.assertIn("director=1", output)
        self.assertIn("title_english=1", output)
        self.assertIn("title_spanish=1", output)
        self.assertIn("genre=1", output)
        self.assertIn("genre_key=1", output)
        self.assertIn("imdb_id=1", output)

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


class MovieGenreKeyTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="genre_user", email="genre@example.com", password="test1234"
        )

    def test_build_genre_key_canonicalizes_genres(self):
        self.assertEqual(build_genre_key("Action, Comedy, Drama"), "Action|Comedy|Drama")
        self.assertEqual(build_genre_key("Drama, Action, Comedy"), "Action|Comedy|Drama")
        self.assertEqual(build_genre_key("Drama"), "Drama")
        self.assertIsNone(build_genre_key(" ,  , "))

    def test_movie_persists_canonical_genre_key(self):
        movie = Movie.objects.create(
            author=self.user,
            title_english="Canonical Movie",
            genre=" Drama, Action, Comedy ",
            type=Movie.MOVIE,
        )

        self.assertEqual(movie.genre_key, "Action|Comedy|Drama")

    def test_normalize_movie_search_text_removes_accents_and_special_characters(self):
        self.assertEqual(normalize_movie_search_text("Tío / Cabaña: Acción!"), "tio cabana accion")

    def test_movie_populates_accentless_search_fields_on_save(self):
        movie = Movie.objects.create(
            author=self.user,
            title_english="My Uncle",
            title_spanish="Mi tío y la cabaña",
            genre="Acción, Drama",
            type=Movie.MOVIE,
            director="María Gómez",
            cast_members="José Núñez",
        )

        self.assertEqual(movie.title_english_search, "my uncle")
        self.assertEqual(movie.title_spanish_search, "mi tio y la cabana")
        self.assertEqual(movie.director_search, "maria gomez")
        self.assertEqual(movie.cast_members_search, "jose nunez")


class MovieQuerySetAnnotationTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user_model = get_user_model()
        cls.author = cls.user_model.objects.create_user(
            username="ratings_author", email="ratings-author@example.com", password="test1234"
        )

    def _create_movie(self, **overrides):
        data = {
            "author": self.author,
            "title_english": overrides.pop("title_english", "Annotated Movie"),
            "type": overrides.pop("type", Movie.MOVIE),
            "external_rating": overrides.pop("external_rating", 8.0),
            "external_votes": overrides.pop("external_votes", 0),
        }
        data.update(overrides)
        return Movie.objects.create(**data)

    def _bulk_rate_movie(self, movie, total_ratings, score, username_prefix):
        users = self.user_model.objects.bulk_create(
            [
                self.user_model(
                    username=f"{username_prefix}_{index}",
                    email=f"{username_prefix}_{index}@example.com",
                )
                for index in range(total_ratings)
            ],
            batch_size=1000,
        )
        MovieRating.objects.bulk_create(
            [MovieRating(user=user, movie=movie, score=score) for user in users],
            batch_size=1000,
        )

    def test_ranking_scores_prefer_real_ratings_after_5000_votes(self):
        movie = self._create_movie(title_english="Real Consensus", external_rating=9.8, external_votes=12000)
        self._bulk_rate_movie(movie, total_ratings=5000, score=7, username_prefix="real_consensus")

        annotated_movie = Movie.objects.with_ranking_scores().get(pk=movie.pk)

        self.assertEqual(annotated_movie.real_ratings_count, 5000)
        self.assertAlmostEqual(annotated_movie.real_ratings_avg, 7.0)
        self.assertAlmostEqual(annotated_movie.ranking_confidence_score, 1.0)
        self.assertAlmostEqual(annotated_movie.ranking_quality_score, 7.0)

    def test_ranking_scores_prefer_external_source_when_external_votes_reach_5000_first(self):
        movie = self._create_movie(title_english="External Consensus", external_rating=8.6, external_votes=6500)
        self._bulk_rate_movie(movie, total_ratings=12, score=5, username_prefix="external_consensus")

        annotated_movie = Movie.objects.with_ranking_scores().get(pk=movie.pk)

        self.assertEqual(annotated_movie.real_ratings_count, 12)
        self.assertAlmostEqual(annotated_movie.real_ratings_avg, 5.0)
        self.assertAlmostEqual(annotated_movie.ranking_confidence_score, 1.0)
        self.assertAlmostEqual(annotated_movie.ranking_quality_score, 8.6)

    def test_ranking_scores_downweight_low_confidence_titles(self):
        movie = self._create_movie(title_english="Low Confidence", external_rating=9.5, external_votes=4000)
        self._bulk_rate_movie(movie, total_ratings=25, score=10, username_prefix="low_confidence")

        annotated_movie = Movie.objects.with_ranking_scores().get(pk=movie.pk)

        self.assertEqual(annotated_movie.real_ratings_count, 25)
        self.assertAlmostEqual(annotated_movie.ranking_confidence_score, 0.4)
        self.assertAlmostEqual(annotated_movie.ranking_quality_score, 3.8)

    def test_display_rating_switches_to_real_average_at_100_votes(self):
        movie = self._create_movie(title_english="Display Threshold", external_rating=8.0)
        self._bulk_rate_movie(movie, total_ratings=99, score=6, username_prefix="display_threshold")

        annotated_before_threshold = Movie.objects.with_display_rating().get(pk=movie.pk)
        self.assertAlmostEqual(annotated_before_threshold.display_rating, 6.02)

        extra_user = self.user_model.objects.create_user(
            username="display_threshold_99",
            email="display_threshold_99@example.com",
            password="test1234",
        )
        MovieRating.objects.create(user=extra_user, movie=movie, score=6)

        annotated_at_threshold = Movie.objects.with_display_rating().get(pk=movie.pk)
        self.assertEqual(annotated_at_threshold.real_ratings_count, 100)
        self.assertAlmostEqual(annotated_at_threshold.display_rating, 6.0)


class FeedMoviesEndpointTests(TestCase):
    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="feed_user", email="feed@example.com", password="test1234"
        )
        self.author = get_user_model().objects.create_user(
            username="catalog_user", email="catalog@example.com", password="test1234"
        )
        self.url = reverse("feed-movies")

    def _create_movie(self, **overrides):
        data = {
            "author": self.author,
            "title_english": overrides.pop("title_english", "Feed Movie"),
            "genre": overrides.pop("genre", "Drama"),
            "type": overrides.pop("type", Movie.MOVIE),
            "external_rating": overrides.pop("external_rating", 8.0),
            "external_votes": overrides.pop("external_votes", 0),
        }
        data.update(overrides)
        return Movie.objects.create(**data)

    def _bulk_rate_movie(self, movie, total_ratings, score, username_prefix):
        users = get_user_model().objects.bulk_create(
            [
                get_user_model()(
                    username=f"{username_prefix}_{index}",
                    email=f"{username_prefix}_{index}@example.com",
                )
                for index in range(total_ratings)
            ],
            batch_size=1000,
        )
        MovieRating.objects.bulk_create(
            [MovieRating(user=user, movie=movie, score=score) for user in users],
            batch_size=1000,
        )

    def test_feed_requires_authentication_and_returns_200_for_authenticated_user(self):
        anon_response = self.client.get(self.url)
        self.assertEqual(anon_response.status_code, status.HTTP_401_UNAUTHORIZED)

        Movie.objects.create(
            author=self.author,
            title_english="Authenticated Feed Movie",
            genre="Drama",
            type=Movie.MOVIE,
            external_rating=8.0,
            release_year=2020,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["external_votes"], 0)
        self.assertEqual(response.data["results"][0]["synopsis"], "")

    def test_feed_excludes_rated_movies_by_default(self):
        rated_movie = self._create_movie(
            title_english="Already Rated",
            genre="Action, Comedy",
            release_year=2021,
            external_rating=7.0,
        )
        fresh_movie = self._create_movie(
            title_english="Fresh Pick",
            genre="Drama",
            release_year=2022,
            external_rating=8.0,
            external_votes=6000,
        )
        MovieRating.objects.create(user=self.user, movie=rated_movie, score=9)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles, [fresh_movie.title_english])

    def test_feed_search_is_accent_insensitive(self):
        matched = self._create_movie(
            title_english="Escape Dream",
            title_spanish="Sueño de fuga",
            genre="Acción, Drama",
            synopsis="Una historia de acción intensa.",
        )
        self._create_movie(
            title_english="Different Movie",
            title_spanish="Comedia ligera",
            genre="Comedy",
        )

        self.client.force_authenticate(user=self.user)
        response_without_tilde = self.client.get(
            self.url,
            {"exclude_rated": "false", "search": "sueno accion"},
        )
        response_with_tilde = self.client.get(
            self.url,
            {"exclude_rated": "false", "search": "sueño acción"},
        )

        self.assertEqual(response_without_tilde.status_code, status.HTTP_200_OK)
        self.assertEqual(response_with_tilde.status_code, status.HTTP_200_OK)
        ids_without_tilde = [item["id"] for item in response_without_tilde.data["results"]]
        ids_with_tilde = [item["id"] for item in response_with_tilde.data["results"]]
        self.assertIn(matched.id, ids_without_tilde)
        self.assertIn(matched.id, ids_with_tilde)

    def test_feed_orders_by_recommendation_score(self):
        UserTasteProfile.objects.create(user=self.user, ratings_count=3)

        preferred_movie = self._create_movie(
            title_english="Preferred Combo",
            genre="Comedy, Action",
            director="Christopher Nolan",
            release_year=2024,
            external_rating=6.0,
            external_votes=7000,
        )
        Movie.objects.create(
            author=self.author,
            title_english="Higher External Rating",
            genre="Drama",
            type=Movie.SERIES,
            director="Other Director",
            release_year=2025,
            external_rating=9.5,
            external_votes=100,
        )

        UserGenrePreference.objects.create(user=self.user, genre="Action|Comedy", count_10=1)
        UserTypePreference.objects.create(user=self.user, content_type=Movie.MOVIE, count_10=1)
        UserDirectorPreference.objects.create(user=self.user, director="Christopher Nolan", count_10=1)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[0], preferred_movie.title_english)

    def test_feed_for_user_without_taste_profile_does_not_break(self):
        high_rated = self._create_movie(
            title_english="Top Rated",
            release_year=2020,
            external_rating=9.1,
            external_votes=6500,
        )
        Movie.objects.create(
            author=self.author,
            title_english="Lower Rated",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=2024,
            external_rating=7.2,
            external_votes=1500,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[0], high_rated.title_english)

    def test_feed_orders_null_release_years_last(self):
        recent_movie = self._create_movie(
            title_english="Recent Year",
            release_year=2024,
            external_rating=8.0,
            external_votes=6500,
        )
        null_year_movie = self._create_movie(
            title_english="Unknown Year",
            release_year=None,
            external_rating=8.0,
            external_votes=6500,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [recent_movie.title_english, null_year_movie.title_english])

    def test_feed_genre_filter_single_value_matches_inside_multi_genre_field(self):
        matched = self._create_movie(
            title_english="Action Comedy Pick",
            genre="Action, Comedy",
            external_votes=7000,
        )
        self._create_movie(
            title_english="Drama Pick",
            genre="Drama",
            external_votes=7000,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"genre": "comedy", "exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles, [matched.title_english])

    def test_feed_genre_filter_two_genres_uses_and_logic(self):
        and_match = self._create_movie(
            title_english="AND Match",
            genre="Action, Comedy, Drama",
            external_votes=7000,
        )
        self._create_movie(
            title_english="Only Action",
            genre="Action",
            external_votes=7000,
        )
        self._create_movie(
            title_english="Only Comedy",
            genre="Comedy",
            external_votes=7000,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"genres": "Action,Comedy", "exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles, [and_match.title_english])

    def test_feed_genre_filter_pagination_keeps_filtered_subset_across_pages(self):
        for index in range(6):
            self._create_movie(
                title_english=f"Sci-Fi Match {index}",
                genre="Sci-Fi, Action",
                external_votes=7000,
                release_year=2020 + index,
            )
        for index in range(4):
            self._create_movie(
                title_english=f"Outside Genre {index}",
                genre="Drama",
                external_votes=7000,
                release_year=2010 + index,
            )

        self.client.force_authenticate(user=self.user)
        first_page = self.client.get(
            self.url,
            {"genre": "Sci-Fi", "exclude_rated": "false", "page_size": 2},
        )
        self.assertEqual(first_page.status_code, status.HTTP_200_OK)
        self.assertEqual(first_page.data["count"], 6)
        self.assertIsNotNone(first_page.data["next"])
        self.assertTrue(all("Sci-Fi" in item["genre"] for item in first_page.data["results"]))

        second_page = self.client.get(first_page.data["next"])
        self.assertEqual(second_page.status_code, status.HTTP_200_OK)
        self.assertTrue(all("Sci-Fi" in item["genre"] for item in second_page.data["results"]))

        third_page = self.client.get(second_page.data["next"])
        self.assertEqual(third_page.status_code, status.HTTP_200_OK)
        self.assertTrue(all("Sci-Fi" in item["genre"] for item in third_page.data["results"]))
        self.assertIsNone(third_page.data["next"])

    def test_feed_genre_filter_accepts_repeated_genres_query_params(self):
        and_match = self._create_movie(
            title_english="Repeated Params AND Match",
            genre="Action, Comedy",
            external_votes=7000,
        )
        self._create_movie(
            title_english="Action Only",
            genre="Action",
            external_votes=7000,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(f"{self.url}?genres=Action&genres=Comedy&exclude_rated=false")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles, [and_match.title_english])

    def test_feed_genre_filter_preserves_existing_ranking_order_within_filtered_results(self):
        UserTasteProfile.objects.create(user=self.user, ratings_count=5)
        top_filtered = self._create_movie(
            title_english="Top Filtered",
            genre="Action, Comedy",
            director="Preferred Director",
            external_rating=7.0,
            external_votes=8000,
        )
        low_filtered = self._create_movie(
            title_english="Low Filtered",
            genre="Action, Comedy",
            director="Other Director",
            external_rating=6.0,
            external_votes=8000,
        )
        self._create_movie(
            title_english="Outside Filter",
            genre="Drama",
            director="Preferred Director",
            external_rating=9.5,
            external_votes=9000,
        )
        UserGenrePreference.objects.create(user=self.user, genre="Action|Comedy", count_10=2)
        UserDirectorPreference.objects.create(user=self.user, director="Preferred Director", count_10=2)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"genres": "Action,Comedy", "exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [top_filtered.title_english, low_filtered.title_english])

    def test_feed_does_not_overprioritize_high_rating_with_low_confidence(self):
        trusted_movie = self._create_movie(
            title_english="Trusted Consensus",
            external_rating=8.1,
            external_votes=6500,
            release_year=2021,
        )
        flashy_movie = self._create_movie(
            title_english="Flashy But Thin",
            external_rating=9.8,
            external_votes=100,
            release_year=2025,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [trusted_movie.title_english, flashy_movie.title_english])

    def test_feed_prioritizes_titles_with_5000_real_ratings(self):
        real_consensus = self._create_movie(
            title_english="Real Consensus",
            external_rating=9.8,
            external_votes=12000,
            release_year=2023,
        )
        external_only = self._create_movie(
            title_english="External Favorite",
            external_rating=7.2,
            external_votes=6500,
            release_year=2024,
        )
        self._bulk_rate_movie(real_consensus, total_ratings=5000, score=8, username_prefix="feed_real_consensus")

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [real_consensus.title_english, external_only.title_english])

    def test_feed_can_prioritize_external_consensus_without_internal_mass(self):
        external_consensus = self._create_movie(
            title_english="External Consensus",
            external_rating=8.6,
            external_votes=6500,
            release_year=2022,
        )
        internal_but_small = self._create_movie(
            title_english="Internal But Small",
            external_rating=7.0,
            external_votes=300,
            release_year=2024,
        )
        self._bulk_rate_movie(external_consensus, total_ratings=12, score=5, username_prefix="feed_external_consensus")
        self._bulk_rate_movie(internal_but_small, total_ratings=200, score=8, username_prefix="feed_internal_small")

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [external_consensus.title_english, internal_but_small.title_english])

    def test_feed_uses_release_year_as_reasonable_tiebreaker(self):
        older_movie = self._create_movie(
            title_english="Older Twin",
            external_rating=8.0,
            external_votes=6500,
            release_year=2021,
        )
        newer_movie = self._create_movie(
            title_english="Newer Twin",
            external_rating=8.0,
            external_votes=6500,
            release_year=2024,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [newer_movie.title_english, older_movie.title_english])

    def test_movies_list_endpoint_still_works(self):
        Movie.objects.create(
            author=self.author,
            title_english="Public Catalog Movie",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=2019,
            external_rating=8.3,
        )

        response = self.client.get(reverse("movie-list"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["title_english"], "Public Catalog Movie")
        self.assertEqual(response.data["results"][0]["external_votes"], 0)
        self.assertEqual(response.data["results"][0]["synopsis"], "")

    def test_feed_reuses_ranking_cache_between_pages(self):
        from core.views import FeedMoviesView

        for index in range(40):
            self._create_movie(
                title_english=f"Cache Candidate {index}",
                genre="Drama",
                external_rating=7.5 + (index % 3) * 0.1,
                external_votes=6000 + index,
                release_year=2000 + index,
            )

        self.client.force_authenticate(user=self.user)
        original_builder = FeedMoviesView._build_ranking_cache_payload
        with patch.object(
            FeedMoviesView,
            "_build_ranking_cache_payload",
            wraps=original_builder,
        ) as ranking_builder:
            first_page = self.client.get(self.url, {"exclude_rated": "false", "page_size": 10, "page": 1})
            second_page = self.client.get(self.url, {"exclude_rated": "false", "page_size": 10, "page": 2})

        self.assertEqual(first_page.status_code, status.HTTP_200_OK)
        self.assertEqual(second_page.status_code, status.HTTP_200_OK)
        self.assertEqual(ranking_builder.call_count, 1)

    def test_feed_rotation_changes_close_scores_without_displacing_clear_winner(self):
        winner = self._create_movie(
            title_english="Clear Winner",
            external_rating=9.6,
            external_votes=9000,
            release_year=2023,
        )
        for index in range(5):
            self._create_movie(
                title_english=f"Close Match {index}",
                external_rating=8.0,
                external_votes=6500 + index,
                release_year=2020,
            )

        self.client.force_authenticate(user=self.user)
        with patch("core.views.FeedMoviesView._resolve_rotation_bucket", return_value=100):
            first_bucket = self.client.get(self.url, {"exclude_rated": "false", "page_size": 6})
        with patch("core.views.FeedMoviesView._resolve_rotation_bucket", return_value=101):
            second_bucket = self.client.get(self.url, {"exclude_rated": "false", "page_size": 6})

        self.assertEqual(first_bucket.status_code, status.HTTP_200_OK)
        self.assertEqual(second_bucket.status_code, status.HTTP_200_OK)
        first_titles = [item["title_english"] for item in first_bucket.data["results"]]
        second_titles = [item["title_english"] for item in second_bucket.data["results"]]

        self.assertEqual(first_titles[0], winner.title_english)
        self.assertEqual(second_titles[0], winner.title_english)
        self.assertNotEqual(first_titles[1:], second_titles[1:])


class DailyFeedPoolServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="pool_user",
            email="pool_user@example.com",
            password="test1234",
        )
        self.author = get_user_model().objects.create_user(
            username="pool_author",
            email="pool_author@example.com",
            password="test1234",
        )

    def _create_movie(self, **overrides):
        data = {
            "author": self.author,
            "title_english": overrides.pop("title_english", "Pool Movie"),
            "genre": overrides.pop("genre", "Drama"),
            "type": overrides.pop("type", Movie.MOVIE),
            "external_rating": overrides.pop("external_rating", 8.0),
            "external_votes": overrides.pop("external_votes", 5000),
            "release_year": overrides.pop("release_year", 2023),
        }
        data.update(overrides)
        return Movie.objects.create(**data)

    def test_rebuilds_same_day_pool_when_version_changes(self):
        service = DailyFeedPoolService(user=self.user, pool_size=5000)
        today = timezone.localdate()
        stale_pool = UserDailyFeedPool.objects.create(
            user=self.user,
            pool_date=today,
            pool_version="legacy-v1",
            expires_at=timezone.now() + timedelta(days=1),
            rotation_seed=1,
        )
        self._create_movie(title_english="Candidate For New Pool", genre="Drama")

        pool = service.get_daily_pool()

        self.assertNotEqual(pool.id, stale_pool.id)
        self.assertEqual(pool.pool_version, service._current_pool_version())
        self.assertEqual(UserDailyFeedPool.objects.filter(user=self.user, pool_date=today).count(), 1)

    def test_builds_strong_genre_candidates_for_sixth_preference(self):
        UserTasteProfile.objects.create(user=self.user, ratings_count=20)
        ranked_genres = ["Action", "Drama", "Comedy", "Horror", "Sci-Fi", "Documentary"]
        for index, genre in enumerate(ranked_genres):
            UserGenrePreference.objects.create(
                user=self.user,
                genre=genre,
                count_10=12 - index,
            )
            self._create_movie(
                title_english=f"{genre} Candidate",
                genre=genre,
                external_votes=7000 + index,
                release_year=2024,
            )

        service = DailyFeedPoolService(user=self.user, pool_size=5000)
        candidate_ids = service._build_candidate_ids(today=timezone.localdate())

        documentary_movie = Movie.objects.get(title_english="Documentary Candidate")
        self.assertIn(documentary_movie.id, candidate_ids)


class MovieRatingEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="movie_user", email="movie@example.com", password="test1234"
        )
        self.other_user = get_user_model().objects.create_user(
            username="other_user", email="other@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Interstellar",
            type=Movie.MOVIE,
            release_year=2014,
        )
        self.url = reverse("movie-rating", kwargs={"pk": self.movie.pk})

    def test_put_creates_and_updates_authenticated_user_rating(self):
        self.client.force_authenticate(user=self.user)

        create_response = self.client.put(self.url, {"score": 8}, format="json")
        self.assertEqual(create_response.status_code, status.HTTP_200_OK)
        self.assertEqual(create_response.data["created"], True)
        self.assertEqual(create_response.data["my_rating"], 8)

        rating = MovieRating.objects.get(user=self.user, movie=self.movie)
        self.assertEqual(rating.score, 8)

        update_response = self.client.put(self.url, {"score": 9}, format="json")
        self.assertEqual(update_response.status_code, status.HTTP_200_OK)
        self.assertEqual(update_response.data["created"], False)
        self.assertEqual(update_response.data["my_rating"], 9)

        rating.refresh_from_db()
        self.assertEqual(rating.score, 9)
        self.assertEqual(MovieRating.objects.filter(user=self.user, movie=self.movie).count(), 1)

        profile = UserTasteProfile.objects.get(user=self.user)
        self.assertEqual(profile.ratings_count, 1)

    def test_put_validates_score_range(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.put(self.url, {"score": 11}, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("score", response.data)
        self.assertFalse(MovieRating.objects.filter(user=self.user, movie=self.movie).exists())

    def test_delete_removes_only_authenticated_user_rating(self):
        self.client.force_authenticate(user=self.user)
        own_rating = MovieRating.objects.create(user=self.user, movie=self.movie, score=7)
        other_rating = MovieRating.objects.create(user=self.other_user, movie=self.movie, score=10)
        response = self.client.delete(self.url)

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(MovieRating.objects.filter(pk=own_rating.pk).exists())
        self.assertTrue(MovieRating.objects.filter(pk=other_rating.pk).exists())

        profile = UserTasteProfile.objects.get(user=self.user)
        self.assertEqual(profile.ratings_count, 0)

    def test_delete_without_existing_rating_returns_controlled_response(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.delete(self.url)

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(response.data["detail"], "Rating not found.")

    def test_rating_endpoint_requires_authentication(self):
        put_response = self.client.put(self.url, {"score": 8}, format="json")
        delete_response = self.client.delete(self.url)

        self.assertEqual(put_response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertEqual(delete_response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_put_rolls_back_rating_when_preference_update_fails(self):
        self.client.force_authenticate(user=self.user)

        with patch("core.signals.update_user_preferences_for_movie_rating", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                self.client.put(self.url, {"score": 8}, format="json")

        self.assertFalse(MovieRating.objects.filter(user=self.user, movie=self.movie).exists())

    def test_delete_rolls_back_rating_delete_when_preference_update_fails(self):
        self.client.force_authenticate(user=self.user)
        rating = MovieRating.objects.create(user=self.user, movie=self.movie, score=8)

        with patch("core.signals.remove_user_preferences_for_movie_rating", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                self.client.delete(self.url)

        self.assertTrue(MovieRating.objects.filter(pk=rating.pk).exists())


class ProfileFavoritesEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="favorites_user", email="favorites@example.com", password="test1234"
        )
        self.other_user = get_user_model().objects.create_user(
            username="favorites_other", email="favorites-other@example.com", password="test1234"
        )
        self.author = get_user_model().objects.create_user(
            username="favorites_author", email="favorites-author@example.com", password="test1234"
        )
        self.movie_1 = Movie.objects.create(author=self.author, title_english="Movie 1", type=Movie.MOVIE, release_year=2001)
        self.movie_2 = Movie.objects.create(author=self.author, title_english="Movie 2", type=Movie.MOVIE, release_year=2002)
        self.movie_3 = Movie.objects.create(author=self.author, title_english="Movie 3", type=Movie.SERIES, release_year=2003)
        self.list_url = reverse("profile-favorites")

    def test_get_returns_three_slots_even_when_empty(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.get(self.list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [
            {"slot": 1, "movie": None},
            {"slot": 2, "movie": None},
            {"slot": 3, "movie": None},
        ])

    def test_put_creates_favorite_for_empty_slot(self):
        self.client.force_authenticate(user=self.user)
        slot_url = reverse("profile-favorite-slot", kwargs={"slot": 1})

        response = self.client.put(slot_url, {"movie_id": self.movie_1.id}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["slot"], 1)
        self.assertEqual(response.data["movie"]["id"], self.movie_1.id)
        favorite = ProfileFavoriteMovie.objects.get(user=self.user, slot=1)
        self.assertEqual(favorite.movie_id, self.movie_1.id)

    def test_put_replaces_existing_favorite_for_slot(self):
        ProfileFavoriteMovie.objects.create(user=self.user, slot=1, movie=self.movie_1)
        self.client.force_authenticate(user=self.user)
        slot_url = reverse("profile-favorite-slot", kwargs={"slot": 1})

        response = self.client.put(slot_url, {"movie_id": self.movie_2.id}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(ProfileFavoriteMovie.objects.filter(user=self.user, slot=1).count(), 1)
        favorite = ProfileFavoriteMovie.objects.get(user=self.user, slot=1)
        self.assertEqual(favorite.movie_id, self.movie_2.id)

    def test_put_rejects_repeated_movie_in_another_slot(self):
        ProfileFavoriteMovie.objects.create(user=self.user, slot=1, movie=self.movie_1)
        self.client.force_authenticate(user=self.user)
        slot_url = reverse("profile-favorite-slot", kwargs={"slot": 2})

        response = self.client.put(slot_url, {"movie_id": self.movie_1.id}, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.data["movie_id"][0],
            "This movie is already assigned to another slot.",
        )
        self.assertFalse(ProfileFavoriteMovie.objects.filter(user=self.user, slot=2).exists())

    def test_delete_clears_slot(self):
        ProfileFavoriteMovie.objects.create(user=self.user, slot=2, movie=self.movie_2)
        self.client.force_authenticate(user=self.user)
        slot_url = reverse("profile-favorite-slot", kwargs={"slot": 2})

        response = self.client.delete(slot_url)

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(ProfileFavoriteMovie.objects.filter(user=self.user, slot=2).exists())

    def test_get_returns_only_authenticated_user_favorites(self):
        ProfileFavoriteMovie.objects.create(user=self.user, slot=1, movie=self.movie_1)
        ProfileFavoriteMovie.objects.create(user=self.other_user, slot=1, movie=self.movie_3)
        self.client.force_authenticate(user=self.user)

        response = self.client.get(self.list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["slot"], 1)
        self.assertEqual(response.data[0]["movie"]["id"], self.movie_1.id)
        self.assertIsNone(response.data[1]["movie"])
        self.assertIsNone(response.data[2]["movie"])


class MovieRatingSignalConsistencyTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="signal_user", email="signal@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Signal Movie",
            type=Movie.MOVIE,
            genre="Action, Sci-Fi",
            director="Nolan",
            release_year=2014,
        )

    def test_create_update_delete_movie_rating_keeps_profile_consistent(self):
        rating = MovieRating.objects.create(user=self.user, movie=self.movie, score=8)

        profile = UserTasteProfile.objects.get(user=self.user)
        self.assertEqual(profile.ratings_count, 1)
        self.assertTrue(UserGenrePreference.objects.filter(user=self.user, genre="Action|Sci-Fi").exists())
        self.assertTrue(UserTypePreference.objects.filter(user=self.user, content_type=Movie.MOVIE).exists())
        self.assertTrue(UserDirectorPreference.objects.filter(user=self.user, director="Nolan").exists())

        rating.score = 10
        rating.save()
        combo_pref = UserGenrePreference.objects.get(user=self.user, genre="Action|Sci-Fi")
        self.assertEqual(combo_pref.count_8, 0)
        self.assertEqual(combo_pref.count_10, 1)

        rating.delete()
        profile.refresh_from_db()
        self.assertEqual(profile.ratings_count, 0)
        self.assertFalse(UserGenrePreference.objects.filter(user=self.user).exists())
        self.assertFalse(UserTypePreference.objects.filter(user=self.user).exists())
        self.assertFalse(UserDirectorPreference.objects.filter(user=self.user).exists())


class RebuildTasteProfilesCommandTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="rebuild_user", email="rebuild@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Rebuild Movie",
            type=Movie.MOVIE,
            genre="Drama",
            director="Villeneuve",
            release_year=2021,
        )

    def test_command_rebuilds_existing_profile_from_movie_ratings(self):
        MovieRating.objects.create(user=self.user, movie=self.movie, score=9)

        UserTasteProfile.objects.update_or_create(user=self.user, defaults={"ratings_count": 0})
        UserGenrePreference.objects.filter(user=self.user).delete()
        UserTypePreference.objects.filter(user=self.user).delete()
        UserDirectorPreference.objects.filter(user=self.user).delete()

        out = io.StringIO()
        call_command("rebuild_taste_profiles", "--user-id", str(self.user.id), stdout=out)

        profile = UserTasteProfile.objects.get(user=self.user)
        self.assertEqual(profile.ratings_count, 1)
        self.assertTrue(UserGenrePreference.objects.filter(user=self.user, genre="Drama").exists())
        self.assertTrue(UserTypePreference.objects.filter(user=self.user, content_type=Movie.MOVIE).exists())
        self.assertTrue(UserDirectorPreference.objects.filter(user=self.user, director="Villeneuve").exists())
        self.assertIn(f"user_id={self.user.id}", out.getvalue())


class CommentModelAndAdminTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="comment_model_user", email="comment-model@example.com", password="test1234"
        )
        self.target_user = get_user_model().objects.create_user(
            username="comment_target_user", email="comment-target@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Interstellar",
            type=Movie.MOVIE,
            release_year=2014,
        )

    def test_model_defaults_visibility_to_public_when_omitted(self):
        comment = Comment.objects.create(author=self.user, movie=self.movie, body="Comentario público")

        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)

    def test_serializer_defaults_visibility_to_public_when_omitted(self):
        serializer = CommentSerializer(data={"body": "Serializer comment"})

        self.assertTrue(serializer.is_valid(), serializer.errors)
        comment = serializer.save(author=self.user, movie=self.movie)

        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)

    def test_admin_can_create_public_comment_without_target_user(self):
        admin_user = get_user_model().objects.create_superuser(
            username="comment_admin",
            email="comment-admin@example.com",
            password="test1234",
        )
        self.client.force_login(admin_user)

        response = self.client.post(
            reverse("admin:core_comment_add"),
            {
                "author": str(self.user.pk),
                "movie": str(self.movie.pk),
                "body": "Creado desde admin",
                "visibility": Comment.VISIBILITY_PUBLIC,
                "_save": "Save",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        comment = Comment.objects.get(body="Creado desde admin")
        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)


class MovieCommentEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="comment_user", email="comment@example.com", password="test1234"
        )
        self.friend_user = get_user_model().objects.create_user(
            username="comment_friend", email="comment-friend@example.com", password="test1234"
        )
        self.other_user = get_user_model().objects.create_user(
            username="comment_other", email="comment-other@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Arrival",
            type=Movie.MOVIE,
            release_year=2016,
        )
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.friend_user,
            status=Friendship.STATUS_ACCEPTED,
        )
        self.list_url = reverse("movie-comments", kwargs={"pk": self.movie.pk})
        self.movie_detail_url = reverse("movie-detail", kwargs={"pk": self.movie.pk})
        self.movie_directed_url = reverse("movie-directed-comments", kwargs={"pk": self.movie.pk})
        self.directed_url = reverse("directed-comments")
        self.received_url = reverse("directed-comments-received")
        self.sent_url = reverse("directed-comments-sent")
        self.me_messages_url = reverse("me-messages")
        self.me_messages_summary_url = reverse("me-messages-summary")
        self.me_messages_mark_as_read_url = reverse("me-messages-mark-as-read")

    def test_get_movie_detail_returns_requested_movie(self):
        response = self.client.get(self.movie_detail_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], self.movie.pk)
        self.assertEqual(response.data["title_english"], "Arrival")
        self.assertEqual(response.data["comments_count"], 0)

    def test_post_creates_public_comment_for_movie_without_valid_friend_mention(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(self.list_url, {"body": "Gran película @comment_other"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Comment.objects.count(), 1)
        comment = Comment.objects.get()
        self.assertEqual(comment.movie, self.movie)
        self.assertEqual(comment.author, self.user)
        self.assertEqual(comment.body, "Gran película @comment_other")
        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)
        self.assertEqual(response.data["movie"], self.movie.pk)
        self.assertEqual(response.data["visibility"], Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(response.data["target_user"])

    def test_post_creates_public_comment_without_mention_fields(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.list_url,
            {"body": "Gran película"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Comment.objects.count(), 1)
        comment = Comment.objects.get()
        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)

    def test_post_creates_public_comment_when_mention_aliases_are_blank(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.list_url,
            {"body": "Comentario público", "mentioned_username": "", "recipient_username": ""},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Comment.objects.count(), 1)
        comment = Comment.objects.get()
        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)

    def test_post_with_accepted_friend_mention_creates_directed_comment(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(self.list_url, {"body": "Tienes que verla @comment_friend"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        comment = Comment.objects.get()
        self.assertEqual(comment.visibility, Comment.VISIBILITY_MENTIONED)
        self.assertEqual(comment.target_user, self.friend_user)
        self.assertFalse(comment.is_read)
        self.assertEqual(response.data["visibility"], Comment.VISIBILITY_MENTIONED)
        self.assertEqual(response.data["target_user"], self.friend_user.pk)

    def test_post_with_explicit_mentioned_username_creates_directed_comment(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.list_url,
            {"body": "Recomendación privada", "mentioned_username": self.friend_user.username},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        comment = Comment.objects.get()
        self.assertEqual(comment.visibility, Comment.VISIBILITY_MENTIONED)
        self.assertEqual(comment.target_user, self.friend_user)

    def test_post_with_explicit_recipient_username_creates_directed_comment(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.list_url,
            {"body": "Te la recomiendo", "recipient_username": self.friend_user.username},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        comment = Comment.objects.get()
        self.assertEqual(comment.visibility, Comment.VISIBILITY_MENTIONED)
        self.assertEqual(comment.target_user, self.friend_user)

    def test_post_with_non_friend_mentioned_username_is_rejected(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.list_url,
            {"body": "No debería publicarse", "mentioned_username": self.other_user.username},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(Comment.objects.count(), 0)

    def test_post_with_invalid_mentioned_username_is_rejected(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.list_url,
            {"body": "No debería publicarse", "mentioned_username": "usuario_inexistente"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(Comment.objects.count(), 0)

    def test_get_lists_only_public_comments_for_requested_movie(self):
        other_movie = Movie.objects.create(
            author=self.other_user,
            title_english="Blade Runner 2049",
            type=Movie.MOVIE,
            release_year=2017,
        )
        public_comment = Comment.objects.create(author=self.user, movie=self.movie, body="Comentario público")
        directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Solo para mi amigo",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        Comment.objects.create(author=self.other_user, movie=other_movie, body="Comentario de otra movie")

        response = self.client.get(self.list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in response.data], [public_comment.id])
        self.assertNotIn(directed_comment.id, [item["id"] for item in response.data])
        self.assertTrue(all(item["movie"] == self.movie.pk for item in response.data))

    def test_directed_comment_detail_is_visible_only_to_author_and_target_user(self):
        directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Mírala cuando puedas @comment_friend",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        detail_url = reverse("comment-detail", kwargs={"pk": directed_comment.pk})

        self.client.force_authenticate(user=self.user)
        author_response = self.client.get(detail_url)
        self.assertEqual(author_response.status_code, status.HTTP_200_OK)

        self.client.force_authenticate(user=self.friend_user)
        target_response = self.client.get(detail_url)
        self.assertEqual(target_response.status_code, status.HTTP_200_OK)

        self.client.force_authenticate(user=self.other_user)
        other_response = self.client.get(detail_url)
        self.assertEqual(other_response.status_code, status.HTTP_404_NOT_FOUND)

        self.client.force_authenticate(user=None)
        anonymous_response = self.client.get(detail_url)
        self.assertEqual(anonymous_response.status_code, status.HTTP_404_NOT_FOUND)

    def test_directed_comment_lists_show_received_and_sent_only_for_authenticated_user(self):
        directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Recomendación privada @comment_friend",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        Comment.objects.create(author=self.other_user, movie=self.movie, body="Comentario público")

        self.client.force_authenticate(user=self.friend_user)
        received_response = self.client.get(self.received_url)
        self.assertEqual(received_response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in received_response.data], [directed_comment.id])

        self.client.force_authenticate(user=self.user)
        sent_response = self.client.get(self.sent_url)
        self.assertEqual(sent_response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in sent_response.data], [directed_comment.id])

        self.client.force_authenticate(user=self.other_user)
        other_received_response = self.client.get(self.received_url)
        other_sent_response = self.client.get(self.sent_url)
        self.assertEqual(other_received_response.status_code, status.HTTP_200_OK)
        self.assertEqual(other_sent_response.status_code, status.HTTP_200_OK)
        self.assertEqual(other_received_response.data, [])
        self.assertEqual(other_sent_response.data, [])

    def test_directed_comments_fallback_endpoint_returns_participant_messages(self):
        directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Recomendación privada @comment_friend",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        Comment.objects.create(
            author=self.other_user,
            movie=self.movie,
            body="No visible para comment_user",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )

        self.client.force_authenticate(user=self.friend_user)
        response = self.client.get(self.directed_url)
        payload = response.data["results"] if isinstance(response.data, dict) else response.data

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], directed_comment.id)

    def test_received_and_fallback_directed_endpoints_support_movie_id_filter(self):
        other_movie = Movie.objects.create(
            author=self.user,
            title_english="Her",
            type=Movie.MOVIE,
            release_year=2013,
        )
        in_scope = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Para comment_friend @comment_friend",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        Comment.objects.create(
            author=self.user,
            movie=other_movie,
            body="Para comment_friend en otra movie @comment_friend",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )

        self.client.force_authenticate(user=self.friend_user)
        received_response = self.client.get(self.received_url, {"movie_id": self.movie.id})
        fallback_response = self.client.get(self.directed_url, {"movie_id": self.movie.id})
        received_payload = received_response.data["results"] if isinstance(received_response.data, dict) else received_response.data
        fallback_payload = fallback_response.data["results"] if isinstance(fallback_response.data, dict) else fallback_response.data

        self.assertEqual(received_response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in received_payload], [in_scope.id])
        self.assertEqual(fallback_response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in fallback_payload], [in_scope.id])

    def test_me_messages_lists_only_valid_received_directed_comments(self):
        valid_directed = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Te la recomiendo @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body="Sin mención válida",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Para @{self.other_user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.other_user,
        )
        Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body=f"Auto mensaje @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Comentario público @{self.user.username}",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.me_messages_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.data["results"] if isinstance(response.data, dict) else response.data
        self.assertEqual([item["id"] for item in payload], [valid_directed.id])
        self.assertFalse(payload[0]["is_read"])

    def test_me_messages_summary_counts_only_valid_received_directed_comments(self):
        unread_comment = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Valido @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        read_comment = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Leído @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
            is_read=True,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body="Inconsistente",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.me_messages_summary_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_messages"], 2)
        self.assertEqual(response.data["unread_count"], 1)
        self.assertTrue(response.data["has_unread_messages"])
        unread_comment.refresh_from_db()
        read_comment.refresh_from_db()
        self.assertFalse(unread_comment.is_read)
        self.assertTrue(read_comment.is_read)

    def test_me_messages_mark_as_read_updates_only_valid_unread_received_messages(self):
        unread_valid = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Sin leer @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
            is_read=False,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body="Inconsistente",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
            is_read=False,
        )
        already_read = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Leído @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
            is_read=True,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.post(self.me_messages_mark_as_read_url, {}, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 1)

        unread_valid.refresh_from_db()
        already_read.refresh_from_db()
        self.assertTrue(unread_valid.is_read)
        self.assertTrue(already_read.is_read)

    def test_directed_comments_by_movie_requires_auth_and_filters_to_participant(self):
        other_movie = Movie.objects.create(
            author=self.other_user,
            title_english="Interestellar",
            type=Movie.MOVIE,
            release_year=2014,
        )
        directed_for_user = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body="Mírala hoy @comment_user",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        Comment.objects.create(
            author=self.other_user,
            movie=self.movie,
            body="No visible para comment_user",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=other_movie,
            body="Otra película @comment_user",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )

        anonymous_response = self.client.get(self.movie_directed_url)
        self.assertEqual(anonymous_response.status_code, status.HTTP_401_UNAUTHORIZED)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.movie_directed_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["other_user"]["username"], self.friend_user.username)
        preview = response.data["results"][0]["messages_preview"]
        self.assertEqual([item["id"] for item in preview], [directed_for_user.id])
        self.assertEqual(preview[0]["direction"], "received")
        self.assertIn(
            reverse(
                "movie-directed-conversation-messages",
                kwargs={"pk": self.movie.pk, "username": self.friend_user.username},
            ),
            response.data["results"][0]["messages_endpoint"],
        )

    def test_post_directed_comment_by_movie_endpoint_requires_valid_friend_mention(self):
        self.client.force_authenticate(user=self.user)

        valid_response = self.client.post(
            self.movie_directed_url,
            {"body": "Solo para mi amigo", "mentioned_username": self.friend_user.username},
            format="json",
        )
        self.assertEqual(valid_response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Comment.objects.count(), 1)
        created = Comment.objects.get()
        self.assertEqual(created.visibility, Comment.VISIBILITY_MENTIONED)
        self.assertEqual(created.target_user, self.friend_user)

        invalid_response = self.client.post(
            self.movie_directed_url,
            {"body": "No válido", "mentioned_username": self.other_user.username},
            format="json",
        )
        self.assertEqual(invalid_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(Comment.objects.count(), 1)

    def test_post_directed_comment_by_movie_endpoint_allows_friend_without_prior_conversation(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.movie_directed_url,
            {"body": "Primera vez que te escribo", "mentioned_username": self.friend_user.username},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        created = Comment.objects.get()
        self.assertEqual(created.author, self.user)
        self.assertEqual(created.target_user, self.friend_user)
        self.assertEqual(created.visibility, Comment.VISIBILITY_MENTIONED)

    def test_post_directed_comment_by_movie_endpoint_allows_friend_with_prior_conversation(self):
        Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"Mensaje previo @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        self.client.force_authenticate(user=self.user)

        response = self.client.post(
            self.movie_directed_url,
            {"body": "Respuesta dirigida", "mentioned_username": self.friend_user.username},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Comment.objects.count(), 2)
        latest = Comment.objects.order_by("-id").first()
        self.assertEqual(latest.author, self.user)
        self.assertEqual(latest.target_user, self.friend_user)

    def test_directed_conversations_group_messages_when_other_user_starts(self):
        first = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"hola @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        second = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body=f"respuesta @{self.friend_user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.movie_directed_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        conversation = response.data["results"][0]
        self.assertEqual(conversation["other_user"]["username"], self.friend_user.username)
        self.assertEqual(conversation["messages_preview"][0]["id"], second.id)
        self.assertEqual(conversation["messages_preview"][0]["direction"], "sent")
        self.assertNotEqual(first.id, second.id)

    def test_directed_conversations_order_and_separation_by_movie_and_interlocutor(self):
        newer_other = get_user_model().objects.create_user(
            username="comment_friend_2",
            email="comment-friend-2@example.com",
            password="test1234",
        )
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=newer_other,
            status=Friendship.STATUS_ACCEPTED,
        )
        other_movie = Movie.objects.create(
            author=self.user,
            title_english="Dune",
            type=Movie.MOVIE,
            release_year=2021,
        )

        old_comment = Comment.objects.create(
            author=self.friend_user,
            movie=self.movie,
            body=f"viejo @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        new_comment = Comment.objects.create(
            author=newer_other,
            movie=self.movie,
            body=f"nuevo @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )
        Comment.objects.create(
            author=self.friend_user,
            movie=other_movie,
            body=f"otra peli @{self.user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.user,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.movie_directed_url)
        usernames = [item["other_user"]["username"] for item in response.data["results"]]

        self.assertEqual(usernames, [newer_other.username, self.friend_user.username])
        self.assertEqual(response.data["results"][0]["messages_preview"][0]["id"], new_comment.id)
        self.assertEqual(response.data["results"][1]["messages_preview"][0]["id"], old_comment.id)

    def test_directed_conversations_preview_exposes_recipient_and_counterpart_for_sent_messages(self):
        second_friend = get_user_model().objects.create_user(
            username="comment_friend_3",
            email="comment-friend-3@example.com",
            password="test1234",
        )
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=second_friend,
            status=Friendship.STATUS_ACCEPTED,
        )

        Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body=f"Para @{self.friend_user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body=f"Para @{second_friend.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=second_friend,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.movie_directed_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        conversations = sorted(response.data["results"], key=lambda item: item["other_user"]["username"])
        self.assertEqual(len(conversations), 2)

        self.assertEqual(conversations[0]["direction"], "sent")
        self.assertEqual(conversations[1]["direction"], "sent")
        self.assertEqual(conversations[0]["counterpart"]["username"], conversations[0]["other_user"]["username"])
        self.assertEqual(conversations[1]["counterpart"]["username"], conversations[1]["other_user"]["username"])
        self.assertEqual(conversations[0]["recipient"]["username"], conversations[0]["other_user"]["username"])
        self.assertEqual(conversations[1]["recipient"]["username"], conversations[1]["other_user"]["username"])

        first_preview = conversations[0]["messages_preview"][0]
        second_preview = conversations[1]["messages_preview"][0]

        self.assertEqual(first_preview["direction"], "sent")
        self.assertEqual(second_preview["direction"], "sent")
        self.assertEqual(first_preview["counterpart"]["username"], conversations[0]["other_user"]["username"])
        self.assertEqual(second_preview["counterpart"]["username"], conversations[1]["other_user"]["username"])
        self.assertNotEqual(first_preview["counterpart"]["username"], second_preview["counterpart"]["username"])
        self.assertEqual(first_preview["recipient"]["username"], conversations[0]["other_user"]["username"])
        self.assertEqual(second_preview["recipient"]["username"], conversations[1]["other_user"]["username"])

    def test_directed_conversation_messages_endpoint_paginates_and_marks_directions(self):
        for index in range(12):
            author = self.user if index % 2 == 0 else self.friend_user
            target = self.friend_user if author == self.user else self.user
            mention = self.friend_user.username if author == self.user else self.user.username
            Comment.objects.create(
                author=author,
                movie=self.movie,
                body=f"mensaje {index} @{mention}",
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user=target,
            )

        self.client.force_authenticate(user=self.user)
        url = reverse(
            "movie-directed-conversation-messages",
            kwargs={"pk": self.movie.pk, "username": self.friend_user.username},
        )
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 12)
        self.assertEqual(len(response.data["results"]), 10)
        self.assertEqual(response.data["results"][0]["direction"], "received")
        self.assertIn(response.data["results"][1]["direction"], {"sent", "received"})

        page_2 = self.client.get(url, {"page": 2})
        self.assertEqual(page_2.status_code, status.HTTP_200_OK)
        self.assertEqual(len(page_2.data["results"]), 2)

    def test_public_comments_pagination_is_stable_and_contains_author_navigation_fields(self):
        for index in range(12):
            Comment.objects.create(
                author=self.user,
                movie=self.movie,
                body=f"publico {index}",
                visibility=Comment.VISIBILITY_PUBLIC,
            )

        response = self.client.get(self.list_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 12)
        self.assertEqual(len(response.data["results"]), 10)
        first = response.data["results"][0]
        self.assertEqual(first["author_username"], self.user.username)
        self.assertTrue(first["author_display_name"])
        self.assertIn("author_avatar", first)

    def test_comment_requires_movie_relation(self):
        with self.assertRaises(IntegrityError):
            Comment.objects.create(author=self.user, body="Inválido")

    def test_only_author_can_update_or_delete_comment(self):
        comment = Comment.objects.create(author=self.user, movie=self.movie, body="Original")
        detail_url = reverse("comment-detail", kwargs={"pk": comment.pk})

        self.client.force_authenticate(user=self.other_user)
        forbidden_update = self.client.put(detail_url, {"body": "Hack"}, format="json")
        forbidden_delete = self.client.delete(detail_url)

        self.assertEqual(forbidden_update.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(forbidden_delete.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(user=self.user)
        allowed_update = self.client.put(detail_url, {"body": "Editado"}, format="json")
        self.assertEqual(allowed_update.status_code, status.HTTP_200_OK)

        comment.refresh_from_db()
        self.assertEqual(comment.body, "Editado")

        allowed_delete = self.client.delete(detail_url)
        self.assertEqual(allowed_delete.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Comment.objects.filter(pk=comment.pk).exists())

    def test_author_can_patch_own_public_comment_text_without_changing_visibility(self):
        comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Texto original",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        detail_url = reverse("comment-detail", kwargs={"pk": comment.pk})

        self.client.force_authenticate(user=self.user)
        response = self.client.patch(detail_url, {"body": "Texto actualizado"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        comment.refresh_from_db()
        self.assertEqual(comment.body, "Texto actualizado")
        self.assertEqual(comment.visibility, Comment.VISIBILITY_PUBLIC)
        self.assertIsNone(comment.target_user)

    def test_author_can_edit_and_delete_own_directed_comment(self):
        directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body=f"Mensaje inicial @{self.friend_user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        detail_url = reverse("comment-detail", kwargs={"pk": directed_comment.pk})

        self.client.force_authenticate(user=self.user)
        edit_response = self.client.patch(detail_url, {"body": f"Mensaje editado @{self.friend_user.username}"}, format="json")
        self.assertEqual(edit_response.status_code, status.HTTP_200_OK)

        directed_comment.refresh_from_db()
        self.assertEqual(directed_comment.body, f"Mensaje editado @{self.friend_user.username}")
        self.assertEqual(directed_comment.visibility, Comment.VISIBILITY_MENTIONED)
        self.assertEqual(directed_comment.target_user_id, self.friend_user.id)

        delete_response = self.client.delete(detail_url)
        self.assertEqual(delete_response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertFalse(Comment.objects.filter(pk=directed_comment.pk).exists())

    def test_non_author_cannot_edit_or_delete_directed_comment(self):
        directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body=f"Privado @{self.friend_user.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend_user,
        )
        detail_url = reverse("comment-detail", kwargs={"pk": directed_comment.pk})

        self.client.force_authenticate(user=self.friend_user)
        forbidden_edit = self.client.patch(detail_url, {"body": "Intento de edición"}, format="json")
        forbidden_delete = self.client.delete(detail_url)

        self.assertEqual(forbidden_edit.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(forbidden_delete.status_code, status.HTTP_403_FORBIDDEN)


class MoviePreferenceServiceTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="taste_user", email="taste@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="The Matrix",
            type=Movie.MOVIE,
            genre=" Sci-Fi, Action, Sci-Fi , ,Action ",
            director=" Lana Wachowski ",
            release_year=1999,
        )

    def test_update_preferences_creates_and_updates_preference_buckets_incrementally(self):
        update_user_preferences_for_movie_rating(user=self.user, movie=self.movie, new_score=8)

        combo_pref = UserGenrePreference.objects.get(user=self.user, genre="Action|Sci-Fi")
        sci_fi = UserGenrePreference.objects.get(user=self.user, genre="Sci-Fi")
        action = UserGenrePreference.objects.get(user=self.user, genre="Action")
        type_pref = UserTypePreference.objects.get(user=self.user, content_type=Movie.MOVIE)
        director_pref = UserDirectorPreference.objects.get(user=self.user, director="Lana Wachowski")

        self.assertEqual(combo_pref.count_8, 1)
        self.assertEqual(sci_fi.count_8, 1)
        self.assertEqual(action.count_8, 1)
        self.assertEqual(type_pref.count_8, 1)
        self.assertEqual(director_pref.count_8, 1)
        self.assertEqual(float(combo_pref.score), 8.0)
        self.assertEqual(float(sci_fi.score), 8.0)

        update_user_preferences_for_movie_rating(user=self.user, movie=self.movie, new_score=10, old_score=8)
        sci_fi.refresh_from_db()
        type_pref.refresh_from_db()

        self.assertEqual(sci_fi.count_8, 0)
        self.assertEqual(sci_fi.count_10, 1)
        self.assertEqual(float(sci_fi.score), 10.0)
        self.assertEqual(type_pref.count_8, 0)
        self.assertEqual(type_pref.count_10, 1)

        profile = UserTasteProfile.objects.get(user=self.user)
        self.assertEqual(profile.ratings_count, 1)

    def test_remove_preferences_deletes_empty_preference_rows(self):
        update_user_preferences_for_movie_rating(user=self.user, movie=self.movie, new_score=6)

        remove_user_preferences_for_movie_rating(user=self.user, movie=self.movie, old_score=6)

        self.assertFalse(UserGenrePreference.objects.filter(user=self.user).exists())
        self.assertFalse(UserTypePreference.objects.filter(user=self.user).exists())
        self.assertFalse(UserDirectorPreference.objects.filter(user=self.user).exists())

        profile = UserTasteProfile.objects.get(user=self.user)
        self.assertEqual(profile.ratings_count, 0)


class MeTasteProfileEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="profile_user", email="profile@example.com", password="test1234"
        )
        self.url = reverse("me-taste-profile")

    def test_requires_authentication(self):
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_returns_profile_and_sorted_preferences(self):
        self.client.force_authenticate(user=self.user)

        UserTasteProfile.objects.create(user=self.user, ratings_count=4)
        UserGenrePreference.objects.create(user=self.user, genre="Comedy", count_9=2)
        UserGenrePreference.objects.create(user=self.user, genre="Action", count_10=1)
        UserTypePreference.objects.create(user=self.user, content_type=Movie.SERIES, count_8=1)
        UserTypePreference.objects.create(user=self.user, content_type=Movie.MOVIE, count_9=3)
        UserDirectorPreference.objects.create(user=self.user, director="Villeneuve", count_8=1)
        UserDirectorPreference.objects.create(user=self.user, director="Nolan", count_10=2)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ratings_count"], 4)
        self.assertIn("last_updated_at", response.data)

        self.assertEqual([item["name"] for item in response.data["genre_preferences"]], ["Action", "Comedy"])
        self.assertEqual([item["name"] for item in response.data["type_preferences"]], [Movie.MOVIE, Movie.SERIES])
        self.assertEqual([item["name"] for item in response.data["director_preferences"]], ["Nolan", "Villeneuve"])

        genre_item = response.data["genre_preferences"][0]
        self.assertEqual(genre_item["score"], "10.00")
        self.assertEqual(genre_item["ratings_count"], 1)
        self.assertEqual(genre_item["count_10"], 1)
        self.assertEqual(genre_item["count_1"], 0)

    def test_creates_empty_profile_when_not_exists(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["ratings_count"], 0)
        self.assertEqual(response.data["genre_preferences"], [])
        self.assertEqual(response.data["type_preferences"], [])
        self.assertEqual(response.data["director_preferences"], [])
        self.assertTrue(UserTasteProfile.objects.filter(user=self.user).exists())


class SocialPrivacyAndFriendshipTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="social_user", email="social@example.com", password="test1234"
        )
        self.public_user = get_user_model().objects.create_user(
            username="public_user", email="public@example.com", password="test1234"
        )
        self.private_user = get_user_model().objects.create_user(
            username="private_user", email="private@example.com", password="test1234"
        )
        self.private_user.profile.is_public = False
        self.private_user.profile.visibility = Profile.Visibility.PRIVATE
        self.private_user.profile.save(update_fields=["is_public", "visibility"])

    def test_private_user_can_follow_public_profile(self):
        self.private_user.profile.visibility = Profile.Visibility.PRIVATE
        self.private_user.profile.is_public = False
        self.private_user.profile.save(update_fields=["visibility", "is_public"])
        self.client.force_authenticate(user=self.private_user)

        response = self.client.post(reverse("follow-toggle", kwargs={"username": self.public_user.username}))

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertTrue(Follow.objects.filter(follower=self.private_user, following=self.public_user).exists())

    def test_cannot_follow_private_profiles(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(reverse("follow-toggle", kwargs={"username": self.private_user.username}))

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(response.data["detail"], "You cannot follow a private profile.")
        self.assertFalse(Follow.objects.filter(follower=self.user, following=self.private_user).exists())

    def test_can_send_friendship_request(self):
        self.client.force_authenticate(user=self.user)

        response = self.client.post(reverse("friendship-request-create", kwargs={"username": self.private_user.username}))

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        friendship = Friendship.objects.get()
        self.assertEqual(friendship.requester, self.user)
        self.assertEqual(friendship.status, Friendship.STATUS_PENDING)

    def test_can_accept_friendship_request(self):
        friendship = Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.private_user,
            status=Friendship.STATUS_PENDING,
        )
        self.client.force_authenticate(user=self.private_user)

        response = self.client.post(reverse("friendship-request-accept", kwargs={"pk": friendship.pk}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        friendship.refresh_from_db()
        self.assertEqual(friendship.status, Friendship.STATUS_ACCEPTED)

    def test_avoids_duplicate_friendship_requests(self):
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.public_user,
            status=Friendship.STATUS_PENDING,
        )
        self.client.force_authenticate(user=self.user)

        response = self.client.post(reverse("friendship-request-create", kwargs={"username": self.public_user.username}))

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(Friendship.objects.count(), 1)

    def test_prevents_self_follow_and_self_friendship(self):
        self.client.force_authenticate(user=self.user)

        follow_response = self.client.post(reverse("follow-toggle", kwargs={"username": self.user.username}))
        friendship_response = self.client.post(reverse("friendship-request-create", kwargs={"username": self.user.username}))

        self.assertEqual(follow_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(friendship_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertFalse(Follow.objects.filter(follower=self.user, following=self.user).exists())
        self.assertFalse(Friendship.objects.filter(requester=self.user, user1=self.user, user2=self.user).exists())

    def test_profile_endpoint_exposes_social_status_fields(self):
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.private_user,
            status=Friendship.STATUS_PENDING,
        )
        self.client.force_authenticate(user=self.user)

        response = self.client.get(reverse("user-profile", kwargs={"username": self.private_user.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["is_public"])
        self.assertEqual(response.data["friendship_status"], "pending_sent")
        self.assertFalse(response.data["can_follow"])
        self.assertFalse(response.data["can_send_friend_request"])


class MeFollowingEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="following_owner", email="following-owner@example.com", password="test1234"
        )
        self.followed_user = get_user_model().objects.create_user(
            username="followed_user", email="followed@example.com", password="test1234"
        )
        self.follower_a = get_user_model().objects.create_user(
            username="follower_a", email="follower-a@example.com", password="test1234"
        )
        self.follower_b = get_user_model().objects.create_user(
            username="follower_b", email="follower-b@example.com", password="test1234"
        )
    def test_me_following_returns_users_followed_by_authenticated_user_with_followers_count(self):
        Follow.objects.create(follower=self.user, following=self.followed_user)
        Follow.objects.create(follower=self.follower_a, following=self.followed_user)
        Follow.objects.create(follower=self.follower_b, following=self.followed_user)
        Follow.objects.create(follower=self.followed_user, following=self.user)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse("me-following"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(len(response.data["results"]), 1)
        item = response.data["results"][0]
        self.assertEqual(item["username"], "followed_user")
        self.assertEqual(item["followers_count"], 3)
        self.assertEqual(set(item.keys()), {"id", "username", "display_name", "avatar_url", "followers_count"})

    def test_users_me_following_alias_resolves_authenticated_user_instead_of_literal_username(self):
        Follow.objects.create(follower=self.user, following=self.followed_user)
        self.client.force_authenticate(user=self.user)

        response = self.client.get(reverse("user-following", kwargs={"username": "me"}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["username"], "followed_user")
        self.assertIn("followers_count", response.data["results"][0])

    def test_users_username_following_includes_followers_count(self):
        Follow.objects.create(follower=self.user, following=self.followed_user)
        Follow.objects.create(follower=self.follower_a, following=self.followed_user)
        self.client.force_authenticate(user=self.user)

        response = self.client.get(reverse("user-following", kwargs={"username": self.user.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"][0]["username"], "followed_user")
        self.assertEqual(response.data["results"][0]["followers_count"], 2)
        self.assertEqual(
            set(response.data["results"][0].keys()),
            {"id", "username", "bio", "avatar", "followers_count"},
        )

    def test_users_username_following_counts_followers_from_global_follow_table(self):
        julian = get_user_model().objects.create_user(
            username="Julian", email="julian@example.com", password="test1234"
        )
        dennisse = get_user_model().objects.create_user(
            username="Dennisse", email="dennisse@example.com", password="test1234"
        )
        peck = get_user_model().objects.create_user(
            username="Peck", email="peck@example.com", password="test1234"
        )
        gato_dorado = get_user_model().objects.create_user(
            username="GatoDorado", email="gato-dorado@example.com", password="test1234"
        )
        julian_hernadez = get_user_model().objects.create_user(
            username="JulianHernadez", email="julian-hernadez@example.com", password="test1234"
        )

        Follow.objects.create(follower=peck, following=julian)
        Follow.objects.create(follower=julian, following=dennisse)
        Follow.objects.create(follower=julian_hernadez, following=dennisse)
        Follow.objects.create(follower=gato_dorado, following=dennisse)

        response = self.client.get(reverse("user-following", kwargs={"username": julian.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0]["username"], "Dennisse")
        self.assertEqual(response.data["results"][0]["followers_count"], 3)

    def test_me_following_uses_global_follow_table_for_each_listed_user_followers_count(self):
        dennisse = get_user_model().objects.create_user(
            username="Dennisse", email="dennisse@example.com", password="test1234"
        )
        peck = get_user_model().objects.create_user(
            username="Peck", email="peck@example.com", password="test1234"
        )
        dennisse_jamaica = get_user_model().objects.create_user(
            username="DennisseJamaica", email="dennisse-jamaica@example.com", password="test1234"
        )
        julian_hernadez = get_user_model().objects.create_user(
            username="JulianHernadez", email="julian-hernadez@example.com", password="test1234"
        )

        Follow.objects.create(follower=self.user, following=dennisse)
        Follow.objects.create(follower=self.user, following=peck)
        Follow.objects.create(follower=self.user, following=dennisse_jamaica)
        Follow.objects.create(follower=julian_hernadez, following=dennisse)
        Follow.objects.create(follower=dennisse, following=peck)
        Follow.objects.create(follower=peck, following=self.user)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(reverse("me-following"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        counts_by_username = {item["username"]: item["followers_count"] for item in response.data["results"]}
        self.assertEqual(counts_by_username["Dennisse"], 2)
        self.assertEqual(counts_by_username["Peck"], 2)
        self.assertEqual(counts_by_username["DennisseJamaica"], 1)


class FriendsListEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="friends_owner", email="friends_owner@example.com", password="test1234"
        )
        self.friend_one = get_user_model().objects.create_user(
            username="julian_friend", email="julian_friend@example.com", password="test1234"
        )
        self.friend_two = get_user_model().objects.create_user(
            username="maria_friend", email="maria_friend@example.com", password="test1234"
        )
        self.pending_user = get_user_model().objects.create_user(
            username="pending_contact", email="pending_contact@example.com", password="test1234"
        )
        self.follow_only_user = get_user_model().objects.create_user(
            username="follow_only", email="follow_only@example.com", password="test1234"
        )
        self.url = reverse("friends-list")

    def test_requires_authentication(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_returns_only_accepted_friendships(self):
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.friend_one,
            status=Friendship.STATUS_ACCEPTED,
        )
        Friendship.objects.create(
            requester=self.friend_two,
            user1=self.friend_two,
            user2=self.user,
            status=Friendship.STATUS_ACCEPTED,
        )
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.pending_user,
            status=Friendship.STATUS_PENDING,
        )
        Follow.objects.create(follower=self.user, following=self.follow_only_user)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        usernames = [item["username"] for item in response.data["results"]]
        self.assertEqual(usernames, ["julian_friend", "maria_friend"])
        self.assertNotIn("pending_contact", usernames)
        self.assertNotIn("follow_only", usernames)
        self.assertEqual(set(response.data["results"][0].keys()), {"id", "username", "avatar"})

    def test_supports_search_filter_by_username(self):
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.friend_one,
            status=Friendship.STATUS_ACCEPTED,
        )
        Friendship.objects.create(
            requester=self.user,
            user1=self.user,
            user2=self.friend_two,
            status=Friendship.STATUS_ACCEPTED,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"search": "jul"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["username"] for item in response.data["results"]], ["julian_friend"])


class CommentReactionAPITests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="react_user", email="react@example.com", password="test1234"
        )
        self.friend = get_user_model().objects.create_user(
            username="react_friend", email="friend@example.com", password="test1234"
        )
        self.stranger = get_user_model().objects.create_user(
            username="react_stranger", email="stranger@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Reaction Movie",
            type=Movie.MOVIE,
        )
        self.public_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        self.directed_comment = Comment.objects.create(
            author=self.user,
            movie=self.movie,
            body="Directed comment",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.friend,
        )

    def _reaction_url(self, comment):
        return reverse("comment-reaction", kwargs={"pk": comment.pk})

    def test_user_can_only_have_one_reaction_per_comment(self):
        self.client.force_authenticate(self.user)

        first_response = self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )
        second_response = self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )

        self.assertEqual(first_response.status_code, status.HTTP_200_OK)
        self.assertEqual(second_response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            CommentReaction.objects.filter(comment=self.public_comment, user=self.user).count(),
            1,
        )
        self.assertEqual(first_response.data["comment_id"], self.public_comment.id)
        self.assertEqual(second_response.data["comment_id"], self.public_comment.id)

    def test_switching_like_to_dislike_updates_existing_reaction(self):
        self.client.force_authenticate(self.user)

        self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )
        response = self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_DISLIKE},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            CommentReaction.objects.filter(comment=self.public_comment, user=self.user).count(),
            1,
        )
        reaction = CommentReaction.objects.get(comment=self.public_comment, user=self.user)
        self.assertEqual(reaction.reaction_type, CommentReaction.REACT_DISLIKE)
        self.assertEqual(response.data["likes_count"], 0)
        self.assertEqual(response.data["dislikes_count"], 1)
        self.assertEqual(response.data["my_reaction"], CommentReaction.REACT_DISLIKE)

    def test_delete_reaction_removes_it(self):
        CommentReaction.objects.create(
            comment=self.public_comment,
            user=self.user,
            reaction_type=CommentReaction.REACT_LIKE,
        )
        self.client.force_authenticate(self.user)

        response = self.client.delete(self._reaction_url(self.public_comment))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["comment_id"], self.public_comment.id)
        self.assertIsNone(response.data["my_reaction"])
        self.assertEqual(response.data["likes_count"], 0)
        self.assertEqual(response.data["dislikes_count"], 0)
        self.assertFalse(CommentReaction.objects.filter(comment=self.public_comment, user=self.user).exists())

    def test_stranger_cannot_react_to_directed_comment(self):
        self.client.force_authenticate(self.stranger)

        response = self.client.put(
            self._reaction_url(self.directed_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertFalse(CommentReaction.objects.filter(comment=self.directed_comment, user=self.stranger).exists())

    def test_reaction_rejects_invalid_values(self):
        self.client.force_authenticate(self.user)

        response = self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": "laugh"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("reaction", response.data)
        self.assertFalse(CommentReaction.objects.filter(comment=self.public_comment, user=self.user).exists())

    def test_comment_serializer_exposes_reaction_counters_and_my_reaction(self):
        CommentReaction.objects.create(
            comment=self.public_comment,
            user=self.user,
            reaction_type=CommentReaction.REACT_LIKE,
        )
        CommentReaction.objects.create(
            comment=self.public_comment,
            user=self.friend,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )
        self.client.force_authenticate(self.user)

        response = self.client.get(reverse("movie-comments", kwargs={"pk": self.movie.pk}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["likes_count"], 1)
        self.assertEqual(response.data[0]["dislikes_count"], 1)
        self.assertEqual(response.data[0]["my_reaction"], CommentReaction.REACT_LIKE)


class NotificationsAPITests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user_model = get_user_model()
        self.owner = self.user_model.objects.create_user(
            username="owner_user", email="owner@example.com", password="test1234"
        )
        self.actor = self.user_model.objects.create_user(
            username="actor_user", email="actor@example.com", password="test1234"
        )
        self.movie = Movie.objects.create(
            author=self.owner,
            title_english="Notification movie",
            type=Movie.MOVIE,
        )
        self.public_comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="Comentario público",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        self.private_comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body=f"Comentario privado para @{self.actor.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.actor,
        )
        self.inbox_message = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body=f"Hola @{self.owner.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.owner,
            is_read=False,
        )
        self.notifications_url = reverse("me-notifications")
        self.notifications_mark_read_url = reverse("me-notifications-mark-read")
        self.notifications_mark_read_batch_url = reverse("notifications-mark-read-batch")
        self.notifications_mark_context_read_url = reverse("notifications-mark-context-read")
        self.notifications_mark_all_read_url = reverse("notifications-mark-all-read")
        self.me_messages_url = reverse("me-messages")

    def _reaction_url(self, comment):
        return reverse("comment-reaction", kwargs={"pk": comment.pk})

    def test_creates_notification_for_public_comment_reaction(self):
        self.client.force_authenticate(self.actor)
        response = self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        notification = UserNotification.objects.get(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
        )
        self.assertEqual(notification.type, UserNotification.TYPE_PUBLIC_COMMENT_REACTION)
        self.assertEqual(notification.target_tab, UserNotification.TARGET_ACTIVITY)
        self.assertFalse(notification.is_read)

    def test_creates_notification_for_private_comment_reaction(self):
        self.client.force_authenticate(self.actor)
        response = self.client.put(
            self._reaction_url(self.private_comment),
            {"reaction": CommentReaction.REACT_DISLIKE},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        notification = UserNotification.objects.get(
            recipient=self.owner,
            actor=self.actor,
            comment=self.private_comment,
        )
        self.assertEqual(notification.type, UserNotification.TYPE_PRIVATE_COMMENT_REACTION)
        self.assertEqual(notification.target_tab, UserNotification.TARGET_PRIVATE_INBOX)
        self.assertEqual(notification.reaction_type, CommentReaction.REACT_DISLIKE)

    def test_does_not_create_notification_for_self_reaction(self):
        self.client.force_authenticate(self.owner)
        response = self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(UserNotification.objects.filter(recipient=self.owner).exists())

    def test_total_unread_includes_private_messages_and_reactions(self):
        UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)
        response = self.client.get(self.notifications_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["total_unread"], 2)
        self.assertIn(response.data["items"][0]["target_tab"], {"activity", "private_inbox"})

    def test_notifications_expose_current_reaction_value_after_switch_to_dislike(self):
        self.client.force_authenticate(self.actor)
        self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )
        self.client.put(
            self._reaction_url(self.public_comment),
            {"reaction": CommentReaction.REACT_DISLIKE},
            format="json",
        )

        self.client.force_authenticate(self.owner)
        response = self.client.get(self.notifications_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        public_reaction_items = [
            item
            for item in response.data["items"]
            if item["type"] == UserNotification.TYPE_PUBLIC_COMMENT_REACTION
        ]
        self.assertEqual(len(public_reaction_items), 1)
        self.assertEqual(public_reaction_items[0]["reaction_value"], CommentReaction.REACT_DISLIKE)
        self.assertEqual(public_reaction_items[0]["reaction_type"], CommentReaction.REACT_DISLIKE)

    def test_notifications_summary_exposes_notification_id_and_mark_read_persists(self):
        notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        summary_response = self.client.get(self.notifications_url)
        self.assertEqual(summary_response.status_code, status.HTTP_200_OK)
        notification_item = next(
            item for item in summary_response.data["items"] if item["type"] == UserNotification.TYPE_PUBLIC_COMMENT_REACTION
        )
        self.assertEqual(notification_item["id"], notification.id)
        self.assertEqual(notification_item["notification_id"], notification.id)
        self.assertEqual(notification_item["text"], notification_item["message"])
        self.assertEqual(summary_response.data["total_unread"], 2)

        mark_read_response = self.client.post(
            self.notifications_mark_read_url,
            {"ids": [notification_item["notification_id"]]},
            format="json",
        )
        self.assertEqual(mark_read_response.status_code, status.HTTP_200_OK)
        self.assertEqual(mark_read_response.data["updated"], 1)
        self.assertEqual(mark_read_response.data["updated_notifications"], 1)
        self.assertEqual(mark_read_response.data["updated_private_messages"], 0)

        notification.refresh_from_db()
        self.assertTrue(notification.is_read)

        refresh_summary_response = self.client.get(self.notifications_url)
        self.assertEqual(refresh_summary_response.status_code, status.HTTP_200_OK)
        self.assertEqual(refresh_summary_response.data["total_unread"], 1)

    def test_mark_read_supports_composite_notification_identifier(self):
        notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        response = self.client.post(
            self.notifications_mark_read_url,
            {"ids": [f"notification:{notification.id}"]},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 1)
        self.assertEqual(response.data["updated_notifications"], 1)
        self.assertEqual(response.data["updated_private_messages"], 0)

        notification.refresh_from_db()
        self.assertTrue(notification.is_read)

    def test_mark_read_supports_hyphen_notification_identifier(self):
        notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        response = self.client.post(
            self.notifications_mark_read_url,
            {"ids": [f"notification-{notification.id}"]},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 1)
        self.assertEqual(response.data["updated_notifications"], 1)
        self.assertEqual(response.data["updated_private_messages"], 0)

        notification.refresh_from_db()
        self.assertTrue(notification.is_read)

    def test_mark_read_supports_pm_identifier_and_reduces_total_unread(self):
        self.client.force_authenticate(self.owner)

        summary_response = self.client.get(self.notifications_url)
        self.assertEqual(summary_response.status_code, status.HTTP_200_OK)
        self.assertEqual(summary_response.data["total_unread"], 1)
        private_item = next(item for item in summary_response.data["items"] if item["type"] == UserNotification.TYPE_PRIVATE_MESSAGE)
        self.assertEqual(private_item["id"], f"pm-{self.inbox_message.id}")
        self.assertEqual(private_item["notification_id"], f"pm-{self.inbox_message.id}")

        mark_read_response = self.client.post(
            self.notifications_mark_read_url,
            {"ids": [private_item["notification_id"]]},
            format="json",
        )
        self.assertEqual(mark_read_response.status_code, status.HTTP_200_OK)
        self.assertEqual(mark_read_response.data["updated"], 1)
        self.assertEqual(mark_read_response.data["updated_notifications"], 0)
        self.assertEqual(mark_read_response.data["updated_private_messages"], 1)

        self.inbox_message.refresh_from_db()
        self.assertTrue(self.inbox_message.is_read)

        refresh_summary_response = self.client.get(self.notifications_url)
        self.assertEqual(refresh_summary_response.status_code, status.HTTP_200_OK)
        self.assertEqual(refresh_summary_response.data["total_unread"], 0)

    def test_mark_read_batch_supports_mixed_notification_and_pm_ids(self):
        notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        response = self.client.post(
            self.notifications_mark_read_batch_url,
            {"ids": [notification.id, f"pm-{self.inbox_message.id}"]},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 2)
        self.assertEqual(response.data["updated_notifications"], 1)
        self.assertEqual(response.data["updated_private_messages"], 1)

        notification.refresh_from_db()
        self.inbox_message.refresh_from_db()
        self.assertTrue(notification.is_read)
        self.assertTrue(self.inbox_message.is_read)

    def test_mark_context_read_private_inbox_marks_private_reactions_and_messages(self):
        private_reaction_notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.private_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_PRIVATE_INBOX,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        response = self.client.post(
            self.notifications_mark_context_read_url,
            {"context": "private_inbox"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 2)
        self.assertEqual(response.data["updated_notifications"], 1)
        self.assertEqual(response.data["updated_private_messages"], 1)

        private_reaction_notification.refresh_from_db()
        self.inbox_message.refresh_from_db()
        self.assertTrue(private_reaction_notification.is_read)
        self.assertTrue(self.inbox_message.is_read)

    def test_mark_context_read_activity_marks_public_comment_reactions_only(self):
        public_reaction_notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        private_reaction_notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.private_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_PRIVATE_INBOX,
            reaction_type=CommentReaction.REACT_DISLIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        response = self.client.post(
            self.notifications_mark_context_read_url,
            {"context": "activity"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 1)
        self.assertEqual(response.data["updated_notifications"], 1)
        self.assertEqual(response.data["updated_private_messages"], 0)

        public_reaction_notification.refresh_from_db()
        private_reaction_notification.refresh_from_db()
        self.assertTrue(public_reaction_notification.is_read)
        self.assertFalse(private_reaction_notification.is_read)

    def test_mark_all_read_marks_public_private_reactions_and_private_messages(self):
        public_reaction_notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.public_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PUBLIC_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_ACTIVITY,
            reaction_type=CommentReaction.REACT_LIKE,
            is_read=False,
        )
        private_reaction_notification = UserNotification.objects.create(
            recipient=self.owner,
            actor=self.actor,
            comment=self.private_comment,
            movie=self.movie,
            type=UserNotification.TYPE_PRIVATE_COMMENT_REACTION,
            target_tab=UserNotification.TARGET_PRIVATE_INBOX,
            reaction_type=CommentReaction.REACT_DISLIKE,
            is_read=False,
        )
        self.client.force_authenticate(self.owner)

        response = self.client.post(self.notifications_mark_all_read_url, {}, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["updated"], 3)
        self.assertEqual(response.data["updated_notifications"], 2)
        self.assertEqual(response.data["updated_private_messages"], 1)

        public_reaction_notification.refresh_from_db()
        private_reaction_notification.refresh_from_db()
        self.inbox_message.refresh_from_db()
        self.assertTrue(public_reaction_notification.is_read)
        self.assertTrue(private_reaction_notification.is_read)
        self.assertTrue(self.inbox_message.is_read)

    def test_me_messages_returns_private_reactions_received_and_given(self):
        outsider = self.user_model.objects.create_user(
            username="outsider_user", email="outsider@example.com", password="test1234"
        )
        outgoing_private_comment = Comment.objects.create(
            author=outsider,
            movie=self.movie,
            body=f"Privado para @{self.actor.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.actor,
        )

        self.client.force_authenticate(self.actor)
        self.client.put(
            self._reaction_url(self.private_comment),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )
        self.client.put(
            self._reaction_url(outgoing_private_comment),
            {"reaction": CommentReaction.REACT_DISLIKE},
            format="json",
        )

        self.client.force_authenticate(self.owner)
        response = self.client.get(self.me_messages_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        private_reactions = [
            item
            for item in response.data
            if item["type"] == UserNotification.TYPE_PRIVATE_COMMENT_REACTION
        ]
        self.assertEqual(len(private_reactions), 1)
        received = private_reactions[0]
        self.assertTrue(received["is_received_reaction"])
        self.assertFalse(received["is_given_reaction"])
        self.assertEqual(received["reaction_value"], CommentReaction.REACT_LIKE)
        self.assertEqual(received["actor"]["username"], self.actor.username)
        self.assertEqual(received["comment_author"]["username"], self.owner.username)

        self.client.force_authenticate(self.actor)
        actor_response = self.client.get(self.me_messages_url)
        self.assertEqual(actor_response.status_code, status.HTTP_200_OK)
        actor_private_reactions = [
            item
            for item in actor_response.data
            if item["type"] == UserNotification.TYPE_PRIVATE_COMMENT_REACTION
        ]
        self.assertEqual(len(actor_private_reactions), 2)
        given_reaction = next(item for item in actor_private_reactions if item["reaction_value"] == CommentReaction.REACT_DISLIKE)
        self.assertFalse(given_reaction["is_received_reaction"])
        self.assertTrue(given_reaction["is_given_reaction"])

    def test_me_messages_hides_private_reaction_after_delete(self):
        self.client.force_authenticate(self.actor)
        self.client.put(
            self._reaction_url(self.private_comment),
            {"reaction": CommentReaction.REACT_DISLIKE},
            format="json",
        )
        self.client.delete(self._reaction_url(self.private_comment))

        self.client.force_authenticate(self.owner)
        response = self.client.get(self.me_messages_url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        private_reactions = [
            item
            for item in response.data
            if item["type"] == UserNotification.TYPE_PRIVATE_COMMENT_REACTION
        ]
        self.assertEqual(private_reactions, [])


class WeeklyRecommendationsTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user_model = get_user_model()
        self.viewer = self.user_model.objects.create_user(
            username="weekly_viewer", email="viewer@example.com", password="test1234"
        )
        self.author = self.user_model.objects.create_user(
            username="weekly_author", email="author@example.com", password="test1234"
        )
        self.followed_user = self.user_model.objects.create_user(
            username="weekly_followed", email="followed@example.com", password="test1234"
        )
        Follow.objects.create(follower=self.viewer, following=self.followed_user)
        self.url = reverse("weekly-recommendations")
        self.reference_datetime = timezone.make_aware(datetime(2026, 3, 18, 12, 0, 0))
        self.previous_week_window = get_previous_closed_week_window(self.reference_datetime)

    def _create_movie(self, title, **overrides):
        data = {
            "author": self.author,
            "title_english": title,
            "type": Movie.MOVIE,
            "genre": overrides.pop("genre", "Drama"),
            "external_rating": overrides.pop("external_rating", 8.0),
            "external_votes": overrides.pop("external_votes", 0),
        }
        data.update(overrides)
        return Movie.objects.create(**data)

    def _create_rating(self, *, movie, user, score, rated_at):
        rating = MovieRating.objects.create(user=user, movie=movie, score=score)
        MovieRating.objects.filter(pk=rating.pk).update(created_at=rated_at, updated_at=rated_at)
        rating.refresh_from_db()
        return rating

    def _refresh_snapshot(self):
        call_command("refresh_weekly_recommendations", reference_datetime=self.reference_datetime.isoformat())
        return WeeklyRecommendationSnapshot.objects.get(
            week_start=self.previous_week_window.start_date,
            week_end=self.previous_week_window.end_date,
        )

    def test_previous_closed_week_uses_last_complete_monday_to_monday_window(self):
        window = get_previous_closed_week_window(self.reference_datetime)

        self.assertEqual(window.start_at.isoformat(), "2026-03-09T00:00:00+00:00")
        self.assertEqual(window.end_at.isoformat(), "2026-03-16T00:00:00+00:00")

    def test_snapshot_excludes_movie_with_zero_ratings_in_closed_week(self):
        included = self._create_movie("Included Movie", genre="Action", external_rating=7.0)
        excluded_no_weekly_ratings = self._create_movie(
            "Excluded Movie No Weekly Ratings",
            genre="Drama",
            external_rating=9.0,
        )
        weekly_user = self.user_model.objects.create_user(
            username="weekly_rater_1", email="weekly_rater_1@example.com", password="test1234"
        )

        self._create_rating(
            movie=included,
            user=weekly_user,
            score=9,
            rated_at=timezone.make_aware(datetime(2026, 3, 10, 10, 0, 0)),
        )

        snapshot = self._refresh_snapshot()
        snapshot_titles = list(snapshot.items.values_list("movie__title_english", flat=True))

        self.assertIn("Included Movie", snapshot_titles)
        self.assertNotIn("Excluded Movie No Weekly Ratings", snapshot_titles)
        self.assertNotIn(
            excluded_no_weekly_ratings.id,
            set(snapshot.items.values_list("movie_id", flat=True)),
        )

    def test_snapshot_excludes_movie_with_ratings_outside_closed_week_window(self):
        included = self._create_movie("Included Movie", genre="Action", external_rating=7.0)
        excluded_outside_window = self._create_movie(
            "Excluded Outside Window",
            genre="Drama",
            external_rating=9.0,
        )
        weekly_user = self.user_model.objects.create_user(
            username="weekly_rater_2", email="weekly_rater_2@example.com", password="test1234"
        )
        stale_user = self.user_model.objects.create_user(
            username="weekly_rater_3", email="weekly_rater_3@example.com", password="test1234"
        )

        self._create_rating(
            movie=included,
            user=weekly_user,
            score=9,
            rated_at=timezone.make_aware(datetime(2026, 3, 10, 10, 0, 0)),
        )
        self._create_rating(
            movie=excluded_outside_window,
            user=stale_user,
            score=10,
            rated_at=timezone.make_aware(datetime(2026, 3, 17, 10, 0, 0)),
        )

        snapshot = self._refresh_snapshot()
        snapshot_titles = list(snapshot.items.values_list("movie__title_english", flat=True))

        self.assertIn("Included Movie", snapshot_titles)
        self.assertNotIn("Excluded Outside Window", snapshot_titles)

    def test_snapshot_includes_movie_with_at_least_one_rating_inside_closed_week(self):
        eligible = self._create_movie("Eligible Movie", genre="Thriller", external_rating=8.3)
        rater = self.user_model.objects.create_user(
            username="weekly_rater_4", email="weekly_rater_4@example.com", password="test1234"
        )

        self._create_rating(
            movie=eligible,
            user=rater,
            score=8,
            rated_at=timezone.make_aware(datetime(2026, 3, 12, 14, 0, 0)),
        )

        candidates = list(get_weekly_recommendation_candidates(self.previous_week_window))
        candidate_ids = {candidate.id for candidate in candidates}

        self.assertIn(eligible.id, candidate_ids)

        snapshot = self._refresh_snapshot()
        self.assertIn(
            eligible.id,
            set(snapshot.items.values_list("movie_id", flat=True)),
        )

    def test_snapshot_deduplicates_equivalent_genre_combinations(self):
        first = self._create_movie("First Genre", genre="Action, Comedy", external_rating=9.0)
        second = self._create_movie("Second Genre", genre="Action, Comedy", external_rating=8.5)
        third = self._create_movie("Third Genre", genre="Comedy, Action", external_rating=8.4)

        for index, movie in enumerate([first, second, third], start=1):
            rater = self.user_model.objects.create_user(
                username=f"genre_rater_{index}",
                email=f"genre_rater_{index}@example.com",
                password="test1234",
            )
            self._create_rating(
                movie=movie,
                user=rater,
                score=10 - index,
                rated_at=timezone.make_aware(datetime(2026, 3, 11, 9 + index, 0, 0)),
            )

        snapshot = self._refresh_snapshot()
        titles = list(snapshot.items.values_list("movie__title_english", flat=True))

        self.assertEqual(titles, ["First Genre"])

    def test_snapshot_limits_results_to_eight_items(self):
        for index in range(10):
            movie = self._create_movie(
                f"Movie {index}",
                genre=f"Genre {index}",
                external_rating=10 - (index * 0.1),
            )
            rater = self.user_model.objects.create_user(
                username=f"limit_rater_{index}",
                email=f"limit_rater_{index}@example.com",
                password="test1234",
            )
            self._create_rating(
                movie=movie,
                user=rater,
                score=10,
                rated_at=timezone.make_aware(datetime(2026, 3, 12, 8, index, 0)),
            )

        snapshot = self._refresh_snapshot()

        self.assertEqual(snapshot.items.count(), 8)
        self.assertEqual(list(snapshot.items.values_list("position", flat=True)), list(range(1, 9)))


    @patch("core.views.get_previous_closed_week_window")
    def test_endpoint_refreshes_snapshot_when_missing(self, mock_window):
        movie = self._create_movie("On Demand Snapshot", genre="Mystery", external_rating=7.8)
        rater = self.user_model.objects.create_user(
            username="ondemand_rater", email="ondemand_rater@example.com", password="test1234"
        )
        self._create_rating(
            movie=movie,
            user=rater,
            score=8,
            rated_at=timezone.make_aware(datetime(2026, 3, 10, 15, 0, 0)),
        )

        mock_window.return_value = self.previous_week_window
        self.assertFalse(
            WeeklyRecommendationSnapshot.objects.filter(
                week_start=self.previous_week_window.start_date,
                week_end=self.previous_week_window.end_date,
            ).exists()
        )

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["movie"]["title_english"], "On Demand Snapshot")
        self.assertTrue(
            WeeklyRecommendationSnapshot.objects.filter(
                week_start=self.previous_week_window.start_date,
                week_end=self.previous_week_window.end_date,
            ).exists()
        )

    @patch("core.views.get_previous_closed_week_window")
    def test_movies_weekly_alias_uses_same_payload(self, mock_window):
        movie = self._create_movie("Alias Movie", genre="Fantasy", external_rating=7.2)
        rater = self.user_model.objects.create_user(
            username="alias_rater", email="alias_rater@example.com", password="test1234"
        )
        self._create_rating(
            movie=movie,
            user=rater,
            score=9,
            rated_at=timezone.make_aware(datetime(2026, 3, 10, 16, 0, 0)),
        )
        self._refresh_snapshot()

        mock_window.return_value = self.previous_week_window
        response = self.client.get(reverse("movies-weekly"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]["movie"]["title_english"], "Alias Movie")

    @patch("core.views.get_previous_closed_week_window")
    def test_endpoint_returns_snapshot_for_previous_closed_week_with_user_fields(self, mock_window):
        movie = self._create_movie("Endpoint Movie", genre="Sci-Fi", external_rating=8.0)
        rating_user = self.user_model.objects.create_user(
            username="endpoint_rater", email="endpoint_rater@example.com", password="test1234"
        )
        self._create_rating(
            movie=movie,
            user=rating_user,
            score=9,
            rated_at=timezone.make_aware(datetime(2026, 3, 10, 15, 0, 0)),
        )
        MovieRating.objects.create(user=self.viewer, movie=movie, score=7)
        MovieRating.objects.create(user=self.followed_user, movie=movie, score=6)
        snapshot = self._refresh_snapshot()

        mock_window.return_value = self.previous_week_window
        self.client.force_authenticate(user=self.viewer)
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        item = response.data[0]
        self.assertEqual(item["movie"]["title_english"], "Endpoint Movie")
        self.assertEqual(item["position"], 1)
        self.assertEqual(float(item["weekly_score"]), float(snapshot.items.get().weekly_score))
        self.assertAlmostEqual(item["display_rating"], item["general_rating"])
        self.assertEqual(item["my_rating"], 7)
        self.assertAlmostEqual(item["following_avg_rating"], 6.0)
        self.assertEqual(item["following_ratings_count"], 1)

    @patch("core.views.get_previous_closed_week_window")
    def test_weekly_endpoint_top_user_selects_user_with_most_followers(self, mock_window):
        movie = self._create_movie("Top User Followers", genre="Drama", external_rating=7.5)
        top_user = self.user_model.objects.create_user(
            username="top_followed", email="top_followed@example.com", password="test1234"
        )
        less_followed = self.user_model.objects.create_user(
            username="less_followed", email="less_followed@example.com", password="test1234"
        )
        follower_1 = self.user_model.objects.create_user(
            username="follower_1", email="follower_1@example.com", password="test1234"
        )
        follower_2 = self.user_model.objects.create_user(
            username="follower_2", email="follower_2@example.com", password="test1234"
        )
        Follow.objects.create(follower=follower_1, following=top_user)
        Follow.objects.create(follower=follower_2, following=top_user)
        Follow.objects.create(follower=follower_1, following=less_followed)
        self._create_rating(
            movie=movie, user=top_user, score=4, rated_at=timezone.make_aware(datetime(2026, 3, 10, 10, 0, 0))
        )
        self._create_rating(
            movie=movie, user=less_followed, score=10, rated_at=timezone.make_aware(datetime(2026, 3, 10, 11, 0, 0))
        )
        self._refresh_snapshot()
        mock_window.return_value = self.previous_week_window

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["top_user"]["id"], top_user.id)
        self.assertEqual(response.data[0]["top_user"]["followers_count"], 2)

    @patch("core.views.get_previous_closed_week_window")
    def test_weekly_endpoint_top_user_ignores_ratings_outside_week(self, mock_window):
        movie = self._create_movie("Top User Week Filter", genre="Action", external_rating=7.1)
        inside_user = self.user_model.objects.create_user(
            username="inside_week_user", email="inside_week_user@example.com", password="test1234"
        )
        outside_user = self.user_model.objects.create_user(
            username="outside_week_user", email="outside_week_user@example.com", password="test1234"
        )
        follower_1 = self.user_model.objects.create_user(
            username="outside_follower_1", email="outside_follower_1@example.com", password="test1234"
        )
        follower_2 = self.user_model.objects.create_user(
            username="outside_follower_2", email="outside_follower_2@example.com", password="test1234"
        )
        follower_3 = self.user_model.objects.create_user(
            username="outside_follower_3", email="outside_follower_3@example.com", password="test1234"
        )
        Follow.objects.create(follower=follower_1, following=outside_user)
        Follow.objects.create(follower=follower_2, following=outside_user)
        Follow.objects.create(follower=follower_3, following=outside_user)
        self._create_rating(
            movie=movie, user=inside_user, score=8, rated_at=timezone.make_aware(datetime(2026, 3, 12, 10, 0, 0))
        )
        self._create_rating(
            movie=movie, user=outside_user, score=9, rated_at=timezone.make_aware(datetime(2026, 3, 17, 10, 0, 0))
        )
        self._refresh_snapshot()
        mock_window.return_value = self.previous_week_window

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]["top_user"]["id"], inside_user.id)

    @patch("core.views.get_previous_closed_week_window")
    def test_weekly_endpoint_top_user_tie_breaks_by_recent_rating_then_lower_user_id(self, mock_window):
        movie = self._create_movie("Top User Tie Break", genre="Comedy", external_rating=6.8)
        user_a = self.user_model.objects.create_user(
            username="tie_user_a", email="tie_user_a@example.com", password="test1234"
        )
        user_b = self.user_model.objects.create_user(
            username="tie_user_b", email="tie_user_b@example.com", password="test1234"
        )
        same_time_1 = self.user_model.objects.create_user(
            username="same_time_1", email="same_time_1@example.com", password="test1234"
        )
        same_time_2 = self.user_model.objects.create_user(
            username="same_time_2", email="same_time_2@example.com", password="test1234"
        )
        for follower_index in range(2):
            follower = self.user_model.objects.create_user(
                username=f"tie_follower_{follower_index}",
                email=f"tie_follower_{follower_index}@example.com",
                password="test1234",
            )
            Follow.objects.create(follower=follower, following=user_a)
            Follow.objects.create(follower=follower, following=user_b)
            Follow.objects.create(follower=follower, following=same_time_1)
            Follow.objects.create(follower=follower, following=same_time_2)
        self._create_rating(
            movie=movie, user=user_a, score=5, rated_at=timezone.make_aware(datetime(2026, 3, 10, 9, 0, 0))
        )
        self._create_rating(
            movie=movie, user=user_b, score=5, rated_at=timezone.make_aware(datetime(2026, 3, 10, 11, 0, 0))
        )
        self._refresh_snapshot()
        mock_window.return_value = self.previous_week_window

        first_response = self.client.get(self.url)

        self.assertEqual(first_response.status_code, status.HTTP_200_OK)
        self.assertEqual(first_response.data[0]["top_user"]["id"], user_b.id)

        tie_time_movie = self._create_movie("Top User Tie Time", genre="Mystery", external_rating=6.6)
        rated_at = timezone.make_aware(datetime(2026, 3, 11, 10, 0, 0))
        self._create_rating(movie=tie_time_movie, user=same_time_1, score=6, rated_at=rated_at)
        self._create_rating(movie=tie_time_movie, user=same_time_2, score=6, rated_at=rated_at)
        self._refresh_snapshot()

        second_response = self.client.get(self.url)
        tie_item = next(item for item in second_response.data if item["movie"]["title_english"] == "Top User Tie Time")
        expected_lowest_id = min(same_time_1.id, same_time_2.id)
        self.assertEqual(tie_item["top_user"]["id"], expected_lowest_id)

    @patch("core.views.get_previous_closed_week_window")
    def test_weekly_endpoint_top_user_avatar_uses_media_url_prefix(self, mock_window):
        movie = self._create_movie("Top User Avatar", genre="Drama", external_rating=7.4)
        top_user = self.user_model.objects.create_user(
            username="avatar_top_user", email="avatar_top_user@example.com", password="test1234"
        )
        top_user.profile.avatar = SimpleUploadedFile(
            "weekly-avatar.jpg",
            b"weekly-avatar-content",
            content_type="image/jpeg",
        )
        top_user.profile.save(update_fields=["avatar"])
        self._create_rating(
            movie=movie, user=top_user, score=8, rated_at=timezone.make_aware(datetime(2026, 3, 10, 10, 0, 0))
        )
        self._refresh_snapshot()
        mock_window.return_value = self.previous_week_window

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data[0]["top_user"]["avatar"].startswith("/media/avatars/"))

    @patch("core.views.get_previous_closed_week_window")
    def test_weekly_endpoint_top_user_avatar_is_null_without_image(self, mock_window):
        movie = self._create_movie("Top User No Avatar", genre="Drama", external_rating=7.3)
        top_user = self.user_model.objects.create_user(
            username="no_avatar_top_user", email="no_avatar_top_user@example.com", password="test1234"
        )
        self._create_rating(
            movie=movie, user=top_user, score=8, rated_at=timezone.make_aware(datetime(2026, 3, 10, 10, 0, 0))
        )
        self._refresh_snapshot()
        mock_window.return_value = self.previous_week_window

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsNone(response.data[0]["top_user"]["avatar"])

    @patch("core.views.get_previous_closed_week_window")
    def test_weekly_endpoint_top_user_is_null_when_movie_has_no_weekly_ratings(self, mock_window):
        snapshot = WeeklyRecommendationSnapshot.objects.create(
            week_start=self.previous_week_window.start_date,
            week_end=self.previous_week_window.end_date,
            items_count=1,
        )
        movie = self._create_movie("No Weekly Ratings Item", genre="Drama", external_rating=7.0)
        WeeklyRecommendationItem.objects.create(
            snapshot=snapshot,
            movie=movie,
            position=1,
            genre=movie.genre,
            weekly_score=7.000,
            week_ratings_count=0,
            week_ratings_sum=0,
        )
        mock_window.return_value = self.previous_week_window

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsNone(response.data[0]["top_user"])


class MovieListViewSearchAndFiltersTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user_model = get_user_model()
        self.author = self.user_model.objects.create_user(
            username="movie_catalog_author",
            email="movie-catalog-author@example.com",
            password="test1234",
        )
        self.viewer = self.user_model.objects.create_user(
            username="movie_catalog_viewer",
            email="movie-catalog-viewer@example.com",
            password="test1234",
        )
        self.url = reverse("movie-list")
        self.search_url = reverse("movie-search")

    def _create_movie(self, title, **overrides):
        data = {
            "author": self.author,
            "title_english": title,
            "title_spanish": overrides.pop("title_spanish", title),
            "type": overrides.pop("type", Movie.MOVIE),
            "genre": overrides.pop("genre", "Drama"),
            "release_year": overrides.pop("release_year", 2020),
            "director": overrides.pop("director", "Director"),
            "cast_members": overrides.pop("cast_members", "Actor"),
            "synopsis": overrides.pop("synopsis", ""),
            "external_rating": overrides.pop("external_rating", 7.0),
            "external_votes": overrides.pop("external_votes", 1000),
        }
        data.update(overrides)
        return Movie.objects.create(**data)

    def test_full_text_search_accepts_q_parameter(self):
        matched = self._create_movie(
            "The Matrix",
            title_spanish="Matrix",
            release_year=1999,
            director="Lana Wachowski",
            external_rating=8.7,
        )
        self._create_movie("Unrelated Movie", title_spanish="Sin relación")

        response = self.client.get(self.search_url, {"q": "matrix"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(matched.id, result_ids)
        self.assertEqual(response.data["results"][0]["id"], matched.id)
        self.assertIn("search_rank", response.data["results"][0])

    def test_movie_search_defaults_to_lightweight_payload(self):
        matched = self._create_movie(
            "The Matrix",
            title_spanish="Matrix",
            release_year=1999,
            director="Lana Wachowski",
            cast_members="Keanu Reeves",
            external_rating=8.7,
            external_votes=1900000,
        )

        response = self.client.get(self.search_url, {"q": "matrix", "page_size": 10})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result = response.data["results"][0]
        self.assertEqual(result["id"], matched.id)
        self.assertEqual(
            set(result),
            {
                "id",
                "image",
                "title_spanish",
                "title_english",
                "type",
                "genre",
                "release_year",
                "director",
                "cast_members",
                "external_rating",
                "external_votes",
                "display_rating",
                "search_rank",
            },
        )
        self.assertNotIn("comments_count", result)
        self.assertNotIn("following_avg_rating", result)
        self.assertNotIn("is_in_my_list", result)

    def test_movie_search_default_queryset_skips_social_and_rating_annotations(self):
        view = MovieSearchView()
        view.request = SimpleNamespace(
            query_params={"q": "matrix"},
            user=AnonymousUser(),
        )

        self.assertIs(view.get_serializer_class(), MovieSearchLightSerializer)
        qs = view.get_queryset()

        heavy_annotations = {
            "real_ratings_count",
            "real_ratings_avg",
            "my_rating",
            "following_avg_rating",
            "following_ratings_count",
            "comments_count",
            "is_in_my_list",
            "is_in_my_recommendations",
            "general_rating",
        }
        self.assertTrue(heavy_annotations.isdisjoint(qs.query.annotations))

    def test_movie_search_include_social_uses_full_payload(self):
        self._create_movie("The Matrix", title_spanish="Matrix", release_year=1999)

        response = self.client.get(self.search_url, {"q": "matrix", "include_social": "true"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result = response.data["results"][0]
        self.assertIn("comments_count", result)
        self.assertIn("following_avg_rating", result)
        self.assertIn("is_in_my_list", result)

        view = MovieSearchView()
        view.request = SimpleNamespace(
            query_params={"q": "matrix", "include_social": "true"},
            user=AnonymousUser(),
        )
        self.assertIs(view.get_serializer_class(), MovieSearchResultSerializer)

    def test_full_text_search_accepts_search_parameter(self):
        matched = self._create_movie(
            "The Matrix",
            title_spanish="Matrix",
            release_year=1999,
            director="Lana Wachowski",
            external_rating=8.7,
        )
        self._create_movie("Unrelated Movie", title_spanish="Sin relación")

        response = self.client.get(self.search_url, {"search": "matrix"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(matched.id, result_ids)

    def test_full_text_search_is_accent_insensitive(self):
        matched = self._create_movie(
            "Amelie",
            title_spanish="Amélie",
            director="Jean-Pierre Jeunet",
        )
        self._create_movie("Accent Free Different Movie", title_spanish="Otra pelicula")

        response = self.client.get(self.search_url, {"q": "amelie"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(matched.id, result_ids)

    def test_autocomplete_still_uses_autocomplete_serializer_without_search_vector(self):
        matched = self._create_movie("The Matrix", title_spanish="Matrix", release_year=1999)

        view = MovieListView()
        view.request = SimpleNamespace(query_params={"autocomplete": "true"})

        self.assertIs(view.get_serializer_class(), MovieAutocompleteSerializer)

        response = self.client.get(self.url, {"autocomplete": "true", "q": "matrix", "limit": 5})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"][0]["id"], matched.id)
        self.assertEqual(
            set(response.data["results"][0]),
            {
                "id",
                "image",
                "title_spanish",
                "title_english",
                "type",
                "release_year",
                "genre",
                "director",
                "cast_members",
            },
        )

        qs = apply_movie_autocomplete_search(Movie.objects.all(), "matrix")
        self.assertNotIn("search_vector", str(qs.query).lower())

    def test_search_supports_multiple_terms_across_fields(self):
        matched = self._create_movie(
            "Space Journey",
            director="Christopher Nolan",
            synopsis="A thriller happening in deep space.",
            genre="Sci-Fi",
        )
        self._create_movie(
            "Only Space",
            director="Jane Doe",
            synopsis="Deep space documentary.",
            genre="Documentary",
        )

        response = self.client.get(self.url, {"search": "nolan space"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [matched.id])

    def test_search_supports_combined_title_and_release_year(self):
        matched = self._create_movie("Titanic", release_year=1997, director="James Cameron")
        self._create_movie("Titanic", release_year=1953, director="Jean Negulesco")

        response = self.client.get(self.url, {"search": "titanic 1997"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [matched.id])

    def test_autocomplete_search_supports_combined_title_and_release_year(self):
        matched = self._create_movie("Titanic", release_year=1997, director="James Cameron")
        self._create_movie("Titanic", release_year=1953, director="Jean Negulesco")

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "search": "titanic 1997", "page_size": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 1)
        self.assertEqual(response.data["results"][0], {
            "id": matched.id,
            "image": matched.image,
            "title_spanish": matched.title_spanish,
            "title_english": matched.title_english,
            "type": matched.type,
            "release_year": matched.release_year,
            "genre": matched.genre,
            "director": matched.director,
            "cast_members": matched.cast_members,
        })

        qs = apply_movie_autocomplete_search(Movie.objects.all(), "titanic 1997")
        sql = str(qs.query).lower()
        self.assertIn("release_year", sql)
        self.assertIn("title_english_search", sql)

    def test_autocomplete_requires_all_terms_across_metadata(self):
        title_year_cast_match = self._create_movie(
            "Cabana Pearl",
            title_spanish="Cabaña perla",
            release_year=2017,
            cast_members="Laura García",
        )
        director_match = self._create_movie(
            "Evil Story",
            title_spanish="La maldad",
            release_year=2017,
            director="María Cabaña",
        )
        self._create_movie(
            "La martina",
            title_spanish="La martina",
            release_year=2017,
            director="Sin coincidencia",
        )
        self._create_movie(
            "Cabin Different Year",
            title_spanish="La cabaña",
            release_year=2015,
            cast_members="Laura García",
        )
        self._create_movie(
            "Cabin Without Article",
            title_spanish="Cabaña",
            release_year=2017,
            director="Sin articulo",
        )

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "la cabaña 2017", "limit": 10},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(set(result_ids), {title_year_cast_match.id, director_match.id})

    def test_autocomplete_search_is_accent_insensitive_without_db_unaccent(self):
        full_phrase = self._create_movie(
            "Family Cabin",
            title_spanish="La cabaña del tío",
            genre="Drama",
        )
        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "la cabana del tio", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(full_phrase.id, result_ids)

        qs = apply_movie_autocomplete_search(Movie.objects.all(), "la cabana del tio")
        sql = str(qs.query).lower()
        self.assertIn("title_spanish_search", sql)
        self.assertNotIn("genre_search", sql)
        self.assertNotIn("unaccent", sql)
        self.assertNotIn("upper(", sql)
        self.assertNotIn("lower(", sql)

    def test_autocomplete_search_does_not_match_genre_terms(self):
        genre_match = self._create_movie(
            "Action Story",
            title_spanish="Aventura",
            genre="Acción",
        )

        with CaptureQueriesContext(connection) as captured_queries:
            response = self.client.get(
                self.url,
                {"autocomplete": "true", "q": "accion", "limit": 5},
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [item["id"] for item in response.data["results"]]
        self.assertNotIn(genre_match.id, result_ids)
        combined_sql = "\n".join(query["sql"] for query in captured_queries).lower()
        self.assertNotIn("genre_search", combined_sql)

        qs = apply_movie_autocomplete_search(Movie.objects.all(), "accion")
        self.assertNotIn("genre_search", str(qs.query).lower())

    def test_autocomplete_search_does_not_match_type_terms(self):
        series = self._create_movie(
            "Unrelated Title",
            title_spanish="Titulo sin coincidencia",
            type=Movie.SERIES,
        )
        movie = self._create_movie(
            "Another Unrelated Title",
            title_spanish="Otro titulo sin coincidencia",
            type=Movie.MOVIE,
        )

        with CaptureQueriesContext(connection) as captured_queries:
            response = self.client.get(
                self.url,
                {"autocomplete": "true", "q": "series", "limit": 5},
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [item["id"] for item in response.data["results"]]
        self.assertNotIn(series.id, result_ids)
        self.assertNotIn(movie.id, result_ids)
        combined_sql = "\n".join(query["sql"] for query in captured_queries).lower()
        self.assertNotIn("type_search", combined_sql)

        qs = apply_movie_autocomplete_search(Movie.objects.all(), "series")
        self.assertNotIn("type_search", str(qs.query).lower())

    def test_autocomplete_prioritizes_titles_when_all_terms_match(self):
        title_match = self._create_movie(
            "The Curious Case of Benjamin Button",
            title_spanish="El curioso caso de Benjamin Button",
            release_year=2008,
        )
        metadata_match = self._create_movie(
            "Unrelated Drama",
            director="Benjamin Button",
            cast_members="Actor",
            release_year=2008,
        )

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "benjamin button", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids[:2], [title_match.id, metadata_match.id])

    def test_autocomplete_paginates_without_capping_total_results_to_limit(self):
        for index in range(5):
            self._create_movie(f"Matrix Infinite {index}", release_year=1999 + index)

        first_page = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "matrix", "limit": 2},
        )
        second_page = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "matrix", "limit": 2, "page": 2},
        )

        self.assertEqual(first_page.status_code, status.HTTP_200_OK)
        self.assertEqual(second_page.status_code, status.HTTP_200_OK)
        self.assertEqual(first_page.data["count"], 5)
        self.assertIsNotNone(first_page.data["next"])
        self.assertIn("previous", first_page.data)
        self.assertEqual(len(first_page.data["results"]), 2)
        self.assertEqual(len(second_page.data["results"]), 2)
        first_page_ids = {movie["id"] for movie in first_page.data["results"]}
        second_page_ids = {movie["id"] for movie in second_page.data["results"]}
        self.assertTrue(first_page_ids.isdisjoint(second_page_ids))

    def test_autocomplete_returns_listbox_metadata_for_benjamin_button(self):
        matched = self._create_movie(
            "The Curious Case of Benjamin Button",
            title_spanish="El curioso caso de Benjamin Button",
            type=Movie.MOVIE,
            genre="Drama, Fantasy, Romance",
            release_year=2008,
            director="David Fincher",
            cast_members="Brad Pitt, Cate Blanchett, Taraji P. Henson",
        )

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "benjamin button", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result = response.data["results"][0]
        self.assertEqual(result["id"], matched.id)
        self.assertEqual(result["title_spanish"], "El curioso caso de Benjamin Button")
        self.assertEqual(result["title_english"], "The Curious Case of Benjamin Button")
        self.assertEqual(result["release_year"], 2008)
        self.assertEqual(result["type"], Movie.MOVIE)
        self.assertEqual(result["genre"], "Drama, Fantasy, Romance")
        self.assertEqual(result["director"], "David Fincher")
        self.assertEqual(result["cast_members"], "Brad Pitt, Cate Blanchett, Taraji P. Henson")
        self.assertNotIn("display_rating", result)
        self.assertNotIn("following_avg_rating", result)

    def test_autocomplete_search_matches_terms_inside_titles(self):
        matched = self._create_movie(
            "The Curious Case of Benjamin Button",
            title_spanish="El curioso caso de Benjamin Button",
            release_year=2008,
        )
        self._create_movie(
            "Curious George",
            title_spanish="El curioso George",
            release_year=2006,
        )

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "benjamin button", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(matched.id, result_ids)

    def test_autocomplete_search_matches_director_and_cast_terms(self):
        directed = self._create_movie(
            "Avatar",
            release_year=2009,
            director="James Cameron",
            cast_members="Sam Worthington, Zoe Saldaña",
        )
        casted = self._create_movie(
            "The Holiday",
            release_year=2006,
            director="Nancy Meyers",
            cast_members="Cameron Diaz, Jude Law, Kate Winslet",
        )
        brad_pitt = self._create_movie(
            "Fight Club",
            release_year=1999,
            director="David Fincher",
            cast_members="Brad Pitt, Edward Norton",
        )
        self._create_movie("Random Movie", director="Jane Doe", cast_members="Actor")

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "search": "james cameron", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(directed.id, result_ids)
        self.assertNotIn(casted.id, result_ids)

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "search": "brad pitt", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [brad_pitt.id])

    def test_autocomplete_extended_lane_matches_cross_title_and_cast_terms(self):
        titanic = self._create_movie(
            "Titanic",
            release_year=1997,
            director="James Cameron",
            cast_members="Leonardo DiCaprio, Kate Winslet",
        )
        benjamin_button = self._create_movie(
            "The Curious Case of Benjamin Button",
            title_spanish="El curioso caso de Benjamin Button",
            release_year=2008,
            director="David Fincher",
            cast_members="Brad Pitt, Cate Blanchett, Taraji P. Henson",
        )
        self._create_movie(
            "Leonardo",
            release_year=2020,
            director="Different Director",
            cast_members="Actor",
        )
        self._create_movie(
            "Benjamin Button Documentary",
            release_year=2011,
            director="Different Director",
            cast_members="Different Cast",
        )

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "titanic leonardo", "limit": 10},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertIn(titanic.id, result_ids)

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "benjamin button brad", "limit": 10},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [benjamin_button.id])

    def test_autocomplete_skips_synopsis_matches_for_lightweight_queries(self):
        self._create_movie(
            "Unrelated Title",
            synopsis="A long synopsis that mentions transformers but not searchable metadata.",
        )

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "transformers", "limit": 5},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["results"], [])

    def test_autocomplete_accepts_q_and_limit_for_small_listbox_payload(self):
        for index in range(4):
            self._create_movie(f"Matrix Result {index}", release_year=1999 + index)

        response = self.client.get(self.url, {"autocomplete": "1", "q": "matrix", "limit": 2})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 4)
        self.assertEqual(len(response.data["results"]), 2)
        self.assertEqual(
            set(response.data["results"][0]),
            {
                "id",
                "image",
                "title_spanish",
                "title_english",
                "type",
                "release_year",
                "genre",
                "director",
                "cast_members",
            },
        )

    def test_autocomplete_adds_recency_score_only_without_explicit_year(self):
        older = self._create_movie("Matrix Recency", release_year=1996)
        recent = self._create_movie("Matrix Recency", release_year=2024)

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "matrix recency", "limit": 10},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids[:2], [recent.id, older.id])

        qs = build_movie_autocomplete_fast_queryset(Movie.objects.all(), "matrix recency")
        self.assertIn("recency_score", qs.query.annotations)

        qs_with_year = build_movie_autocomplete_fast_queryset(
            Movie.objects.all(),
            "matrix recency 1996",
        )
        self.assertNotIn("recency_score", qs_with_year.query.annotations)

    def test_autocomplete_splits_year_terms_into_release_year_filter(self):
        titanic_1997 = self._create_movie("Titanic", release_year=1997)
        self._create_movie("Titanic", release_year=1953)
        self._create_movie("Unrelated 1997", release_year=1997)

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "titanic 1997", "limit": 10},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [titanic_1997.id])

    def test_autocomplete_year_filter_remains_accent_insensitive_for_text_terms(self):
        matched = self._create_movie("The Shack", title_spanish="La cabaña", release_year=2017)
        self._create_movie("La cabaña", title_spanish="La cabaña", release_year=2010)
        self._create_movie("Unrelated", title_spanish="Sin relación", release_year=2017)

        response = self.client.get(
            self.url,
            {"autocomplete": "true", "q": "la cabana 2017", "limit": 10},
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [matched.id])

    def test_autocomplete_year_only_returns_movies_for_that_year(self):
        newest = self._create_movie("Year Match B", release_year=1997)
        oldest = self._create_movie("Year Match A", release_year=1997)
        self._create_movie("Different Year", release_year=1998)

        response = self.client.get(self.url, {"autocomplete": "true", "q": "1997", "limit": 10})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(set(result_ids), {newest.id, oldest.id})

    def test_autocomplete_does_not_search_year_with_text_like(self):
        self._create_movie("Titanic", release_year=1997)

        with CaptureQueriesContext(connection) as captured_queries:
            response = self.client.get(
                self.url,
                {"autocomplete": "true", "q": "titanic 1997", "limit": 10},
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        combined_sql = "\n".join(query["sql"] for query in captured_queries)
        self.assertIn('"core_movie"."release_year" = 1997', combined_sql)
        self.assertNotIn("LIKE '%1997%'", combined_sql)
        self.assertNotIn("UPPER(", combined_sql)

    def test_search_supports_combined_genre_and_release_year(self):
        matched = self._create_movie("Drama 1997", genre="Drama", release_year=1997)
        self._create_movie("Drama Other Year", genre="Drama", release_year=2001)
        self._create_movie("Action 1997", genre="Action", release_year=1997)

        response = self.client.get(self.url, {"search": "drama 1997"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [matched.id])

    def test_search_is_accent_insensitive(self):
        matched = self._create_movie(
            "Action Dream",
            title_spanish="Sueño de fuga",
            genre="Acción, Drama",
            synopsis="Una fuga de prisión con mucha acción.",
        )
        self._create_movie(
            "Another Movie",
            title_spanish="Película sin relación",
            genre="Drama",
        )

        response_without_tilde = self.client.get(self.url, {"search": "sueno accion"})
        response_with_tilde = self.client.get(self.url, {"search": "sueño acción"})

        self.assertEqual(response_without_tilde.status_code, status.HTTP_200_OK)
        self.assertEqual(response_with_tilde.status_code, status.HTTP_200_OK)
        ids_without_tilde = [movie["id"] for movie in response_without_tilde.data["results"]]
        ids_with_tilde = [movie["id"] for movie in response_with_tilde.data["results"]]
        self.assertIn(matched.id, ids_without_tilde)
        self.assertIn(matched.id, ids_with_tilde)

    def test_list_includes_following_stats_excluding_viewer_rating(self):
        followed_user = self.user_model.objects.create_user(
            username="movie_catalog_followed",
            email="movie-catalog-followed@example.com",
            password="test1234",
        )
        movie = self._create_movie("Following Stats Movie")
        Follow.objects.create(follower=self.viewer, following=followed_user)
        MovieRating.objects.create(user=followed_user, movie=movie, score=9)
        MovieRating.objects.create(user=self.viewer, movie=movie, score=4)

        self.client.force_authenticate(self.viewer)
        response = self.client.get(self.url, {"search": "Following Stats Movie"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.data["results"][0]
        self.assertEqual(payload["following_avg_rating"], 9.0)
        self.assertEqual(payload["following_ratings_count"], 1)

    def test_list_includes_null_avg_and_zero_count_without_followed_ratings(self):
        movie = self._create_movie("No Followed Ratings Movie")
        MovieRating.objects.create(user=self.viewer, movie=movie, score=8)

        self.client.force_authenticate(self.viewer)
        response = self.client.get(self.url, {"search": "No Followed Ratings Movie"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.data["results"][0]
        self.assertIsNone(payload["following_avg_rating"])
        self.assertEqual(payload["following_ratings_count"], 0)

    def test_genre_filter_matches_individual_genre_inside_csv_string(self):
        matched = self._create_movie("Action Comedy Mix", genre="Action, Comedy")
        self._create_movie("Drama Piece", genre="Drama")

        response = self.client.get(self.url, {"genre": "Comedy"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        result_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(result_ids, [matched.id])

    def test_pagination_is_kept(self):
        for index in range(12):
            self._create_movie(f"Movie {index}", release_year=2000 + index)

        response = self.client.get(self.url, {"page_size": 5})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["count"], 12)
        self.assertEqual(len(response.data["results"]), 5)
        self.assertIsNotNone(response.data["next"])

    def test_authenticated_ordering_uses_profile_preferences(self):
        UserTasteProfile.objects.create(user=self.viewer, ratings_count=4)
        UserGenrePreference.objects.create(user=self.viewer, genre="Action|Comedy", count_10=2)

        preferred = self._create_movie(
            "Preferred by Genre",
            genre="Action, Comedy",
            release_year=2005,
            external_rating=6.0,
        )
        fallback = self._create_movie(
            "Fallback Drama",
            genre="Drama",
            release_year=2024,
            external_rating=9.0,
        )

        self.client.force_authenticate(self.viewer)
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        ordered_ids = [movie["id"] for movie in response.data["results"]]
        self.assertEqual(ordered_ids[:2], [preferred.id, fallback.id])


class PublicCommentsFeedViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.viewer = get_user_model().objects.create_user(
            username="public_feed_viewer",
            email="public-feed-viewer@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.viewer)
        self.url = reverse("public-comments-feed")
        self.movie = Movie.objects.create(
            author=self.viewer,
            title_english="Public Feed Movie",
            type=Movie.MOVIE,
            genre="Drama",
        )

    def _user(self, username):
        return get_user_model().objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="test1234",
        )

    def _add_followers(self, user, total):
        for index in range(total):
            follower = self._user(f"{user.username}_follower_{index}")
            Follow.objects.create(follower=follower, following=user)

    def test_public_comments_feed_orders_categories_and_excludes_non_public_items(self):
        category_1_low = self._user("category1_low")
        category_1_high = self._user("category1_high")
        category_2_low = self._user("category2_low")
        category_2_high = self._user("category2_high")
        category_3_user = self._user("category3_friend")
        private_user = self._user("private_category")

        private_user.profile.is_public = False
        private_user.profile.visibility = Profile.Visibility.PRIVATE
        private_user.profile.save(update_fields=["is_public", "visibility"])

        Follow.objects.create(follower=self.viewer, following=category_2_low)
        Follow.objects.create(follower=self.viewer, following=category_2_high)

        Friendship.objects.create(
            requester=self.viewer,
            user1=self.viewer,
            user2=category_3_user,
            status=Friendship.STATUS_ACCEPTED,
        )

        self._add_followers(category_1_low, 2)
        self._add_followers(category_1_high, 4)
        self._add_followers(category_2_low, 1)
        self._add_followers(category_2_high, 3)
        self._add_followers(category_3_user, 5)

        category_1_low_comment = Comment.objects.create(
            author=category_1_low,
            movie=self.movie,
            body="category 1 low",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        category_1_high_comment = Comment.objects.create(
            author=category_1_high,
            movie=self.movie,
            body="category 1 high",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        category_2_low_comment = Comment.objects.create(
            author=category_2_low,
            movie=self.movie,
            body="category 2 low",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        category_2_high_comment = Comment.objects.create(
            author=category_2_high,
            movie=self.movie,
            body="category 2 high",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        category_3_comment = Comment.objects.create(
            author=category_3_user,
            movie=self.movie,
            body="category 3",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        category_4_old_comment = Comment.objects.create(
            author=self.viewer,
            movie=self.movie,
            body="category 4 old",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        category_4_new_comment = Comment.objects.create(
            author=self.viewer,
            movie=self.movie,
            body="category 4 new",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        Comment.objects.create(
            author=category_1_high,
            movie=self.movie,
            body="directed comment",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.viewer,
        )
        private_user_comment = Comment.objects.create(
            author=private_user,
            movie=self.movie,
            body="private author comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        expected_order = [
            category_1_high_comment.id,
            category_1_low_comment.id,
            private_user_comment.id,
            category_2_high_comment.id,
            category_2_low_comment.id,
            category_3_comment.id,
            category_4_new_comment.id,
            category_4_old_comment.id,
        ]
        self.assertEqual([item["id"] for item in response.data], expected_order)

        first_item = response.data[0]
        self.assertIn("author", first_item)
        self.assertIn("author_followers_count", first_item)
        self.assertIn("is_following_author", first_item)
        self.assertIn("is_friend_author", first_item)
        self.assertIn("likes_count", first_item)
        self.assertIn("dislikes_count", first_item)
        self.assertIn("my_reaction", first_item)
        self.assertIn("movie", first_item)
        self.assertEqual(first_item["author_followers_count"], 4)
        self.assertFalse(first_item["is_following_author"])
        self.assertFalse(first_item["is_friend_author"])

    def test_public_comments_feed_requires_authentication(self):
        self.client.force_authenticate(None)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)


class ProfileFeedActivityViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.viewer = get_user_model().objects.create_user(
            username="profile_feed_viewer",
            email="profile-feed-viewer@example.com",
            password="test1234",
        )
        self.actor = get_user_model().objects.create_user(
            username="profile_feed_actor",
            email="profile-feed-actor@example.com",
            password="test1234",
        )
        self.friend = get_user_model().objects.create_user(
            username="profile_feed_friend",
            email="profile-feed-friend@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.viewer)
        self.url = reverse("profile-feed-activity")
        self.movie = Movie.objects.create(
            author=self.viewer,
            title_english="Profile Feed Movie",
            title_spanish="Pelicula Feed",
            release_year=2024,
            type=Movie.MOVIE,
            image="https://cdn.example.com/profile-feed.jpg",
        )
        self.movie_without_image = Movie.objects.create(
            author=self.viewer,
            title_english="No image movie",
            release_year=2023,
            type=Movie.MOVIE,
        )

    def _add_follow(self, actor):
        Follow.objects.create(follower=self.viewer, following=actor)

    def _add_friendship(self, friend_user):
        Friendship.objects.create(
            requester=self.viewer,
            user1=min(self.viewer, friend_user, key=lambda u: u.id),
            user2=max(self.viewer, friend_user, key=lambda u: u.id),
            status=Friendship.STATUS_ACCEPTED,
        )

    def _fetch_ids(self, *, scope):
        response = self.client.get(self.url, {"scope": scope})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return [item["id"] for item in response.data["results"]], response

    def _set_created_at(self, model_cls, object_id, created_at):
        model_cls.objects.filter(pk=object_id).update(created_at=created_at)

    def test_requires_authentication(self):
        self.client.force_authenticate(None)
        response = self.client.get(self.url, {"scope": "following"})
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_missing_scope_defaults_to_me(self):
        own_rating = MovieRating.objects.create(user=self.viewer, movie=self.movie, score=9)

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        ids = [item["id"] for item in response.data["results"]]
        self.assertIn(f"rating:{own_rating.id}", ids)

    def test_invalid_scope_falls_back_to_me(self):
        own_rating = MovieRating.objects.create(user=self.viewer, movie=self.movie, score=6)

        response = self.client.get(self.url, {"scope": "invalid-scope"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        ids = [item["id"] for item in response.data["results"]]
        self.assertIn(f"rating:{own_rating.id}", ids)

    def test_following_returns_ratings_from_followed_users(self):
        self._add_follow(self.actor)
        rating = MovieRating.objects.create(user=self.actor, movie=self.movie, score=8)

        ids, _ = self._fetch_ids(scope="following")
        self.assertIn(f"rating:{rating.id}", ids)

    def test_following_returns_public_comments_from_followed_users(self):
        self._add_follow(self.actor)
        comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Public comment in following feed",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        ids, _ = self._fetch_ids(scope="following")
        self.assertIn(f"public_comment:{comment.id}", ids)

    def test_following_returns_public_comment_likes_from_followed_users(self):
        self._add_follow(self.actor)
        comment = Comment.objects.create(
            author=self.friend,
            movie=self.movie,
            body="Public comment liked by followed actor",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        reaction = CommentReaction.objects.create(
            user=self.actor,
            comment=comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )

        ids, _ = self._fetch_ids(scope="following")
        self.assertIn(f"public_comment_like:{reaction.id}", ids)

    def test_friends_returns_ratings_from_accepted_friends(self):
        self._add_friendship(self.friend)
        rating = MovieRating.objects.create(user=self.friend, movie=self.movie, score=7)

        ids, _ = self._fetch_ids(scope="friends")
        self.assertIn(f"rating:{rating.id}", ids)

    def test_friends_returns_public_comments_from_accepted_friends(self):
        self._add_friendship(self.friend)
        comment = Comment.objects.create(
            author=self.friend,
            movie=self.movie,
            body="Friend public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        ids, _ = self._fetch_ids(scope="friends")
        self.assertIn(f"public_comment:{comment.id}", ids)

    def test_friends_returns_public_comment_likes_from_accepted_friends(self):
        self._add_friendship(self.friend)
        comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Public comment liked by friend",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        reaction = CommentReaction.objects.create(
            user=self.friend,
            comment=comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )

        ids, _ = self._fetch_ids(scope="friends")
        self.assertIn(f"public_comment_like:{reaction.id}", ids)

    def test_excludes_authenticated_user_activity(self):
        self._add_follow(self.actor)
        own_rating = MovieRating.objects.create(user=self.viewer, movie=self.movie, score=9)
        actor_rating = MovieRating.objects.create(user=self.actor, movie=self.movie_without_image, score=6)

        ids, _ = self._fetch_ids(scope="following")
        self.assertNotIn(f"rating:{own_rating.id}", ids)
        self.assertIn(f"rating:{actor_rating.id}", ids)

    def test_scope_me_returns_authenticated_user_activity(self):
        own_rating = MovieRating.objects.create(user=self.viewer, movie=self.movie, score=9)
        own_comment = Comment.objects.create(
            author=self.viewer,
            movie=self.movie,
            body="Own public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        comment_to_react = Comment.objects.create(
            author=self.actor,
            movie=self.movie_without_image,
            body="Someone else comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        own_like = CommentReaction.objects.create(
            user=self.viewer,
            comment=comment_to_react,
            reaction_type=CommentReaction.REACT_LIKE,
        )
        own_dislike = CommentReaction.objects.create(
            user=self.viewer,
            comment=comment_to_react,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )

        ids, response = self._fetch_ids(scope="me")

        self.assertIn(f"rating:{own_rating.id}", ids)
        self.assertIn(f"public_comment:{own_comment.id}", ids)
        self.assertIn(f"public_comment_like:{own_like.id}", ids)
        self.assertIn(f"public_comment_dislike:{own_dislike.id}", ids)
        returned_types = {item["activity_type"] for item in response.data["results"]}
        self.assertTrue({"rating", "public_comment", "public_comment_like", "public_comment_dislike"}.issubset(returned_types))

    def test_activity_feed_items_include_updated_at_and_activity_at(self):
        own_rating = MovieRating.objects.create(user=self.viewer, movie=self.movie, score=9)

        response = self.client.get(self.url, {"scope": "me"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        rating_item = next(item for item in response.data["results"] if item["id"] == f"rating:{own_rating.id}")

        self.assertIn("created_at", rating_item)
        self.assertIn("updated_at", rating_item)
        self.assertIn("activity_at", rating_item)
        self.assertEqual(rating_item["activity_at"], rating_item["updated_at"])

    def test_activity_at_uses_updated_at_when_reaction_changes(self):
        comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Public comment for reaction updates",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        reaction = CommentReaction.objects.create(
            user=self.viewer,
            comment=comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )

        before_update = reaction.updated_at
        reaction.reaction_type = CommentReaction.REACT_DISLIKE
        reaction.save()
        reaction.refresh_from_db()

        self.assertGreater(reaction.updated_at, before_update)

        response = self.client.get(self.url, {"scope": "me"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        reaction_item = next(
            item
            for item in response.data["results"]
            if item["id"] == f"public_comment_reaction:{reaction.id}"
        )

        self.assertEqual(reaction_item["activity_at"], reaction_item["updated_at"])
        self.assertEqual(
            parse_datetime(reaction_item["updated_at"]),
            reaction.updated_at,
        )

    def test_scope_me_includes_valid_directed_comments_and_excludes_inconsistent_ones(self):
        valid_directed = Comment.objects.create(
            author=self.viewer,
            movie=self.movie,
            body=f"Privado para @{self.actor.username}",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.actor,
        )
        inconsistent_directed = Comment.objects.create(
            author=self.viewer,
            movie=self.movie,
            body="Sin mención al target",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.actor,
        )

        ids, response = self._fetch_ids(scope="me")
        self.assertIn(f"directed_comment:{valid_directed.id}", ids)
        self.assertNotIn(f"directed_comment:{inconsistent_directed.id}", ids)
        directed_item = next(item for item in response.data["results"] if item["id"] == f"directed_comment:{valid_directed.id}")
        self.assertEqual(directed_item["target_user"]["id"], self.actor.id)

    def test_excludes_private_direct_comments(self):
        self._add_follow(self.actor)
        public_comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Visible comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        private_comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Mentioned only comment",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.viewer,
        )

        ids, _ = self._fetch_ids(scope="following")
        self.assertIn(f"public_comment:{public_comment.id}", ids)
        self.assertNotIn(f"public_comment:{private_comment.id}", ids)

    def test_excludes_likes_on_private_direct_comments(self):
        self._add_follow(self.actor)
        private_comment = Comment.objects.create(
            author=self.friend,
            movie=self.movie,
            body="Private comment should not surface through likes",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.viewer,
        )
        private_like = CommentReaction.objects.create(
            user=self.actor,
            comment=private_comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )

        ids, _ = self._fetch_ids(scope="following")
        self.assertNotIn(f"public_comment_like:{private_like.id}", ids)

    def test_following_returns_public_comment_dislikes_from_followed_users(self):
        self._add_follow(self.actor)
        comment = Comment.objects.create(
            author=self.friend,
            movie=self.movie,
            body="Comment disliked by followed actor",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        dislike = CommentReaction.objects.create(
            user=self.actor,
            comment=comment,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )

        ids, _ = self._fetch_ids(scope="following")
        self.assertIn(f"public_comment_dislike:{dislike.id}", ids)

    def test_friends_returns_public_comment_dislikes_from_accepted_friends(self):
        self._add_friendship(self.friend)
        comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Public comment disliked by friend",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        dislike = CommentReaction.objects.create(
            user=self.friend,
            comment=comment,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )

        ids, _ = self._fetch_ids(scope="friends")
        self.assertIn(f"public_comment_dislike:{dislike.id}", ids)

    def test_excludes_dislikes_on_private_direct_comments(self):
        self._add_follow(self.actor)
        private_comment = Comment.objects.create(
            author=self.friend,
            movie=self.movie,
            body="Private comment should not surface through dislikes",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.viewer,
        )
        private_dislike = CommentReaction.objects.create(
            user=self.actor,
            comment=private_comment,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )

        ids, _ = self._fetch_ids(scope="following")
        self.assertNotIn(f"public_comment_dislike:{private_dislike.id}", ids)

    def test_only_returns_allowed_activity_types(self):
        self._add_follow(self.actor)
        rating = MovieRating.objects.create(user=self.actor, movie=self.movie, score=8)
        comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Allowed public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        reaction = CommentReaction.objects.create(
            user=self.actor,
            comment=comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )
        dislike = CommentReaction.objects.create(
            user=self.actor,
            comment=comment,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )

        response = self.client.get(self.url, {"scope": "following"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        allowed_types = {"rating", "public_comment", "public_comment_like", "public_comment_dislike"}
        returned_types = {item["activity_type"] for item in response.data["results"]}
        self.assertTrue(returned_types.issubset(allowed_types))
        self.assertIn(f"rating:{rating.id}", [item["id"] for item in response.data["results"]])
        self.assertIn(f"public_comment:{comment.id}", [item["id"] for item in response.data["results"]])
        self.assertIn(f"public_comment_like:{reaction.id}", [item["id"] for item in response.data["results"]])
        self.assertIn(f"public_comment_dislike:{dislike.id}", [item["id"] for item in response.data["results"]])

    def test_orders_by_created_at_desc_with_stable_tie_breaker(self):
        self._add_follow(self.actor)
        baseline = timezone.now()

        newer_rating = MovieRating.objects.create(user=self.actor, movie=self.movie, score=5)
        older_comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie_without_image,
            body="Older public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        tie_comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Tie comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        tie_like = CommentReaction.objects.create(
            user=self.actor,
            comment=tie_comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )
        tie_dislike = CommentReaction.objects.create(
            user=self.actor,
            comment=tie_comment,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )

        newer_time = baseline + timezone.timedelta(minutes=2)
        older_time = baseline - timezone.timedelta(minutes=2)
        tie_time = baseline
        self._set_created_at(MovieRating, newer_rating.id, newer_time)
        self._set_created_at(Comment, older_comment.id, older_time)
        self._set_created_at(Comment, tie_comment.id, tie_time)
        self._set_created_at(CommentReaction, tie_like.id, tie_time)
        self._set_created_at(CommentReaction, tie_dislike.id, tie_time)

        ids_first, _ = self._fetch_ids(scope="following")
        ids_second, _ = self._fetch_ids(scope="following")

        self.assertEqual(ids_first, ids_second)
        self.assertEqual(ids_first[0], f"rating:{newer_rating.id}")
        self.assertLess(ids_first.index(f"public_comment_dislike:{tie_dislike.id}"), ids_first.index(f"public_comment_like:{tie_like.id}"))
        self.assertLess(ids_first.index(f"public_comment_like:{tie_like.id}"), ids_first.index(f"public_comment:{tie_comment.id}"))
        self.assertEqual(ids_first[-1], f"public_comment:{older_comment.id}")

    def test_tie_breaker_uses_numeric_entity_id_not_lexicographic_id(self):
        self._add_follow(self.actor)
        baseline = timezone.now()

        first_rating = MovieRating.objects.create(user=self.actor, movie=self.movie, score=4)
        second_rating = MovieRating.objects.create(user=self.actor, movie=self.movie_without_image, score=6)
        # Fuerza IDs con distinta longitud para detectar orden lexicográfico incorrecto (p.ej. 10 vs 2).
        extra_count = max(0, 10 - second_rating.id)
        for _ in range(extra_count):
            MovieRating.objects.create(user=self.friend, movie=self.movie, score=5)
        tenth_rating = MovieRating.objects.create(user=self.actor, movie=self.movie, score=7)

        self._set_created_at(MovieRating, first_rating.id, baseline)
        self._set_created_at(MovieRating, second_rating.id, baseline)
        self._set_created_at(MovieRating, tenth_rating.id, baseline)

        ids, _ = self._fetch_ids(scope="following")
        first_idx = ids.index(f"rating:{first_rating.id}")
        second_idx = ids.index(f"rating:{second_rating.id}")
        tenth_idx = ids.index(f"rating:{tenth_rating.id}")

        self.assertLess(tenth_idx, second_idx)
        self.assertLess(second_idx, first_idx)

    def test_scope_me_prioritizes_directed_comment_over_like_when_created_at_ties(self):
        baseline = timezone.now()
        directed = Comment.objects.create(
            author=self.viewer,
            movie=self.movie,
            body=f"@{self.actor.username} mensaje privado",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.actor,
        )
        public_comment = Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="Public comment to react",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        like = CommentReaction.objects.create(
            user=self.viewer,
            comment=public_comment,
            reaction_type=CommentReaction.REACT_LIKE,
        )

        self._set_created_at(Comment, directed.id, baseline)
        self._set_created_at(CommentReaction, like.id, baseline)

        response = self.client.get(self.url, {"scope": "me", "page_size": 1})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        first_item = response.data["results"][0]
        self.assertEqual(first_item["id"], f"directed_comment:{directed.id}")
        self.assertEqual(first_item["activity_type"], "directed_comment")

    def test_returns_standard_paginated_format(self):
        self._add_follow(self.actor)
        MovieRating.objects.create(user=self.actor, movie=self.movie, score=8)
        Comment.objects.create(
            author=self.actor,
            movie=self.movie,
            body="First public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        Comment.objects.create(
            author=self.actor,
            movie=self.movie_without_image,
            body="Second public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        response = self.client.get(self.url, {"scope": "following", "page_size": 2})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("count", response.data)
        self.assertIn("next", response.data)
        self.assertIn("previous", response.data)
        self.assertIn("results", response.data)
        self.assertEqual(len(response.data["results"]), 2)
        self.assertIsNotNone(response.data["next"])

        next_page = self.client.get(response.data["next"])
        self.assertEqual(next_page.status_code, status.HTTP_200_OK)
        self.assertIsNotNone(next_page.data["previous"])

    def test_actor_avatar_is_null_or_valid_string(self):
        self._add_follow(self.actor)
        no_avatar_rating = MovieRating.objects.create(user=self.actor, movie=self.movie, score=4)
        response = self.client.get(self.url, {"scope": "following"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        item_by_id = {item["id"]: item for item in response.data["results"]}
        self.assertIsNone(item_by_id[f"rating:{no_avatar_rating.id}"]["actor"]["avatar"])

        self.actor.profile.avatar = SimpleUploadedFile(
            "avatar.jpg",
            b"avatar-content",
            content_type="image/jpeg",
        )
        self.actor.profile.save(update_fields=["avatar"])
        avatar_rating = MovieRating.objects.create(user=self.actor, movie=self.movie_without_image, score=10)

        response = self.client.get(self.url, {"scope": "following"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        item_by_id = {item["id"]: item for item in response.data["results"]}
        self.assertIsInstance(item_by_id[f"rating:{avatar_rating.id}"]["actor"]["avatar"], str)
        self.assertTrue(item_by_id[f"rating:{avatar_rating.id}"]["actor"]["avatar"].startswith("http://testserver/media/avatars/"))

    def test_movie_image_contract_is_consistent_with_or_without_image(self):
        self._add_follow(self.actor)
        rating_with_image = MovieRating.objects.create(user=self.actor, movie=self.movie, score=8)
        rating_without_image = MovieRating.objects.create(user=self.actor, movie=self.movie_without_image, score=7)

        response = self.client.get(self.url, {"scope": "following"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        item_by_id = {item["id"]: item for item in response.data["results"]}

        self.assertEqual(
            item_by_id[f"rating:{rating_with_image.id}"]["movie"]["image"],
            "https://cdn.example.com/profile-feed.jpg",
        )
        self.assertIsNone(item_by_id[f"rating:{rating_without_image.id}"]["movie"]["image"])

    def test_movie_metadata_includes_type_genre_display_rating_and_my_rating(self):
        self._add_follow(self.actor)
        self.movie.genre = "Action, Comedy"
        self.movie.external_rating = 8.5
        self.movie.external_votes = 1200
        self.movie.save(update_fields=["genre", "external_rating", "external_votes", "genre_key"])
        MovieRating.objects.create(user=self.actor, movie=self.movie, score=9)
        MovieRating.objects.create(user=self.viewer, movie=self.movie, score=7)

        response = self.client.get(self.url, {"scope": "following"})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        first_item = response.data["results"][0]
        movie_payload = first_item["movie"]

        self.assertEqual(movie_payload["type"], Movie.MOVIE)
        self.assertEqual(movie_payload["genre"], "Action, Comedy")
        self.assertAlmostEqual(movie_payload["display_rating"], 8.485, places=3)
        self.assertEqual(movie_payload["my_rating"], 7)
        self.assertAlmostEqual(movie_payload["following_avg_rating"], 9.0)
        self.assertEqual(movie_payload["following_ratings_count"], 1)


class ProfilePrivacyVisibilityTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = get_user_model().objects.create_user(username="owneruser", email="owner@example.com", password="test1234")
        self.viewer = get_user_model().objects.create_user(username="vieweruser", email="viewer@example.com", password="test1234")
        self.friend = get_user_model().objects.create_user(username="frienduser", email="friend@example.com", password="test1234")
        self.third = get_user_model().objects.create_user(username="thirduser1", email="third@example.com", password="test1234")
        self.movie = Movie.objects.create(author=self.owner, title_english="Privacy Movie", type=Movie.MOVIE, external_rating=8.0)

    def test_profile_public_is_visible_to_authenticated_user(self):
        self.owner.profile.visibility = Profile.Visibility.PUBLIC
        self.owner.profile.save(update_fields=["visibility"])
        self.client.force_authenticate(self.viewer)
        response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_profile_private_is_not_visible_to_non_friend(self):
        self.owner.profile.visibility = Profile.Visibility.PRIVATE
        self.owner.profile.save(update_fields=["visibility"])
        self.client.force_authenticate(self.viewer)
        response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["can_view_full_profile"])
        self.assertEqual(response.data["profile_access"], "restricted")
        self.assertEqual(set(response.data.keys()), {"id", "username", "first_name", "last_name", "display_name", "avatar", "can_view_full_profile", "profile_access"})

    def test_profile_private_is_visible_to_accepted_friend(self):
        self.owner.profile.visibility = Profile.Visibility.PRIVATE
        self.owner.profile.save(update_fields=["visibility"])
        Friendship.objects.create(requester=self.owner, user1=self.owner, user2=self.friend, status=Friendship.STATUS_ACCEPTED)
        self.client.force_authenticate(self.friend)
        response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_blocked_user_cannot_view_public_profile_and_owner_can_view_self(self):
        self.owner.profile.visibility = Profile.Visibility.PUBLIC
        self.owner.profile.save(update_fields=["visibility"])
        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)

        self.client.force_authenticate(self.viewer)
        blocked_response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))
        self.assertEqual(blocked_response.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(self.owner)
        own_response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))
        self.assertEqual(own_response.status_code, status.HTTP_200_OK)

    def test_user_profile_exposes_public_personal_data_with_visibility_rules(self):
        self.owner.first_name = "Dennisse"
        self.owner.last_name = "Jamaica"
        self.owner.save(update_fields=["first_name", "last_name"])
        self.owner.profile.birth_date = timezone.now().date() - timedelta(days=365 * 36)
        self.owner.profile.birth_date_visible = True
        self.owner.profile.gender_identity = Profile.GenderIdentity.FEMALE
        self.owner.profile.gender_identity_visible = True
        self.owner.profile.save(update_fields=["birth_date", "birth_date_visible", "gender_identity", "gender_identity_visible"])

        self.client.force_authenticate(self.viewer)
        response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["first_name"], "Dennisse")
        self.assertEqual(response.data["last_name"], "Jamaica")
        self.assertEqual(response.data["gender_identity"], Profile.GenderIdentity.FEMALE)
        self.assertIsInstance(response.data["age"], int)
        self.assertNotIn("email", response.data)
        self.assertNotIn("birth_date", response.data)

    def test_user_profile_hides_age_and_gender_identity_when_not_visible(self):
        self.owner.first_name = "Dennisse"
        self.owner.last_name = "Jamaica"
        self.owner.save(update_fields=["first_name", "last_name"])
        self.owner.profile.birth_date = timezone.now().date() - timedelta(days=365 * 36)
        self.owner.profile.birth_date_visible = False
        self.owner.profile.gender_identity = Profile.GenderIdentity.FEMALE
        self.owner.profile.gender_identity_visible = False
        self.owner.profile.save(update_fields=["birth_date", "birth_date_visible", "gender_identity", "gender_identity_visible"])

        self.client.force_authenticate(self.viewer)
        response = self.client.get(reverse("user-profile", kwargs={"username": self.owner.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["first_name"], "Dennisse")
        self.assertEqual(response.data["last_name"], "Jamaica")
        self.assertIsNone(response.data["age"])
        self.assertIsNone(response.data["gender_identity"])

    def test_cannot_follow_private_profile(self):
        self.owner.profile.visibility = Profile.Visibility.PRIVATE
        self.owner.profile.save(update_fields=["visibility"])
        self.client.force_authenticate(self.viewer)
        response = self.client.post(reverse("follow-toggle", kwargs={"username": self.owner.username}))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_transition_public_to_private_removes_followers_but_keeps_outgoing_follows(self):
        target_public = get_user_model().objects.create_user(
            username="pubtarget",
            email="pubtarget@example.com",
            password="test1234",
        )
        follower_a = get_user_model().objects.create_user(
            username="followera",
            email="followera@example.com",
            password="test1234",
        )
        follower_b = get_user_model().objects.create_user(
            username="followerb",
            email="followerb@example.com",
            password="test1234",
        )
        Follow.objects.create(follower=follower_a, following=self.owner)
        Follow.objects.create(follower=follower_b, following=self.owner)
        Follow.objects.create(follower=self.owner, following=target_public)

        self.client.force_authenticate(self.owner)
        response = self.client.patch(
            reverse("profile-privacy"),
            {"visibility": Profile.Visibility.PRIVATE},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.owner.profile.refresh_from_db()
        self.assertEqual(self.owner.profile.visibility, Profile.Visibility.PRIVATE)
        self.assertFalse(self.owner.profile.is_public)
        self.assertFalse(Follow.objects.filter(following=self.owner).exists())
        self.assertTrue(Follow.objects.filter(follower=self.owner, following=target_public).exists())

    def test_private_author_public_comment_detail_visible_to_unblocked_user(self):
        self.owner.profile.visibility = Profile.Visibility.PRIVATE
        self.owner.profile.is_public = False
        self.owner.profile.save(update_fields=["visibility", "is_public"])
        comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="private author public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )

        self.client.force_authenticate(self.viewer)
        response = self.client.get(reverse("comment-detail", kwargs={"pk": comment.id}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["id"], comment.id)

    def test_private_author_public_comment_accepts_reactions_for_unblocked_user(self):
        self.owner.profile.visibility = Profile.Visibility.PRIVATE
        self.owner.profile.is_public = False
        self.owner.profile.save(update_fields=["visibility", "is_public"])
        comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="private author can still be reacted",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        self.client.force_authenticate(self.viewer)

        response = self.client.put(
            reverse("comment-reaction", kwargs={"pk": comment.id}),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["likes_count"], 1)
        self.assertTrue(CommentReaction.objects.filter(comment=comment, user=self.viewer).exists())

    def test_blocked_user_cannot_react_to_public_comment(self):
        comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="blocked user cannot react",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)
        self.client.force_authenticate(self.viewer)

        response = self.client.put(
            reverse("comment-reaction", kwargs={"pk": comment.id}),
            {"reaction": CommentReaction.REACT_LIKE},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertFalse(CommentReaction.objects.filter(comment=comment, user=self.viewer).exists())

    def test_autoblock_not_allowed_and_unique_constraint_works(self):
        self.client.force_authenticate(self.owner)
        response = self.client.post(reverse("profile-privacy-blocked-users"), {"user_id": self.owner.id}, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)
        with self.assertRaises(IntegrityError):
            UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)

    def test_movie_public_comments_are_hidden_only_for_blocked_viewer(self):
        hidden_comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="hidden from blocked",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        visible_comment = Comment.objects.create(
            author=self.friend,
            movie=self.movie,
            body="visible for blocked",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)

        self.client.force_authenticate(self.viewer)
        blocked_response = self.client.get(reverse("movie-comments", kwargs={"pk": self.movie.id}))
        blocked_ids = {item["id"] for item in blocked_response.data}
        self.assertNotIn(hidden_comment.id, blocked_ids)
        self.assertIn(visible_comment.id, blocked_ids)

        self.client.force_authenticate(self.third)
        third_response = self.client.get(reverse("movie-comments", kwargs={"pk": self.movie.id}))
        third_ids = {item["id"] for item in third_response.data}
        self.assertIn(hidden_comment.id, third_ids)
        self.assertIn(visible_comment.id, third_ids)

    def test_movie_aggregated_ratings_remain_unchanged_after_block(self):
        MovieRating.objects.create(user=self.owner, movie=self.movie, score=10)
        MovieRating.objects.create(user=self.friend, movie=self.movie, score=6)

        self.client.force_authenticate(self.viewer)
        before = self.client.get(reverse("movie-detail", kwargs={"pk": self.movie.id}))
        before_general = before.data["general_rating"]
        before_display = before.data["display_rating"]
        before_count = before.data["real_ratings_count"]

        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)
        after = self.client.get(reverse("movie-detail", kwargs={"pk": self.movie.id}))

        self.assertEqual(after.status_code, status.HTTP_200_OK)
        self.assertEqual(after.data["general_rating"], before_general)
        self.assertEqual(after.data["display_rating"], before_display)
        self.assertEqual(after.data["real_ratings_count"], before_count)

    def test_user_search_endpoint_returns_partial_matches_without_self_or_blocked_users(self):
        dennisse = get_user_model().objects.create_user(
            username="Dennisse",
            email="dennisse@example.com",
            password="test1234",
        )
        get_user_model().objects.create_user(
            username="dennys",
            email="dennys@example.com",
            password="test1234",
        )
        get_user_model().objects.create_user(
            username="den_owner_match",
            email="den-owner@example.com",
            password="test1234",
        )
        self.owner.username = "den_owner"
        self.owner.save(update_fields=["username"])
        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=dennisse)

        self.client.force_authenticate(self.owner)
        response = self.client.get(reverse("user-search"), {"q": "den"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsInstance(response.data, list)
        usernames = [item["username"] for item in response.data]
        self.assertIn("dennys", usernames)
        self.assertNotIn(self.owner.username, usernames)
        self.assertNotIn("Dennisse", usernames)
        self.assertEqual(set(response.data[0].keys()), {"id", "username"})

    def test_user_search_endpoint_is_case_insensitive(self):
        get_user_model().objects.create_user(
            username="Dennisse",
            email="dennisse-case@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.owner)

        response = self.client.get(reverse("user-search"), {"q": "dennisse"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["username"] for item in response.data], ["Dennisse"])

    def test_user_search_endpoint_accepts_at_prefix_and_normalizes_query(self):
        get_user_model().objects.create_user(
            username="Dennisse",
            email="dennisse-at@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.owner)

        response = self.client.get(reverse("user-search"), {"q": "@Dennisse"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["username"] for item in response.data], ["Dennisse"])

    def test_user_search_endpoint_returns_empty_list_when_query_is_blank_after_normalization(self):
        get_user_model().objects.create_user(
            username="Dennisse",
            email="dennisse-empty@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.owner)

        response = self.client.get(reverse("user-search"), {"q": "   @   "})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data, [])

    def test_user_search_endpoint_works_without_trailing_slash(self):
        get_user_model().objects.create_user(
            username="Dennisse",
            email="dennisse-noslash@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.owner)

        response = self.client.get("/api/users/search", {"q": "den"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("Dennisse", [item["username"] for item in response.data])

    def test_user_search_endpoint_route_does_not_conflict_with_user_profile_dynamic_route(self):
        search_user = get_user_model().objects.create_user(
            username="search",
            email="search-user@example.com",
            password="test1234",
        )
        self.client.force_authenticate(self.owner)

        search_response = self.client.get(reverse("user-search"), {"q": "sea"})
        profile_response = self.client.get(reverse("user-profile", kwargs={"username": search_user.username}))

        self.assertEqual(search_response.status_code, status.HTTP_200_OK)
        self.assertEqual(profile_response.status_code, status.HTTP_200_OK)
        self.assertIn("search", [item["username"] for item in search_response.data])
        self.assertEqual(profile_response.data["username"], "search")


class VisitedProfileDataEndpointsTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.viewer = get_user_model().objects.create_user(
            username="viewer_user",
            email="viewer@example.com",
            password="test1234",
        )
        self.visited = get_user_model().objects.create_user(
            username="dennisse",
            email="dennisse@example.com",
            password="test1234",
        )
        self.friend_one = get_user_model().objects.create_user(
            username="JulianHernandez",
            email="julianh@example.com",
            password="test1234",
        )
        self.friend_two = get_user_model().objects.create_user(
            username="Julian",
            email="julian@example.com",
            password="test1234",
        )
        self.other_actor = get_user_model().objects.create_user(
            username="other_actor",
            email="other-actor@example.com",
            password="test1234",
        )
        self.author = get_user_model().objects.create_user(
            username="catalog_owner",
            email="catalog@example.com",
            password="test1234",
        )
        self.movie_1 = Movie.objects.create(author=self.author, title_english="Movie A", type=Movie.MOVIE, release_year=2020)
        self.movie_2 = Movie.objects.create(author=self.author, title_english="Movie B", type=Movie.MOVIE, release_year=2021)
        self.movie_3 = Movie.objects.create(author=self.author, title_english="Movie C", type=Movie.SERIES, release_year=2022)

        Friendship.objects.create(
            requester=self.visited,
            user1=min(self.visited, self.friend_one, key=lambda u: u.id),
            user2=max(self.visited, self.friend_one, key=lambda u: u.id),
            status=Friendship.STATUS_ACCEPTED,
        )
        Friendship.objects.create(
            requester=self.friend_two,
            user1=min(self.visited, self.friend_two, key=lambda u: u.id),
            user2=max(self.visited, self.friend_two, key=lambda u: u.id),
            status=Friendship.STATUS_ACCEPTED,
        )
        ProfileFavoriteMovie.objects.create(user=self.visited, slot=1, movie=self.movie_1)
        ProfileFavoriteMovie.objects.create(user=self.visited, slot=2, movie=self.movie_2)
        ProfileFavoriteMovie.objects.create(user=self.visited, slot=3, movie=self.movie_3)

        self.visited_rating = MovieRating.objects.create(user=self.visited, movie=self.movie_1, score=9)
        self.visited_comment = Comment.objects.create(
            author=self.visited,
            movie=self.movie_2,
            body="Comentario público de Dennisse",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        self.other_rating = MovieRating.objects.create(user=self.other_actor, movie=self.movie_3, score=6)

        self.client.force_authenticate(self.viewer)

    def test_user_friends_returns_accepted_friendships_for_visited_user(self):
        response = self.client.get(reverse("user-friends", kwargs={"username": self.visited.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        usernames = {item["username"] for item in response.data["results"]}
        self.assertEqual(usernames, {"JulianHernandez", "Julian"})

    def test_user_favorites_returns_visited_user_favorite_slots(self):
        response = self.client.get(reverse("user-favorites", kwargs={"username": self.visited.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["slot"] for item in response.data], [1, 2, 3])
        self.assertEqual(response.data[0]["movie"]["id"], self.movie_1.id)
        self.assertEqual(response.data[1]["movie"]["id"], self.movie_2.id)
        self.assertEqual(response.data[2]["movie"]["id"], self.movie_3.id)

    def test_user_movie_recommendations_returns_visited_user_items(self):
        MovieRecommendationItem.objects.create(user=self.visited, movie=self.movie_1)
        MovieRecommendationItem.objects.create(user=self.visited, movie=self.movie_2)
        MovieListItem.objects.create(user=self.visited, movie=self.movie_3)

        response = self.client.get(reverse("user-movie-recommendations", kwargs={"username": self.visited.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in response.data], [self.movie_2.id, self.movie_1.id])
        self.assertEqual(set(response.data[0].keys()), {"id", "title_spanish", "title_english", "image"})

    def test_user_movie_recommendations_private_profile_requires_access(self):
        private_user = get_user_model().objects.create_user(
            username="private_reco_owner",
            email="private-reco-owner@example.com",
            password="test1234",
        )
        private_user.profile.visibility = Profile.Visibility.PRIVATE
        private_user.profile.is_public = False
        private_user.profile.save(update_fields=["visibility", "is_public"])
        MovieRecommendationItem.objects.create(user=private_user, movie=self.movie_1)

        response = self.client.get(reverse("user-movie-recommendations", kwargs={"username": private_user.username}))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        Friendship.objects.create(
            requester=private_user,
            user1=min(private_user, self.viewer, key=lambda u: u.id),
            user2=max(private_user, self.viewer, key=lambda u: u.id),
            status=Friendship.STATUS_ACCEPTED,
        )
        allowed_response = self.client.get(reverse("user-movie-recommendations", kwargs={"username": private_user.username}))
        self.assertEqual(allowed_response.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in allowed_response.data], [self.movie_1.id])

    def test_profile_favorites_own_semantics_remain_for_authenticated_user(self):
        followed = get_user_model().objects.create_user(
            username="followed_for_viewer",
            email="followed-for-viewer@example.com",
            password="test1234",
        )
        Follow.objects.create(follower=self.viewer, following=followed)
        MovieRating.objects.create(user=self.viewer, movie=self.movie_1, score=4)
        MovieRating.objects.create(user=followed, movie=self.movie_1, score=7)

        response = self.client.get(reverse("profile-favorites"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        movie_payload = response.data[0]["movie"]
        self.assertEqual(movie_payload["my_rating"], 4)
        self.assertEqual(movie_payload["owner_rating"], 4)
        self.assertAlmostEqual(movie_payload["following_avg_rating"], 7.0)
        self.assertEqual(movie_payload["following_ratings_count"], 1)

    def test_user_favorites_replicates_visited_profile_feed_perspective(self):
        Follow.objects.create(follower=self.visited, following=self.friend_one)
        Follow.objects.create(follower=self.visited, following=self.friend_two)
        MovieRating.objects.create(user=self.friend_one, movie=self.movie_1, score=5)
        MovieRating.objects.create(user=self.friend_two, movie=self.movie_1, score=7)
        MovieRating.objects.create(user=self.viewer, movie=self.movie_1, score=1)

        self.client.force_authenticate(self.visited)
        own_response = self.client.get(reverse("profile-favorites"))
        self.client.force_authenticate(self.viewer)
        visited_response = self.client.get(reverse("user-favorites", kwargs={"username": self.visited.username}))

        self.assertEqual(own_response.status_code, status.HTTP_200_OK)
        self.assertEqual(visited_response.status_code, status.HTTP_200_OK)
        own_movie = own_response.data[0]["movie"]
        visited_movie = visited_response.data[0]["movie"]
        self.assertEqual(visited_movie["following_avg_rating"], own_movie["following_avg_rating"])
        self.assertEqual(visited_movie["following_ratings_count"], own_movie["following_ratings_count"])
        self.assertEqual(visited_movie["my_rating"], own_movie["my_rating"])
        self.assertEqual(visited_movie["owner_rating"], own_movie["my_rating"])
        self.assertAlmostEqual(visited_movie["owner_following_avg_rating"], own_movie["following_avg_rating"])
        self.assertEqual(visited_movie["owner_following_ratings_count"], own_movie["following_ratings_count"])
        self.assertNotEqual(visited_movie["my_rating"], 1)

    def test_user_favorites_friend_access_without_follow_keeps_visited_perspective(self):
        private_user = get_user_model().objects.create_user(
            username="private_friend_owner",
            email="private-friend-owner@example.com",
            password="test1234",
        )
        private_user.profile.visibility = Profile.Visibility.PRIVATE
        private_user.profile.is_public = False
        private_user.profile.save(update_fields=["visibility", "is_public"])
        friend_actor = get_user_model().objects.create_user(
            username="friend_actor_for_private",
            email="friend-actor-private@example.com",
            password="test1234",
        )
        Friendship.objects.create(
            requester=private_user,
            user1=min(private_user, self.viewer, key=lambda u: u.id),
            user2=max(private_user, self.viewer, key=lambda u: u.id),
            status=Friendship.STATUS_ACCEPTED,
        )
        Follow.objects.create(follower=private_user, following=friend_actor)
        private_movie = Movie.objects.create(author=self.author, title_english="Private Favorite", type=Movie.MOVIE)
        ProfileFavoriteMovie.objects.create(user=private_user, slot=1, movie=private_movie)
        MovieRating.objects.create(user=private_user, movie=private_movie, score=9)
        MovieRating.objects.create(user=friend_actor, movie=private_movie, score=7)

        response = self.client.get(reverse("user-favorites", kwargs={"username": private_user.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        movie_payload = response.data[0]["movie"]
        self.assertEqual(movie_payload["my_rating"], 9)
        self.assertEqual(movie_payload["owner_rating"], 9)
        self.assertAlmostEqual(movie_payload["following_avg_rating"], 7.0)
        self.assertEqual(movie_payload["following_ratings_count"], 1)

    def test_private_profile_without_access_returns_restricted_flag(self):
        private_user = get_user_model().objects.create_user(
            username="private_no_access",
            email="private-no-access@example.com",
            password="test1234",
        )
        private_user.profile.visibility = Profile.Visibility.PRIVATE
        private_user.profile.is_public = False
        private_user.profile.save(update_fields=["visibility", "is_public"])

        response = self.client.get(reverse("user-profile", kwargs={"username": private_user.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data["can_view_full_profile"])
        self.assertEqual(response.data["profile_access"], "restricted")
        self.assertEqual(response.data["username"], private_user.username)

    def test_user_activity_returns_only_visited_user_public_activity(self):
        response = self.client.get(reverse("user-activity", kwargs={"username": self.visited.username}))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        activity_ids = {item["id"] for item in response.data["results"]}
        self.assertIn(f"rating:{self.visited_rating.id}", activity_ids)
        self.assertIn(f"public_comment:{self.visited_comment.id}", activity_ids)
        self.assertNotIn(f"rating:{self.other_rating.id}", activity_ids)


class PersonalDataEndpointTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user_model = get_user_model()

    def test_register_requires_and_persists_first_name_and_last_name(self):
        url = reverse("register")
        adult_birth_date = (timezone.now().date() - timedelta(days=365 * 20)).isoformat()
        payload = {
            "username": "newuser01",
            "email": "newuser01@example.com",
            "password": "strongpass123",
            "password_confirmation": "strongpass123",
            "first_name": "Ada",
            "last_name": "Lovelace",
            "birth_date": adult_birth_date,
        }

        response = self.client.post(url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        created = self.user_model.objects.get(username="newuser01")
        self.assertEqual(created.first_name, "Ada")
        self.assertEqual(created.last_name, "Lovelace")
        self.assertEqual(str(created.profile.birth_date), adult_birth_date)
        self.assertTrue(created.profile.birth_date_locked)
        self.assertEqual(response.data["user"]["first_name"], "Ada")
        self.assertEqual(response.data["user"]["last_name"], "Lovelace")
        self.assertIn("token", response.data)

    def test_register_accepts_syntactically_valid_nonstandard_domain(self):
        url = reverse("register")
        adult_birth_date = (timezone.now().date() - timedelta(days=365 * 20)).isoformat()
        payload = {
            "username": "localdom1",
            "email": "usuario@dominio.local",
            "password": "strongpass123",
            "password_confirmation": "strongpass123",
            "first_name": "Local",
            "last_name": "Domain",
            "birth_date": adult_birth_date,
        }

        response = self.client.post(url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        created = self.user_model.objects.get(username="localdom1")
        self.assertEqual(created.email, "usuario@dominio.local")

    def test_register_rejects_underage_birth_date(self):
        url = reverse("register")
        underage_birth_date = (timezone.now().date() - timedelta(days=365 * 12)).isoformat()
        payload = {
            "username": "younguser1",
            "email": "younguser1@example.com",
            "password": "strongpass123",
            "password_confirmation": "strongpass123",
            "first_name": "Mini",
            "last_name": "User",
            "birth_date": underage_birth_date,
        }

        response = self.client.post(url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("birth_date", response.data)
        self.assertIn("Debes tener al menos 13 años para registrarte.", response.data["birth_date"])

    def test_register_rejects_future_birth_date(self):
        url = reverse("register")
        future_birth_date = (timezone.now().date() + timedelta(days=1)).isoformat()
        payload = {
            "username": "futureuser",
            "email": "futureuser@example.com",
            "password": "strongpass123",
            "password_confirmation": "strongpass123",
            "first_name": "Future",
            "last_name": "User",
            "birth_date": future_birth_date,
        }

        response = self.client.post(url, payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("birth_date", response.data)

    def test_personal_data_birth_date_initial_set_locks_field(self):
        user = self.user_model.objects.create_user(
            username="personaluser",
            email="personaluser@example.com",
            password="test1234",
            first_name="Test",
            last_name="User",
        )
        self.client.force_authenticate(user=user)
        url = reverse("me-personal-data")

        response = self.client.patch(url, {"birth_date": "1990-06-15"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        user.refresh_from_db()
        self.assertEqual(str(user.profile.birth_date), "1990-06-15")
        self.assertTrue(user.profile.birth_date_locked)

    def test_personal_data_birth_date_cannot_change_after_lock(self):
        user = self.user_model.objects.create_user(
            username="lockedbirth",
            email="lockedbirth@example.com",
            password="test1234",
            first_name="Locked",
            last_name="Birth",
        )
        user.profile.birth_date = datetime(1992, 3, 10).date()
        user.profile.birth_date_locked = True
        user.profile.save(update_fields=["birth_date", "birth_date_locked"])
        self.client.force_authenticate(user=user)
        url = reverse("me-personal-data")

        response = self.client.patch(url, {"birth_date": "1994-04-10"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("birth_date", response.data)

    def test_personal_data_age_is_computed_from_birth_date(self):
        user = self.user_model.objects.create_user(
            username="ageuser123",
            email="ageuser@example.com",
            password="test1234",
            first_name="Age",
            last_name="User",
        )
        birth_date = datetime(2000, 1, 2).date()
        user.profile.birth_date = birth_date
        user.profile.birth_date_locked = True
        user.profile.save(update_fields=["birth_date", "birth_date_locked"])
        self.client.force_authenticate(user=user)
        url = reverse("me-personal-data")

        response = self.client.get(url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        now = timezone.now().date()
        expected_age = now.year - birth_date.year - (
            (now.month, now.day) < (birth_date.month, birth_date.day)
        )
        self.assertEqual(response.data["age"], expected_age)

    def test_legacy_user_without_birth_date_still_works_in_personal_data(self):
        user = self.user_model.objects.create_user(
            username="legacyuser",
            email="legacyuser@example.com",
            password="test1234",
            first_name="Legacy",
            last_name="User",
        )
        user.profile.birth_date = None
        user.profile.birth_date_locked = False
        user.profile.save(update_fields=["birth_date", "birth_date_locked"])
        self.client.force_authenticate(user=user)
        url = reverse("me-personal-data")

        get_response = self.client.get(url)
        self.assertEqual(get_response.status_code, status.HTTP_200_OK)
        self.assertIsNone(get_response.data["birth_date"])
        self.assertIsNone(get_response.data["age"])

        set_response = self.client.patch(url, {"birth_date": "1991-05-20"}, format="json")
        self.assertEqual(set_response.status_code, status.HTTP_200_OK)

        user.refresh_from_db()
        self.assertEqual(str(user.profile.birth_date), "1991-05-20")
        self.assertTrue(user.profile.birth_date_locked)
