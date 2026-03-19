import io
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from core.models import (
    Movie,
    MovieRating,
    build_genre_key,
    UserDirectorPreference,
    UserGenrePreference,
    UserTasteProfile,
    UserTypePreference,
)
from core.services import (
    remove_user_preferences_for_movie_rating,
    update_user_preferences_for_movie_rating,
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
        self.client = APIClient()
        self.user = get_user_model().objects.create_user(
            username="feed_user", email="feed@example.com", password="test1234"
        )
        self.author = get_user_model().objects.create_user(
            username="catalog_user", email="catalog@example.com", password="test1234"
        )
        self.url = reverse("feed-movies")

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
        rated_movie = Movie.objects.create(
            author=self.author,
            title_english="Already Rated",
            genre="Action, Comedy",
            type=Movie.MOVIE,
            release_year=2021,
            external_rating=7.0,
        )
        fresh_movie = Movie.objects.create(
            author=self.author,
            title_english="Fresh Pick",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=2022,
            external_rating=8.0,
        )
        MovieRating.objects.create(user=self.user, movie=rated_movie, score=9)

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles, [fresh_movie.title_english])

    def test_feed_orders_by_recommendation_score(self):
        preferred_movie = Movie.objects.create(
            author=self.author,
            title_english="Preferred Combo",
            genre="Comedy, Action",
            type=Movie.MOVIE,
            director="Christopher Nolan",
            release_year=2024,
            external_rating=6.0,
        )
        Movie.objects.create(
            author=self.author,
            title_english="Higher External Rating",
            genre="Drama",
            type=Movie.SERIES,
            director="Other Director",
            release_year=2025,
            external_rating=9.5,
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
        high_rated = Movie.objects.create(
            author=self.author,
            title_english="Top Rated",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=2020,
            external_rating=9.1,
        )
        Movie.objects.create(
            author=self.author,
            title_english="Lower Rated",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=2024,
            external_rating=7.2,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[0], high_rated.title_english)

    def test_feed_orders_null_release_years_last(self):
        recent_movie = Movie.objects.create(
            author=self.author,
            title_english="Recent Year",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=2024,
            external_rating=8.0,
        )
        null_year_movie = Movie.objects.create(
            author=self.author,
            title_english="Unknown Year",
            genre="Drama",
            type=Movie.MOVIE,
            release_year=None,
            external_rating=8.0,
        )

        self.client.force_authenticate(user=self.user)
        response = self.client.get(self.url, {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        titles = [item["title_english"] for item in response.data["results"]]
        self.assertEqual(titles[:2], [recent_movie.title_english, null_year_movie.title_english])

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
        update_user_preferences_for_movie_rating(user=self.user, movie=self.movie, new_score=7)

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

        with patch("core.views.update_user_preferences_for_movie_rating", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                self.client.put(self.url, {"score": 8}, format="json")

        self.assertFalse(MovieRating.objects.filter(user=self.user, movie=self.movie).exists())

    def test_delete_rolls_back_rating_delete_when_preference_update_fails(self):
        self.client.force_authenticate(user=self.user)
        rating = MovieRating.objects.create(user=self.user, movie=self.movie, score=8)

        with patch("core.views.remove_user_preferences_for_movie_rating", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                self.client.delete(self.url)

        self.assertTrue(MovieRating.objects.filter(pk=rating.pk).exists())


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
