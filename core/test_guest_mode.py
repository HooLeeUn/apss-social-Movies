from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.core.files.base import ContentFile
from django.db import connection
from django.db.models import F
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient, APIRequestFactory

from .models import Comment, CommentReaction, Movie, MovieRating, Profile, VideoComment
from .serializers import MovieListSerializer


User = get_user_model()


class GuestModeTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = User.objects.create_user(username="CatherineFX", password="password")
        self.reactor = User.objects.create_user(username="public-reactor", password="password")
        self.private_owner = User.objects.create_user(username="private-owner", password="password")
        self.private_owner.profile.visibility = Profile.Visibility.PRIVATE
        self.private_owner.profile.is_public = False
        self.private_owner.profile.save(update_fields=["visibility", "is_public"])
        self.movie = Movie.objects.create(
            author=self.owner,
            title_english="Public movie",
            external_rating="8.0",
            external_votes=100,
        )
        self.comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="Public comment",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        self.public_reaction = CommentReaction.objects.create(
            comment=self.comment,
            user=self.reactor,
            reaction_type=CommentReaction.REACT_LIKE,
        )
        self.directed_comment = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            target_user=self.reactor,
            body="Private directed comment",
            visibility=Comment.VISIBILITY_MENTIONED,
        )
        self.private_reaction = CommentReaction.objects.create(
            comment=self.directed_comment,
            user=self.reactor,
            reaction_type=CommentReaction.REACT_DISLIKE,
        )
        self.video = VideoComment.objects.create(
            user=self.owner,
            movie=self.movie,
            video=ContentFile(b"video", name="guest.mp4"),
            duration_seconds=1,
            mime_type="video/mp4",
            file_size=5,
        )

    def test_anonymous_public_read_endpoints(self):
        urls = [
            reverse("feed-movies"),
            reverse("movie-detail", args=[self.movie.id]),
            reverse("movie-comments", args=[self.movie.id]),
            reverse("movie-video-comments", args=[self.movie.id]),
            reverse("video-comment-detail", args=[self.video.id]),
            reverse("public-comments-feed"),
            reverse("user-profile", args=[self.owner.username]),
            reverse("user-activity", args=[self.owner.username]),
            reverse("user-video-reactions", args=[self.owner.username]),
            reverse("user-favorites", args=[self.owner.username]),
            reverse("user-movie-recommendations", args=[self.owner.username]),
        ]

        for url in urls:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_anonymous_movie_fields_are_neutral(self):
        response = self.client.get(reverse("movie-detail", args=[self.movie.id]))

        self.assertIsNone(response.data["my_rating"])
        self.assertFalse(response.data["is_in_my_list"])
        self.assertFalse(response.data["is_in_my_recommendations"])
        self.assertEqual(response.data["following_ratings_count"], 0)

    def test_anonymous_movie_feed_is_paginated_and_does_not_query_private_user_data(self):
        with CaptureQueriesContext(connection) as queries:
            response = self.client.get(reverse("feed-movies"))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("results", response.data)
        self.assertGreaterEqual(len(response.data["results"]), 1)
        self.assertLessEqual(len(queries), 3)
        pagination_sql = "\n".join(query["sql"].lower() for query in queries.captured_queries[:2])
        self.assertNotIn("avg(", pagination_sql)
        self.assertNotIn("group by", pagination_sql)
        self.assertNotIn("core_comment", pagination_sql)
        sql = "\n".join(query["sql"].lower() for query in queries.captured_queries)
        for private_table in (
            "core_userdailyfeedpool",
            "core_userdailyfeedcandidate",
            "core_usertasteprofile",
            "core_movielistitem",
            "core_movierecommendationitem",
            "core_follow",
            "core_friendship",
        ):
            self.assertNotIn(private_table, sql)
        self.assertNotIn('core_movierating"."user_id" =', sql)

    def test_anonymous_movie_feed_prioritizes_persisted_rating_then_poster(self):
        unrated_with_poster = Movie.objects.create(
            author=self.owner,
            title_english="Unrated with poster",
            type=Movie.MOVIE,
            external_rating=None,
            image="https://example.com/unrated.jpg",
        )
        lower_rated_with_poster = Movie.objects.create(
            author=self.owner,
            title_english="Lower rated with poster",
            type=Movie.SERIES,
            external_rating="7.8",
            image="https://example.com/lower.jpg",
        )
        equal_rated_without_poster = Movie.objects.create(
            author=self.owner,
            title_english="Equal rated without poster",
            type=Movie.MOVIE,
            external_rating="9.1",
            image="",
        )
        equal_rated_with_poster = Movie.objects.create(
            author=self.owner,
            title_english="Equal rated with poster",
            type=Movie.SERIES,
            external_rating="9.1",
            image="https://example.com/top.jpg",
        )

        response = self.client.get(reverse("feed-movies"), {"page_size": 50})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("results", response.data)
        result_ids = [item["id"] for item in response.data["results"]]
        self.assertLess(
            result_ids.index(equal_rated_with_poster.id),
            result_ids.index(equal_rated_without_poster.id),
        )
        self.assertLess(
            result_ids.index(equal_rated_without_poster.id),
            result_ids.index(lower_rated_with_poster.id),
        )
        self.assertLess(
            result_ids.index(lower_rated_with_poster.id),
            result_ids.index(unrated_with_poster.id),
        )
        returned_types = {item["type"] for item in response.data["results"]}
        self.assertIn(Movie.MOVIE, returned_types)
        self.assertIn(Movie.SERIES, returned_types)

    def test_anonymous_movie_feed_supports_single_and_repeated_genres(self):
        action_scifi = Movie.objects.create(
            author=self.owner,
            title_english="Action Sci-Fi movie",
            genre="Action, Sci-Fi",
            external_rating="7.5",
            external_votes=50,
        )

        single = self.client.get(reverse("feed-movies"), {"genres": "Action"})
        repeated = self.client.get(f'{reverse("feed-movies")}?genres=Action&genres=Sci-Fi')

        self.assertEqual(single.status_code, status.HTTP_200_OK)
        self.assertEqual(repeated.status_code, status.HTTP_200_OK)
        self.assertIn(action_scifi.id, [item["id"] for item in single.data["results"]])
        self.assertEqual([item["id"] for item in repeated.data["results"]], [action_scifi.id])

    def test_authenticated_movie_feed_keeps_personalized_fields(self):
        MovieRating.objects.create(user=self.owner, movie=self.movie, score=9)
        self.client.force_authenticate(self.owner)

        response = self.client.get(reverse("feed-movies"), {"exclude_rated": "false"})

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        movie = next(item for item in response.data["results"] if item["id"] == self.movie.id)
        self.assertEqual(movie["my_rating"], 9)
        self.assertIn("is_in_my_list", movie)
        self.assertIn("is_in_my_recommendations", movie)

    def test_movie_list_serializer_accepts_anonymous_request(self):
        request = APIRequestFactory().get(reverse("feed-movies"))
        request.user = AnonymousUser()
        movie = (
            Movie.objects.filter(pk=self.movie.pk)
            .with_display_rating()
            .with_my_rating(request.user)
            .with_in_my_list(request.user)
            .with_in_my_recommendations(request.user)
            .with_comment_stats()
            .with_following_rating_stats(request.user)
            .annotate(general_rating=F("display_rating"))
            .get()
        )

        payload = MovieListSerializer(movie, context={"request": request}).data

        self.assertIsNone(payload["my_rating"])
        self.assertFalse(payload["is_in_my_list"])
        self.assertFalse(payload["is_in_my_recommendations"])

    def test_anonymous_private_profile_and_public_subresources_are_blocked(self):
        urls = [
            reverse("user-profile", args=[self.private_owner.username]),
            reverse("user-activity", args=[self.private_owner.username]),
            reverse("user-video-reactions", args=[self.private_owner.username]),
            reverse("user-favorites", args=[self.private_owner.username]),
            reverse("user-movie-recommendations", args=[self.private_owner.username]),
        ]

        for url in urls:
            with self.subTest(url=url):
                self.assertEqual(self.client.get(url).status_code, status.HTTP_403_FORBIDDEN)

    def test_anonymous_writes_remain_protected(self):
        attempts = [
            ("put", reverse("movie-rating", args=[self.movie.id]), {"score": 8}),
            ("post", reverse("movie-comments", args=[self.movie.id]), {"body": "No"}),
            ("post", reverse("comments-directed"), {"body": "No"}),
            ("post", reverse("movie-video-comments", args=[self.movie.id]), {}),
            ("put", reverse("comment-reaction", args=[self.comment.id]), {"reaction": "like"}),
            ("put", reverse("video-comment-reaction", args=[self.video.id]), {"reaction": "like"}),
            ("post", reverse("user-follow", args=[self.owner.username]), {}),
            ("post", reverse("user-friend-request", args=[self.owner.username]), {}),
            ("post", reverse("movie-list-toggle", args=[self.movie.id]), {}),
            ("post", reverse("movie-recommendation-toggle", args=[self.movie.id]), {}),
        ]

        for method, url, payload in attempts:
            with self.subTest(method=method, url=url):
                response = getattr(self.client, method)(url, payload, format="json")
                self.assertIn(response.status_code, {status.HTTP_401_UNAUTHORIZED, status.HTTP_403_FORBIDDEN})

    def test_authenticated_movie_detail_keeps_personalized_shape(self):
        self.client.force_authenticate(self.owner)
        response = self.client.get(reverse("movie-detail", args=[self.movie.id]))

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIn("my_rating", response.data)
        self.assertIn("is_in_my_list", response.data)
        self.assertIn("is_in_my_recommendations", response.data)

    def test_public_comment_reactions_have_same_base_set_for_guest_and_authenticated_viewer(self):
        url = reverse("user-activity", args=[self.owner.username])
        params = {"activity_type": "public_comment_reaction"}

        anonymous_response = self.client.get(url, params)
        self.client.force_authenticate(self.reactor)
        authenticated_response = self.client.get(url, params)

        self.assertEqual(anonymous_response.status_code, status.HTTP_200_OK)
        self.assertGreater(anonymous_response.data["count"], 0)
        anonymous_ids = {item["id"] for item in anonymous_response.data["results"]}
        authenticated_ids = {item["id"] for item in authenticated_response.data["results"]}
        expected_id = f"public_comment_reaction:{self.public_reaction.id}"
        self.assertIn(expected_id, anonymous_ids)
        self.assertEqual(anonymous_ids, authenticated_ids)
        self.assertNotIn(
            f"private_comment_reaction:{self.private_reaction.id}",
            anonymous_ids,
        )

    def test_private_profile_public_comment_reactions_remain_hidden_from_guest(self):
        private_comment = Comment.objects.create(
            author=self.private_owner,
            movie=self.movie,
            body="Public comment on a private profile",
            visibility=Comment.VISIBILITY_PUBLIC,
        )
        CommentReaction.objects.create(
            comment=private_comment,
            user=self.reactor,
            reaction_type=CommentReaction.REACT_LIKE,
        )

        response = self.client.get(
            reverse("user-activity", args=[self.private_owner.username]),
            {"activity_type": "public_comment_reaction"},
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
