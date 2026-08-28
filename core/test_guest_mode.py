from django.contrib.auth import get_user_model
from django.core.files.base import ContentFile
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from .models import Comment, CommentReaction, Movie, Profile, VideoComment


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
