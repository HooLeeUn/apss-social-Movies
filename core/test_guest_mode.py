from django.contrib.auth import get_user_model
from django.core.files.base import ContentFile
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from .models import Comment, Movie, Profile, VideoComment


User = get_user_model()


class GuestModeTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = User.objects.create_user(username="public-owner", password="password")
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
