from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Follow, Movie, UserVisibilityBlock, VideoComment


class FollowingVideoReactionsTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.viewer = self.make_user("viewer")
        self.followed = self.make_user("followed")
        self.not_followed = self.make_user("not_followed")
        self.movie = Movie.objects.create(
            author=self.viewer,
            title_spanish="Película",
            title_english="Movie",
            type=Movie.MOVIE,
            image="posters/movie.jpg",
        )
        Follow.objects.create(follower=self.viewer, following=self.followed)
        self.client.force_authenticate(self.viewer)
        self.url = reverse("following-video-reactions")

    @staticmethod
    def make_user(username):
        return get_user_model().objects.create_user(username=username, password="test1234")

    def make_video(self, user, filename, *, hidden=False):
        return VideoComment.objects.create(
            user=user,
            movie=self.movie,
            video=SimpleUploadedFile(filename, b"video", content_type="video/mp4"),
            duration_seconds=12,
            mime_type="video/mp4",
            file_size=5,
            is_hidden=hidden,
        )

    def result_ids(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return response, [item["id"] for item in response.data["results"]]

    def test_requires_authentication(self):
        self.client.force_authenticate(user=None)
        self.assertEqual(self.client.get(self.url).status_code, status.HTTP_401_UNAUTHORIZED)

    def test_returns_followed_video_with_complete_rendering_context(self):
        video = self.make_video(self.followed, "followed.mp4")

        response, ids = self.result_ids()

        self.assertEqual(ids, [video.id])
        item = response.data["results"][0]
        self.assertEqual(item["user"]["id"], self.followed.id)
        self.assertEqual(item["user"]["username"], self.followed.username)
        self.assertIn("avatar", item["user"])
        self.assertTrue(item["video_url"].endswith(video.video.url))
        self.assertEqual(item["movie"]["id"], self.movie.id)
        self.assertEqual(item["movie"]["title_spanish"], "Película")
        self.assertEqual(item["movie"]["title_english"], "Movie")
        self.assertEqual(item["movie"]["type"], Movie.MOVIE)
        self.assertIn("image", item["movie"])
        self.assertEqual(item["likes_count"], 0)
        self.assertEqual(item["dislikes_count"], 0)
        self.assertIsNone(item["my_reaction"])

    def test_excludes_not_followed_own_and_hidden_videos(self):
        visible = self.make_video(self.followed, "visible.mp4")
        self.make_video(self.not_followed, "not-followed.mp4")
        self.make_video(self.viewer, "own.mp4")
        self.make_video(self.followed, "hidden.mp4", hidden=True)

        _, ids = self.result_ids()

        self.assertEqual(ids, [visible.id])

    def test_restriction_in_either_direction_is_bilateral_and_removable(self):
        video = self.make_video(self.followed, "blocked.mp4")
        for owner, blocked in (
            (self.viewer, self.followed),
            (self.followed, self.viewer),
        ):
            block = UserVisibilityBlock.objects.create(owner=owner, blocked_user=blocked)
            self.assertNotIn(video.id, self.result_ids()[1])
            block.delete()
            self.assertIn(video.id, self.result_ids()[1])

    def test_orders_newest_first(self):
        older = self.make_video(self.followed, "older.mp4")
        newer = self.make_video(self.followed, "newer.mp4")
        now = timezone.now()
        VideoComment.objects.filter(pk=older.pk).update(created_at=now - timedelta(days=1))
        VideoComment.objects.filter(pk=newer.pk).update(created_at=now)

        self.assertEqual(self.result_ids()[1], [newer.id, older.id])

    def test_uses_default_profile_feed_pagination(self):
        videos = [self.make_video(self.followed, f"video-{index}.mp4") for index in range(11)]

        first = self.client.get(self.url)
        second = self.client.get(self.url, {"page": 2})

        self.assertEqual(first.status_code, status.HTTP_200_OK)
        self.assertEqual(first.data["count"], 11)
        self.assertEqual(len(first.data["results"]), 10)
        self.assertIsNotNone(first.data["next"])
        self.assertEqual(second.status_code, status.HTTP_200_OK)
        self.assertEqual([item["id"] for item in second.data["results"]], [videos[0].id])

    def test_existing_profile_activity_route_is_unchanged(self):
        self.assertEqual(reverse("profile-feed-activity"), "/api/profile-feed/activity/")
