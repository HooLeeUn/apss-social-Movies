from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Friendship, Movie, Profile, VideoComment


class VisitedProfileVideoReactionTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.viewer = self.make_user("ProfileViewer")
        self.catherine = self.make_user("CatherineFX")
        self.other_user = self.make_user("Dennisse.Jamaica")
        self.movie_one = Movie.objects.create(
            author=self.viewer,
            title_spanish="Intensa mente 2",
            title_english="Inside Out 2",
            type=Movie.MOVIE,
            image="posters/inside-out-2.jpg",
        )
        self.movie_two = Movie.objects.create(
            author=self.viewer,
            title_spanish="Robot salvaje",
            title_english="The Wild Robot",
            type=Movie.MOVIE,
            image="posters/the-wild-robot.jpg",
        )
        self.older_video = self.make_video(self.catherine, self.movie_one, "older.mp4")
        self.newer_video = self.make_video(self.catherine, self.movie_two, "newer.mp4")
        self.other_video = self.make_video(self.other_user, self.movie_one, "other.mp4")
        now = timezone.now()
        VideoComment.objects.filter(pk=self.older_video.pk).update(created_at=now - timedelta(days=2))
        VideoComment.objects.filter(pk=self.newer_video.pk).update(created_at=now - timedelta(days=1))
        VideoComment.objects.filter(pk=self.other_video.pk).update(created_at=now)
        self.client.force_authenticate(self.viewer)
        self.url = reverse("user-activity", kwargs={"username": self.catherine.username})

    @staticmethod
    def make_user(username):
        return get_user_model().objects.create_user(username=username, password="test1234")

    @staticmethod
    def make_video(user, movie, filename):
        return VideoComment.objects.create(
            user=user,
            movie=movie,
            video=SimpleUploadedFile(filename, b"video", content_type="video/mp4"),
            duration_seconds=12,
            mime_type="video/mp4",
            file_size=5,
        )

    def video_activities(self, response):
        return [
            item for item in response.data["results"]
            if item["activity_type"] == "video_reaction_created"
        ]

    def test_returns_only_visited_users_videos_with_required_fields_and_newest_first(self):
        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        videos = self.video_activities(response)
        self.assertEqual(
            [item["video_comment_id"] for item in videos],
            [self.newer_video.id, self.older_video.id],
        )
        self.assertNotIn(self.other_video.id, [item["video_comment_id"] for item in videos])

        item = videos[0]
        self.assertEqual(item["actor"]["username"], self.catherine.username)
        self.assertTrue(item["video_url"].endswith(self.newer_video.video.url))
        self.assertIsNotNone(item["timestamp"])
        self.assertEqual(item["movie"]["id"], self.movie_two.id)
        self.assertEqual(item["movie"]["title_spanish"], "Robot salvaje")
        self.assertEqual(item["movie"]["title_english"], "The Wild Robot")
        self.assertEqual(item["movie"]["type"], Movie.MOVIE)
        self.assertTrue(item["movie"]["image"].endswith("posters/the-wild-robot.jpg"))

    def test_private_profile_without_friendship_cannot_access_video_activity(self):
        self.catherine.profile.visibility = Profile.Visibility.PRIVATE
        self.catherine.profile.is_public = False
        self.catherine.profile.save(update_fields=["visibility", "is_public"])

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_private_profile_friend_can_access_video_activity(self):
        self.catherine.profile.visibility = Profile.Visibility.PRIVATE
        self.catherine.profile.is_public = False
        self.catherine.profile.save(update_fields=["visibility", "is_public"])
        Friendship.objects.create(
            requester=self.catherine,
            user1=min(self.catherine, self.viewer, key=lambda user: user.id),
            user2=max(self.catherine, self.viewer, key=lambda user: user.id),
            status=Friendship.STATUS_ACCEPTED,
        )

        response = self.client.get(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(self.video_activities(response)), 2)
