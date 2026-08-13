from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Movie, VideoComment, VideoCommentReaction


class VideoCommentReactionAPITests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = self.make_user("video_owner")
        self.user = self.make_user("video_viewer")
        self.other_user = self.make_user("other_viewer")
        self.movie = Movie.objects.create(
            author=self.owner,
            title_english="Reaction Movie",
            type=Movie.MOVIE,
            genre="Drama",
        )
        self.video_comment = self.make_video_comment(self.owner)
        self.url = reverse("video-comment-reaction", kwargs={"pk": self.video_comment.pk})
        self.list_url = reverse("movie-video-comments", kwargs={"pk": self.movie.pk})
        self.client.force_authenticate(self.user)

    def make_user(self, username):
        return get_user_model().objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="test1234",
        )

    def make_video_comment(self, user):
        return VideoComment.objects.create(
            user=user,
            movie=self.movie,
            video=SimpleUploadedFile("reaction.mp4", b"video", content_type="video/mp4"),
            duration_seconds=5,
            mime_type="video/mp4",
            file_size=5,
        )

    def react(self, reaction):
        return self.client.put(self.url, {"reaction": reaction}, format="json")

    def test_user_without_reaction_sees_zero_counts_and_null_reaction(self):
        response = self.client.get(self.list_url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        item = response.data["results"][0]
        self.assertEqual(item["likes_count"], 0)
        self.assertEqual(item["dislikes_count"], 0)
        self.assertIsNone(item["my_reaction"])

    def test_like_toggles_on_and_off(self):
        created = self.react(VideoCommentReaction.REACT_LIKE)
        self.assertEqual(created.status_code, status.HTTP_200_OK)
        self.assertEqual(created.data["my_reaction"], "like")
        self.assertEqual(created.data["likes_count"], 1)

        removed = self.react(VideoCommentReaction.REACT_LIKE)
        self.assertEqual(removed.status_code, status.HTTP_200_OK)
        self.assertIsNone(removed.data["my_reaction"])
        self.assertEqual(removed.data["likes_count"], 0)
        self.assertFalse(VideoCommentReaction.objects.exists())

    def test_dislike_can_be_created_and_switched_to_like(self):
        disliked = self.react(VideoCommentReaction.REACT_DISLIKE)
        self.assertEqual(disliked.data["my_reaction"], "dislike")
        self.assertEqual(disliked.data["dislikes_count"], 1)

        liked = self.react(VideoCommentReaction.REACT_LIKE)
        self.assertEqual(liked.data["my_reaction"], "like")
        self.assertEqual(liked.data["likes_count"], 1)
        self.assertEqual(liked.data["dislikes_count"], 0)
        self.assertEqual(VideoCommentReaction.objects.count(), 1)

    def test_like_can_be_switched_to_dislike(self):
        self.react(VideoCommentReaction.REACT_LIKE)
        response = self.react(VideoCommentReaction.REACT_DISLIKE)

        self.assertEqual(response.data["my_reaction"], "dislike")
        self.assertEqual(response.data["likes_count"], 0)
        self.assertEqual(response.data["dislikes_count"], 1)
        self.assertEqual(VideoCommentReaction.objects.count(), 1)

    def test_two_users_have_independent_reactions_and_correct_counts(self):
        self.react(VideoCommentReaction.REACT_LIKE)
        self.client.force_authenticate(self.other_user)
        response = self.react(VideoCommentReaction.REACT_DISLIKE)

        self.assertEqual(response.data["likes_count"], 1)
        self.assertEqual(response.data["dislikes_count"], 1)
        self.assertEqual(response.data["my_reaction"], "dislike")
        self.client.force_authenticate(self.user)
        own_view = self.client.get(self.list_url).data["results"][0]
        self.assertEqual(own_view["my_reaction"], "like")

    def test_delete_endpoint_removes_reaction(self):
        self.react(VideoCommentReaction.REACT_LIKE)
        response = self.client.delete(self.url)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsNone(response.data["my_reaction"])
        self.assertEqual(response.data["likes_count"], 0)

    def test_unauthenticated_user_cannot_react(self):
        self.client.force_authenticate(None)

        response = self.client.put(self.url, {"reaction": "like"}, format="json")

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertFalse(VideoCommentReaction.objects.exists())

    def test_deleting_video_cascades_to_reactions(self):
        reaction = VideoCommentReaction.objects.create(
            video_comment=self.video_comment,
            user=self.user,
            reaction_type=VideoCommentReaction.REACT_LIKE,
        )

        self.video_comment.delete()

        self.assertFalse(VideoCommentReaction.objects.filter(pk=reaction.pk).exists())

    def test_video_list_query_count_is_constant_as_video_count_grows(self):
        with CaptureQueriesContext(connection) as one_video_queries:
            response = self.client.get(self.list_url)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            list(response.data["results"])

        for _ in range(4):
            self.make_video_comment(self.owner)

        with CaptureQueriesContext(connection) as five_video_queries:
            response = self.client.get(self.list_url)
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            list(response.data["results"])

        self.assertEqual(len(five_video_queries), len(one_video_queries))

