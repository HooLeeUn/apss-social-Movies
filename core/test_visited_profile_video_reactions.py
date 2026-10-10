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


class VisitedProfilePublicCommentReactionTests(TestCase):
    def setUp(self):
        from core.models import Comment, CommentReaction
        self.Comment = Comment
        self.CommentReaction = CommentReaction
        self.owner = get_user_model().objects.create_user(username="comment_owner")
        self.viewer = get_user_model().objects.create_user(username="comment_viewer")
        self.other = get_user_model().objects.create_user(username="comment_other")
        self.movie = Movie.objects.create(author=self.owner, title_english="Comment movie", type=Movie.MOVIE)
        self.comment = Comment.objects.create(author=self.owner, movie=self.movie, body="Public")
        CommentReaction.objects.create(comment=self.comment, user=self.viewer, reaction_type="like")
        CommentReaction.objects.create(comment=self.comment, user=self.other, reaction_type="dislike")
        self.client = APIClient()
        self.client.force_authenticate(self.viewer)
        self.url = reverse("user-activity", kwargs={"username": self.owner.username}) + "?activity_type=public_comment"

    def payload(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        return response.data["results"][0]["payload"]

    def test_profile_and_movie_use_same_counts_and_reaction_record(self):
        payload = self.payload()
        self.assertEqual((payload["likes_count"], payload["dislikes_count"], payload["my_reaction"]), (1, 1, "like"))
        reaction_url = reverse("comment-reaction", kwargs={"pk": self.comment.pk})
        record = self.CommentReaction.objects.get(comment=self.comment, user=self.viewer)
        old_updated_at = record.updated_at
        response = self.client.put(reaction_url, {"reaction": "dislike"}, format="json")
        self.assertEqual(response.status_code, 200)
        record.refresh_from_db()
        self.assertGreater(record.updated_at, old_updated_at)
        self.assertEqual(self.CommentReaction.objects.filter(comment=self.comment, user=self.viewer).count(), 1)
        payload = self.payload()
        self.assertEqual((payload["likes_count"], payload["dislikes_count"], payload["my_reaction"]), (0, 2, "dislike"))
        movie_response = self.client.get(reverse("movie-comments", kwargs={"pk": self.movie.pk}))
        comment = movie_response.data["results"][0]
        for key in ("likes_count", "dislikes_count", "my_reaction"):
            self.assertEqual(comment[key], payload[key])
        self.assertEqual(self.client.delete(reaction_url).status_code, 200)
        self.assertIsNone(self.payload()["my_reaction"])

    def test_public_count_and_profile_exclude_directed_and_hidden_comments(self):
        self.Comment.objects.create(author=self.owner, movie=self.movie, body="Hidden", is_hidden=True)
        self.Comment.objects.create(author=self.owner, movie=self.movie, body="Directed",
                                    visibility=self.Comment.VISIBILITY_MENTIONED, target_user=self.viewer)
        unrelated = Movie.objects.create(author=self.owner, title_english="Other", type=Movie.SERIES)
        self.Comment.objects.create(author=self.other, movie=unrelated, body="Other movie")
        response = self.client.get(reverse("movie-comments", kwargs={"pk": self.movie.pk}))
        self.assertEqual(response.data["count"], 1)
        response = self.client.get(self.url)
        self.assertEqual(response.data["count"], 1)

    def test_guest_counts_and_authenticated_unreacted_state(self):
        self.client.force_authenticate(None)
        self.assertIsNone(self.payload()["my_reaction"])
        self.assertEqual(self.client.put(reverse("comment-reaction", kwargs={"pk": self.comment.pk}),
                                         {"reaction": "like"}, format="json").status_code, 401)
        self.CommentReaction.objects.filter(user=self.viewer).delete()
        self.client.force_authenticate(self.viewer)
        self.assertIsNone(self.payload()["my_reaction"])

    def test_comment_annotations_are_batched_without_per_comment_queries(self):
        from django.db import connection
        from django.test.utils import CaptureQueriesContext
        from core.social_feed import SocialActivityFeedService
        def serialize():
            return SocialActivityFeedService.serialize_public_comment_queryset(
                SocialActivityFeedService._public_comment_activity_queryset(actor_ids=[self.owner.pk], viewer=self.viewer)
            )
        with CaptureQueriesContext(connection) as one:
            serialize()
        for index in range(8):
            self.Comment.objects.create(author=self.owner, movie=self.movie, body=f"Public {index}")
        with CaptureQueriesContext(connection) as many:
            rows = serialize()
        self.assertEqual(len(rows), 9)
        self.assertEqual(len(one), len(many))
        self.assertEqual(len(many), 1)

    def test_hidden_and_restricted_comments_keep_existing_permissions(self):
        from core.models import UserVisibilityBlock
        reaction_url = reverse("comment-reaction", kwargs={"pk": self.comment.pk})
        self.Comment.objects.filter(pk=self.comment.pk).update(is_hidden=True)
        self.assertEqual(self.client.put(reaction_url, {"reaction": "like"}, format="json").status_code, 404)
        self.Comment.objects.filter(pk=self.comment.pk).update(is_hidden=False)
        UserVisibilityBlock.objects.create(owner=self.owner, blocked_user=self.viewer)
        self.assertEqual(self.client.get(self.url).status_code, 403)
        self.assertEqual(self.client.put(reaction_url, {"reaction": "like"}, format="json").status_code, 404)
