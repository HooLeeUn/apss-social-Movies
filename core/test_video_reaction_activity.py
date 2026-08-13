from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Movie, MovieRating, VideoComment, VideoCommentReaction


class VideoReactionProfileActivityTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = self.make_user("activity_video_owner")
        self.reactor = self.make_user("activity_video_reactor")
        self.other_reactor = self.make_user("activity_other_reactor")
        self.movie = Movie.objects.create(
            author=self.owner,
            title_english="Video Activity Movie",
            type=Movie.MOVIE,
            genre="Drama",
        )
        self.video = self.make_video(self.owner)
        self.url = reverse("profile-feed-activity")

    def make_user(self, username):
        return get_user_model().objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="test1234",
        )

    def make_video(self, owner):
        return VideoComment.objects.create(
            user=owner,
            movie=self.movie,
            video=SimpleUploadedFile("activity.mp4", b"video", content_type="video/mp4"),
            duration_seconds=4,
            mime_type="video/mp4",
            file_size=5,
        )

    def make_reaction(self, *, user=None, reaction_type="like", video=None):
        return VideoCommentReaction.objects.create(
            user=user or self.reactor,
            video_comment=video or self.video,
            reaction_type=reaction_type,
        )

    def activity(self, user, **params):
        self.client.force_authenticate(user)
        response = self.client.get(self.url, {"scope": "me", **params})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return response

    def item_for(self, response, reaction):
        return next(
            item
            for item in response.data["results"]
            if item["reaction_id"] == reaction.id
        )

    def test_received_like_contains_actor_owner_movie_and_video(self):
        reaction = self.make_reaction(reaction_type="like")

        item = self.item_for(self.activity(self.owner), reaction)

        self.assertEqual(item["activity_type"], "video_reaction_received")
        self.assertEqual(item["reaction_type"], "like")
        self.assertEqual(item["actor"]["id"], self.reactor.id)
        self.assertEqual(item["video_owner"]["id"], self.owner.id)
        self.assertEqual(item["movie"]["id"], self.movie.id)
        self.assertEqual(item["video_comment_id"], self.video.id)
        self.assertTrue(item["is_received_reaction"])
        self.assertFalse(item["is_given_reaction"])

    def test_received_dislike_uses_explicit_type(self):
        reaction = self.make_reaction(reaction_type="dislike")

        item = self.item_for(self.activity(self.owner), reaction)

        self.assertEqual(item["activity_type"], "video_reaction_received")
        self.assertEqual(item["reaction_value"], "dislike")

    def test_given_like_contains_reactor_and_video_owner(self):
        reaction = self.make_reaction(reaction_type="like")

        item = self.item_for(self.activity(self.reactor), reaction)

        self.assertEqual(item["activity_type"], "video_reaction_given")
        self.assertEqual(item["actor"]["id"], self.reactor.id)
        self.assertEqual(item["video_owner"]["id"], self.owner.id)
        self.assertTrue(item["is_given_reaction"])
        self.assertFalse(item["is_received_reaction"])

    def test_given_dislike_uses_explicit_type(self):
        reaction = self.make_reaction(reaction_type="dislike")

        item = self.item_for(self.activity(self.reactor), reaction)

        self.assertEqual(item["activity_type"], "video_reaction_given")
        self.assertEqual(item["reaction_type"], "dislike")

    def test_switching_reaction_updates_the_only_current_activity(self):
        reaction = self.make_reaction(reaction_type="like")
        reaction.reaction_type = "dislike"
        reaction.save(update_fields=["reaction_type", "updated_at"])

        response = self.activity(self.owner)
        items = [item for item in response.data["results"] if item["reaction_id"] == reaction.id]

        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["reaction_type"], "dislike")

    def test_deleted_reaction_and_deleted_video_remove_activity(self):
        removed_reaction = self.make_reaction()
        removed_reaction_id = removed_reaction.id
        removed_reaction.delete()
        self.assertFalse(
            any(item["reaction_id"] == removed_reaction_id for item in self.activity(self.owner).data["results"])
        )

        cascaded_reaction = self.make_reaction()
        reaction_id = cascaded_reaction.id
        self.video.delete()
        self.assertFalse(
            any(item["reaction_id"] == reaction_id for item in self.activity(self.owner).data["results"])
        )

    def test_two_users_produce_two_unique_received_activities(self):
        first = self.make_reaction(user=self.reactor)
        second = self.make_reaction(user=self.other_reactor, reaction_type="dislike")

        items = [
            item for item in self.activity(self.owner).data["results"]
            if item["activity_type"] == "video_reaction_received"
        ]

        self.assertEqual({item["reaction_id"] for item in items}, {first.id, second.id})
        self.assertEqual(len(items), 2)

    def test_reaction_to_own_video_is_not_duplicated(self):
        reaction = self.make_reaction(user=self.owner)

        matches = [
            item for item in self.activity(self.owner).data["results"]
            if item["reaction_id"] == reaction.id
        ]

        self.assertEqual(matches, [])

    def test_video_activity_is_mixed_chronologically_before_pagination(self):
        rating = MovieRating.objects.create(user=self.owner, movie=self.movie, score=8)
        reaction = self.make_reaction()
        baseline = timezone.now()
        MovieRating.objects.filter(pk=rating.pk).update(created_at=baseline)
        VideoCommentReaction.objects.filter(pk=reaction.pk).update(
            created_at=baseline + timezone.timedelta(minutes=1)
        )

        response = self.activity(self.owner, page_size=1)

        self.assertEqual(response.data["count"], 2)
        self.assertEqual(response.data["results"][0]["reaction_id"], reaction.id)
        self.assertIsNotNone(response.data["next"])

    def test_query_count_does_not_grow_per_video_reaction(self):
        self.make_reaction()
        with CaptureQueriesContext(connection) as one_reaction_queries:
            self.activity(self.owner, page_size=50)

        for index in range(4):
            user = self.make_user(f"query_reactor_{index}")
            self.make_reaction(user=user)

        with CaptureQueriesContext(connection) as five_reaction_queries:
            self.activity(self.owner, page_size=50)

        self.assertEqual(len(five_reaction_queries), len(one_reaction_queries))
