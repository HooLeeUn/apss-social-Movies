from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APIClient

from core.models import Comment, CommentReaction, Movie


class ReceivedCommentReactionSummaryTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = self.user("summary_owner")
        self.first_reactor = self.user("summary_reactor_one")
        self.second_reactor = self.user("summary_reactor_two")
        self.movie = Movie.objects.create(
            author=self.owner, title_english="Summary Movie", type=Movie.MOVIE, genre="Drama"
        )
        self.first_comment = Comment.objects.create(
            author=self.owner, movie=self.movie, body="First comment"
        )
        self.second_comment = Comment.objects.create(
            author=self.owner, movie=self.movie, body="Second comment"
        )

    def user(self, username):
        return get_user_model().objects.create_user(username=username, password="test1234")

    def activity(self, user=None, **params):
        self.client.force_authenticate(user or self.owner)
        return self.client.get(reverse("profile-feed-activity"), {"scope": "me", **params})

    def summaries(self):
        return [
            item for item in self.activity().data["results"]
            if item["activity_type"] == "comment_reactions_received_summary"
        ]

    def test_current_reactions_are_grouped_per_comment_with_creation_date(self):
        first = CommentReaction.objects.create(
            comment=self.first_comment, user=self.first_reactor, reaction_type="like"
        )
        CommentReaction.objects.create(
            comment=self.first_comment, user=self.second_reactor, reaction_type="like"
        )
        CommentReaction.objects.create(
            comment=self.second_comment, user=self.first_reactor, reaction_type="dislike"
        )

        summaries = self.summaries()
        self.assertEqual(len(summaries), 2)
        first_summary = next(item for item in summaries if item["comment_id"] == self.first_comment.id)
        self.assertEqual(first_summary["payload"]["comment_text"], self.first_comment.body)
        self.assertEqual(first_summary["comment_text"], self.first_comment.body)
        self.assertEqual(first_summary["likes_count"], 2)
        self.assertEqual(
            {user["id"] for user in first_summary["users_who_liked"]},
            {self.first_reactor.id, self.second_reactor.id},
        )
        self.assertEqual(first_summary["dislikes_count"], 0)
        self.assertEqual(first_summary["object_created_at"], self.first_comment.created_at.isoformat().replace("+00:00", "Z"))
        self.assertNotEqual(first_summary["object_created_at"], first_summary["latest_reaction_at"])

        first.reaction_type = "dislike"
        first.save(update_fields=["reaction_type", "updated_at"])
        switched = next(item for item in self.summaries() if item["comment_id"] == self.first_comment.id)
        self.assertEqual(switched["likes_count"], 1)
        self.assertEqual(switched["dislikes_count"], 1)
        self.assertEqual(switched["users_who_disliked"][0]["id"], self.first_reactor.id)

        first.delete()
        deleted = next(item for item in self.summaries() if item["comment_id"] == self.first_comment.id)
        self.assertEqual(deleted["dislikes_count"], 0)

    def test_summary_with_only_like_preserves_reaction_and_object_timestamps(self):
        reaction = CommentReaction.objects.create(
            comment=self.first_comment, user=self.first_reactor, reaction_type="like"
        )

        summary = self.summaries()[0]

        self.assertEqual(summary["payload"]["comment_text"], "First comment")
        self.assertEqual(summary["likes_count"], 1)
        self.assertEqual(summary["dislikes_count"], 0)
        self.assertEqual(summary["users_who_liked"][0]["id"], self.first_reactor.id)
        self.assertEqual(summary["users_who_disliked"], [])
        self.assertEqual(
            summary["object_created_at"],
            self.first_comment.created_at.isoformat().replace("+00:00", "Z"),
        )
        self.assertEqual(
            summary["latest_reaction_at"],
            reaction.updated_at.isoformat().replace("+00:00", "Z"),
        )

    def test_summary_with_only_dislike_contains_exact_comment_text(self):
        exact_text = "A" * 140
        self.second_comment.body = exact_text
        self.second_comment.save(update_fields=["body"])
        CommentReaction.objects.create(
            comment=self.second_comment, user=self.second_reactor, reaction_type="dislike"
        )

        summary = self.summaries()[0]

        self.assertEqual(summary["comment_id"], self.second_comment.id)
        self.assertEqual(summary["payload"]["comment_text"], exact_text)
        self.assertEqual(summary["likes_count"], 0)
        self.assertEqual(summary["dislikes_count"], 1)
        self.assertEqual(summary["users_who_liked"], [])
        self.assertEqual(summary["users_who_disliked"][0]["id"], self.second_reactor.id)

    def test_given_reaction_stays_individual_and_groups_are_not_split_by_pagination(self):
        CommentReaction.objects.create(
            comment=self.first_comment, user=self.first_reactor, reaction_type="like"
        )
        CommentReaction.objects.create(
            comment=self.first_comment, user=self.second_reactor, reaction_type="like"
        )
        foreign_comment = Comment.objects.create(
            author=self.first_reactor, movie=self.movie, body="Foreign comment"
        )
        given = CommentReaction.objects.create(
            comment=foreign_comment, user=self.owner, reaction_type="like"
        )

        response = self.activity(page_size=1)
        self.assertEqual(response.data["count"], 4)
        all_items = self.activity(page_size=50).data["results"]
        received = [item for item in all_items if item["activity_type"] == "comment_reactions_received_summary"]
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0]["likes_count"], 2)
        given_items = [item for item in all_items if item["reaction_id"] == given.id]
        self.assertEqual(len(given_items), 1)
        self.assertTrue(given_items[0]["is_given_reaction"])

    def test_summaries_sort_by_latest_reaction_not_object_creation(self):
        older_object_reaction = CommentReaction.objects.create(
            comment=self.first_comment, user=self.first_reactor, reaction_type="like"
        )
        newer_object_reaction = CommentReaction.objects.create(
            comment=self.second_comment, user=self.second_reactor, reaction_type="like"
        )
        now = timezone.now()
        CommentReaction.objects.filter(pk=newer_object_reaction.pk).update(updated_at=now)
        CommentReaction.objects.filter(pk=older_object_reaction.pk).update(
            updated_at=now + timezone.timedelta(minutes=1)
        )
        summaries = self.summaries()
        self.assertEqual(summaries[0]["comment_id"], self.first_comment.id)
