from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import Comment, Movie
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG1PrivateMessageTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.owner = users.objects.create_user(username="g1_owner")
        self.alice = users.objects.create_user(username="g1_alice")
        self.bob = users.objects.create_user(username="g1_bob")
        self.movies = [
            Movie.objects.create(
                author=self.owner, title_english=f"G1 movie {index}", type=Movie.MOVIE
            )
            for index in range(2)
        ]

    def create_message(self, index, *, author=None, recipient=None, body=None):
        message = Comment.objects.create(
            author=author or self.owner,
            target_user=recipient or (self.alice if index % 2 else self.bob),
            movie=self.movies[index % len(self.movies)],
            body=body if body is not None else f"private message {index}",
            visibility=Comment.VISIBILITY_MENTIONED,
        )
        Comment.objects.filter(pk=message.pk).update(
            created_at=timezone.now() - timedelta(seconds=index)
        )
        return message

    def private_message_fetch(self, batch_size):
        sequence = next(
            sequence
            for sequence in SocialActivityFeedService._adaptive_sequences(
                user=self.owner, actor_ids=[self.owner.id]
            )
            if sequence.name == "private_messages"
        )
        return sequence.fetch(None, batch_size)

    def test_selector_query_count_is_constant_for_1_10_and_50_rows(self):
        previous = 0
        observed = {}
        for total in (1, 10, 50):
            for index in range(previous, total):
                self.create_message(index)
            previous = total
            with CaptureQueriesContext(connection) as queries:
                rows, frontier, exhausted, inspected = self.private_message_fetch(total)
            observed[total] = len(queries)
            self.assertEqual(len(queries), 1)
            self.assertEqual(len(rows), total)
            self.assertIsNone(frontier)
            self.assertTrue(exhausted)
            self.assertEqual(inspected, total)
        self.assertEqual(observed, {1: 1, 10: 1, 50: 1})

    def test_sent_conversations_movies_and_timestamps_match_legacy(self):
        stamp = timezone.now()
        messages = [self.create_message(index) for index in range(6)]
        # Exercise the existing id tie-breaker with equal activity timestamps.
        Comment.objects.filter(pk__in=[messages[0].pk, messages[1].pk]).update(
            created_at=stamp
        )
        legacy = SocialActivityFeedService.build_feed(user=self.owner, scope="me")
        candidate = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.owner, scope="me", k=len(legacy), fallback_to_legacy=False
        )
        self.assertEqual(candidate, legacy)
        self.assertEqual(
            {item["payload"]["direction"] for item in candidate}, {"sent"}
        )

    def test_privacy_predicate_still_rejects_invalid_directed_messages(self):
        visible = self.create_message(0)
        self.create_message(1, recipient=self.owner)
        self.create_message(2, body="")

        rows, _, _, _ = self.private_message_fetch(10)

        self.assertEqual([row["object_id"] for row in rows], [visible.id])
        legacy = SocialActivityFeedService.build_feed(user=self.owner, scope="me")
        candidate = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.owner, scope="me", k=10, fallback_to_legacy=False
        )
        self.assertEqual(candidate, legacy)
