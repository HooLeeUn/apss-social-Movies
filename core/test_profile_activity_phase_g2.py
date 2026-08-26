from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Comment, CommentReaction, Movie
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG2HydrationTests(TestCase):
    """Regression coverage for candidate-only, request-local hydration."""

    @classmethod
    def setUpTestData(cls):
        users = get_user_model()
        cls.owner = users.objects.create_user(username="g2_owner")
        cls.recipient = users.objects.create_user(username="g2_recipient")
        cls.movie = Movie.objects.create(
            author=cls.owner, title_english="G2 shared movie", type=Movie.MOVIE
        )

    def create_private_summaries(self, start, stop):
        comment_ids = []
        for index in range(start, stop):
            comment = Comment.objects.create(
                author=self.owner,
                target_user=self.recipient,
                movie=self.movie,
                body=f"private G2 comment {index}",
                visibility=Comment.VISIBILITY_MENTIONED,
            )
            reactor = get_user_model().objects.create_user(username=f"g2_reactor_{index}")
            CommentReaction.objects.create(
                comment=comment, user=reactor, reaction_type=CommentReaction.REACT_LIKE
            )
            comment_ids.append(comment.id)
        return comment_ids

    def hydrate_summaries(self, comment_ids):
        with CaptureQueriesContext(connection) as queries:
            rows = SocialActivityFeedService.hydrate_comment_reaction_summaries(
                comment_ids=comment_ids, viewer=self.owner
            )
        return rows, len(queries)

    def test_hydration_queries_are_constant_for_10_50_and_100_items(self):
        ids = []
        observed = {}
        for total in (10, 50, 100):
            ids.extend(self.create_private_summaries(len(ids), total))
            rows, query_count = self.hydrate_summaries(ids)
            observed[total] = query_count
            self.assertEqual(len(rows), total)
            self.assertEqual(query_count, 2)

        self.assertEqual(observed, {10: 2, 50: 2, 100: 2})

    def test_duplicate_movie_comment_and_user_references_do_not_add_queries(self):
        comment = Comment.objects.create(
            author=self.owner,
            target_user=self.recipient,
            movie=self.movie,
            body="one duplicated-reference comment",
            visibility=Comment.VISIBILITY_MENTIONED,
        )
        for index in range(25):
            reactor = get_user_model().objects.create_user(username=f"g2_duplicate_{index}")
            CommentReaction.objects.create(
                comment=comment,
                user=reactor,
                reaction_type=(
                    CommentReaction.REACT_LIKE
                    if index % 2 else CommentReaction.REACT_DISLIKE
                ),
            )

        rows, query_count = self.hydrate_summaries([comment.id] * 25)

        self.assertEqual(query_count, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["comment_id"], comment.id)
        self.assertEqual(rows[0]["movie"]["id"], self.movie.id)
        self.assertEqual(rows[0]["actor"]["id"], self.owner.id)
        self.assertEqual(rows[0]["likes_count"], 12)
        self.assertEqual(rows[0]["dislikes_count"], 13)

    def test_private_summary_candidate_output_and_order_match_legacy(self):
        self.create_private_summaries(0, 8)

        legacy = SocialActivityFeedService.build_feed(user=self.owner, scope="me")
        candidate, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.owner,
            scope="me",
            k=len(legacy),
            fallback_to_legacy=False,
            return_metadata=True,
            profile_enabled=True,
        )

        self.assertTrue(metadata["certified"])
        self.assertEqual(candidate, legacy)
        self.assertEqual(metadata["profile"]["hydration_queries"], 2)
        self.assertEqual(
            metadata["profile"]["hydration_families"]["comment_summaries"]["queries"],
            2,
        )
