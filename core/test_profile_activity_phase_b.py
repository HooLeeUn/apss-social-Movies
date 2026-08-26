from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.activity_feed_test_utils import LEGACY_STABLE_FAMILY_RANK
from core.models import Comment, CommentReaction, Movie, MovieRating
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseBCandidateTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.owner = users.objects.create_user(username="phase_b_owner")
        self.other = users.objects.create_user(username="phase_b_other")
        self.movie = Movie.objects.create(
            author=self.owner, title_english="Phase B", type=Movie.MOVIE
        )

    def assert_candidate_matches_legacy(self):
        legacy = SocialActivityFeedService.build_feed(user=self.owner, scope="me")
        candidate = SocialActivityFeedService.build_feed_candidate_full_materialization(
            user=self.owner, scope="me"
        )
        self.assertEqual(candidate, legacy)

    def test_candidate_ratings_and_public_comments_match_legacy(self):
        MovieRating.objects.create(user=self.owner, movie=self.movie, score=8)
        Comment.objects.create(author=self.owner, movie=self.movie, body="public")
        self.assert_candidate_matches_legacy()

    def test_public_reactions_summaries_and_many_rows_match_legacy(self):
        owned = Comment.objects.create(author=self.owner, movie=self.movie, body="owned")
        foreign = Comment.objects.create(author=self.other, movie=self.movie, body="foreign")
        CommentReaction.objects.create(comment=foreign, user=self.owner, reaction_type="like")
        for index in range(4):
            reactor = get_user_model().objects.create_user(username=f"phase_b_reactor_{index}")
            CommentReaction.objects.create(comment=owned, user=reactor, reaction_type="like")
        self.assert_candidate_matches_legacy()

    def test_private_message_and_reaction_keep_python_mention_validation(self):
        valid = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body=f"@{self.other.username} private",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.other,
        )
        CommentReaction.objects.create(comment=valid, user=self.other, reaction_type="like")
        Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="missing mention",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.other,
        )
        self.assert_candidate_matches_legacy()

    def test_all_seven_selectors_are_lazy(self):
        selectors = (
            lambda: SocialActivityFeedService.rating_candidates_queryset(actor_ids=[self.owner.id], viewer=self.owner),
            lambda: SocialActivityFeedService.public_comment_candidates_queryset(actor_ids=[self.owner.id], viewer=self.owner),
            lambda: SocialActivityFeedService.public_reaction_candidates_queryset(actor_ids=[self.owner.id], viewer=self.owner),
            lambda: SocialActivityFeedService.private_message_candidates_queryset(actor_ids=[self.owner.id], viewer=self.owner),
            lambda: SocialActivityFeedService.private_reaction_candidates_queryset(actor_ids=[self.owner.id], viewer=self.owner),
            lambda: SocialActivityFeedService.video_created_candidates_queryset(actor=self.owner, viewer=self.owner),
            lambda: SocialActivityFeedService.video_reaction_candidates_queryset(viewer=self.owner),
        )
        with CaptureQueriesContext(connection) as queries:
            querysets = [selector() for selector in selectors]
        self.assertEqual(len(queries), 0)
        self.assertTrue(all(queryset._result_cache is None for queryset in querysets))

    def test_candidate_rank_is_the_phase_a_frozen_rank(self):
        self.assertEqual(
            SocialActivityFeedService.LEGACY_FAMILY_RANK,
            LEGACY_STABLE_FAMILY_RANK,
        )

