from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from django.utils import timezone

from core.models import Comment, CommentReaction, Movie, MovieRating
from core.social_feed import SocialActivityFeedService


class AdaptiveFrontierUnitTests(SimpleTestCase):
    def test_list_cursor_promotes_lookahead_without_offset_or_duplicate(self):
        stamp = timezone.now()
        rows = [{
            "namespace": SocialActivityFeedService.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY,
            "object_id": index, "latest_activity_at": stamp - timedelta(seconds=index),
            "latest_reaction_id": index, "family_rank": 0, "source_rows": 12,
        } for index in range(1, 15)]
        fetch = SocialActivityFeedService._adaptive_list_fetcher(rows=rows)
        first, frontier, exhausted, inspected = fetch(None, 10)
        self.assertEqual(len(first), 10)
        self.assertEqual(frontier["object_id"], 11)
        self.assertFalse(exhausted)
        self.assertEqual(inspected, 132)
        second, _, exhausted, _ = fetch(frontier["_cursor"], 10)
        self.assertEqual([row["object_id"] for row in second], [12, 13, 14])
        self.assertTrue(exhausted)


class ProfileActivityPhaseDAdaptiveTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.owner = users.objects.create_user(username="phase_d_owner")
        self.other = users.objects.create_user(username="phase_d_other")
        self.movie = Movie.objects.create(author=self.owner, title_english="Phase D", type=Movie.MOVIE)

    def assert_top_k(self, k, **options):
        legacy = SocialActivityFeedService.build_feed(user=self.owner, scope="me")[:k]
        candidate, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.owner, scope="me", k=k, return_metadata=True,
            fallback_to_legacy=False, **options,
        )
        self.assertTrue(metadata["certified"], metadata)
        self.assertEqual(candidate, legacy)
        self.assertEqual(metadata["hydrated_rows"], len(candidate))
        return metadata

    def test_deep_k_and_unbalanced_ratings_expand_only_competitive_stream(self):
        base = timezone.now()
        for index in range(55):
            movie = Movie.objects.create(author=self.owner, title_english=f"rating {index}", type=Movie.MOVIE)
            rating = MovieRating.objects.create(user=self.owner, movie=movie, score=7)
            MovieRating.objects.filter(pk=rating.pk).update(updated_at=base + timedelta(minutes=index))
        Comment.objects.create(author=self.owner, movie=self.movie, body="old")
        for k in (1, 10, 20, 30, 50):
            metadata = self.assert_top_k(k, batch_size=4)
            self.assertGreaterEqual(metadata["batches_by_family"]["ratings"], 1)
            self.assertEqual(metadata["hydrated_rows"], k)

    def test_many_received_rows_are_one_logical_candidate_not_fixed_limit(self):
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="summary")
        for index in range(12):
            reactor = get_user_model().objects.create_user(username=f"d_reactor_{index}")
            CommentReaction.objects.create(comment=comment, user=reactor, reaction_type="like")
        for index in range(14):
            movie = Movie.objects.create(author=self.owner, title_english=f"mixed {index}", type=Movie.MOVIE)
            MovieRating.objects.create(user=self.owner, movie=movie, score=8)
        metadata = self.assert_top_k(10, batch_size=3)
        self.assertGreater(metadata["source_rows_inspected"], metadata["hydrated_rows"])
        self.assertLessEqual(metadata["hydrated_rows"], 10)

    def test_updates_use_effective_timestamp_and_total_ties_match_legacy(self):
        stamp = timezone.now()
        rating = MovieRating.objects.create(user=self.owner, movie=self.movie, score=5)
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="tie")
        MovieRating.objects.filter(pk=rating.pk).update(updated_at=stamp)
        Comment.objects.filter(pk=comment.pk).update(created_at=stamp)
        reaction = CommentReaction.objects.create(
            comment=Comment.objects.create(author=self.other, movie=self.movie, body="foreign"),
            user=self.owner, reaction_type="like",
        )
        CommentReaction.objects.filter(pk=reaction.pk).update(updated_at=stamp + timedelta(minutes=1))
        self.assert_top_k(3, batch_size=1)

    def test_guard_rail_never_returns_approximation(self):
        for index in range(8):
            movie = Movie.objects.create(author=self.owner, title_english=f"guard {index}", type=Movie.MOVIE)
            MovieRating.objects.create(user=self.owner, movie=movie, score=6)
        candidate, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.owner, scope="me", k=5, batch_size=1, max_batches=1,
            return_metadata=True,
        )
        self.assertFalse(metadata["certified"])
        self.assertEqual(metadata["fallback_reason"], "max_batches")
        self.assertEqual(candidate, SocialActivityFeedService.build_feed(user=self.owner, scope="me")[:5])

    def test_empty_families_and_zero_k_are_exact(self):
        self.assert_top_k(10, batch_size=2)
        candidate, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.owner, scope="me", k=0, return_metadata=True,
        )
        self.assertEqual(candidate, [])
        self.assertTrue(metadata["certified"])
