from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Movie, MovieRating
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG7Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(
            username="g7-secret-user", email="g7-secret@example.test"
        )
        movie = Movie.objects.create(
            author=cls.user, title_english="G7 secret movie", type=Movie.MOVIE
        )
        MovieRating.objects.create(user=cls.user, movie=movie, rating=8)

    def candidate(self, enabled):
        return SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user, scope="me", k=10, fallback_to_legacy=False,
            return_metadata=True, profile_enabled=enabled,
        )

    def test_wall_cpu_gap_gc_connection_and_fingerprints_are_safe(self):
        _, metadata = self.candidate(True)
        profile = metadata["profile"]
        for prefix in ("candidate", "hydration"):
            for suffix in ("wall_ms", "cpu_ms", "wall_minus_cpu_ms"):
                self.assertGreaterEqual(profile[f"{prefix}_{suffix}"], 0)
        self.assertIn(profile["connection_state"], ("cold", "reused", "unknown"))
        for family in profile["families"].values():
            for section in ("queryset_definition", "fetch"):
                self.assertGreaterEqual(family[section]["wall_ms"], 0)
                self.assertGreaterEqual(family[section]["cpu_ms"], 0)
                self.assertGreaterEqual(family[section]["wall_minus_cpu_ms"], 0)
                self.assertGreaterEqual(family[section]["gc_collections"], 0)
        serialized = str(profile)
        self.assertNotIn(self.user.username, serialized)
        self.assertNotIn(self.user.email, serialized)
        self.assertNotIn("G7 secret movie", serialized)
        self.assertTrue(all(len(item["fingerprint"]) == 16 for item in profile["query_profiles"]))

    def test_disabled_profiler_preserves_payload_and_skips_g7_clocks(self):
        profiled, _ = self.candidate(True)
        with patch("core.social_feed.process_time") as cpu_clock, patch("core.social_feed.gc.get_stats") as gc_stats:
            plain, metadata = self.candidate(False)
        self.assertEqual(profiled, plain)
        self.assertNotIn("profile", metadata)
        cpu_clock.assert_not_called()
        gc_stats.assert_not_called()

    def test_definition_instrumentation_does_not_evaluate_queryset(self):
        with self.assertNumQueries(0):
            queryset = SocialActivityFeedService.rating_candidates_queryset(
                actor_ids=[self.user.id], viewer=self.user
            )
        with self.assertNumQueries(1):
            list(queryset)
