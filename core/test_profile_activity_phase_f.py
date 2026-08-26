from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from core.models import Movie, MovieRating
from core.profile_activity_shadow import report_candidate_profile
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseFTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="phase_f_owner")
        movie = Movie.objects.create(author=self.user, title_english="Phase F", type=Movie.MOVIE)
        MovieRating.objects.create(user=self.user, movie=movie, rating=8)
        self.client.force_login(self.user)
        self.url = reverse("profile-feed-activity")

    @override_settings(
        PROFILE_ACTIVITY_SHADOW_ENABLED=False,
        PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=True,
    )
    @patch.object(SocialActivityFeedService, "build_feed_candidate_adaptive")
    def test_profile_true_does_not_run_candidate_when_shadow_false(self, candidate):
        self.assertEqual(self.client.get(self.url, {"scope": "me"}).status_code, 200)
        candidate.assert_not_called()

    @override_settings(
        PROFILE_ACTIVITY_SHADOW_ENABLED=True,
        PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=False,
        PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=1.0,
    )
    @patch("core.profile_activity_shadow.report_candidate_profile")
    def test_profile_false_has_no_detailed_profiler_or_log(self, profile_report):
        with patch.object(
            SocialActivityFeedService, "build_feed_candidate_adaptive",
            wraps=SocialActivityFeedService.build_feed_candidate_adaptive,
        ) as candidate:
            self.client.get(self.url, {"scope": "me"})
        self.assertFalse(candidate.call_args.kwargs["profile_enabled"])
        profile_report.assert_not_called()

    @override_settings(
        PROFILE_ACTIVITY_SHADOW_ENABLED=True,
        PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=True,
        PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=1.0,
    )
    def test_profile_true_emits_numeric_metrics_and_response_remains_legacy(self):
        with override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=False):
            expected = self.client.get(self.url, {"scope": "me"}).json()
        with self.assertLogs("core.profile_activity_shadow", level="INFO") as captured:
            response = self.client.get(self.url, {"scope": "me"})
        self.assertEqual(response.json(), expected)
        profile_log = next(line for line in captured.output if "CANDIDATE_PROFILE" in line)
        for field in (
            "queries=", "hydration_ms=", "logical_count_ms=", "frontier_ms=",
            "certification_ms=", "families=",
        ):
            self.assertIn(field, profile_log)
        for pii in (self.user.username, self.user.email, "comment body", "message body", "video_url"):
            self.assertNotIn(pii, profile_log)

    def test_direct_candidate_profile_covers_all_families(self):
        _, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user, scope="me", k=10, fallback_to_legacy=False,
            return_metadata=True, profile_enabled=True,
        )
        self.assertEqual(set(metadata["profile"]["families"]), {
            "ratings", "public_comments", "public_reactions_given", "private_messages",
            "videos_created", "video_reactions_given",
            "comment_reaction_summaries_received",
            "comment_reaction_summaries_received_private_hybrid",
            "video_reaction_summaries_received",
        })

    def test_profile_report_ignores_missing_metrics(self):
        with self.assertNoLogs("core.profile_activity_shadow", level="INFO"):
            report_candidate_profile({})
