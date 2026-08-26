from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from core.models import Movie, MovieRating
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG3Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(
            username="g3-owner", email="g3-private@example.test"
        )
        movie = Movie.objects.create(
            author=cls.user, title_english="G3 private title", type=Movie.MOVIE
        )
        MovieRating.objects.create(user=cls.user, movie=movie, rating=7)

    def candidate(self, enabled):
        return SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user, scope="me", k=10, fallback_to_legacy=False,
            return_metadata=True, profile_enabled=enabled,
        )

    def test_profile_has_balanced_sql_python_family_and_residual_metrics(self):
        _, metadata = self.candidate(True)
        profile = metadata["profile"]
        for field in (
            "hydration_total_ms", "hydration_sql_ms", "hydration_python_ms",
            "hydration_accounted_ms", "hydration_unaccounted_ms",
            "hydration_components", "hydration_families",
        ):
            self.assertIn(field, profile)
        self.assertAlmostEqual(
            profile["hydration_total_ms"],
            profile["hydration_sql_ms"] + profile["hydration_python_ms"],
            delta=0.002,
        )
        self.assertAlmostEqual(
            profile["hydration_total_ms"],
            profile["hydration_accounted_ms"] + profile["hydration_unaccounted_ms"],
            delta=0.002,
        )
        self.assertLessEqual(profile["hydration_unaccounted_ms"], profile["hydration_total_ms"])
        for family in profile["hydration_families"].values():
            for field in ("sql_ms", "python_ms", "total_ms", "rows", "queries", "fetch_ms", "serialize_ms"):
                self.assertIn(field, family)

    def test_profiler_is_absent_when_disabled_and_payload_is_identical(self):
        profiled, profiled_metadata = self.candidate(True)
        plain, plain_metadata = self.candidate(False)
        self.assertEqual(profiled, plain)
        self.assertIn("profile", profiled_metadata)
        self.assertNotIn("profile", plain_metadata)

    @override_settings(
        PROFILE_ACTIVITY_SHADOW_ENABLED=True,
        PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=True,
        PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=1.0,
    )
    def test_numeric_profile_log_has_no_payload_or_pii(self):
        self.client.force_login(self.user)
        with self.assertLogs("core.profile_activity_shadow", level="INFO") as logs:
            self.client.get(reverse("profile-feed-activity"), {"scope": "me"})
        profile_log = next(item for item in logs.output if "CANDIDATE_PROFILE" in item)
        for secret in (
            self.user.username, self.user.email, "G3 private title",
            "message body", "comment body", "https://video.example/secret",
        ):
            self.assertNotIn(secret, profile_log)

    @override_settings(
        PROFILE_ACTIVITY_SHADOW_ENABLED=True,
        PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=False,
    )
    @patch("core.profile_activity_shadow.report_candidate_profile")
    def test_disabled_flag_never_reports_detailed_profile(self, report):
        self.client.force_login(self.user)
        self.client.get(reverse("profile-feed-activity"), {"scope": "me"})
        report.assert_not_called()
