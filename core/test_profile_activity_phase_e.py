import os
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse

from core.models import Movie, MovieRating
from core.social_feed import SocialActivityFeedService
from core.profile_activity_shadow import report_profile_activity_shadow
from config import settings as project_settings


def adaptive_metadata(**overrides):
    result = {
        "certified": True,
        "fallback_reason": None,
        "batches_by_family": {"ratings": 1},
        "source_rows_inspected": 2,
        "logical_candidates_inspected": 2,
        "hydrated_rows": 2,
    }
    result.update(overrides)
    return result


@override_settings(PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=0)
class ProfileActivityPhaseEShadowTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="phase_e_owner")
        for index in range(30):
            movie = Movie.objects.create(
                author=self.user, title_english=f"Phase E {index}", type=Movie.MOVIE
            )
            MovieRating.objects.create(user=self.user, movie=movie, rating=8)
        self.client.force_login(self.user)
        self.url = reverse("profile-feed-activity")

    def request(self, **params):
        return self.client.get(self.url, {"scope": "me", **params})

    @override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=False)
    @patch.object(SocialActivityFeedService, "build_feed_candidate_adaptive")
    @patch.object(SocialActivityFeedService, "count_feed_candidate_logical")
    @patch("core.profile_activity_shadow.report_profile_activity_shadow")
    def test_shadow_off_has_no_candidate_count_or_reporter(self, reporter, logical_count, candidate):
        with patch("core.views.perf_counter") as timer:
            response = self.request()
        self.assertEqual(response.status_code, 200)
        candidate.assert_not_called()
        logical_count.assert_not_called()
        reporter.assert_not_called()
        timer.assert_not_called()

    @override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=True)
    def test_match_and_count_match_reported_while_json_stays_legacy(self):
        with override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=False):
            expected = self.request().json()
        legacy = SocialActivityFeedService.build_feed(user=self.user, scope="me")
        with (
            patch.object(
                SocialActivityFeedService, "build_feed_candidate_adaptive",
                return_value=(legacy[:10], adaptive_metadata(hydrated_rows=10)),
            ) as candidate,
            patch.object(
                SocialActivityFeedService, "count_feed_candidate_logical",
                return_value=len(legacy),
            ) as logical_count,
            patch("core.profile_activity_shadow.report_profile_activity_shadow") as reporter,
        ):
            response = self.request()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        metadata = reporter.call_args.args[0]
        self.assertTrue(metadata["matched"])
        self.assertTrue(metadata["count_matched"])
        candidate.assert_called_once()
        logical_count.assert_called_once()
        reporter.assert_called_once()

    @override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=True)
    def test_candidate_mismatch_is_reported_and_legacy_returns_200(self):
        with override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=False):
            expected = self.request().json()
        with (
            patch.object(
                SocialActivityFeedService, "build_feed_candidate_adaptive",
                return_value=([], adaptive_metadata(hydrated_rows=0)),
            ),
            patch.object(SocialActivityFeedService, "count_feed_candidate_logical", return_value=2),
            patch("core.profile_activity_shadow.report_profile_activity_shadow") as reporter,
        ):
            response = self.request()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        self.assertFalse(reporter.call_args.args[0]["matched"])
        self.assertEqual(reporter.call_args.args[0]["mismatch_reason"], "length")

    @override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=True)
    def test_uncertified_and_count_mismatch_are_observational_only(self):
        with override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=False):
            expected = self.request().json()
        with (
            patch.object(
                SocialActivityFeedService, "build_feed_candidate_adaptive",
                return_value=([], adaptive_metadata(
                    certified=False, fallback_reason="max_batches", hydrated_rows=0
                )),
            ),
            patch.object(SocialActivityFeedService, "count_feed_candidate_logical", return_value=999),
            patch("core.profile_activity_shadow.report_profile_activity_shadow") as reporter,
        ):
            response = self.request()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        metadata = reporter.call_args.args[0]
        self.assertFalse(metadata["candidate_certified"])
        self.assertTrue(metadata["fallback_used"])
        self.assertEqual(metadata["fallback_reason"], "max_batches")
        self.assertFalse(metadata["count_matched"])

    @override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=True)
    def test_k_uses_resolved_page_and_bounded_page_size(self):
        cases = [
            ({"page": 1}, 10),
            ({"page": 2, "page_size": 1}, 2),
            ({"page": 3}, 30),
            ({"page": 1, "page_size": 10}, 10),
            ({"page": 1, "page_size": 50}, 50),
            ({"page": 1, "page_size": 500}, 50),
        ]
        for params, expected_k in cases:
            with self.subTest(params=params):
                with (
                    patch.object(
                        SocialActivityFeedService, "build_feed_candidate_adaptive",
                        return_value=([], adaptive_metadata(hydrated_rows=0)),
                    ) as candidate,
                    patch.object(
                        SocialActivityFeedService, "count_feed_candidate_logical",
                        return_value=2,
                    ),
                ):
                    response = self.request(**params)
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(candidate.call_args.kwargs["k"], expected_k)

    def test_candidate_logical_count_matches_legacy(self):
        legacy = SocialActivityFeedService.build_feed(user=self.user, scope="me")
        candidate_count = SocialActivityFeedService.count_feed_candidate_logical(
            user=self.user, scope="me"
        )
        self.assertEqual(candidate_count, len(legacy))


class ProfileActivityPhaseEObservabilityTests(SimpleTestCase):
    def test_environment_parsers_accept_render_values(self):
        with patch.dict(os.environ, {
            "SHADOW_TRUE": "true", "SHADOW_FALSE": "false", "SHADOW_RATE": "1.0",
        }):
            self.assertIs(project_settings.env_bool("SHADOW_TRUE"), True)
            self.assertIs(project_settings.env_bool("SHADOW_FALSE"), False)
            self.assertEqual(project_settings.env_float("SHADOW_RATE", 0.0), 1.0)

    @override_settings(PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=1.0)
    def test_sample_rate_one_emits_stable_marker(self):
        metadata = {
            "matched": True, "count_matched": True,
            "candidate_certified": True, "fallback_used": False,
            "fallback_reason": None, "k": 30, "legacy_item_count": 30,
            "candidate_item_count": 30, "source_rows_inspected": 35,
            "logical_candidates_inspected": 34, "hydrated_rows": 30,
            "duration_legacy_seconds": 0.01, "duration_candidate_seconds": 0.02,
            "mismatch_reason": None, "batches_by_family": {"ratings": 3},
            "error_type": None,
            "legacy_fingerprint": "legacy-hash", "candidate_fingerprint": "candidate-hash",
        }
        with self.assertLogs("core.profile_activity_shadow", level="INFO") as captured:
            report_profile_activity_shadow(metadata)
        message = captured.output[0]
        self.assertIn("PROFILE_ACTIVITY_SHADOW", message)
        self.assertIn("matched=True", message)
        self.assertIn("certified=True", message)
        self.assertIn("k=30", message)
