from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from core.models import Movie, MovieRating
from core.social_feed import SocialActivityFeedService


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
        for index in range(2):
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
    def test_shadow_off_does_not_execute_candidate(self, candidate):
        response = self.request()
        self.assertEqual(response.status_code, 200)
        candidate.assert_not_called()

    @override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=True)
    def test_match_and_count_match_reported_while_json_stays_legacy(self):
        with override_settings(PROFILE_ACTIVITY_SHADOW_ENABLED=False):
            expected = self.request().json()
        legacy = SocialActivityFeedService.build_feed(user=self.user, scope="me")
        with (
            patch.object(
                SocialActivityFeedService, "build_feed_candidate_adaptive",
                return_value=(legacy, adaptive_metadata()),
            ),
            patch.object(
                SocialActivityFeedService, "count_feed_candidate_logical",
                return_value=len(legacy),
            ),
            patch("core.profile_activity_shadow.report_profile_activity_shadow") as reporter,
        ):
            response = self.request()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        metadata = reporter.call_args.args[0]
        self.assertTrue(metadata["matched"])
        self.assertTrue(metadata["count_matched"])

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
