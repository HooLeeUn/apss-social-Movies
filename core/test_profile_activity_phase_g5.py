from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Movie, MovieRating
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG5Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(
            username="g5-owner", email="g5-private@example.test"
        )
        movie = Movie.objects.create(
            author=cls.user, title_english="G5 private title", type=Movie.MOVIE
        )
        MovieRating.objects.create(user=cls.user, movie=movie, rating=8)

    def candidate(self, enabled):
        return SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user, scope="me", k=10, fallback_to_legacy=False,
            return_metadata=True, profile_enabled=enabled,
        )

    def test_g5_metrics_are_non_negative_and_residual_is_bounded(self):
        _, metadata = self.candidate(True)
        profile = metadata["profile"]
        global_fields = (
            "hydration_query_build_ms", "hydration_sql_execute_ms",
            "hydration_row_fetch_conversion_ms",
            "hydration_model_materialization_ms", "hydration_serialize_ms",
            "hydration_wall_ms", "hydration_cpu_ms", "hydration_unaccounted_ms",
        )
        for field in global_fields:
            self.assertGreaterEqual(profile[field], 0, field)
        self.assertLessEqual(
            profile["hydration_unaccounted_ms"], profile["hydration_wall_ms"]
        )

        family = profile["hydration_families"]["ratings"]
        for field in (
            "query_build_ms", "sql_execute_ms", "row_fetch_and_conversion_ms",
            "model_materialization_ms", "queryset_iteration_ms", "serialize_ms",
            "materialization_wall_ms", "materialization_cpu_ms",
            "selected_column_count", "select_related_count",
        ):
            self.assertGreaterEqual(family[field], 0, field)
        self.assertGreater(family["selected_column_count"], 0)
        self.assertIs(family["possible_overfetch"], True)

    def test_profiler_off_preserves_payload_and_has_no_g5_metadata(self):
        profiled, profiled_metadata = self.candidate(True)
        plain, plain_metadata = self.candidate(False)
        self.assertEqual(profiled, plain)
        self.assertIn("profile", profiled_metadata)
        self.assertNotIn("profile", plain_metadata)

    def test_profile_metadata_contains_no_payload_pii(self):
        _, metadata = self.candidate(True)
        rendered = repr(metadata["profile"])
        for secret in (
            self.user.username, self.user.email, "G5 private title",
            "message body", "comment body", "https://video.example/secret",
        ):
            self.assertNotIn(secret, rendered)
