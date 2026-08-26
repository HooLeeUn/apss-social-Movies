from django.contrib.auth import get_user_model
from django.test import TestCase

from core.models import Comment, Movie
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG4Tests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="phase_g4_owner")
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Repeated movie",
            title_spanish="Película repetida",
            type=Movie.MOVIE,
        )
        for index in range(8):
            Comment.objects.create(
                author=self.user,
                movie=self.movie,
                body=f"Repeated activity {index}",
                visibility=Comment.VISIBILITY_PUBLIC,
            )

    def build_candidate(self):
        return SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user,
            scope=SocialActivityFeedService.SCOPE_ME,
            k=8,
            fallback_to_legacy=False,
        )

    def test_duplicate_movie_and_actor_payloads_are_built_once_and_reused(self):
        candidate = self.build_candidate()
        legacy = SocialActivityFeedService.build_feed(
            user=self.user, scope=SocialActivityFeedService.SCOPE_ME
        )[:8]

        self.assertEqual(candidate, legacy)
        self.assertEqual(len(candidate), 8)
        self.assertTrue(all(row["movie"] is candidate[0]["movie"] for row in candidate))
        self.assertTrue(all(row["actor"] is candidate[0]["actor"] for row in candidate))

    def test_localized_titles_are_preserved_and_cache_is_request_local(self):
        first = self.build_candidate()
        self.assertEqual(first[0]["movie"]["title_english"], "Repeated movie")
        self.assertEqual(first[0]["movie"]["title_spanish"], "Película repetida")

        Movie.objects.filter(pk=self.movie.pk).update(
            title_english="Updated movie", title_spanish="Película actualizada"
        )
        second = self.build_candidate()
        self.assertEqual(second[0]["movie"]["title_english"], "Updated movie")
        self.assertEqual(second[0]["movie"]["title_spanish"], "Película actualizada")
        self.assertIsNot(first[0]["movie"], second[0]["movie"])

    def test_profiler_metrics_and_order_remain_available(self):
        candidate, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user,
            scope=SocialActivityFeedService.SCOPE_ME,
            k=8,
            fallback_to_legacy=False,
            return_metadata=True,
            profile_enabled=True,
        )
        profile = metadata["profile"]
        for metric in (
            "hydration_total_ms", "hydration_sql_ms", "hydration_python_ms",
            "hydration_accounted_ms", "hydration_unaccounted_ms",
            "hydration_families", "hydration_components",
        ):
            self.assertIn(metric, profile)
        self.assertEqual(
            candidate,
            SocialActivityFeedService.build_feed(
                user=self.user, scope=SocialActivityFeedService.SCOPE_ME
            )[:8],
        )
