from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseG6Tests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(username="g6-owner")

    def test_candidate_queryset_definitions_are_lazy(self):
        service = SocialActivityFeedService
        actor_ids = [self.user.id]
        with CaptureQueriesContext(connection) as captured:
            querysets = (
                service.rating_candidates_queryset(actor_ids=actor_ids, viewer=self.user),
                service.public_comment_candidates_queryset(actor_ids=actor_ids, viewer=self.user),
                service.public_reaction_candidates_queryset(actor_ids=actor_ids, viewer=self.user),
                service.private_message_candidates_queryset(actor_ids=actor_ids, viewer=self.user),
                service.video_created_candidates_queryset(actor=self.user, viewer=self.user),
                service.video_reaction_candidates_queryset(viewer=self.user),
                service._public_received_reaction_rows(viewer=self.user),
                service._private_received_reaction_rows(viewer=self.user),
                service._video_received_reaction_rows(viewer=self.user),
            )
        self.assertEqual(len(captured), 0)

        with CaptureQueriesContext(connection) as captured:
            list(querysets[0][:1])
        self.assertEqual(len(captured), 1)

    def test_adaptive_setup_has_one_required_private_privacy_query(self):
        with CaptureQueriesContext(connection) as captured:
            sequences = SocialActivityFeedService._adaptive_sequences(
                user=self.user, actor_ids=[self.user.id]
            )

        self.assertEqual(len(sequences), 9)
        self.assertEqual(len(captured), 1)
        self.assertIn("core_commentreaction", captured[0]["sql"].lower())

    def test_g6_profile_separates_queryset_definition_from_fetch(self):
        _, metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            user=self.user, scope="me", k=10, fallback_to_legacy=False,
            return_metadata=True, profile_enabled=True,
        )
        profile = metadata["profile"]
        self.assertIn("queryset_definition_ms", profile["hydration_components"])
        for family in profile["hydration_families"].values():
            self.assertIn("queryset_definition_ms", family)

    def test_profiler_does_not_change_candidate_contract(self):
        arguments = dict(
            user=self.user, scope="me", k=10, fallback_to_legacy=False,
            return_metadata=True,
        )
        profiled, _ = SocialActivityFeedService.build_feed_candidate_adaptive(
            **arguments, profile_enabled=True
        )
        plain, plain_metadata = SocialActivityFeedService.build_feed_candidate_adaptive(
            **arguments, profile_enabled=False
        )
        self.assertEqual(profiled, plain)
        self.assertNotIn("profile", plain_metadata)
