from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Profile


class MeOnboardingTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="onboarding", password="password")
        self.other_user = get_user_model().objects.create_user(username="other", password="password")
        self.client = APIClient()
        self.client.force_authenticate(self.user)
        self.url = reverse("me-onboarding")

    def patch(self, tour, tour_status, current_step=None, version=1, **extra):
        payload = {
            "tour": tour,
            "status": tour_status,
            "version": version,
            "current_step": current_step,
            **extra,
        }
        return self.client.patch(self.url, payload, format="json")

    def test_new_user_starts_with_all_tours_pending(self):
        profile = self.user.profile
        for tour in ("feed", "profile_feed", "detail_movie"):
            self.assertEqual(getattr(profile, f"{tour}_tour_status"), "pending")
            self.assertEqual(getattr(profile, f"{tour}_tour_version"), 1)
            self.assertIsNone(getattr(profile, f"{tour}_tour_current_step"))

    def test_get_returns_all_tours(self):
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(set(response.data), {"feed", "profile_feed", "detail_movie"})
        self.assertEqual(response.data["feed"], {"status": "pending", "version": 1, "current_step": None})

    def test_feed_can_start_update_step_and_complete(self):
        response = self.patch("feed", "in_progress", 0)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["feed"]["current_step"], 0)

        response = self.patch("feed", "in_progress", 3)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["feed"]["current_step"], 3)

        response = self.patch("feed", "completed", 99)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["feed"], {"status": "completed", "version": 1, "current_step": None})

    def test_profile_feed_can_be_skipped_from_pending(self):
        response = self.patch("profile_feed", "skipped", 4)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["profile_feed"]["status"], "skipped")
        self.assertIsNone(response.data["profile_feed"]["current_step"])

    def test_detail_movie_progress_is_independent_and_persists(self):
        self.patch("feed", "in_progress", 1)
        self.patch("profile_feed", "skipped")
        response = self.patch("detail_movie", "in_progress", 4)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        refreshed = self.client.get(self.url)
        self.assertEqual(refreshed.data["feed"]["status"], "in_progress")
        self.assertEqual(refreshed.data["profile_feed"]["status"], "skipped")
        self.assertEqual(refreshed.data["detail_movie"]["status"], "in_progress")
        self.assertEqual(refreshed.data["detail_movie"]["current_step"], 4)

    def test_request_cannot_select_or_modify_another_user(self):
        response = self.patch("feed", "in_progress", 2, user_id=self.other_user.id)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.other_user.profile.refresh_from_db()
        self.assertEqual(self.other_user.profile.feed_tour_status, "pending")
        self.assertIsNone(self.other_user.profile.feed_tour_current_step)

    def test_invalid_tour_status_version_and_negative_step_are_rejected(self):
        invalid_payloads = (
            {"tour": "unknown", "status": "in_progress", "version": 1, "current_step": 0},
            {"tour": "feed", "status": "unknown", "version": 1, "current_step": 0},
            {"tour": "feed", "status": "in_progress", "version": 0, "current_step": 0},
            {"tour": "feed", "status": "in_progress", "version": 1, "current_step": -1},
        )
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                response = self.client.patch(self.url, payload, format="json")
                self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_in_progress_requires_step_and_terminal_states_are_terminal(self):
        self.assertEqual(self.patch("feed", "in_progress").status_code, status.HTTP_400_BAD_REQUEST)
        self.patch("feed", "skipped", 3)
        response = self.patch("feed", "in_progress", 1)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_authentication_is_required(self):
        self.client.force_authenticate(user=None)
        self.assertEqual(self.client.get(self.url).status_code, status.HTTP_401_UNAUTHORIZED)


class ExistingUserOnboardingMigrationContractTests(TestCase):
    def test_profile_defaults_match_existing_user_backfill_values(self):
        fields = Profile._meta.fields
        onboarding_fields = {field.name: field for field in fields if "_tour_" in field.name}
        for name, field in onboarding_fields.items():
            expected = None if name.endswith("current_step") else (1 if name.endswith("version") else "pending")
            self.assertEqual(field.get_default(), expected)
