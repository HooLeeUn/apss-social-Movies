from datetime import date

from django.contrib import admin
from django.contrib.auth import get_user_model
from django.core import mail
from django.test import TestCase, override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from .admin import VideoCommentAdmin
from .models import Comment, ContentReport, Movie, PendingUserRegistration, VideoComment


class UGCReportTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.reporter = get_user_model().objects.create_user(username="reporter1", password="password")
        self.author = get_user_model().objects.create_user(username="contentowner", password="password")
        self.movie = Movie.objects.create(author=self.author, title_english="Reported Movie", type=Movie.MOVIE, release_year=2025)
        self.comment = Comment.objects.create(author=self.author, movie=self.movie, body="text")
        self.video = VideoComment.objects.create(user=self.author, movie=self.movie, video="video_comments/report.mp4", duration_seconds=1, mime_type="video/mp4", file_size=1)
        self.url = reverse("content-report-create")

    def post(self, payload, user=None):
        if user is not None:
            self.client.force_authenticate(user=user)
        return self.client.post(self.url, payload, format="json")

    def test_reports_comment_video_and_user(self):
        self.client.force_authenticate(self.reporter)
        payloads = [
            {"type": "comment", "object_id": self.comment.pk, "reason": "spam_or_scam"},
            {"type": "video_comment", "object_id": self.video.pk, "reason": "inappropriate_content"},
            {"type": "user", "reported_user": self.author.pk, "reason": "harassment_or_threats"},
        ]
        for payload in payloads:
            response = self.client.post(self.url, payload, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED)
            self.assertEqual(set(response.data), {"id", "status", "created_at"})
            self.assertNotIn("admin_notes", response.data)
        self.assertEqual(ContentReport.objects.count(), 3)

    def test_cannot_report_self_or_own_content(self):
        self.client.force_authenticate(self.author)
        for payload in [
            {"type": "user", "reported_user": self.author.pk, "reason": "other"},
            {"type": "comment", "object_id": self.comment.pk, "reason": "other"},
            {"type": "video_comment", "object_id": self.video.pk, "reason": "other"},
        ]:
            self.assertEqual(self.client.post(self.url, payload, format="json").status_code, status.HTTP_403_FORBIDDEN)

    def test_missing_object_is_404(self):
        response = self.post({"type": "comment", "object_id": 999999, "reason": "other"}, self.reporter)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_invalid_reason_is_400(self):
        response = self.post({"type": "comment", "object_id": self.comment.pk, "reason": "invented"}, self.reporter)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_active_duplicate_is_400(self):
        payload = {"type": "comment", "object_id": self.comment.pk, "reason": "other"}
        self.assertEqual(self.post(payload, self.reporter).status_code, status.HTTP_201_CREATED)
        self.assertEqual(self.post(payload, self.reporter).status_code, status.HTTP_400_BAD_REQUEST)

    def test_anonymous_is_rejected(self):
        response = self.client.post(self.url, {"type": "user", "reported_user": self.author.pk, "reason": "other"}, format="json")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)


class HiddenVideoCommentTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="videoauthor", password="password")
        self.movie = Movie.objects.create(author=self.user, title_english="Hidden Movie", type=Movie.MOVIE, release_year=2025)
        self.visible = VideoComment.objects.create(user=self.user, movie=self.movie, video="video_comments/visible.mp4", duration_seconds=1, mime_type="video/mp4", file_size=1)
        self.hidden = VideoComment.objects.create(user=self.user, movie=self.movie, video="video_comments/hidden.mp4", duration_seconds=1, mime_type="video/mp4", file_size=1, is_hidden=True)
        self.client = APIClient()

    def test_hidden_video_excluded_from_list_detail_and_profile_activity(self):
        response = self.client.get(reverse("movie-video-comments", kwargs={"pk": self.movie.pk}))
        self.assertEqual([item["id"] for item in response.data["results"]], [self.visible.pk])
        self.assertEqual(self.client.get(reverse("video-comment-detail", kwargs={"pk": self.hidden.pk})).status_code, 404)
        profile_response = self.client.get(reverse("user-video-reactions", kwargs={"username": self.user.username}))
        self.assertNotIn(self.hidden.pk, [item.get("id") for item in profile_response.data.get("results", [])])
        self.assertTrue(VideoComment.objects.filter(pk=self.hidden.pk).exists())

    def test_admin_can_restore_hidden_video(self):
        self.assertIn("is_hidden", VideoCommentAdmin.list_display)
        self.assertIn("is_hidden", VideoCommentAdmin.list_filter)
        self.hidden.is_hidden = False
        self.hidden.save(update_fields=["is_hidden"])
        self.assertTrue(VideoComment.objects.visible().filter(pk=self.hidden.pk).exists())


@override_settings(EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
class TermsAcceptanceTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.url = reverse("register")
        self.payload = {
            "username": "termsuser", "email": "terms@example.com", "first_name": "Terms",
            "last_name": "User", "password": "password123", "password_confirmation": "password123",
            "birth_date": date(2000, 1, 1).isoformat(), "accept_terms": True, "terms_version": "2026-09",
        }

    def test_terms_are_required(self):
        payload = dict(self.payload, accept_terms=False)
        self.assertEqual(self.client.post(self.url, payload, format="json").status_code, 400)
        payload = dict(self.payload)
        payload.pop("terms_version")
        self.assertEqual(self.client.post(self.url, payload, format="json").status_code, 400)

    def test_acceptance_survives_email_confirmation(self):
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, 201)
        pending = PendingUserRegistration.objects.get(username="termsuser")
        self.assertIsNotNone(pending.terms_accepted_at)
        self.client.get(reverse("register-confirm-email", kwargs={"token": pending.token}))
        profile = get_user_model().objects.get(username="termsuser").profile
        self.assertIsNotNone(profile.terms_accepted_at)
        self.assertEqual(profile.terms_version, "2026-09")

    def test_existing_user_can_still_authenticate(self):
        user = get_user_model().objects.create_user(username="existingterms", password="password")
        self.assertTrue(self.client.login(username=user.username, password="password"))
        self.assertIsNone(user.profile.terms_accepted_at)
