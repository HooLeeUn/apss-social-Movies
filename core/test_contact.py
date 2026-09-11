from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.core import mail
from django.test import override_settings
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from .models import ContactMessage


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    DEFAULT_FROM_EMAIL="ReCCool <noreply@reccool.com>",
    CONTACT_EMAIL_SUPPORT="support@reccool.com",
    CONTACT_EMAIL_COMMERCIAL="contact@reccool.com",
    CONTACT_EMAIL_SUGGESTIONS="info@reccool.com",
)
class ContactViewTests(APITestCase):
    def setUp(self):
        cache.clear()
        self.user = get_user_model().objects.create_user(
            username="contact-user", email="contact-user@example.com", password="password"
        )
        self.url = reverse("contact")
        self.payload = {
            "category": "technical",
            "subject": "Problema al cargar un video",
            "message": "El video no termina de cargar.",
        }

    def authenticate(self):
        self.client.force_authenticate(self.user)

    def test_authenticated_user_can_send_and_message_is_recorded(self):
        self.authenticate()

        response = self.client.post(self.url, self.payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data, {"detail": "Message sent successfully."})
        saved = ContactMessage.objects.get()
        self.assertEqual(saved.user, self.user)
        self.assertEqual(saved.subject, self.payload["subject"])
        self.assertEqual(saved.message, self.payload["message"])
        self.assertTrue(saved.email_sent)
        self.assertIsNone(saved.email_error)

    def test_unauthenticated_user_is_rejected(self):
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_invalid_category_is_rejected(self):
        self.authenticate()
        self.payload["category"] = "other"
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("category", response.data)

    def test_subject_longer_than_50_characters_is_rejected(self):
        self.authenticate()
        self.payload["subject"] = "x" * 51
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("subject", response.data)

    def test_message_longer_than_1500_characters_is_rejected(self):
        self.authenticate()
        self.payload["message"] = "x" * 1501
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("message", response.data)

    def test_whitespace_only_subject_and_message_are_rejected(self):
        self.authenticate()
        self.payload.update(subject="   ", message=" \t ")
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("subject", response.data)
        self.assertIn("message", response.data)

    def test_server_resolves_recipient_and_ignores_client_recipient(self):
        self.authenticate()
        self.payload["recipient_email"] = "attacker@example.com"
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(mail.outbox[0].to, ["support@reccool.com"])
        self.assertEqual(mail.outbox[0].from_email, "ReCCool <noreply@reccool.com>")
        self.assertEqual(ContactMessage.objects.get().recipient_email, "support@reccool.com")

    @patch("core.views.send_mail", side_effect=RuntimeError("SMTP unavailable"))
    def test_email_failure_is_recorded_and_returns_controlled_error(self, _send_mail):
        self.authenticate()
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        self.assertEqual(
            response.data, {"detail": "The message was saved, but the email could not be sent."}
        )
        saved = ContactMessage.objects.get()
        self.assertFalse(saved.email_sent)
        self.assertEqual(saved.email_error, "SMTP unavailable")

    @override_settings(CONTACT_EMAIL_SUPPORT="")
    def test_missing_recipient_setting_is_recorded_and_returns_controlled_error(self):
        self.authenticate()
        with self.assertLogs("core.views", level="ERROR") as logs:
            response = self.client.post(self.url, self.payload, format="json")

        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE)
        self.assertEqual(
            response.data, {"detail": "No recipient email is configured for this category."}
        )
        saved = ContactMessage.objects.get()
        self.assertEqual(saved.recipient_email, "")
        self.assertFalse(saved.email_sent)
        self.assertEqual(
            saved.email_error,
            "Contact recipient setting CONTACT_EMAIL_SUPPORT is not configured.",
        )
        self.assertIn("CONTACT_EMAIL_SUPPORT is not configured", logs.output[0])
        self.assertEqual(len(mail.outbox), 0)

    def test_subject_is_trimmed_and_line_breaks_are_rejected(self):
        self.authenticate()
        self.payload["subject"] = "  Consulta  "
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(ContactMessage.objects.get().subject, "Consulta")

        self.payload["subject"] = "Injected\nBcc: attacker@example.com"
        response = self.client.post(self.url, self.payload, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_categories_select_corresponding_configured_recipients(self):
        self.authenticate()
        expected_recipients = {
            "technical": "support@reccool.com",
            "commercial": "contact@reccool.com",
            "requests_suggestions": "info@reccool.com",
        }

        for category, expected_recipient in expected_recipients.items():
            with self.subTest(category=category):
                self.payload["category"] = category
                response = self.client.post(self.url, self.payload, format="json")
                self.assertEqual(response.status_code, status.HTTP_201_CREATED)
                self.assertEqual(mail.outbox[-1].to, [expected_recipient])
                self.assertEqual(
                    ContactMessage.objects.get(category=category).recipient_email,
                    expected_recipient,
                )
