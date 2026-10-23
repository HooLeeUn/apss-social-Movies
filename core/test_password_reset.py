from datetime import datetime, timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.auth.tokens import default_token_generator
from django.core import mail
from django.core.cache import cache
from django.test import TestCase, override_settings
from django.utils.encoding import force_bytes
from django.utils.http import urlsafe_base64_encode
from rest_framework.authtoken.models import Token
from rest_framework.test import APIClient

from .models import PasswordResetDelivery
from .password_reset import GENERIC_RESPONSE, deliver_password_resets


@override_settings(EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend", FRONTEND_BASE_URL="https://reccool.example")
class PasswordResetTests(TestCase):
    request_url = "/api/auth/password-reset/request/"
    confirm_url = "/api/auth/password-reset/confirm/"
    password = "Fresh-strong-password-72!"

    def setUp(self):
        cache.clear()
        self.client = APIClient()
        self.user = get_user_model().objects.create_user("resetowner", "Owner@Example.com", "Old-password-34!")
        self.uid = urlsafe_base64_encode(force_bytes(self.user.pk))
        self.token = default_token_generator.make_token(self.user)

    def confirm(self, **changes):
        data = {"uid": self.uid, "token": self.token, "new_password": self.password, "confirm_password": self.password}
        data.update(changes)
        return self.client.post(self.confirm_url, data, format="json")

    def test_request_indistinguishable_for_active_missing_and_inactive(self):
        get_user_model().objects.create_user("inactive", "inactive@example.com", "Old-password-34!", is_active=False)
        responses = []
        for email in [" OWNER@example.COM ", "missing@example.com", "inactive@example.com"]:
            with self.assertNumQueries(1):
                response = self.client.post(self.request_url, {"email": email}, format="json")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), GENERIC_RESPONSE)
            self.assertNotIn("token", response.json())
            responses.append(response.content)
        self.assertEqual(len(set(responses)), 1)
        self.assertEqual(PasswordResetDelivery.objects.first().email, "owner@example.com")
        self.assertEqual(deliver_password_resets(), 3)
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(mail.outbox[0].to, [self.user.email])
        self.assertIn(f"https://reccool.example/reset-password/{self.uid}/", mail.outbox[0].body)
        self.assertIn("1 hora", mail.outbox[0].body)
        self.assertFalse(PasswordResetDelivery.objects.exists())

    def test_valid_reset_hashes_password_revokes_auth_and_allows_login(self):
        auth = Token.objects.create(user=self.user)
        self.assertEqual(self.confirm().status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(self.password))
        self.assertNotEqual(self.user.password, self.password)
        self.assertFalse(Token.objects.filter(pk=auth.pk).exists())
        response = self.client.post("/api/token/", {"username": self.user.username, "password": self.password})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.post("/api/token/", {"username": self.user.username, "password": "Old-password-34!"}).status_code, 400)

    def test_token_expires_after_one_hour(self):
        now = datetime(2026, 10, 23, 12)
        with patch.object(default_token_generator, "_now", return_value=now):
            self.token = default_token_generator.make_token(self.user)
        with patch.object(default_token_generator, "_now", return_value=now + timedelta(seconds=3601)):
            self.assertEqual(self.confirm().status_code, 400)

    def test_token_valid_before_expiry(self):
        now = datetime(2026, 10, 23, 12)
        with patch.object(default_token_generator, "_now", return_value=now):
            self.token = default_token_generator.make_token(self.user)
        with patch.object(default_token_generator, "_now", return_value=now + timedelta(seconds=3599)):
            self.assertEqual(self.confirm().status_code, 200)

    def test_invalid_token(self):
        self.assertEqual(self.confirm(token="invalid").status_code, 400)

    def test_invalid_uid(self):
        for uid in ["!!!", "OTk5OTk5OTk5", "_w", "eA"]:
            self.assertEqual(self.confirm(uid=uid).status_code, 400)

    def test_used_token_cannot_be_reused(self):
        self.assertEqual(self.confirm().status_code, 200)
        self.assertEqual(self.confirm().status_code, 400)

    def test_other_password_change_invalidates_token(self):
        self.user.set_password("Other-password-84!")
        self.user.save()
        self.assertEqual(self.confirm().status_code, 400)

    def test_inactive_or_deleted_user_cannot_confirm(self):
        self.user.is_active = False
        self.user.save()
        self.assertEqual(self.confirm().status_code, 400)
        self.user.delete()
        self.assertEqual(self.confirm().status_code, 400)

    def test_passwords_must_match(self):
        self.assertEqual(self.confirm(confirm_password="different").status_code, 400)
        self.user.refresh_from_db()
        self.assertTrue(default_token_generator.check_token(self.user, self.token))

    def test_configured_password_validators(self):
        for password in ["short", "password", "123456789012", self.user.username]:
            self.assertEqual(self.confirm(new_password=password, confirm_password=password).status_code, 400)

    def test_password_whitespace_is_preserved(self):
        password = "  Fresh-strong-password-72!  "
        self.assertEqual(self.confirm(new_password=password, confirm_password=password).status_code, 200)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(password))

    def test_request_throttle_independent_of_address(self):
        for _ in range(5):
            self.assertEqual(self.client.post(self.request_url, {"email": "missing@example.com"}).status_code, 200)
        self.assertEqual(self.client.post(self.request_url, {"email": self.user.email}).status_code, 429)

    def test_confirm_throttle(self):
        for _ in range(20):
            self.assertEqual(self.confirm(token="invalid").status_code, 400)
        self.assertEqual(self.confirm().status_code, 429)

    def test_bad_auth_header_does_not_change_public_response(self):
        self.client.credentials(HTTP_AUTHORIZATION="Token invalid")
        self.assertEqual(self.client.post(self.request_url, {"email": self.user.email}).json(), GENERIC_RESPONSE)

    @patch("core.password_reset.send_mail", side_effect=RuntimeError("sensitive SMTP detail"))
    def test_delivery_errors_do_not_expose_sensitive_details(self, send):
        response = self.client.post(self.request_url, {"email": self.user.email})
        with self.assertLogs("core.password_reset", level="WARNING") as logs:
            deliver_password_resets()
        self.assertEqual(response.json(), GENERIC_RESPONSE)
        self.assertNotIn("sensitive", str(logs.output))
        self.assertFalse(PasswordResetDelivery.objects.exists())

    def test_stale_jobs_are_discarded(self):
        from django.utils import timezone
        job = PasswordResetDelivery.objects.create(email=self.user.email)
        PasswordResetDelivery.objects.filter(pk=job.pk).update(created_at=timezone.now() - timedelta(hours=2))
        with patch("core.password_reset.send_mail") as send:
            deliver_password_resets()
        send.assert_not_called()
