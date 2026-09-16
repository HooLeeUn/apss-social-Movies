import re
from datetime import timedelta
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core import mail
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone
from rest_framework.authtoken.models import Token
from rest_framework.test import APIClient

from .models import (
    Comment,
    CommentReaction,
    Follow,
    Friendship,
    Movie,
    MovieRating,
    PendingAccountDeletion,
    VideoComment,
    VideoCommentReaction,
)


User = get_user_model()


@override_settings(
    EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend",
    FRONTEND_BASE_URL="https://staging.reccool.example",
)
class AccountManagementTests(TestCase):
    password = "Valid-test-password-934!"

    def setUp(self):
        self.client = APIClient()
        self.user = User.objects.create_user(username="owner", email="owner@example.com", password=self.password)
        self.other = User.objects.create_user(username="other", email="other@example.com", password=self.password)
        self.token = Token.objects.create(user=self.user)
        self.client.credentials(HTTP_AUTHORIZATION=f"Token {self.token.key}")

    def test_authenticated_delete_erases_account_relations_and_token(self):
        movie = Movie.objects.create(author=self.other, title_english="Test")
        rating = MovieRating.objects.create(user=self.user, movie=movie, score=6)
        comment = Comment.objects.create(author=self.user, movie=movie, body="UGC")
        CommentReaction.objects.create(user=self.other, comment=comment, reaction_type="like")
        Follow.objects.create(follower=self.user, following=self.other)
        Friendship.objects.create(requester=self.user, user1=self.user, user2=self.other)

        response = self.client.post(reverse("delete-account"), {"password": self.password}, format="json")

        self.assertEqual(response.status_code, 200)
        self.assertFalse(User.objects.filter(pk=self.user.pk).exists())
        self.assertFalse(Token.objects.filter(key=self.token.key).exists())
        self.assertFalse(MovieRating.objects.filter(pk=rating.pk).exists())
        self.assertFalse(Comment.objects.filter(pk=comment.pk).exists())
        self.assertFalse(Follow.objects.filter(follower_id=self.user.pk).exists())
        self.assertFalse(Friendship.objects.filter(requester_id=self.user.pk).exists())

    def test_wrong_password_does_not_delete_any_account(self):
        response = self.client.post(reverse("delete-account"), {"password": "wrong"}, format="json")
        self.assertEqual(response.status_code, 400)
        self.assertTrue(User.objects.filter(pk=self.user.pk).exists())
        self.assertTrue(User.objects.filter(pk=self.other.pk).exists())

    def test_client_cannot_select_another_user_for_deletion(self):
        response = self.client.post(
            reverse("delete-account"),
            {"password": self.password, "user_id": self.other.pk},
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(User.objects.filter(pk=self.other.pk).exists())

    @patch("core.account_deletion._delete_file_safely")
    def test_account_media_is_deleted_through_its_storage(self, delete_file):
        self.user.profile.avatar = SimpleUploadedFile("avatar.jpg", b"avatar", content_type="image/jpeg")
        self.user.profile.save()
        movie = Movie.objects.create(author=self.other, title_english="Media")
        VideoComment.objects.create(
            user=self.user,
            movie=movie,
            video=SimpleUploadedFile("clip.mp4", b"video", content_type="video/mp4"),
            duration_seconds=1,
            mime_type="video/mp4",
            file_size=5,
        )
        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(reverse("delete-account"), {"password": self.password}, format="json")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(delete_file.call_count, 2)

    def test_rating_and_dynamic_aggregate_exclude_deleted_user(self):
        movie = Movie.objects.create(author=self.other, title_english="Ratings")
        MovieRating.objects.create(user=self.user, movie=movie, score=6)
        MovieRating.objects.create(user=self.other, movie=movie, score=10)
        before = Movie.objects.filter(pk=movie.pk).with_rating_stats().get()
        self.assertEqual(float(before.real_ratings_avg), 8.0)
        self.assertEqual(before.real_ratings_count, 2)
        self.client.post(reverse("delete-account"), {"password": self.password}, format="json")
        after = Movie.objects.filter(pk=movie.pk).with_rating_stats().get()
        self.assertEqual(float(after.real_ratings_avg), 10.0)
        self.assertEqual(after.real_ratings_count, 1)

    def test_dynamic_reaction_and_follow_counts_exclude_deleted_user(self):
        movie = Movie.objects.create(author=self.other, title_english="Counts")
        comment = Comment.objects.create(author=self.other, movie=movie, body="Keep")
        CommentReaction.objects.create(user=self.user, comment=comment, reaction_type="like")
        video = VideoComment.objects.create(
            user=self.other, movie=movie, video="video_comments/keep.mp4",
            duration_seconds=1, mime_type="video/mp4", file_size=1,
        )
        VideoCommentReaction.objects.create(user=self.user, video_comment=video, reaction_type="like")
        Follow.objects.create(follower=self.user, following=self.other)
        self.client.post(reverse("delete-account"), {"password": self.password}, format="json")
        self.assertEqual(Comment.objects.with_reaction_stats(self.other).get(pk=comment.pk).likes_count, 0)
        self.assertEqual(VideoComment.objects.with_reaction_stats(self.other).get(pk=video.pk).likes_count, 0)
        self.assertEqual(Follow.objects.filter(following=self.other).count(), 0)

    def _request_public_deletion(self, email):
        return APIClient().post(reverse("account-deletion-request"), {"email": email}, format="json")

    def test_public_request_is_non_enumerating_and_email_targets_frontend(self):
        existing = self._request_public_deletion(self.user.email)
        body = mail.outbox[-1].body
        missing = self._request_public_deletion("missing@example.com")
        self.assertEqual(existing.status_code, missing.status_code)
        self.assertEqual(existing.data, missing.data)
        self.assertIn("https://staging.reccool.example/delete-account/confirm/", body)
        self.assertNotIn("/api/account-deletion/confirm/", body)

    def _deletion_token(self):
        self._request_public_deletion(self.user.email)
        return re.search(r"/delete-account/confirm/([^\s]+)", mail.outbox[-1].body).group(1)

    def test_valid_public_token_deletes_and_cannot_be_reused(self):
        token = self._deletion_token()
        url = reverse("account-deletion-confirm", kwargs={"token": token})
        response = APIClient().post(url)
        self.assertEqual(response.status_code, 200)
        self.assertFalse(User.objects.filter(pk=self.user.pk).exists())
        self.assertEqual(APIClient().post(url).status_code, 400)

    def test_expired_public_token_fails(self):
        token = self._deletion_token()
        PendingAccountDeletion.objects.update(expires_at=timezone.now() - timedelta(seconds=1))
        response = APIClient().post(reverse("account-deletion-confirm", kwargs={"token": token}))
        self.assertEqual(response.status_code, 400)
        self.assertTrue(User.objects.filter(pk=self.user.pk).exists())

    def test_change_password_requires_current_valid_password(self):
        response = self.client.post(reverse("change-password"), {
            "current_password": "wrong", "new_password": "Other-valid-password-823!",
            "new_password_confirmation": "Other-valid-password-823!",
        }, format="json")
        self.assertEqual(response.status_code, 400)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(self.password))

    def test_change_password_validates_and_invalidates_token(self):
        invalid = self.client.post(reverse("change-password"), {
            "current_password": self.password, "new_password": "123", "new_password_confirmation": "123",
        }, format="json")
        self.assertEqual(invalid.status_code, 400)
        new_password = "Other-valid-password-823!"
        response = self.client.post(reverse("change-password"), {
            "current_password": self.password, "new_password": new_password,
            "new_password_confirmation": new_password,
        }, format="json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data["reauthentication_required"])
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(new_password))
        self.assertFalse(Token.objects.filter(key=self.token.key).exists())
        self.assertEqual(self.client.get(reverse("me")).status_code, 401)
