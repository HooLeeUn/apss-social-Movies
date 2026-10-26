from importlib import import_module
from types import SimpleNamespace

from django.apps import apps
from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse
from rest_framework.test import APIClient

from .models import Comment, DirectedCommentRecipient, Friendship, Movie, UserNotification, UserVisibilityBlock


class DirectedGroupTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.a, cls.b, cls.c, cls.d = [get_user_model().objects.create_user(username=name) for name in ("Alice", "Bob", "Carol", "Outside")]
        cls.movie = Movie.objects.create(author=cls.a, title_english="Group movie", type=Movie.MOVIE)
        for first, second in ((cls.a, cls.b), (cls.a, cls.c), (cls.b, cls.c)):
            Friendship.objects.create(requester=first, user1=first, user2=second, status=Friendship.STATUS_ACCEPTED)

    def setUp(self):
        self.client = APIClient()
        self.url = reverse("movie-directed-comments", kwargs={"pk": self.movie.pk})

    def send(self, user=None, **payload):
        self.client.force_authenticate(user or self.a)
        return self.client.post(self.url, {"body": "hello", **payload}, format="json")

    def group(self):
        response = self.send(mentioned_usernames=["Bob", "Carol"])
        self.assertEqual(response.status_code, 201, response.data)
        return Comment.objects.get(pk=response.data["id"])

    def test_legacy_single_recipient_payloads(self):
        for field in ("mentioned_username", "recipient_username"):
            with self.subTest(field=field):
                response = self.send(**{field: "bob"})
                self.assertEqual(response.status_code, 201)
                self.assertEqual(response.data["target_user"], self.b.pk)
                self.assertFalse(response.data["is_group"])

    def test_one_comment_two_recipients_and_individual_notifications(self):
        comment = self.group()
        self.assertEqual(Comment.objects.count(), 1)
        self.assertEqual(set(comment.recipient_states.values_list("user_id", flat=True)), {self.b.pk, self.c.pk})
        notifications = UserNotification.objects.filter(comment=comment, type="private_message")
        self.assertEqual(set(notifications.values_list("recipient_id", flat=True)), {self.b.pk, self.c.pk})
        self.assertFalse(notifications.filter(recipient=self.a).exists())

    def test_all_members_see_detail_and_inbox_outsider_cannot(self):
        comment = self.group()
        for user in (self.a, self.b, self.c, self.d):
            with self.subTest(user=user.username):
                self.client.force_authenticate(user)
                response = self.client.get(reverse("comment-detail", kwargs={"pk": comment.pk}))
                self.assertEqual(response.status_code, 404 if user == self.d else 200)
                inbox = self.client.get(reverse("me-messages")).data
                self.assertEqual(any(item["id"] == comment.pk for item in inbox), user != self.d)

    def test_received_list_and_notification_summary_include_second_recipient(self):
        comment = self.group()
        self.client.force_authenticate(self.c)
        received = self.client.get(reverse("directed-comments-received")).data
        self.assertIn(comment.pk, [item["id"] for item in (received["results"] if isinstance(received, dict) else received) if isinstance(item, dict)])
        summary = self.client.get(reverse("me-notifications")).data
        self.assertEqual(summary["total_unread"], 1)
        self.assertEqual([item["id"] for item in summary["items"]], [f"pm-{comment.pk}"])

    def test_independent_read_state_for_every_mark_read_endpoint(self):
        for endpoint, payload in (
            ("notifications-mark-read-batch", lambda pk: {"ids": [f"pm-{pk}"]}),
            ("me-notifications-mark-read", lambda pk: {"private_message_id": pk}),
            ("notifications-mark-context-read", lambda pk: {"context": "private_inbox"}),
            ("notifications-mark-all-read", lambda pk: {}),
            ("me-messages-mark-as-read", lambda pk: {}),
        ):
            with self.subTest(endpoint=endpoint):
                comment = self.group()
                self.client.force_authenticate(self.b)
                response = self.client.post(reverse(endpoint), payload(comment.pk), format="json")
                self.assertEqual(response.status_code, 200, response.data)
                self.assertTrue(comment.recipient_states.get(user=self.b).is_read)
                self.assertFalse(comment.recipient_states.get(user=self.c).is_read)
                self.client.force_authenticate(self.c)
                self.assertGreater(self.client.get(reverse("me-messages-summary")).data["unread_count"], 0)
                item = next(item for item in self.client.get(reverse("me-messages")).data if item["id"] == comment.pk)
                self.assertFalse(item["is_read"])

    def test_each_member_reply_preserves_group_without_mentions(self):
        original = self.group()
        for author in (self.b, self.c, self.a):
            with self.subTest(author=author.username):
                response = self.send(author, reply_to_comment_id=original.pk, conversation_key=original.conversation_key)
                self.assertEqual(response.status_code, 201, response.data)
                reply = Comment.objects.get(pk=response.data["id"])
                expected = {self.a.pk, self.b.pk, self.c.pk} - {author.pk}
                self.assertEqual(set(reply.recipient_states.values_list("user_id", flat=True)), expected)
                self.assertEqual(response.data["conversation_key"], original.conversation_key)
                self.assertEqual(set(reply.notifications.values_list("recipient_id", flat=True)), expected)

    def test_notification_reply_uses_existing_directed_endpoint(self):
        original = self.group()
        self.client.force_authenticate(self.b)
        response = self.client.post(reverse("directed-comments"), {
            "body": "Reply from bell", "movie_id": self.movie.pk,
            "mentioned_username": "Alice", "reply_to_comment_id": original.pk,
        }, format="json")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["conversation_key"], original.conversation_key)
        self.assertEqual({item["id"] for item in response.data["recipients"]}, {self.a.pk, self.c.pk})

    def test_order_case_and_duplicate_mentions_have_same_key(self):
        first = self.group()
        response = self.send(body="@carol @BOB @bob hello")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["conversation_key"], first.conversation_key)
        self.assertEqual(len(response.data["recipients"]), 2)

    def test_structured_duplicates_are_removed(self):
        response = self.send(mentioned_usernames=["bob", "BOB", "Carol"])
        self.assertEqual(response.status_code, 201)
        self.assertEqual(len(response.data["recipients"]), 2)

    def test_invalid_nonfriend_and_self_are_rejected(self):
        for username in ("Outside", "Missing", "Alice"):
            with self.subTest(username=username):
                self.assertEqual(self.send(mentioned_usernames=["Bob", username]).status_code, 400)
        self.assertEqual(Comment.objects.count(), 0)

    def test_bilateral_and_between_recipient_restrictions_reject_group(self):
        for first, second in ((self.a, self.b), (self.b, self.a), (self.b, self.c), (self.c, self.b)):
            with self.subTest(first=first.username, second=second.username):
                block = UserVisibilityBlock.objects.create(owner=first, blocked_user=second)
                self.assertEqual(self.send(mentioned_usernames=["Bob", "Carol"]).status_code, 403)
                block.delete()
        self.assertEqual(Comment.objects.count(), 0)

    def test_legacy_rows_remain_readable_and_replyable(self):
        original = Comment.objects.create(author=self.a, target_user=self.b, movie=self.movie, body="historical", visibility="mentioned")
        response = self.send(self.b, reply_to_comment_id=original.pk)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.data["conversation_key"], original.conversation_key)
        self.client.force_authenticate(self.b)
        self.assertEqual(self.client.post(reverse("me-messages-mark-as-read")).status_code, 200)
        original.refresh_from_db()
        self.assertTrue(original.is_read)

    def test_backfill_copies_legacy_read_state_and_is_idempotent(self):
        for read in (False, True):
            original = Comment.objects.create(author=self.a, target_user=self.b, movie=self.movie, body="historical", visibility="mentioned", is_read=read)
            backfill = import_module("core.migrations.0077_backfill_directed_recipients").backfill
            backfill(apps, SimpleNamespace(connection=SimpleNamespace(alias="default")))
            backfill(apps, SimpleNamespace(connection=SimpleNamespace(alias="default")))
            self.assertEqual(original.recipient_states.count(), 1)
            self.assertEqual(original.recipient_states.get().is_read, read)

    def test_reactions_enforce_membership(self):
        comment = self.group()
        for user in (self.b, self.c, self.d):
            self.client.force_authenticate(user)
            response = self.client.put(reverse("comment-reaction", kwargs={"pk": comment.pk}), {"reaction": "like"}, format="json")
            self.assertEqual(response.status_code, 404 if user == self.d else 200)

    def test_group_and_pair_history_are_separate_and_private(self):
        group = self.group()
        pair = self.send(mentioned_username="Bob")
        self.client.force_authenticate(self.b)
        conversations = self.client.get(self.url).data["results"]
        self.assertEqual(len(conversations), 2)
        group_data = next(item for item in conversations if item["is_group"])
        self.assertEqual({user["id"] for user in group_data["other_participants"]}, {self.a.pk, self.c.pk})
        history = self.client.get(group_data["messages_endpoint"]).data
        self.assertEqual([item["id"] for item in history["results"]], [group.pk])
        old_url = reverse("movie-directed-conversation-messages", kwargs={"pk": self.movie.pk, "username": "Alice"})
        self.assertEqual([item["id"] for item in self.client.get(old_url).data["results"]], [pair.data["id"]])
        self.client.force_authenticate(self.d)
        self.assertEqual(self.client.get(group_data["messages_endpoint"]).data["results"], [])

    def test_outsider_cannot_forge_reply_or_key(self):
        original = self.group()
        self.assertEqual(self.send(self.d, reply_to_comment_id=original.pk).status_code, 404)
        self.assertEqual(self.send(self.d, conversation_key=original.conversation_key).status_code, 400)
        self.assertEqual(self.send(self.b, reply_to_comment_id=original.pk, conversation_key="wrong").status_code, 400)

    def test_numeric_notification_id_updates_only_its_recipient(self):
        original = self.group()
        notification = original.notifications.get(recipient=self.c)
        self.client.force_authenticate(self.c)
        response = self.client.post(reverse("notifications-mark-read-batch"), {"ids": [notification.pk]}, format="json")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(original.recipient_states.get(user=self.c).is_read)
        self.assertFalse(original.recipient_states.get(user=self.b).is_read)

    def test_outsider_cannot_mark_another_groups_message_read(self):
        original = self.group()
        self.client.force_authenticate(self.d)
        self.client.post(reverse("notifications-mark-read-batch"), {"ids": [f"pm-{original.pk}"]}, format="json")
        self.assertEqual(original.recipient_states.filter(is_read=True).count(), 0)

    def test_recipient_unique_constraint(self):
        from django.db import IntegrityError, transaction
        original = self.group()
        with self.assertRaises(IntegrityError), transaction.atomic():
            DirectedCommentRecipient.objects.create(comment=original, user=self.b)

    def test_profile_activity_never_exposes_group_content_to_outsider(self):
        from .social_feed import SocialActivityFeedService
        original = self.group()
        for field in ("movie_display_rating", "viewer_movie_rating", "movie_following_avg_rating", "movie_following_ratings_count"):
            setattr(original, field, None)
        self.assertEqual(SocialActivityFeedService.serialize_private_message_queryset([original], viewer=self.d), [])
        items = SocialActivityFeedService.serialize_private_message_queryset([original], viewer=self.c)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["payload"]["conversation_key"], original.conversation_key)
        self.assertEqual({user["id"] for user in items[0]["payload"]["participants"]}, {self.a.pk, self.b.pk, self.c.pk})
