from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from rest_framework.test import APIClient

from .models import Comment, Friendship, Movie, UserNotification


class DirectedDispatchTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.a, cls.b, cls.c, cls.d = [get_user_model().objects.create_user(username=name) for name in ("Sender", "First", "Second", "Third")]
        cls.movie = Movie.objects.create(author=cls.a, title_english="Dispatch movie", type=Movie.MOVIE)
        for target in (cls.b, cls.c, cls.d):
            cls.befriend(cls.a, target)

    @classmethod
    def befriend(cls, first, second):
        return Friendship.objects.create(requester=first, user1=first, user2=second, status=Friendship.STATUS_ACCEPTED)

    def setUp(self):
        self.client = APIClient()
        self.url = reverse("movie-directed-comments", kwargs={"pk": self.movie.pk})

    def send(self, user=None, **payload):
        self.client.force_authenticate(user or self.a)
        response = self.client.post(self.url, {"body": "@First @Second Original message", **payload}, format="json")
        self.assertEqual(response.status_code, 201, response.data)
        return response

    def conversations(self, user):
        self.client.force_authenticate(user)
        return self.client.get(self.url).data["results"]

    def pair_key(self, first, second):
        return f"movie:{self.movie.pk}:participants:{'-'.join(str(pk) for pk in sorted((first.pk, second.pk)))}"

    def test_missing_recipient_friendship_creates_one_comment_and_two_individual_threads(self):
        response = self.send()
        original = Comment.objects.get(pk=response.data["id"])
        self.assertTrue(original.individual_dispatch)
        self.assertIsNone(original.conversation_key)
        self.assertEqual(Comment.objects.count(), 1)
        self.assertEqual(set(original.recipient_states.values_list("user_id", flat=True)), {self.b.pk, self.c.pk})
        self.assertEqual(set(original.notifications.values_list("recipient_id", flat=True)), {self.b.pk, self.c.pk})
        self.assertFalse(original.notifications.filter(recipient=self.a).exists())
        expected_keys = {self.pair_key(self.a, self.b), self.pair_key(self.a, self.c)}
        self.assertEqual({context["conversation_key"] for context in response.data["conversation_contexts"]}, expected_keys)
        for viewer in (self.a, self.b, self.c):
            conversations = self.conversations(viewer)
            self.assertEqual(len(conversations), 2 if viewer == self.a else 1)
            self.assertTrue(all(not item["is_group"] for item in conversations))
            for item in conversations:
                self.assertEqual(len(item["participants"]), 2)
                self.assertEqual(len(item["messages_preview"]), 1)
                self.assertEqual(item["messages_preview"][0]["id"], original.pk)
                if viewer != self.a:
                    self.assertEqual(item["other_participants"][0]["id"], self.a.pk)
                history = self.client.get(item["messages_endpoint"]).data["results"]
                self.assertEqual([message["id"] for message in history], [original.pk])
                self.assertEqual(history[0]["conversation_key"], item["conversation_key"])

    def test_detail_inbox_and_notification_are_scoped_to_the_receiving_pair(self):
        original = self.send().data["id"]
        for viewer, other in ((self.b, self.c), (self.c, self.b)):
            self.client.force_authenticate(viewer)
            detail = self.client.get(reverse("comment-detail", kwargs={"pk": original})).data
            self.assertEqual({user["id"] for user in detail["participants"]}, {self.a.pk, viewer.pk})
            self.assertEqual([user["id"] for user in detail["recipients"]], [viewer.pk])
            self.assertEqual(detail["target_user"], viewer.pk)
            inbox = self.client.get(reverse("me-messages")).data
            message = next(item for item in inbox if item["id"] == original)
            self.assertEqual({user["id"] for user in message["participants"]}, {self.a.pk, viewer.pk})
            self.assertNotIn("conversation_contexts", message)
            notification = self.client.get(reverse("me-notifications")).data["items"][0]
            self.assertEqual(notification["conversation_key"], self.pair_key(self.a, viewer))
        self.client.force_authenticate(self.a)
        for params in ({}, {"paginated": "1"}):
            data = self.client.get(reverse("me-messages"), params).data
            messages = data["results"] if isinstance(data, dict) else data
            self.assertEqual(len(messages), 1)
            self.assertEqual(len(messages[0]["conversation_contexts"]), 2)

    def test_each_recipient_reply_goes_only_to_the_original_author(self):
        original = self.send().data["id"]
        for sender, excluded in ((self.b, self.c), (self.c, self.b)):
            response = self.send(sender, body="reply without mentions", reply_to_comment_id=original)
            reply = Comment.objects.get(pk=response.data["id"])
            self.assertEqual(list(reply.recipient_states.values_list("user_id", flat=True)), [self.a.pk])
            self.assertEqual(list(reply.notifications.values_list("recipient_id", flat=True)), [self.a.pk])
            self.assertEqual(response.data["conversation_key"], self.pair_key(self.a, sender))
            self.client.force_authenticate(excluded)
            self.assertEqual(self.client.get(reverse("comment-detail", kwargs={"pk": reply.pk})).status_code, 404)

    def test_author_reply_stays_in_selected_pair(self):
        original = self.send().data["id"]
        for recipient in (self.b, self.c):
            response = self.send(body="reply", reply_to_comment_id=original, conversation_key=self.pair_key(self.a, recipient))
            self.assertEqual([user["id"] for user in response.data["recipients"]], [recipient.pk])
            self.assertEqual(response.data["conversation_key"], self.pair_key(self.a, recipient))

    def test_ambiguous_author_reply_requires_pair_context(self):
        original = self.send().data["id"]
        self.client.force_authenticate(self.a)
        response = self.client.post(self.url, {"body": "ambiguous", "reply_to_comment_id": original}, format="json")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(Comment.objects.count(), 1)

    def test_notification_replies_from_each_recipient_preserve_pair(self):
        original = self.send().data["id"]
        for sender in (self.b, self.c):
            self.client.force_authenticate(sender)
            response = self.client.post(reverse("directed-comments"), {
                "body": "notification reply", "movie_id": self.movie.pk,
                "reply_to_comment_id": original, "conversation_key": self.pair_key(self.a, sender),
                "mentioned_username": self.a.username,
            }, format="json")
            self.assertEqual(response.status_code, 201, response.data)
            self.assertEqual([user["id"] for user in response.data["recipients"]], [self.a.pk])
            self.assertEqual(list(UserNotification.objects.filter(comment_id=response.data["id"]).values_list("recipient_id", flat=True)), [self.a.pk])

    def test_recipient_cannot_select_another_recipients_pair(self):
        original = self.send().data["id"]
        self.client.force_authenticate(self.b)
        response = self.client.post(self.url, {"body": "forged", "reply_to_comment_id": original, "conversation_key": self.pair_key(self.a, self.c)}, format="json")
        self.assertEqual(response.status_code, 400)

    def test_all_pairs_accepted_create_one_group_and_each_member_can_reply(self):
        self.befriend(self.b, self.c)
        response = self.send()
        original = Comment.objects.get(pk=response.data["id"])
        self.assertFalse(original.individual_dispatch)
        self.assertTrue(response.data["is_group"])
        key = response.data["conversation_key"]
        for viewer in (self.a, self.b, self.c):
            self.assertEqual([item["conversation_key"] for item in self.conversations(viewer)], [key])
            reply = self.send(viewer, body="group reply", reply_to_comment_id=original.pk, conversation_key=key)
            self.assertEqual({user["id"] for user in reply.data["recipients"]}, {self.a.pk, self.b.pk, self.c.pk} - {viewer.pk})
            self.assertEqual(reply.data["conversation_key"], key)

    def test_four_participants_require_every_pair(self):
        for first, second in ((self.b, self.c), (self.b, self.d)):
            self.befriend(first, second)
        first = self.send(mentioned_usernames=[self.b.username, self.c.username, self.d.username])
        self.assertFalse(first.data["is_group"])
        self.assertEqual(len(first.data["conversation_contexts"]), 3)
        self.befriend(self.c, self.d)
        second = self.send(mentioned_usernames=[self.b.username, self.c.username, self.d.username])
        self.assertTrue(second.data["is_group"])
        self.assertEqual(len(second.data["participants"]), 4)
        self.assertEqual(Comment.objects.count(), 2)

    def test_friendship_changes_do_not_reclassify_existing_messages(self):
        dispatch = self.send().data["id"]
        friendship = self.befriend(self.b, self.c)
        self.assertEqual(len(self.conversations(self.a)), 2)
        group = self.send().data["id"]
        friendship.delete()
        conversations = self.conversations(self.a)
        self.assertEqual(sum(item["is_group"] for item in conversations), 1)
        self.assertEqual(len(conversations), 3)
        self.client.force_authenticate(self.a)
        response = self.client.post(self.url, {"body": "invalid group reply", "reply_to_comment_id": group}, format="json")
        self.assertEqual(response.status_code, 400)
        self.assertTrue(Comment.objects.get(pk=dispatch).individual_dispatch)
        self.assertFalse(Comment.objects.get(pk=group).individual_dispatch)

    def test_friendship_and_restriction_checks_are_batched(self):
        observed = []
        for recipients in ([self.b.username, self.c.username], [self.b.username, self.c.username, self.d.username]):
            self.client.force_authenticate(self.a)
            with CaptureQueriesContext(connection) as queries:
                response = self.client.post(self.url, {"body": "batched", "mentioned_usernames": recipients}, format="json")
            self.assertEqual(response.status_code, 201)
            friendship_queries = [query for query in queries if 'FROM "core_friendship"' in query["sql"]]
            self.assertEqual(len(friendship_queries), 1)
            observed.append(len(friendship_queries))
        self.assertEqual(observed, [1, 1])

    def test_pending_friendship_does_not_qualify_as_complete_friendship(self):
        friendship = self.befriend(self.b, self.c)
        Friendship.objects.filter(pk=friendship.pk).update(status=Friendship.STATUS_PENDING)
        response = self.send()
        self.assertFalse(response.data["is_group"])
        self.assertEqual(len(response.data["conversation_contexts"]), 2)

    def test_profile_activity_hides_other_dispatch_recipients(self):
        from .social_feed import SocialActivityFeedService
        original = Comment.objects.get(pk=self.send().data["id"])
        for field in ("movie_display_rating", "viewer_movie_rating", "movie_following_avg_rating", "movie_following_ratings_count"):
            setattr(original, field, None)
        for viewer in (self.b, self.c):
            items = SocialActivityFeedService.serialize_private_message_queryset([original], viewer=viewer)
            self.assertEqual(len(items), 1)
            self.assertEqual({user["id"] for user in items[0]["payload"]["participants"]}, {self.a.pk, viewer.pk})
            self.assertEqual(items[0]["payload"]["conversation_key"], self.pair_key(self.a, viewer))
        items = SocialActivityFeedService.serialize_private_message_queryset([original], viewer=self.a)
        self.assertEqual(len(items), 2)
        self.assertNotEqual(items[0]["id"], items[1]["id"])
        self.assertEqual(SocialActivityFeedService.serialize_private_message_queryset([original], viewer=self.d), [])

    def test_dispatch_reaction_activity_cannot_cross_individual_pairs(self):
        from .models import CommentReaction
        from .social_feed import SocialActivityFeedService
        original = Comment.objects.get(pk=self.send().data["id"])
        reaction = CommentReaction.objects.create(comment=original, user=self.b, reaction_type="like")
        self.assertEqual(SocialActivityFeedService.serialize_private_reaction_queryset([reaction], viewer=self.c), [])
        for viewer in (self.a, self.b):
            self.assertEqual(len(SocialActivityFeedService.serialize_private_reaction_queryset([reaction], viewer=viewer)), 1)

    def test_listing_dispatches_does_not_add_queries_per_comment(self):
        self.send()
        self.client.force_authenticate(self.a)
        with CaptureQueriesContext(connection) as single:
            response = self.client.get(self.url)
        for _ in range(4):
            self.send()
        with CaptureQueriesContext(connection) as many:
            response = self.client.get(self.url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(single), len(many))

    def test_reaction_notification_identifies_the_authors_correct_pair(self):
        original = self.send().data["id"]
        self.client.force_authenticate(self.c)
        response = self.client.put(reverse("comment-reaction", kwargs={"pk": original}), {"reaction": "like"}, format="json")
        self.assertEqual(response.status_code, 200)
        self.client.force_authenticate(self.a)
        notifications = self.client.get(reverse("me-notifications")).data["items"]
        reaction = next(item for item in notifications if item["type"] == "private_comment_reaction")
        self.assertEqual(reaction["conversation_key"], self.pair_key(self.a, self.c))
