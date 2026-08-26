from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APIClient

from core.activity_feed_test_utils import (
    LEGACY_STABLE_FAMILY_RANK,
    assert_activity_responses_equivalent,
    legacy_characterization_sort_key,
    legacy_effective_timestamp,
    normalize_activity_response,
)
from core.models import Comment, CommentReaction, Movie, MovieRating
from core.social_feed import SocialActivityFeedService


class ActivityFeedContractHelpersTests(SimpleTestCase):
    def test_effective_timestamp_and_total_characterization_key(self):
        created = timezone.now() - timedelta(days=1)
        activity = {
            "activity_type": "rating", "created_at": created,
            "activity_at": created + timedelta(hours=1),
            "_sort_activity_priority": 3, "_sort_entity_id": 9,
        }
        self.assertEqual(legacy_effective_timestamp(activity), activity["activity_at"])
        self.assertEqual(legacy_characterization_sort_key(activity), (
            activity["activity_at"], 3, 9, LEGACY_STABLE_FAMILY_RANK["rating"]
        ))
        activity["activity_type"] = "public_comment"
        self.assertEqual(legacy_effective_timestamp(activity), created)

    def test_semantic_normalization_ignores_only_origin(self):
        item = {"id": "rating:1", "activity_type": "rating", "payload": {"score": 8}}
        left = {"count": 1, "next": "https://one.test/api/profile-feed/activity/?page=2", "previous": None, "results": [item]}
        right = {"count": 1, "next": "http://two.test/api/profile-feed/activity/?page=2", "previous": None, "results": [item]}
        self.assertEqual(normalize_activity_response(left), normalize_activity_response(right))
        assert_activity_responses_equivalent(self, left, right)


class LegacyProfileActivityPhaseATests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.owner = users.objects.create_user(username="phase_a_owner", password="test1234")
        self.other = users.objects.create_user(username="phase_a_other", password="test1234")
        self.movie = Movie.objects.create(author=self.owner, title_english="Phase A", type=Movie.MOVIE)
        self.client = APIClient()
        self.client.force_authenticate(self.owner)
        self.url = reverse("profile-feed-activity")

    def activity(self, **params):
        response = self.client.get(self.url, {"scope": "me", **params})
        self.assertEqual(response.status_code, 200)
        return response

    def test_empty_response_freezes_page_number_envelope(self):
        self.assertEqual(self.activity().data, {
            "count": 0, "next": None, "previous": None, "results": []
        })

    def test_rating_update_uses_activity_at_but_comment_uses_created_at(self):
        base = timezone.now() - timedelta(days=2)
        first = MovieRating.objects.create(user=self.owner, movie=self.movie, score=7)
        second_movie = Movie.objects.create(author=self.owner, title_english="Second", type=Movie.MOVIE)
        second = MovieRating.objects.create(user=self.owner, movie=second_movie, score=8)
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="legacy public")
        MovieRating.objects.filter(pk=first.pk).update(created_at=base, updated_at=base + timedelta(hours=4))
        MovieRating.objects.filter(pk=second.pk).update(created_at=base + timedelta(hours=2), updated_at=base + timedelta(hours=2))
        Comment.objects.filter(pk=comment.pk).update(created_at=base + timedelta(hours=3), updated_at=base + timedelta(hours=8))

        results = self.activity(page_size=50).data["results"]
        relevant = [item for item in results if item["id"] in {
            f"rating:{first.id}", f"rating:{second.id}", f"public_comment:{comment.id}"
        }]
        self.assertEqual([item["id"] for item in relevant], [
            f"rating:{first.id}", f"public_comment:{comment.id}", f"rating:{second.id}"
        ])
        for item in relevant:
            self.assertEqual(item["type"], item["activity_type"])
            self.assertEqual(item["timestamp"], item["created_at"])
            self.assertIn("updated_at", item)
            self.assertIn("activity_at", item)
            self.assertEqual(item["actor"]["id"], self.owner.id)
            self.assertIsNotNone(item["movie"])
            self.assertIn("payload", item)

    def test_many_received_rows_become_one_summary_while_given_stays_individual(self):
        owned = Comment.objects.create(author=self.owner, movie=self.movie, body="one logical item")
        for index in range(11):
            reactor = get_user_model().objects.create_user(username=f"phase_a_reactor_{index}")
            CommentReaction.objects.create(comment=owned, user=reactor, reaction_type="like" if index < 7 else "dislike")
        foreign = Comment.objects.create(author=self.other, movie=self.movie, body="foreign")
        given = CommentReaction.objects.create(comment=foreign, user=self.owner, reaction_type="like")

        results = self.activity(page_size=50).data["results"]
        summaries = [item for item in results if item["activity_type"] == "comment_reactions_received_summary"]
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["id"], f"comment_reactions_received_summary:{owned.id}")
        self.assertEqual(summary["likes_count"], 7)
        self.assertEqual(summary["dislikes_count"], 4)
        self.assertEqual(len(summary["users_who_liked"]), 7)
        self.assertEqual(len(summary["users_who_disliked"]), 4)
        self.assertEqual(summary["owner"]["id"], self.owner.id)
        self.assertEqual(summary["movie"]["id"], self.movie.id)
        self.assertEqual(summary["comment_text"], owned.body)
        self.assertEqual(summary["activity_at"], summary["latest_reaction_at"])
        given_items = [item for item in results if item["id"] == f"public_comment_reaction:{given.id}"]
        self.assertEqual(len(given_items), 1)
        self.assertTrue(given_items[0]["is_given_reaction"])

    def test_page_number_boundaries_and_links_are_legacy(self):
        base = timezone.now()
        ratings = []
        for index in range(11):
            movie = Movie.objects.create(author=self.owner, title_english=f"Page {index}", type=Movie.MOVIE)
            rating = MovieRating.objects.create(user=self.owner, movie=movie, score=5)
            MovieRating.objects.filter(pk=rating.pk).update(created_at=base + timedelta(minutes=index), updated_at=base + timedelta(minutes=index))
            ratings.append(rating)

        first = self.activity(page=1, page_size=10)
        second = self.activity(page=2, page_size=10)
        self.assertEqual(first.data["count"], 11)
        self.assertEqual(len(first.data["results"]), 10)
        self.assertIsNotNone(first.data["next"])
        self.assertIsNone(first.data["previous"])
        self.assertEqual(second.data["count"], 11)
        self.assertEqual([item["id"] for item in second.data["results"]], [f"rating:{ratings[0].id}"])
        self.assertIsNone(second.data["next"])
        self.assertIsNotNone(second.data["previous"])
        self.assertEqual(len(self.activity(page_size=1).data["results"]), 1)
        self.assertEqual(len(self.activity(page_size=50).data["results"]), 11)

    def test_exact_cross_family_tie_preserves_append_order(self):
        stamp = timezone.now()
        rating = MovieRating.objects.create(user=self.owner, movie=self.movie, score=9)
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="tie")
        MovieRating.objects.filter(pk=rating.pk).update(created_at=stamp, updated_at=stamp)
        Comment.objects.filter(pk=comment.pk).update(created_at=stamp, updated_at=stamp)
        raw = SocialActivityFeedService.build_feed(user=self.owner, scope="me")
        tied = [item for item in raw if item["id"] in {f"rating:{rating.id}", f"public_comment:{comment.id}"}]
        # Priorities differ today; this also freezes the family rank that would
        # decide a future total tie without changing production sorting.
        self.assertEqual([item["activity_type"] for item in tied], ["rating", "public_comment"])
        self.assertGreater(LEGACY_STABLE_FAMILY_RANK["rating"], LEGACY_STABLE_FAMILY_RANK["public_comment"])
