from datetime import timedelta
from urllib.parse import parse_qs, urlparse

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from core.models import Comment, CommentReaction, Movie


class MeMessagesCursorPaginationTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.owner = users.objects.create_user(username="page_owner", password="test1234")
        self.other = users.objects.create_user(username="page_other", password="test1234")
        self.movie = Movie.objects.create(author=self.owner, title_english="Cursor movie", type=Movie.MOVIE)
        self.client = APIClient()
        self.client.force_authenticate(self.owner)
        self.url = reverse("me-messages")

    def _message(self, index, created_at=None):
        item = Comment.objects.create(
            author=self.owner,
            target_user=self.other,
            movie=self.movie,
            body=f"private {index}",
            visibility=Comment.VISIBILITY_MENTIONED,
        )
        if created_at is not None:
            Comment.objects.filter(pk=item.pk).update(created_at=created_at)
            item.refresh_from_db()
        return item

    def _reaction(self, message, created_at=None):
        item = CommentReaction.objects.create(
            comment=message, user=self.other, reaction_type=CommentReaction.REACT_LIKE,
        )
        if created_at is not None:
            CommentReaction.objects.filter(pk=item.pk).update(created_at=created_at)
            item.refresh_from_db()
        return item

    def _page(self, url=None):
        response = self.client.get(url or self.url, {"paginated": "1"} if url is None else None)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return response

    def test_legacy_contract_remains_a_flat_list(self):
        self._message(1)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertIsInstance(response.data, list)
        self.assertEqual(response.data[0]["type"], "private_message")

    def test_empty_and_single_item_pages(self):
        empty = self._page().data
        self.assertEqual(empty, {"next": None, "previous": None, "results": []})

        message = self._message(1)
        single = self._page().data
        self.assertEqual([item["id"] for item in single["results"]], [message.id])
        self.assertIsNone(single["next"])

    def test_page_boundaries_for_less_exactly_and_more_than_page_size(self):
        for expected_total in (9, 10, 11):
            Comment.objects.all().delete()
            for index in range(expected_total):
                self._message(index)
            first = self._page().data
            self.assertEqual(len(first["results"]), min(expected_total, 10))
            self.assertEqual(first["next"] is not None, expected_total > 10)
            if first["next"]:
                second = self._page(first["next"]).data
                self.assertEqual(len(second["results"]), 1)
                self.assertIsNone(second["next"])

    def test_interleaved_families_cross_pages_without_gaps_or_duplicates(self):
        base = timezone.now()
        expected = []
        for index in range(7):
            message = self._message(index, base - timedelta(minutes=index * 2))
            reaction = self._reaction(message, base - timedelta(minutes=index * 2 + 1))
            expected.extend([message.id, f"private-reaction-{reaction.id}"])

        first = self._page().data
        second = self._page(first["next"]).data
        actual = [item["id"] for item in first["results"] + second["results"]]
        self.assertEqual(actual, expected)
        self.assertEqual(len(actual), len(set(map(str, actual))))
        self.assertIsNone(second["next"])

    def test_identical_timestamps_order_messages_before_reactions_then_id_desc(self):
        shared = timezone.now()
        first_message = self._message(1, shared)
        second_message = self._message(2, shared)
        first_reaction = self._reaction(first_message, shared)
        second_reaction = self._reaction(second_message, shared)

        ids = [item["id"] for item in self._page().data["results"]]
        self.assertEqual(ids, [
            second_message.id,
            first_message.id,
            f"private-reaction-{second_reaction.id}",
            f"private-reaction-{first_reaction.id}",
        ])

    def test_only_reactions_continue_across_pages(self):
        messages = [self._message(index) for index in range(11)]
        # Make source messages too old so the first page consists only of reactions.
        old = timezone.now() - timedelta(days=1)
        Comment.objects.filter(pk__in=[item.pk for item in messages]).update(created_at=old)
        reactions = [self._reaction(item) for item in messages]
        first = self._page().data
        second = self._page(first["next"]).data
        reaction_ids = [item["id"] for item in first["results"] + second["results"] if item["type"] != "private_message"]
        self.assertEqual(len(reaction_ids), len(reactions))
        self.assertEqual(len(reaction_ids), len(set(reaction_ids)))

    def test_newer_insertion_between_pages_does_not_shift_cursor(self):
        original = [self._message(index) for index in range(15)]
        first = self._page().data
        self._message("inserted")
        second = self._page(first["next"]).data

        combined = [item["id"] for item in first["results"] + second["results"]]
        expected = [item.id for item in reversed(original)]
        self.assertEqual(combined, expected)
        self.assertEqual(len(combined), len(set(combined)))

    def test_invalid_cursor_is_rejected(self):
        response = self.client.get(self.url, {"paginated": "1", "cursor": "tampered"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_paginated_query_count_does_not_grow_per_item(self):
        self._message(0)
        with CaptureQueriesContext(connection) as one_item_queries:
            self._page()
        for index in range(20):
            self._message(index + 1)
        with CaptureQueriesContext(connection) as many_item_queries:
            response = self._page()

        self.assertEqual(len(response.data["results"]), 10)
        self.assertEqual(len(many_item_queries), len(one_item_queries))
        self.assertLessEqual(len(many_item_queries), 4)

    def test_next_keeps_opt_in_parameter(self):
        for index in range(11):
            self._message(index)
        next_url = self._page().data["next"]
        query = parse_qs(urlparse(next_url).query)
        self.assertEqual(query["paginated"], ["1"])
        self.assertIn("cursor", query)
