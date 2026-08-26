from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from core.models import (
    Comment,
    CommentReaction,
    Movie,
    UserVisibilityBlock,
    VideoComment,
    VideoCommentReaction,
)
from core.social_feed import SocialActivityFeedService


class ProfileActivityPhaseCLogicalGroupTests(TestCase):
    def setUp(self):
        users = get_user_model()
        self.owner = users.objects.create_user(username="phase_c_owner")
        self.reactors = [
            users.objects.create_user(username=f"phase_c_reactor_{index}")
            for index in range(12)
        ]
        self.movie = Movie.objects.create(
            author=self.owner, title_english="Phase C", type=Movie.MOVIE
        )

    def assert_three_modes_equal(self):
        legacy = SocialActivityFeedService.build_feed(user=self.owner, scope="me")
        full = SocialActivityFeedService.build_feed_candidate(
            user=self.owner, scope="me",
            mode=SocialActivityFeedService.CANDIDATE_MODE_FULL,
        )
        grouped = SocialActivityFeedService.build_feed_candidate(
            user=self.owner, scope="me",
            mode=SocialActivityFeedService.CANDIDATE_MODE_LOGICAL_GROUPS,
        )
        self.assertEqual(grouped, full)
        self.assertEqual(grouped, legacy)
        return grouped

    def make_video(self):
        return VideoComment.objects.create(
            user=self.owner,
            movie=self.movie,
            video=SimpleUploadedFile("phase-c.mp4", b"video", content_type="video/mp4"),
            duration_seconds=1,
            mime_type="video/mp4",
            file_size=5,
        )

    def test_public_many_rows_are_one_candidate_and_one_exact_summary(self):
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="many")
        for index, reactor in enumerate(self.reactors):
            CommentReaction.objects.create(
                comment=comment,
                user=reactor,
                reaction_type="like" if index < 8 else "dislike",
            )

        candidates = SocialActivityFeedService.comment_received_logical_candidates(
            viewer=self.owner
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["object_id"], comment.id)
        self.assertEqual(candidates[0]["source_rows"], 12)
        summary = next(item for item in self.assert_three_modes_equal() if item["id"] == f"comment_reactions_received_summary:{comment.id}")
        self.assertEqual((summary["payload"]["likes_count"], summary["payload"]["dislikes_count"]), (8, 4))

    def test_private_validation_happens_before_grouping(self):
        valid = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="private body",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.reactors[0],
        )
        invalid = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.reactors[1],
        )
        CommentReaction.objects.create(comment=valid, user=self.reactors[0], reaction_type="like")
        CommentReaction.objects.create(comment=invalid, user=self.reactors[1], reaction_type="like")

        candidates = SocialActivityFeedService.comment_received_logical_candidates(viewer=self.owner)
        self.assertEqual([candidate["object_id"] for candidate in candidates], [valid.id])
        self.assert_three_modes_equal()

    def test_given_public_reaction_remains_individual(self):
        foreign = Comment.objects.create(
            author=self.reactors[0], movie=self.movie, body="foreign"
        )
        given = CommentReaction.objects.create(
            comment=foreign, user=self.owner, reaction_type="like"
        )
        grouped = self.assert_three_modes_equal()
        self.assertTrue(any(item["id"] == f"public_comment_reaction:{given.id}" for item in grouped))
        self.assertEqual(SocialActivityFeedService.count_comment_received_logical_items(viewer=self.owner), 0)

    def test_switch_and_delete_update_timestamp_latest_actor_and_counts(self):
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="mutable")
        first = CommentReaction.objects.create(comment=comment, user=self.reactors[0], reaction_type="like")
        second = CommentReaction.objects.create(comment=comment, user=self.reactors[1], reaction_type="like")
        switched_at = timezone.now() + timezone.timedelta(minutes=1)
        CommentReaction.objects.filter(pk=first.pk).update(reaction_type="dislike", updated_at=switched_at)

        candidate = SocialActivityFeedService.comment_received_logical_candidates(viewer=self.owner)[0]
        self.assertEqual(candidate["latest_activity_at"], switched_at)
        self.assertEqual(candidate["latest_reaction_id"], first.id)
        summary = next(item for item in self.assert_three_modes_equal() if item["payload"].get("comment_id") == comment.id and "summary" in item["activity_type"])
        self.assertEqual((summary["payload"]["likes_count"], summary["payload"]["dislikes_count"]), (1, 1))

        second.delete()
        summary = next(item for item in self.assert_three_modes_equal() if item["id"].endswith(f":{comment.id}"))
        self.assertEqual((summary["payload"]["likes_count"], summary["payload"]["dislikes_count"]), (0, 1))

    def test_video_group_switch_delete_and_payload_match(self):
        video = self.make_video()
        first = VideoCommentReaction.objects.create(video_comment=video, user=self.reactors[0], reaction_type="like")
        second = VideoCommentReaction.objects.create(video_comment=video, user=self.reactors[1], reaction_type="dislike")
        first.reaction_type = "dislike"
        first.save(update_fields=["reaction_type", "updated_at"])

        groups = SocialActivityFeedService.video_received_logical_candidates(viewer=self.owner)
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0]["object_id"], video.id)
        self.assertEqual(SocialActivityFeedService.count_video_received_logical_items(viewer=self.owner), 1)
        summary = next(item for item in self.assert_three_modes_equal() if item["id"] == f"video_reactions_received_summary:{video.id}")
        self.assertEqual(summary["payload"]["dislikes_count"], 2)
        self.assertTrue(summary["payload"]["video_url"].endswith(video.video.url))

        second.delete()
        summary = next(item for item in self.assert_three_modes_equal() if item["id"] == f"video_reactions_received_summary:{video.id}")
        self.assertEqual(summary["payload"]["dislikes_count"], 1)

    def test_batch_hydration_query_count_is_constant_in_number_of_groups(self):
        comments = [
            Comment.objects.create(author=self.owner, movie=self.movie, body=f"batch {index}")
            for index in range(3)
        ]
        for index, comment in enumerate(comments):
            CommentReaction.objects.create(comment=comment, user=self.reactors[index], reaction_type="like")

        with CaptureQueriesContext(connection) as queries:
            summaries = SocialActivityFeedService.hydrate_comment_reaction_summaries(
                comment_ids=[comment.id for comment in comments], viewer=self.owner
            )
        self.assertEqual(len(summaries), 3)
        self.assertEqual(len(queries), 2)

    def test_logical_count_uses_comment_union_key(self):
        public = Comment.objects.create(author=self.owner, movie=self.movie, body="public")
        private = Comment.objects.create(
            author=self.owner,
            movie=self.movie,
            body="private",
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user=self.reactors[1],
        )
        CommentReaction.objects.create(comment=public, user=self.reactors[0], reaction_type="like")
        CommentReaction.objects.create(comment=private, user=self.reactors[1], reaction_type="like")
        candidates = SocialActivityFeedService.comment_received_logical_candidates(viewer=self.owner)
        self.assertEqual({item["object_id"] for item in candidates}, {public.id, private.id})
        self.assertEqual(SocialActivityFeedService.count_comment_received_logical_items(viewer=self.owner), 2)
        self.assert_three_modes_equal()

    def test_latest_id_breaks_timestamp_ties_after_privacy_filtering(self):
        comment = Comment.objects.create(author=self.owner, movie=self.movie, body="tie")
        first = CommentReaction.objects.create(comment=comment, user=self.reactors[0], reaction_type="like")
        latest = CommentReaction.objects.create(comment=comment, user=self.reactors[1], reaction_type="dislike")
        blocked = CommentReaction.objects.create(comment=comment, user=self.reactors[2], reaction_type="like")
        tied_at = timezone.now() + timezone.timedelta(minutes=2)
        CommentReaction.objects.filter(pk__in=[first.id, latest.id, blocked.id]).update(updated_at=tied_at)
        UserVisibilityBlock.objects.create(owner=self.reactors[2], blocked_user=self.owner)

        candidate = SocialActivityFeedService.comment_received_logical_candidates(viewer=self.owner)[0]
        self.assertEqual(candidate["source_rows"], 2)
        self.assertEqual(candidate["latest_reaction_id"], latest.id)
        summary = next(item for item in self.assert_three_modes_equal() if item["id"] == f"comment_reactions_received_summary:{comment.id}")
        self.assertEqual((summary["payload"]["likes_count"], summary["payload"]["dislikes_count"]), (1, 1))
