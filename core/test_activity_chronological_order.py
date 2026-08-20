from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIClient

from core.models import (
    Comment,
    CommentReaction,
    Movie,
    MovieRating,
    VideoComment,
    VideoCommentReaction,
)


class ProfileActivityChronologicalOrderTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.user = self.make_user("chronology_user")
        self.video_owner = self.make_user("chronology_video_owner")
        self.comment_owner = self.make_user("chronology_comment_owner")
        self.movie = Movie.objects.create(
            author=self.user,
            title_english="Chronology Movie",
            type=Movie.MOVIE,
            genre="Drama",
        )
        self.url = reverse("profile-feed-activity")
        self.client.force_authenticate(self.user)

    def make_user(self, username):
        return get_user_model().objects.create_user(
            username=username,
            email=f"{username}@example.com",
            password="test1234",
        )

    def make_video(self, owner):
        return VideoComment.objects.create(
            user=owner,
            movie=self.movie,
            video=SimpleUploadedFile("chronology.mp4", b"video", content_type="video/mp4"),
            duration_seconds=4,
            mime_type="video/mp4",
            file_size=5,
        )

    def activity(self, **params):
        response = self.client.get(self.url, {"scope": "me", **params})
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        return response

    def test_given_reactions_use_current_event_time_before_global_pagination(self):
        now = timezone.now()
        video = self.make_video(self.video_owner)
        first_comment = Comment.objects.create(
            author=self.comment_owner, movie=self.movie, body="Recently disliked old comment"
        )
        second_comment = Comment.objects.create(
            author=self.comment_owner, movie=self.movie, body="Comment liked 23 hours ago"
        )
        old_object_time = now - timezone.timedelta(days=8)
        Comment.objects.filter(pk__in=[first_comment.pk, second_comment.pk]).update(
            created_at=old_object_time
        )

        video_like = VideoCommentReaction.objects.create(
            user=self.user, video_comment=video, reaction_type="like"
        )
        recent_dislike = CommentReaction.objects.create(
            user=self.user, comment=first_comment, reaction_type="like"
        )
        comment_like = CommentReaction.objects.create(
            user=self.user, comment=second_comment, reaction_type="like"
        )
        VideoCommentReaction.objects.filter(pk=video_like.pk).update(
            created_at=now - timezone.timedelta(days=2),
            updated_at=now - timezone.timedelta(days=2),
        )
        CommentReaction.objects.filter(pk=recent_dislike.pk).update(
            created_at=now - timezone.timedelta(days=3),
            updated_at=now - timezone.timedelta(days=3),
        )
        CommentReaction.objects.filter(pk=comment_like.pk).update(
            created_at=now - timezone.timedelta(hours=23),
            updated_at=now - timezone.timedelta(hours=23),
        )

        before_switch = self.activity(page_size=50).data["results"]
        self.assertEqual(
            [
                item["reaction_id"]
                for item in before_switch
                if item["reaction_id"] in {video_like.id, recent_dislike.id, comment_like.id}
            ],
            [comment_like.id, video_like.id, recent_dislike.id],
        )

        recent_dislike.reaction_type = "dislike"
        recent_dislike.save(update_fields=["reaction_type", "updated_at"])

        expected_ids = [recent_dislike.id, comment_like.id, video_like.id]
        all_results = self.activity(page_size=50).data["results"]
        reaction_results = [item for item in all_results if item["reaction_id"] in expected_ids]
        self.assertEqual([item["reaction_id"] for item in reaction_results], expected_ids)
        self.assertEqual(reaction_results[0]["reaction_type"], "dislike")
        self.assertGreater(reaction_results[0]["activity_at"], reaction_results[1]["activity_at"])
        self.assertEqual(len({item["id"] for item in all_results}), len(all_results))

        first_page = self.activity(page_size=1)
        second_page = self.activity(page_size=1, page=2)
        third_page = self.activity(page_size=1, page=3)
        self.assertEqual(first_page.data["results"][0]["reaction_id"], recent_dislike.id)
        self.assertEqual(second_page.data["results"][0]["reaction_id"], comment_like.id)
        self.assertEqual(third_page.data["results"][0]["reaction_id"], video_like.id)

    def test_received_video_summary_uses_latest_reaction_before_older_event(self):
        august_11 = timezone.datetime(2026, 8, 11, tzinfo=timezone.get_current_timezone())
        event_at_2020 = timezone.datetime(2026, 8, 19, 20, 20, tzinfo=timezone.get_current_timezone())
        reaction_at_2030 = timezone.datetime(2026, 8, 19, 20, 30, tzinfo=timezone.get_current_timezone())
        video = self.make_video(self.user)
        VideoComment.objects.filter(pk=video.pk).update(created_at=august_11)
        reaction = VideoCommentReaction.objects.create(
            user=self.video_owner, video_comment=video, reaction_type="like"
        )
        VideoCommentReaction.objects.filter(pk=reaction.pk).update(
            created_at=reaction_at_2030,
            updated_at=reaction_at_2030,
        )
        rating = MovieRating.objects.create(user=self.user, movie=self.movie, score=8)
        MovieRating.objects.filter(pk=rating.pk).update(
            created_at=event_at_2020,
            updated_at=event_at_2020,
        )

        results = self.activity(page_size=50).data["results"]
        summary = next(
            item for item in results
            if item["activity_type"] == "video_reactions_received_summary"
        )
        self.assertEqual(results[0]["id"], summary["id"])
        self.assertEqual(summary["latest_reaction_at"], reaction_at_2030.isoformat().replace("+00:00", "Z"))
        self.assertEqual(summary["object_created_at"], august_11.isoformat().replace("+00:00", "Z"))
        self.assertEqual(results[1]["id"], f"rating:{rating.id}")
