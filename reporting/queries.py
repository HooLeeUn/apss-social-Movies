from django.contrib.auth import get_user_model
from django.db.models import Q
from core.models import MovieRating, Comment, VideoComment, MovieRecommendationHistory, CommentReaction, VideoCommentReaction, Follow
from .demographics import segment

SOURCES = {
    "ratings": (MovieRating, "user", "updated_at", "movie"),
    "comments": (Comment, "author", "created_at", "movie"),
    "videos": (VideoComment, "user", "created_at", "movie"),
    "added": (MovieRecommendationHistory, "user", "started_at", "movie"),
    "removed": (MovieRecommendationHistory, "user", "ended_at", "movie"),
    "comment_likes": (CommentReaction, "user", "updated_at", "comment__movie"),
    "comment_dislikes": (CommentReaction, "user", "updated_at", "comment__movie"),
    "video_likes": (VideoCommentReaction, "user", "updated_at", "video_comment__movie"),
    "video_dislikes": (VideoCommentReaction, "user", "updated_at", "video_comment__movie"),
    "follows": (Follow, "follower", "created_at", None),
}


def temporal(event, period):
    if period.start is None:
        return Q()
    return Q(**{event + "__gte": period.start, event + "__lt": period.end})


def source(metric, filters, period):
    model, actor, event, movie = SOURCES[metric]
    qs = model.objects.order_by().filter(temporal(event, period))
    if metric == "comments":
        qs = qs.filter(visibility="public")
    if metric.startswith("comment_"):
        qs = qs.filter(comment__visibility="public", reaction_type="like" if metric.endswith("likes") and not metric.endswith("dislikes") else "dislike")
    if metric.startswith("video_"):
        qs = qs.filter(reaction_type="dislike" if metric.endswith("dislikes") else "like")
    if metric == "removed":
        qs = qs.filter(ended_at__isnull=False)
    if movie and filters.get("content_type"):
        qs = qs.filter(**{movie + "__type": filters["content_type"]})
    return segment(qs, actor, event, filters), actor


def users(filters, period):
    qs = get_user_model().objects.order_by().filter(profile__isnull=False).filter(temporal("date_joined", period))
    return segment(qs, "", "date_joined", filters, accumulated=period.start is None)


def recommendations(filters, period):
    qs = MovieRecommendationHistory.objects.order_by().filter(started_at__lt=period.end).filter(Q(ended_at__isnull=True) | Q(ended_at__gte=period.start))
    if filters.get("content_type"):
        qs = qs.filter(movie__type=filters["content_type"])
    # Age on the first instant of overlap, rather than a possibly much older addition.
    from django.db.models.functions import Greatest
    from django.db.models import Value, DateTimeField
    qs = qs.annotate(reference_at=Greatest("started_at", Value(period.start, output_field=DateTimeField())))
    return segment(qs, "user", "reference_at", filters)
