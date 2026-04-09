from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from django.db.models import Case, F, IntegerField, Q, When

from .models import Comment, CommentReaction, Follow, Friendship, MovieRating


SocialFeedScope = Literal["following", "friends"]


@dataclass(frozen=True)
class SocialActivity:
    """
    Estructura interna neutral para exponer actividades heterogéneas
    (ratings, comentarios públicos y likes en comentarios públicos).
    """

    id: str
    activity_type: str
    created_at: object
    actor: dict
    movie: dict
    payload: dict


class SocialActivityFeedService:
    ACTIVITY_RATING = "rating"
    ACTIVITY_PUBLIC_COMMENT = "public_comment"
    ACTIVITY_PUBLIC_COMMENT_LIKE = "public_comment_like"

    SCOPE_FOLLOWING: SocialFeedScope = "following"
    SCOPE_FRIENDS: SocialFeedScope = "friends"

    COMMENT_EXCERPT_LENGTH = 120

    @classmethod
    def build_feed(cls, *, user, scope: SocialFeedScope) -> list[dict]:
        """
        Devuelve una lista uniforme de actividades sociales ordenadas por
        created_at DESC.

        Nota: devolvemos dicts listos para DRF Serializer, así la vista futura
        no duplica lógica de composición.
        """
        actor_ids = cls._get_actor_ids_for_scope(user=user, scope=scope)
        actor_ids = [actor_id for actor_id in set(actor_ids) if actor_id != user.id]
        if not actor_ids:
            return []

        activities = [
            *cls._serialize_rating_activities(actor_ids=actor_ids),
            *cls._serialize_public_comment_activities(actor_ids=actor_ids),
            *cls._serialize_public_comment_like_activities(actor_ids=actor_ids),
        ]

        # Orden global unificado entre modelos distintos.
        activities.sort(key=lambda item: (item["created_at"], item["id"]), reverse=True)
        return activities

    @classmethod
    def _get_actor_ids_for_scope(cls, *, user, scope: SocialFeedScope) -> list[int]:
        if scope == cls.SCOPE_FOLLOWING:
            return list(
                Follow.objects.filter(follower_id=user.id)
                .values_list("following_id", flat=True)
            )

        if scope == cls.SCOPE_FRIENDS:
            friend_pairs = Friendship.objects.filter(
                status=Friendship.STATUS_ACCEPTED,
            ).filter(
                Q(user1_id=user.id) | Q(user2_id=user.id)
            )

            return list(
                friend_pairs.annotate(
                    friend_id=Case(
                        When(user1_id=user.id, then=F("user2_id")),
                        default=F("user1_id"),
                        output_field=IntegerField(),
                    )
                ).values_list("friend_id", flat=True)
            )

        raise ValueError(f"Unsupported social feed scope: {scope}")

    @classmethod
    def _serialize_rating_activities(cls, *, actor_ids: list[int]) -> Iterable[dict]:
        queryset = (
            MovieRating.objects.filter(user_id__in=actor_ids)
            .select_related("user", "user__profile", "movie")
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"rating:{rating.id}",
                "activity_type": cls.ACTIVITY_RATING,
                "created_at": rating.created_at,
                "actor": cls._serialize_actor(rating.user),
                "movie": cls._serialize_movie(rating.movie),
                "payload": {
                    "score": rating.score,
                },
            }
            for rating in queryset
        ]

    @classmethod
    def _serialize_public_comment_activities(cls, *, actor_ids: list[int]) -> Iterable[dict]:
        queryset = (
            Comment.objects.filter(
                author_id__in=actor_ids,
                visibility=Comment.VISIBILITY_PUBLIC,
            )
            .select_related("author", "author__profile", "movie")
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"public_comment:{comment.id}",
                "activity_type": cls.ACTIVITY_PUBLIC_COMMENT,
                "created_at": comment.created_at,
                "actor": cls._serialize_actor(comment.author),
                "movie": cls._serialize_movie(comment.movie),
                "payload": {
                    "comment_id": comment.id,
                    "content": comment.body,
                },
            }
            for comment in queryset
        ]

    @classmethod
    def _serialize_public_comment_like_activities(cls, *, actor_ids: list[int]) -> Iterable[dict]:
        queryset = (
            CommentReaction.objects.filter(
                user_id__in=actor_ids,
                reaction_type=CommentReaction.REACT_LIKE,
                comment__visibility=Comment.VISIBILITY_PUBLIC,
            )
            .select_related(
                "user",
                "user__profile",
                "comment",
                "comment__author",
                "comment__author__profile",
                "comment__movie",
            )
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"public_comment_like:{reaction.id}",
                "activity_type": cls.ACTIVITY_PUBLIC_COMMENT_LIKE,
                "created_at": reaction.created_at,
                "actor": cls._serialize_actor(reaction.user),
                "movie": cls._serialize_movie(reaction.comment.movie),
                "payload": {
                    "comment_id": reaction.comment_id,
                    "comment_excerpt": cls._truncate_excerpt(reaction.comment.body),
                    "comment_author": cls._serialize_actor(reaction.comment.author),
                },
            }
            for reaction in queryset
        ]

    @classmethod
    def _serialize_actor(cls, user) -> dict:
        avatar_url = None
        if hasattr(user, "profile") and user.profile and user.profile.avatar:
            avatar_url = user.profile.avatar.url

        return {
            "id": user.id,
            "username": user.username,
            "avatar": avatar_url,
        }

    @classmethod
    def _serialize_movie(cls, movie) -> dict:
        return {
            "id": movie.id,
            "title_english": movie.title_english,
            "title_spanish": movie.title_spanish,
            "release_year": movie.release_year,
            "image": movie.image,
        }

    @classmethod
    def _truncate_excerpt(cls, value: str) -> str:
        text = (value or "").strip()
        if len(text) <= cls.COMMENT_EXCERPT_LENGTH:
            return text
        return f"{text[: cls.COMMENT_EXCERPT_LENGTH - 1]}…"
