from __future__ import annotations

from typing import Iterable, Literal

from django.db.models import Avg, Case, Count, F, FloatField, IntegerField, OuterRef, Q, Subquery, Value, When
from django.db.models.functions import Coalesce

from .models import Comment, CommentReaction, Follow, Friendship, Movie, MovieRating, UserVisibilityBlock


SocialFeedScope = Literal["following", "friends"]


class SocialActivityFeedService:
    ACTIVITY_RATING = "rating"
    ACTIVITY_PUBLIC_COMMENT = "public_comment"
    ACTIVITY_PUBLIC_COMMENT_LIKE = "public_comment_like"
    ACTIVITY_PUBLIC_COMMENT_DISLIKE = "public_comment_dislike"

    SCOPE_FOLLOWING: SocialFeedScope = "following"
    SCOPE_FRIENDS: SocialFeedScope = "friends"

    COMMENT_EXCERPT_LENGTH = 120
    VALID_SCOPES = frozenset({SCOPE_FOLLOWING, SCOPE_FRIENDS})
    _ACTIVITY_SORT_PRIORITY = {
        ACTIVITY_RATING: 3,
        ACTIVITY_PUBLIC_COMMENT: 2,
        ACTIVITY_PUBLIC_COMMENT_LIKE: 1,
        ACTIVITY_PUBLIC_COMMENT_DISLIKE: 0,
    }

    @classmethod
    def is_valid_scope(cls, scope: str | None) -> bool:
        return scope in cls.VALID_SCOPES

    @classmethod
    def build_feed(cls, *, user, scope: SocialFeedScope) -> list[dict]:
        """
        Devuelve una lista uniforme de actividades sociales ordenadas por
        created_at DESC.

        Nota: devolvemos dicts listos para DRF Serializer, así la vista futura
        no duplica lógica de composición.
        """
        if not cls.is_valid_scope(scope):
            raise ValueError(f"Unsupported social feed scope: {scope}")

        actor_ids = cls._get_actor_ids_for_scope(user=user, scope=scope)
        actor_ids = [actor_id for actor_id in set(actor_ids) if actor_id != user.id]
        if not actor_ids:
            return []

        activities = [
            *cls._serialize_rating_activities(actor_ids=actor_ids, viewer=user),
            *cls._serialize_public_comment_activities(actor_ids=actor_ids, viewer=user),
            *cls._serialize_public_comment_like_activities(actor_ids=actor_ids, viewer=user),
            *cls._serialize_public_comment_dislike_activities(actor_ids=actor_ids, viewer=user),
        ]

        # Orden global unificado entre modelos distintos con desempate estable
        # por `id` para paginación por páginas (infinite scroll).
        activities.sort(
            key=lambda item: (
                item["created_at"],
                item["_sort_entity_id"],
                item["_sort_activity_priority"],
            ),
            reverse=True,
        )
        return activities

    @classmethod
    def _get_actor_ids_for_scope(cls, *, user, scope: SocialFeedScope) -> list[int]:
        if scope == cls.SCOPE_FOLLOWING:
            actor_ids = list(
                Follow.objects.filter(follower_id=user.id)
                .values_list("following_id", flat=True)
            )
            blocked_actor_ids = set(
                UserVisibilityBlock.objects.filter(
                    blocked_user_id=user.id,
                    owner_id__in=actor_ids,
                ).values_list("owner_id", flat=True)
            )
            return [actor_id for actor_id in actor_ids if actor_id not in blocked_actor_ids]

        if scope == cls.SCOPE_FRIENDS:
            friend_pairs = Friendship.objects.filter(
                status=Friendship.STATUS_ACCEPTED,
            ).filter(
                Q(user1_id=user.id) | Q(user2_id=user.id)
            )

            actor_ids = list(
                friend_pairs.annotate(
                    friend_id=Case(
                        When(user1_id=user.id, then=F("user2_id")),
                        default=F("user1_id"),
                        output_field=IntegerField(),
                    )
                ).values_list("friend_id", flat=True)
            )
            blocked_actor_ids = set(
                UserVisibilityBlock.objects.filter(
                    blocked_user_id=user.id,
                    owner_id__in=actor_ids,
                ).values_list("owner_id", flat=True)
            )
            return [actor_id for actor_id in actor_ids if actor_id not in blocked_actor_ids]

        # build_feed() valida scope antes de llegar aquí.
        raise ValueError(f"Unsupported social feed scope: {scope}")

    @classmethod
    def _serialize_rating_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        movie_display_rating_subquery = cls._movie_display_rating_subquery(movie_id_ref="movie_id")
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="movie_id",
        )
        queryset = (
            MovieRating.objects.filter(user_id__in=actor_ids)
            .select_related("user", "user__profile", "movie")
            .annotate(
                movie_display_rating=Subquery(movie_display_rating_subquery, output_field=FloatField()),
                viewer_movie_rating=Subquery(viewer_rating_subquery, output_field=IntegerField()),
                movie_following_avg_rating=Subquery(
                    cls._viewer_following_avg_rating_subquery(viewer=viewer, movie_id_ref="movie_id"),
                    output_field=FloatField(),
                ),
                movie_following_ratings_count=Coalesce(
                    Subquery(
                        cls._viewer_following_ratings_count_subquery(viewer=viewer, movie_id_ref="movie_id"),
                        output_field=IntegerField(),
                    ),
                    Value(0),
                ),
            )
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"rating:{rating.id}",
                "activity_type": cls.ACTIVITY_RATING,
                "created_at": rating.created_at,
                "_sort_entity_id": rating.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_RATING],
                "actor": cls._serialize_actor(rating.user),
                "movie": cls._serialize_movie(
                    rating.movie,
                    display_rating=rating.movie_display_rating,
                    my_rating=rating.viewer_movie_rating,
                    following_avg_rating=rating.movie_following_avg_rating,
                    following_ratings_count=rating.movie_following_ratings_count,
                ),
                "payload": {
                    "score": rating.score,
                },
            }
            for rating in queryset
        ]

    @classmethod
    def _serialize_public_comment_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        movie_display_rating_subquery = cls._movie_display_rating_subquery(movie_id_ref="movie_id")
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="movie_id",
        )
        queryset = (
            Comment.objects.filter(
                author_id__in=actor_ids,
                visibility=Comment.VISIBILITY_PUBLIC,
            )
            .select_related("author", "author__profile", "movie")
            .annotate(
                movie_display_rating=Subquery(movie_display_rating_subquery, output_field=FloatField()),
                viewer_movie_rating=Subquery(viewer_rating_subquery, output_field=IntegerField()),
                movie_following_avg_rating=Subquery(
                    cls._viewer_following_avg_rating_subquery(viewer=viewer, movie_id_ref="movie_id"),
                    output_field=FloatField(),
                ),
                movie_following_ratings_count=Coalesce(
                    Subquery(
                        cls._viewer_following_ratings_count_subquery(viewer=viewer, movie_id_ref="movie_id"),
                        output_field=IntegerField(),
                    ),
                    Value(0),
                ),
            )
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"public_comment:{comment.id}",
                "activity_type": cls.ACTIVITY_PUBLIC_COMMENT,
                "created_at": comment.created_at,
                "_sort_entity_id": comment.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PUBLIC_COMMENT],
                "actor": cls._serialize_actor(comment.author),
                "movie": cls._serialize_movie(
                    comment.movie,
                    display_rating=comment.movie_display_rating,
                    my_rating=comment.viewer_movie_rating,
                    following_avg_rating=comment.movie_following_avg_rating,
                    following_ratings_count=comment.movie_following_ratings_count,
                ),
                "payload": {
                    "comment_id": comment.id,
                    "content": comment.body,
                },
            }
            for comment in queryset
        ]

    @classmethod
    def _serialize_public_comment_like_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        return cls._serialize_public_comment_reaction_activities(
            actor_ids=actor_ids,
            viewer=viewer,
            reaction_type=CommentReaction.REACT_LIKE,
            activity_type=cls.ACTIVITY_PUBLIC_COMMENT_LIKE,
        )

    @classmethod
    def _serialize_public_comment_dislike_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        return cls._serialize_public_comment_reaction_activities(
            actor_ids=actor_ids,
            viewer=viewer,
            reaction_type=CommentReaction.REACT_DISLIKE,
            activity_type=cls.ACTIVITY_PUBLIC_COMMENT_DISLIKE,
        )

    @classmethod
    def _serialize_public_comment_reaction_activities(
        cls,
        *,
        actor_ids: list[int],
        viewer,
        reaction_type: str,
        activity_type: str,
    ) -> Iterable[dict]:
        movie_display_rating_subquery = cls._movie_display_rating_subquery(movie_id_ref="comment__movie_id")
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="comment__movie_id",
        )
        queryset = (
            CommentReaction.objects.filter(
                user_id__in=actor_ids,
                reaction_type=reaction_type,
                comment__visibility=Comment.VISIBILITY_PUBLIC,
            )
            .exclude(
                comment__author__visibility_blocks__blocked_user_id=viewer.id,
            )
            .select_related(
                "user",
                "user__profile",
                "comment",
                "comment__author",
                "comment__author__profile",
                "comment__movie",
            )
            .annotate(
                movie_display_rating=Subquery(movie_display_rating_subquery, output_field=FloatField()),
                viewer_movie_rating=Subquery(viewer_rating_subquery, output_field=IntegerField()),
                movie_following_avg_rating=Subquery(
                    cls._viewer_following_avg_rating_subquery(viewer=viewer, movie_id_ref="comment__movie_id"),
                    output_field=FloatField(),
                ),
                movie_following_ratings_count=Coalesce(
                    Subquery(
                        cls._viewer_following_ratings_count_subquery(viewer=viewer, movie_id_ref="comment__movie_id"),
                        output_field=IntegerField(),
                    ),
                    Value(0),
                ),
            )
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"{activity_type}:{reaction.id}",
                "activity_type": activity_type,
                "created_at": reaction.created_at,
                "_sort_entity_id": reaction.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[activity_type],
                "actor": cls._serialize_actor(reaction.user),
                "movie": cls._serialize_movie(
                    reaction.comment.movie,
                    display_rating=reaction.movie_display_rating,
                    my_rating=reaction.viewer_movie_rating,
                    following_avg_rating=reaction.movie_following_avg_rating,
                    following_ratings_count=reaction.movie_following_ratings_count,
                ),
                "payload": {
                    "comment_id": reaction.comment_id,
                    "comment_excerpt": cls._truncate_excerpt(reaction.comment.body),
                    "comment_author": cls._serialize_compact_user(reaction.comment.author),
                },
            }
            for reaction in queryset
        ]

    @classmethod
    def _serialize_compact_user(cls, user) -> dict:
        return {
            "id": user.id,
            "username": user.username,
        }

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
    def _serialize_movie(
        cls,
        movie,
        *,
        display_rating=None,
        my_rating=None,
        following_avg_rating=None,
        following_ratings_count=0,
    ) -> dict:
        return {
            "id": movie.id,
            "title_english": movie.title_english,
            "title_spanish": movie.title_spanish,
            "release_year": movie.release_year,
            "image": movie.image,
            "type": movie.type,
            "genre": movie.genre,
            "display_rating": display_rating,
            "my_rating": my_rating,
            "following_avg_rating": following_avg_rating,
            "following_ratings_count": following_ratings_count,
        }

    @classmethod
    def _movie_display_rating_subquery(cls, *, movie_id_ref: str):
        return Movie.objects.with_display_rating().filter(
            pk=OuterRef(movie_id_ref),
        ).values("display_rating")[:1]

    @classmethod
    def _viewer_movie_rating_subquery(cls, *, viewer, movie_id_ref: str):
        if not viewer or not viewer.is_authenticated:
            return MovieRating.objects.none().values("score")[:1]

        return MovieRating.objects.filter(
            user_id=viewer.id,
            movie_id=OuterRef(movie_id_ref),
        ).values("score")[:1]

    @classmethod
    def _viewer_following_ratings_queryset(cls, *, viewer, movie_id_ref: str):
        if not viewer or not viewer.is_authenticated:
            return MovieRating.objects.none().values("movie_id")

        followed_user_ids = Follow.objects.filter(
            follower_id=viewer.id,
        ).exclude(
            following_id=viewer.id,
        ).values("following_id")

        return MovieRating.objects.filter(
            movie_id=OuterRef(movie_id_ref),
            user_id__in=followed_user_ids,
        ).values("movie_id")

    @classmethod
    def _viewer_following_avg_rating_subquery(cls, *, viewer, movie_id_ref: str):
        return cls._viewer_following_ratings_queryset(
            viewer=viewer,
            movie_id_ref=movie_id_ref,
        ).annotate(
            avg_score=Avg("score"),
        ).values("avg_score")[:1]

    @classmethod
    def _viewer_following_ratings_count_subquery(cls, *, viewer, movie_id_ref: str):
        return cls._viewer_following_ratings_queryset(
            viewer=viewer,
            movie_id_ref=movie_id_ref,
        ).annotate(
            total=Count("id"),
        ).values("total")[:1]

    @classmethod
    def _truncate_excerpt(cls, value: str) -> str:
        text = (value or "").strip()
        if len(text) <= cls.COMMENT_EXCERPT_LENGTH:
            return text
        return f"{text[: cls.COMMENT_EXCERPT_LENGTH - 1]}…"
