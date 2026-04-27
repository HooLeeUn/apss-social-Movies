from __future__ import annotations

from typing import Iterable, Literal, cast

from django.db.models import Avg, Case, Count, F, FloatField, IntegerField, OuterRef, Q, Subquery, Value, When
from django.db.models.functions import Coalesce

from .models import Comment, CommentReaction, Follow, Friendship, Movie, MovieRating, UserVisibilityBlock


SocialFeedScope = Literal["following", "friends", "me"]


class SocialActivityFeedService:
    ACTIVITY_RATING = "rating"
    ACTIVITY_PUBLIC_COMMENT = "public_comment"
    ACTIVITY_PRIVATE_MESSAGE = "private_message"
    ACTIVITY_PUBLIC_COMMENT_REACTION = "public_comment_reaction"
    ACTIVITY_PRIVATE_COMMENT_REACTION = "private_comment_reaction"

    SCOPE_ME: SocialFeedScope = "me"
    SCOPE_FOLLOWING: SocialFeedScope = "following"
    SCOPE_FRIENDS: SocialFeedScope = "friends"

    DEFAULT_SCOPE: SocialFeedScope = SCOPE_ME
    COMMENT_EXCERPT_LENGTH = 120
    VALID_SCOPES = frozenset({SCOPE_ME, SCOPE_FOLLOWING, SCOPE_FRIENDS})
    _ACTIVITY_SORT_PRIORITY = {
        ACTIVITY_RATING: 3,
        ACTIVITY_PRIVATE_MESSAGE: 2,
        ACTIVITY_PUBLIC_COMMENT: 2,
        ACTIVITY_PUBLIC_COMMENT_REACTION: 1,
        ACTIVITY_PRIVATE_COMMENT_REACTION: 1,
    }

    @classmethod
    def is_valid_scope(cls, scope: str | None) -> bool:
        return scope in cls.VALID_SCOPES

    @classmethod
    def normalize_scope(cls, scope: str | None) -> SocialFeedScope:
        if scope in cls.VALID_SCOPES:
            return cast(SocialFeedScope, scope)
        return cls.DEFAULT_SCOPE

    @classmethod
    def build_feed(cls, *, user, scope: SocialFeedScope) -> list[dict]:
        """
        Devuelve una lista uniforme de actividades sociales ordenadas por
        activity_at DESC.

        Nota: devolvemos dicts listos para DRF Serializer, así la vista futura
        no duplica lógica de composición.
        """
        if not cls.is_valid_scope(scope):
            raise ValueError(f"Unsupported social feed scope: {scope}")

        actor_ids = cls._get_actor_ids_for_scope(user=user, scope=scope)
        actor_ids = list(set(actor_ids))
        if not actor_ids:
            return []

        activities = [
            *cls._serialize_rating_activities(actor_ids=actor_ids, viewer=user),
            *cls._serialize_public_comment_activities(actor_ids=actor_ids, viewer=user),
            *cls._serialize_public_comment_reaction_activities(actor_ids=actor_ids, viewer=user),
        ]
        if scope == cls.SCOPE_ME:
            activities.extend(
                [
                    *cls._serialize_private_message_activities(actor_ids=actor_ids, viewer=user),
                    *cls._serialize_private_comment_reaction_activities(actor_ids=actor_ids, viewer=user),
                ]
            )

        # Orden global unificado entre modelos distintos con desempate estable
        # por `id` para paginación por páginas (infinite scroll).
        activities.sort(
            key=lambda item: (
                item["activity_at"],
                item["_sort_activity_priority"],
                item["_sort_entity_id"],
            ),
            reverse=True,
        )
        return activities

    @classmethod
    def build_feed_for_actor(cls, *, viewer, actor) -> list[dict]:
        if actor is None:
            return []

        actor_ids = [actor.id]
        activities = [
            *cls._serialize_rating_activities(actor_ids=actor_ids, viewer=viewer),
            *cls._serialize_public_comment_activities(actor_ids=actor_ids, viewer=viewer),
            *cls._serialize_public_comment_reaction_activities(actor_ids=actor_ids, viewer=viewer),
        ]
        if viewer and actor and viewer.id == actor.id:
            activities.extend(
                [
                    *cls._serialize_private_message_activities(actor_ids=actor_ids, viewer=viewer),
                    *cls._serialize_private_comment_reaction_activities(actor_ids=actor_ids, viewer=viewer),
                ]
            )
        activities.sort(
            key=lambda item: (
                item["activity_at"],
                item["_sort_activity_priority"],
                item["_sort_entity_id"],
            ),
            reverse=True,
        )
        return activities

    @classmethod
    def _get_actor_ids_for_scope(cls, *, user, scope: SocialFeedScope) -> list[int]:
        if scope == cls.SCOPE_ME:
            return [user.id]

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
                "updated_at": rating.updated_at,
                "activity_at": rating.updated_at or rating.created_at,
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
                "updated_at": comment.updated_at,
                "activity_at": comment.updated_at or comment.created_at,
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
    def _serialize_private_message_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        movie_display_rating_subquery = cls._movie_display_rating_subquery(movie_id_ref="movie_id")
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="movie_id",
        )
        queryset = (
            Comment.objects.filter(
                author_id__in=actor_ids,
                visibility=Comment.VISIBILITY_MENTIONED,
                target_user__isnull=False,
            )
            .select_related("author", "author__profile", "movie", "target_user")
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

        valid_comments = [comment for comment in queryset if comment.has_valid_target_mention()]

        return [
            {
                "id": f"{cls.ACTIVITY_PRIVATE_MESSAGE}:{comment.id}",
                "activity_type": cls.ACTIVITY_PRIVATE_MESSAGE,
                "created_at": comment.created_at,
                "updated_at": comment.updated_at,
                "activity_at": comment.updated_at or comment.created_at,
                "_sort_entity_id": comment.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PRIVATE_MESSAGE],
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
                    "sender": cls._serialize_compact_user(comment.author),
                    "recipient": cls._serialize_compact_user(comment.target_user),
                    "target_user": cls._serialize_compact_user(comment.target_user),
                    "direction": "sent" if viewer and comment.author_id == viewer.id else "received",
                    "counterpart": cls._serialize_compact_user(
                        comment.target_user if viewer and comment.author_id == viewer.id else comment.author
                    ),
                },
            }
            for comment in valid_comments
        ]

    @classmethod
    def _serialize_public_comment_reaction_activities(
        cls,
        *,
        actor_ids: list[int],
        viewer,
    ) -> Iterable[dict]:
        movie_display_rating_subquery = cls._movie_display_rating_subquery(movie_id_ref="comment__movie_id")
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="comment__movie_id",
        )
        is_self_scope = bool(viewer and set(actor_ids) == {viewer.id})
        queryset = CommentReaction.objects.filter(
            comment__visibility=Comment.VISIBILITY_PUBLIC,
        )
        if is_self_scope:
            queryset = queryset.filter(user_id=viewer.id)
        else:
            queryset = queryset.filter(Q(comment__author_id__in=actor_ids) | Q(user_id__in=actor_ids))

        queryset = (
            queryset.exclude(user_id=F("comment__author_id"))
            .exclude(
                comment__author__visibility_blocks__blocked_user_id=viewer.id,
            )
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
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
            .order_by("-updated_at", "-created_at", "-id")
        )

        return [
            {
                "id": f"{cls.ACTIVITY_PUBLIC_COMMENT_REACTION}:{reaction.id}",
                "activity_type": cls.ACTIVITY_PUBLIC_COMMENT_REACTION,
                "created_at": reaction.created_at,
                "updated_at": reaction.updated_at,
                "activity_at": reaction.updated_at or reaction.created_at,
                "_sort_entity_id": reaction.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PUBLIC_COMMENT_REACTION],
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
                    "reaction_id": reaction.id,
                    "comment_excerpt": cls._truncate_excerpt(reaction.comment.body),
                    "comment_author": cls._serialize_compact_user(reaction.comment.author),
                    "reaction_value": reaction.reaction_type,
                    "reaction_type": reaction.reaction_type,
                    "is_given_reaction": viewer and reaction.user_id == viewer.id,
                    "is_received_reaction": viewer and reaction.comment.author_id == viewer.id,
                },
            }
            for reaction in queryset
        ]

    @classmethod
    def _serialize_private_comment_reaction_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        queryset = (
            CommentReaction.objects.filter(
                comment__author_id__in=actor_ids,
                comment__visibility=Comment.VISIBILITY_MENTIONED,
            )
            .exclude(user_id=F("comment__author_id"))
            .select_related(
                "user",
                "user__profile",
                "comment",
                "comment__author",
                "comment__target_user",
                "comment__movie",
            )
            .order_by("-created_at", "-id")
        )
        valid_reactions = [reaction for reaction in queryset if reaction.comment.has_valid_target_mention()]
        return [
            {
                "id": f"{cls.ACTIVITY_PRIVATE_COMMENT_REACTION}:{reaction.id}",
                "activity_type": cls.ACTIVITY_PRIVATE_COMMENT_REACTION,
                "created_at": reaction.created_at,
                "updated_at": reaction.updated_at,
                "activity_at": reaction.updated_at or reaction.created_at,
                "_sort_entity_id": reaction.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PRIVATE_COMMENT_REACTION],
                "actor": cls._serialize_actor(reaction.user),
                "movie": cls._serialize_movie(reaction.comment.movie),
                "payload": {
                    "comment_id": reaction.comment_id,
                    "reaction_id": reaction.id,
                    "comment_excerpt": cls._truncate_excerpt(reaction.comment.body),
                    "comment_author": cls._serialize_compact_user(reaction.comment.author),
                    "reaction_type": reaction.reaction_type,
                    "reaction_value": reaction.reaction_type,
                    "is_given_reaction": viewer and reaction.user_id == viewer.id,
                    "is_received_reaction": viewer and reaction.comment.author_id == viewer.id,
                },
            }
            for reaction in valid_reactions
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
