from __future__ import annotations

from typing import Iterable, Literal, cast

from django.db.models import Avg, Case, Count, F, FloatField, IntegerField, OuterRef, Q, Subquery, Value, When
from django.db.models.functions import Coalesce

from .models import (
    Comment,
    CommentReaction,
    Follow,
    Friendship,
    Movie,
    MovieRating,
    UserVisibilityBlock,
    VideoComment,
    VideoCommentReaction,
)


SocialFeedScope = Literal["following", "friends", "me"]


class SocialActivityFeedService:
    ACTIVITY_RATING = "rating"
    ACTIVITY_PUBLIC_COMMENT = "public_comment"
    ACTIVITY_PRIVATE_MESSAGE = "private_message"
    ACTIVITY_PUBLIC_COMMENT_REACTION = "public_comment_reaction"
    ACTIVITY_PRIVATE_COMMENT_REACTION = "private_comment_reaction"
    ACTIVITY_VIDEO_REACTION_RECEIVED = "video_reaction_received"
    ACTIVITY_VIDEO_REACTION_GIVEN = "video_reaction_given"
    ACTIVITY_VIDEO_REACTION_CREATED = "video_reaction_created"
    ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY = "comment_reactions_received_summary"
    ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY = "video_reactions_received_summary"

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
        ACTIVITY_VIDEO_REACTION_RECEIVED: 1,
        ACTIVITY_VIDEO_REACTION_GIVEN: 1,
        ACTIVITY_VIDEO_REACTION_CREATED: 2,
        ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY: 1,
        ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY: 1,
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
        Devuelve una lista uniforme de actividades sociales ordenadas por el
        timestamp efectivo del evento DESC.

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
                    *cls._serialize_video_reaction_created_activities(actor=user, viewer=user),
                    *cls._serialize_video_reaction_activities(viewer=user),
                ]
            )
            activities = cls._consolidate_received_reactions(activities)

        # Orden global unificado entre modelos distintos con desempate estable
        # por `id` para paginación por páginas (infinite scroll).
        activities.sort(
            key=lambda item: (
                cls._activity_sort_timestamp(item),
                item["_sort_activity_priority"],
                item["_sort_entity_id"],
            ),
            reverse=True,
        )
        return activities

    @classmethod
    def _activity_sort_timestamp(cls, activity: dict):
        """Return the timestamp of the event represented by an activity.

        Reaction rows can survive a like/dislike switch, so their ``created_at``
        is the time of the first reaction while ``activity_at`` is the time of
        the current reaction.  Received-reaction summaries likewise expose the
        latest reaction as ``activity_at``.  Creation activities keep their
        original creation timestamp.
        """
        creation_activity_types = {
            cls.ACTIVITY_PUBLIC_COMMENT,
            cls.ACTIVITY_PRIVATE_MESSAGE,
            cls.ACTIVITY_VIDEO_REACTION_CREATED,
        }
        if activity["activity_type"] in creation_activity_types:
            return activity["created_at"]
        return activity["activity_at"]

    @classmethod
    def _consolidate_received_reactions(cls, activities: list[dict]) -> list[dict]:
        """Replace received reaction events with one current-state item per object."""
        families = {
            cls.ACTIVITY_PUBLIC_COMMENT_REACTION: (
                cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY, "comment_id"
            ),
            cls.ACTIVITY_PRIVATE_COMMENT_REACTION: (
                cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY, "comment_id"
            ),
            cls.ACTIVITY_VIDEO_REACTION_RECEIVED: (
                cls.ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY, "video_comment_id"
            ),
        }
        kept = []
        groups = {}
        for activity in activities:
            payload = activity.get("payload") or {}
            family = families.get(activity["activity_type"])
            if not family or not payload.get("is_received_reaction"):
                kept.append(activity)
                continue
            summary_type, object_key = family
            key = (summary_type, payload[object_key])
            group = groups.setdefault(key, [])
            group.append(activity)

        for (summary_type, object_id), reactions in groups.items():
            latest = max(reactions, key=lambda item: (item["activity_at"], item["_sort_entity_id"]))
            first_payload = reactions[0]["payload"]
            liked = [item["actor"] for item in reactions if item["payload"]["reaction_type"] == CommentReaction.REACT_LIKE]
            disliked = [item["actor"] for item in reactions if item["payload"]["reaction_type"] == CommentReaction.REACT_DISLIKE]
            object_key = "comment_id" if summary_type == cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY else "video_comment_id"
            payload = {
                object_key: object_id,
                "owner": first_payload.get("comment_author") or first_payload.get("video_owner"),
                "likes_count": len(liked),
                "dislikes_count": len(disliked),
                "users_who_liked": liked,
                "users_who_disliked": disliked,
                "latest_reaction_at": latest["activity_at"],
                "object_created_at": first_payload["object_created_at"],
                "is_received_reaction": True,
                "is_given_reaction": False,
            }
            if summary_type == cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY:
                payload["comment_text"] = reactions[0]["_comment_text"]
            if first_payload.get("video_url") is not None:
                payload["video_url"] = first_payload["video_url"]
            kept.append({
                "id": f"{summary_type}:{object_id}",
                "activity_type": summary_type,
                # The feed's established paginator sorts on ``created_at``;
                # summaries use their latest reaction there while preserving
                # the object's visual date explicitly in the payload.
                "created_at": latest["activity_at"],
                "updated_at": latest["activity_at"],
                "activity_at": latest["activity_at"],
                "_sort_entity_id": object_id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[summary_type],
                "actor": first_payload.get("comment_author") or first_payload.get("video_owner"),
                "movie": latest["movie"],
                "payload": payload,
            })
        return kept

    @classmethod
    def build_feed_for_actor(cls, *, viewer, actor) -> list[dict]:
        if actor is None:
            return []

        actor_ids = [actor.id]
        activities = [
            *cls._serialize_rating_activities(actor_ids=actor_ids, viewer=viewer),
            *cls._serialize_public_comment_activities(actor_ids=actor_ids, viewer=viewer),
            *cls._serialize_public_comment_reaction_activities(actor_ids=actor_ids, viewer=viewer),
            *cls._serialize_video_reaction_created_activities(actor=actor, viewer=viewer),
        ]
        if viewer and actor and viewer.id == actor.id:
            activities.extend(
                [
                    *cls._serialize_private_message_activities(actor_ids=actor_ids, viewer=viewer),
                    *cls._serialize_private_comment_reaction_activities(actor_ids=actor_ids, viewer=viewer),
                    *cls._serialize_video_reaction_activities(viewer=viewer),
                ]
            )
        activities.sort(
            key=lambda item: (
                item["created_at"],
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
                "activity_at": cls._resolve_activity_at(created_at=rating.created_at, updated_at=rating.updated_at),
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
                "activity_at": cls._resolve_activity_at(created_at=comment.created_at, updated_at=comment.updated_at),
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
                "activity_at": cls._resolve_activity_at(created_at=comment.created_at, updated_at=comment.updated_at),
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
        queryset = (
            CommentReaction.objects.filter(
                comment__visibility=Comment.VISIBILITY_PUBLIC,
            )
            .filter(Q(comment__author_id__in=actor_ids) | Q(user_id__in=actor_ids))
            .exclude(user_id=F("comment__author_id"))
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
            .order_by("-created_at", "-id")
        )

        return [
            {
                "id": f"{cls.ACTIVITY_PUBLIC_COMMENT_REACTION}:{reaction.id}",
                "activity_type": cls.ACTIVITY_PUBLIC_COMMENT_REACTION,
                "created_at": reaction.created_at,
                "updated_at": reaction.updated_at,
                "activity_at": cls._resolve_activity_at(created_at=reaction.created_at, updated_at=reaction.updated_at),
                "_sort_entity_id": reaction.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PUBLIC_COMMENT_REACTION],
                "actor": cls._serialize_actor(reaction.user),
                "_comment_text": reaction.comment.body,
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
                    "comment_author": cls._serialize_actor(reaction.comment.author),
                    "reaction_value": reaction.reaction_type,
                    "reaction_type": reaction.reaction_type,
                    "is_given_reaction": viewer and reaction.user_id == viewer.id,
                    "is_received_reaction": viewer and reaction.comment.author_id == viewer.id,
                    "object_created_at": reaction.comment.created_at,
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
                "activity_at": cls._resolve_activity_at(created_at=reaction.created_at, updated_at=reaction.updated_at),
                "_sort_entity_id": reaction.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PRIVATE_COMMENT_REACTION],
                "actor": cls._serialize_actor(reaction.user),
                "_comment_text": reaction.comment.body,
                "movie": cls._serialize_movie(reaction.comment.movie),
                "payload": {
                    "comment_id": reaction.comment_id,
                    "reaction_id": reaction.id,
                    "comment_excerpt": cls._truncate_excerpt(reaction.comment.body),
                    "comment_author": cls._serialize_actor(reaction.comment.author),
                    "reaction_type": reaction.reaction_type,
                    "reaction_value": reaction.reaction_type,
                    "is_given_reaction": viewer and reaction.user_id == viewer.id,
                    "is_received_reaction": viewer and reaction.comment.author_id == viewer.id,
                    "object_created_at": reaction.comment.created_at,
                },
            }
            for reaction in valid_reactions
        ]

    @classmethod
    def _serialize_video_reaction_activities(cls, *, viewer) -> Iterable[dict]:
        """Build current-state video reaction activity for the authenticated user."""
        movie_display_rating_subquery = cls._movie_display_rating_subquery(
            movie_id_ref="video_comment__movie_id"
        )
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="video_comment__movie_id",
        )
        queryset = (
            VideoCommentReaction.objects.filter(
                Q(user_id=viewer.id) | Q(video_comment__user_id=viewer.id)
            )
            # A reaction to one's own video would otherwise be both given and received.
            .exclude(user_id=F("video_comment__user_id"))
            .exclude(
                video_comment__user__visibility_blocks__blocked_user_id=viewer.id,
            )
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
            .select_related(
                "user",
                "user__profile",
                "video_comment",
                "video_comment__user",
                "video_comment__user__profile",
                "video_comment__movie",
            )
            .annotate(
                movie_display_rating=Subquery(
                    movie_display_rating_subquery,
                    output_field=FloatField(),
                ),
                viewer_movie_rating=Subquery(
                    viewer_rating_subquery,
                    output_field=IntegerField(),
                ),
                movie_following_avg_rating=Subquery(
                    cls._viewer_following_avg_rating_subquery(
                        viewer=viewer,
                        movie_id_ref="video_comment__movie_id",
                    ),
                    output_field=FloatField(),
                ),
                movie_following_ratings_count=Coalesce(
                    Subquery(
                        cls._viewer_following_ratings_count_subquery(
                            viewer=viewer,
                            movie_id_ref="video_comment__movie_id",
                        ),
                        output_field=IntegerField(),
                    ),
                    Value(0),
                ),
            )
            .order_by("-updated_at", "-id")
        )

        activities = []
        for reaction in queryset:
            is_received = reaction.video_comment.user_id == viewer.id
            activity_type = (
                cls.ACTIVITY_VIDEO_REACTION_RECEIVED
                if is_received
                else cls.ACTIVITY_VIDEO_REACTION_GIVEN
            )
            activities.append(
                {
                    "id": f"{activity_type}:{reaction.id}",
                    "activity_type": activity_type,
                    "created_at": reaction.created_at,
                    "updated_at": reaction.updated_at,
                    "activity_at": cls._resolve_activity_at(
                        created_at=reaction.created_at,
                        updated_at=reaction.updated_at,
                    ),
                    "_sort_entity_id": reaction.id,
                    "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[activity_type],
                    "actor": cls._serialize_actor(reaction.user),
                    "movie": cls._serialize_movie(
                        reaction.video_comment.movie,
                        display_rating=reaction.movie_display_rating,
                        my_rating=reaction.viewer_movie_rating,
                        following_avg_rating=reaction.movie_following_avg_rating,
                        following_ratings_count=reaction.movie_following_ratings_count,
                    ),
                    "payload": {
                        "reaction_id": reaction.id,
                        "reaction_type": reaction.reaction_type,
                        "reaction_value": reaction.reaction_type,
                        "video_comment_id": reaction.video_comment_id,
                        "video_owner": cls._serialize_actor(
                            reaction.video_comment.user
                        ),
                        "is_received_reaction": is_received,
                        "is_given_reaction": not is_received,
                        "object_created_at": reaction.video_comment.created_at,
                        "video_url": reaction.video_comment.video.url if reaction.video_comment.video else None,
                    },
                }
            )
        return activities

    @classmethod
    def _serialize_video_reaction_created_activities(cls, *, actor, viewer) -> Iterable[dict]:
        """Derive an actor's upload activity from their current videos."""
        queryset = cls.video_reaction_created_queryset(actor=actor, viewer=viewer)
        return cls.serialize_video_reaction_created_queryset(queryset)

    @classmethod
    def video_reaction_created_queryset(cls, *, actor, viewer):
        """Return the directly-filtered, fully annotated video queryset."""
        movie_display_rating_subquery = cls._movie_display_rating_subquery(
            movie_id_ref="movie_id"
        )
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="movie_id",
        )
        return (
            VideoComment.objects.filter(user_id=actor.id)
            .select_related("user", "user__profile", "movie")
            .with_reaction_stats(viewer)
            .annotate(
                movie_display_rating=Subquery(
                    movie_display_rating_subquery,
                    output_field=FloatField(),
                ),
                viewer_movie_rating=Subquery(
                    viewer_rating_subquery,
                    output_field=IntegerField(),
                ),
                movie_following_avg_rating=Subquery(
                    cls._viewer_following_avg_rating_subquery(
                        viewer=viewer,
                        movie_id_ref="movie_id",
                    ),
                    output_field=FloatField(),
                ),
                movie_following_ratings_count=Coalesce(
                    Subquery(
                        cls._viewer_following_ratings_count_subquery(
                            viewer=viewer,
                            movie_id_ref="movie_id",
                        ),
                        output_field=IntegerField(),
                    ),
                    Value(0),
                ),
            )
            .order_by("-created_at", "-id")
        )

    @classmethod
    def serialize_video_reaction_created_queryset(cls, queryset) -> list[dict]:
        """Serialize an already scoped queryset (including a paginated slice)."""
        return [
            {
                "id": f"{cls.ACTIVITY_VIDEO_REACTION_CREATED}:{video.id}",
                "activity_type": cls.ACTIVITY_VIDEO_REACTION_CREATED,
                "created_at": video.created_at,
                "updated_at": video.updated_at,
                "activity_at": video.created_at,
                "timestamp": video.created_at,
                "_sort_entity_id": video.id,
                "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[
                    cls.ACTIVITY_VIDEO_REACTION_CREATED
                ],
                "actor": cls._serialize_actor(video.user),
                "movie": cls._serialize_movie(
                    video.movie,
                    display_rating=video.movie_display_rating,
                    my_rating=video.viewer_movie_rating,
                    following_avg_rating=video.movie_following_avg_rating,
                    following_ratings_count=video.movie_following_ratings_count,
                ),
                "payload": {
                    "video_comment_id": video.id,
                    "video_url": video.video.url if video.video else None,
                    "likes_count": video.likes_count,
                    "dislikes_count": video.dislikes_count,
                    "my_reaction": video.my_reaction,
                },
            }
            for video in queryset
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

    @staticmethod
    def _resolve_activity_at(*, created_at, updated_at):
        return updated_at or created_at
