from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter
from typing import Callable, Iterable, Literal, cast

from django.db import connection
from django.db.models import Avg, Case, Count, F, FloatField, IntegerField, Max, OuterRef, Q, Subquery, Value, When
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


@dataclass
class _AdaptiveSequence:
    """Candidate-only cursor state; never serialized by the API."""

    name: str
    fetch: Callable[[tuple | None, int], tuple[list[dict], dict | None, bool, int]]
    after_key: tuple | None = None
    candidates: list[dict] = field(default_factory=list)
    frontier: dict | None = None
    exhausted: bool = False
    batches: int = 0
    source_rows: int = 0


class _CandidateProfile:
    """Request-local numeric instrumentation; never contains query text or PII."""

    def __init__(self):
        self.started = perf_counter()
        self.phase = "setup"
        self.queries_total = 0
        self.queries_by_phase = {}
        self.family_ms = {}
        self.frontier_ms = 0.0
        self.certification_ms = 0.0
        self.hydration_ms = 0.0

    def execute(self, execute, sql, params, many, context):
        self.queries_total += 1
        self.queries_by_phase[self.phase] = self.queries_by_phase.get(self.phase, 0) + 1
        return execute(sql, params, many, context)

    def measure(self, phase, callback):
        previous = self.phase
        self.phase = phase
        started = perf_counter()
        try:
            return callback()
        finally:
            elapsed = (perf_counter() - started) * 1000
            self.family_ms[phase] = self.family_ms.get(phase, 0.0) + elapsed
            self.phase = previous


class SocialActivityFeedService:
    CANDIDATE_MODE_FULL = "full_legacy_like"
    CANDIDATE_MODE_LOGICAL_GROUPS = "logical_group_candidates"
    CANDIDATE_MODE_ADAPTIVE = "adaptive_top_k"
    DEFAULT_ADAPTIVE_BATCH_SIZE = 10
    DEFAULT_ADAPTIVE_MAX_BATCHES = 100
    DEFAULT_ADAPTIVE_MAX_CANDIDATE_ROWS = 10000
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
    PROFILE_ACTIVITY_TYPES = frozenset({
        ACTIVITY_RATING,
        ACTIVITY_PUBLIC_COMMENT,
        ACTIVITY_PUBLIC_COMMENT_REACTION,
    })
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
    # Frozen in Phase A.  This is deliberately candidate-only: ``build_feed``
    # keeps its established sort implementation until the candidate is proven
    # in production-like datasets.
    LEGACY_FAMILY_RANK = {
        ACTIVITY_RATING: 7,
        ACTIVITY_PUBLIC_COMMENT: 6,
        ACTIVITY_PUBLIC_COMMENT_REACTION: 5,
        ACTIVITY_PRIVATE_MESSAGE: 4,
        ACTIVITY_PRIVATE_COMMENT_REACTION: 3,
        ACTIVITY_VIDEO_REACTION_CREATED: 2,
        ACTIVITY_VIDEO_REACTION_GIVEN: 1,
        ACTIVITY_VIDEO_REACTION_RECEIVED: 1,
        ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY: 0,
        ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY: -1,
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
    def build_feed_candidate_full_materialization(cls, *, user, scope: SocialFeedScope) -> list[dict]:
        """Build the Phase-B candidate without changing the production path.

        All selector rows are intentionally materialized before hydration.  No
        limit, batching, keyset pagination, or summary optimization belongs in
        this phase.
        """
        if not cls.is_valid_scope(scope):
            raise ValueError(f"Unsupported social feed scope: {scope}")
        actor_ids = list(set(cls._get_actor_ids_for_scope(user=user, scope=scope)))
        if not actor_ids:
            return []

        activities = []
        families = (
            (cls.rating_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_rating_ids, cls.serialize_rating_queryset),
            (cls.public_comment_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_public_comment_ids, cls.serialize_public_comment_queryset),
            (cls.public_reaction_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_public_reaction_ids, cls.serialize_public_reaction_queryset),
        )
        for candidates, hydrator, converter in families:
            ids = list(candidates.values_list("pk", flat=True))
            activities.extend(converter(hydrator(ids, viewer=user), viewer=user))

        if scope == cls.SCOPE_ME:
            private_families = (
                (cls.private_message_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_private_message_ids, cls.serialize_private_message_queryset),
                (cls.private_reaction_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_private_reaction_ids, cls.serialize_private_reaction_queryset),
                (cls.video_created_candidates_queryset(actor=user, viewer=user), cls.hydrate_video_created_ids, cls.serialize_video_reaction_created_queryset),
                (cls.video_reaction_candidates_queryset(viewer=user), cls.hydrate_video_reaction_ids, cls.serialize_video_reaction_queryset),
            )
            for candidates, hydrator, converter in private_families:
                ids = list(candidates.values_list("pk", flat=True))
                activities.extend(converter(hydrator(ids, viewer=user), viewer=user))
            activities = cls._consolidate_received_reactions(activities)

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
    def build_feed_candidate(cls, *, user, scope: SocialFeedScope, mode: str = CANDIDATE_MODE_FULL) -> list[dict]:
        """Select an internal implementation mode; neither mode is used by a view."""
        if mode == cls.CANDIDATE_MODE_FULL:
            return cls.build_feed_candidate_full_materialization(user=user, scope=scope)
        if mode == cls.CANDIDATE_MODE_LOGICAL_GROUPS:
            return cls.build_feed_candidate_logical_groups(user=user, scope=scope)
        raise ValueError(f"Unsupported candidate mode: {mode}")

    @classmethod
    def build_feed_candidate_logical_groups(cls, *, user, scope: SocialFeedScope) -> list[dict]:
        """Build Phase C using logical received groups and batched hydration.

        This still materializes all seven families and performs the same final
        Python sort.  It is an equivalence candidate, not a production pager or
        the adaptive global merge planned for Phase D.
        """
        if not cls.is_valid_scope(scope):
            raise ValueError(f"Unsupported social feed scope: {scope}")
        if scope != cls.SCOPE_ME:
            return cls.build_feed_candidate_full_materialization(user=user, scope=scope)
        actor_ids = list(set(cls._get_actor_ids_for_scope(user=user, scope=scope)))
        if not actor_ids:
            return []

        activities = []
        ordinary_families = (
            (cls.rating_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_rating_ids, cls.serialize_rating_queryset),
            (cls.public_comment_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_public_comment_ids, cls.serialize_public_comment_queryset),
        )
        for candidates, hydrator, converter in ordinary_families:
            activities.extend(converter(hydrator(list(candidates.values_list("pk", flat=True)), viewer=user), viewer=user))

        # Given public reactions remain row-shaped; only received reactions are
        # replaced by logical comment candidates.
        public_given_ids = list(
            cls.public_reaction_candidates_queryset(actor_ids=actor_ids, viewer=user)
            .filter(user_id=user.id)
            .values_list("pk", flat=True)
        )
        activities.extend(cls.serialize_public_reaction_queryset(
            cls.hydrate_public_reaction_ids(public_given_ids, viewer=user), viewer=user
        ))

        if scope == cls.SCOPE_ME:
            non_reaction_families = (
                (cls.private_message_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.hydrate_private_message_ids, cls.serialize_private_message_queryset),
                (cls.video_created_candidates_queryset(actor=user, viewer=user), cls.hydrate_video_created_ids, cls.serialize_video_reaction_created_queryset),
            )
            for candidates, hydrator, converter in non_reaction_families:
                activities.extend(converter(hydrator(list(candidates.values_list("pk", flat=True)), viewer=user), viewer=user))

            video_given_ids = list(
                cls.video_reaction_candidates_queryset(viewer=user)
                .filter(user_id=user.id)
                .values_list("pk", flat=True)
            )
            activities.extend(cls.serialize_video_reaction_queryset(
                cls.hydrate_video_reaction_ids(video_given_ids, viewer=user), viewer=user
            ))

            comment_groups = cls.comment_received_logical_candidates(viewer=user)
            video_groups = cls.video_received_logical_candidates(viewer=user)
            activities.extend(cls.hydrate_comment_reaction_summaries(
                comment_ids=[group["object_id"] for group in comment_groups], viewer=user
            ))
            activities.extend(cls.hydrate_video_reaction_summaries(
                video_comment_ids=[group["object_id"] for group in video_groups], viewer=user
            ))

        activities.sort(key=lambda item: (
            cls._activity_sort_timestamp(item),
            item["_sort_activity_priority"],
            item["_sort_entity_id"],
        ), reverse=True)
        return activities

    @classmethod
    def _candidate_key(cls, candidate: dict) -> tuple:
        return (
            candidate["effective_at"], candidate["priority"],
            candidate["entity_id"], candidate["family_rank"],
        )

    @classmethod
    def _adaptive_queryset_fetcher(
        cls, *, queryset, namespace: str, timestamp_field: str,
        entity_field: str = "id", object_field: str = "id",
        validator=None,
    ):
        """Build a descending keyset reader with one look-ahead frontier.

        ``batch_size + 1`` is intentional: the extra lightweight row is the
        certified best unread item.  It is retained as a frontier and becomes
        part of the next batch rather than being queried again.
        """
        priority = cls._ACTIVITY_SORT_PRIORITY[namespace]
        family_rank = cls.LEGACY_FAMILY_RANK[namespace]

        def fetch(after_key, batch_size):
            page = queryset
            if after_key is not None:
                after_at, after_entity = after_key
                page = page.filter(
                    Q(**{f"{timestamp_field}__lt": after_at})
                    | Q(**{timestamp_field: after_at, f"{entity_field}__lt": after_entity})
                )
            page = page.order_by(f"-{timestamp_field}", f"-{entity_field}")
            rows = list(page[:batch_size + 1])
            accepted = []
            for row in rows:
                if validator is not None and not validator(row):
                    continue
                effective_at = getattr(row, timestamp_field)
                entity_id = getattr(row, entity_field)
                accepted.append({
                    "namespace": namespace,
                    "object_id": getattr(row, object_field),
                    "effective_at": effective_at,
                    "priority": priority,
                    "entity_id": entity_id,
                    "family_rank": family_rank,
                    "source_rows": 1,
                })
            # A rejected final row cannot be a privacy-safe frontier.  The
            # caller must continue this sequence until a valid look-ahead or
            # exhaustion, so we expose no frontier in that case.
            exhausted = len(rows) <= batch_size
            visible = accepted[:batch_size]
            frontier = accepted[batch_size] if len(accepted) > batch_size else None
            if rows:
                last = rows[-1]
                cursor = (getattr(last, timestamp_field), getattr(last, entity_field))
            else:
                cursor = after_key
            for item in visible:
                item["_cursor"] = cursor
            if frontier is not None:
                frontier["_cursor"] = cursor
            return visible, frontier, exhausted, len(rows)

        return fetch

    @classmethod
    def _adaptive_list_fetcher(cls, *, rows: list[dict]):
        """Safe hybrid cursor for Python-validated private summary groups."""
        ordered = sorted(rows, key=lambda row: (
            row["latest_activity_at"],
            cls._ACTIVITY_SORT_PRIORITY[row["namespace"]], row["object_id"],
            row["family_rank"],
        ), reverse=True)

        def fetch(after_key, batch_size):
            start = 0 if after_key is None else after_key
            window = ordered[start:start + batch_size + 1]
            converted = [{
                **row,
                "effective_at": row["latest_activity_at"],
                "priority": cls._ACTIVITY_SORT_PRIORITY[row["namespace"]],
                "entity_id": row["object_id"],
            } for row in window]
            frontier = converted[batch_size] if len(converted) > batch_size else None
            next_cursor = start + len(window)
            for item in converted:
                item["_cursor"] = next_cursor
            return converted[:batch_size], frontier, len(window) <= batch_size, sum(
                row.get("source_rows", 1) for row in window
            )
        return fetch

    @classmethod
    def _adaptive_group_fetcher(cls, *, queryset, namespace: str, object_field: str):
        priority = cls._ACTIVITY_SORT_PRIORITY[namespace]
        family_rank = cls.LEGACY_FAMILY_RANK[namespace]

        def fetch(after_key, batch_size):
            page = queryset
            if after_key is not None:
                after_at, after_id = after_key
                page = page.filter(
                    Q(latest_activity_at__lt=after_at)
                    | Q(latest_activity_at=after_at, **{f"{object_field}__lt": after_id})
                )
            rows = list(page.order_by("-latest_activity_at", f"-{object_field}")[:batch_size + 1])
            converted = [{
                "namespace": namespace, "object_id": row[object_field],
                "effective_at": row["latest_activity_at"], "priority": priority,
                "entity_id": row[object_field], "family_rank": family_rank,
                "source_rows": row["source_rows"],
            } for row in rows]
            frontier = converted[batch_size] if len(converted) > batch_size else None
            cursor = ((rows[-1]["latest_activity_at"], rows[-1][object_field]) if rows else after_key)
            for item in converted:
                item["_cursor"] = cursor
            return converted[:batch_size], frontier, len(rows) <= batch_size, sum(
                row["source_rows"] for row in rows
            )
        return fetch

    @classmethod
    def _adaptive_sequences(cls, *, user, actor_ids, profiler=None) -> list[_AdaptiveSequence]:
        def ordinary(name, queryset, namespace, timestamp="candidate_activity_at", validator=None):
            return _AdaptiveSequence(name=name, fetch=cls._adaptive_queryset_fetcher(
                queryset=queryset, namespace=namespace, timestamp_field=timestamp,
                validator=validator,
            ))

        sequences = [
            ordinary("ratings", cls.rating_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.ACTIVITY_RATING),
            ordinary("public_comments", cls.public_comment_candidates_queryset(actor_ids=actor_ids, viewer=user), cls.ACTIVITY_PUBLIC_COMMENT),
            ordinary("public_reactions_given", cls.public_reaction_candidates_queryset(actor_ids=actor_ids, viewer=user).filter(user_id=user.id), cls.ACTIVITY_PUBLIC_COMMENT_REACTION),
            # G1: the Python privacy predicate reads ``visibility`` and
            # ``body``.  The base lightweight selector deliberately omits
            # payload fields, so accessing those deferred attributes used to
            # issue two queries per row.  Load only those predicate fields and
            # the target relation for this Candidate stream; hydration remains
            # unchanged and still owns all response payload construction.
            ordinary(
                "private_messages",
                cls.private_message_candidates_queryset(
                    actor_ids=actor_ids, viewer=user
                ).only(
                    "id", "author_id", "target_user_id", "movie_id",
                    "created_at", "body", "visibility",
                ).select_related("target_user"),
                cls.ACTIVITY_PRIVATE_MESSAGE,
                validator=lambda comment: comment.has_valid_target_mention(),
            ),
            ordinary("videos_created", cls.video_created_candidates_queryset(actor=user, viewer=user), cls.ACTIVITY_VIDEO_REACTION_CREATED),
            ordinary("video_reactions_given", cls.video_reaction_candidates_queryset(viewer=user).filter(user_id=user.id), cls.ACTIVITY_VIDEO_REACTION_GIVEN),
        ]

        latest_public_id = (
            cls._public_received_reaction_rows(viewer=user)
            .filter(comment_id=OuterRef("comment_id"))
            .order_by("-updated_at", "-id").values("id")[:1]
        )
        public_comment_groups = cls._public_received_reaction_rows(viewer=user).values(
            "comment_id"
        ).annotate(
            latest_activity_at=Max("updated_at"),
            latest_reaction_id=Subquery(latest_public_id), source_rows=Count("id"),
        )
        sequences.append(_AdaptiveSequence(
            name="comment_reaction_summaries_received",
            fetch=cls._adaptive_group_fetcher(
                queryset=public_comment_groups,
                namespace=cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY,
                object_field="comment_id",
            ),
        ))

        # Only private received rows require Phase C's Python privacy check.
        # They are over-read once as lightweight rows; an unvalidated row is
        # never used as a frontier. Public and private comments cannot overlap
        # because visibility is a single-valued model field.
        private_group_builder = lambda: cls.private_comment_received_logical_candidates(
            viewer=user
        )
        private_groups = (
            profiler.measure(
                "comment_reaction_summaries_received_private_hybrid",
                private_group_builder,
            )
            if profiler else private_group_builder()
        )
        sequences.append(_AdaptiveSequence(
            name="comment_reaction_summaries_received_private_hybrid",
            fetch=cls._adaptive_list_fetcher(rows=private_groups),
        ))

        latest_video_id = (
            cls._video_received_reaction_rows(viewer=user)
            .filter(video_comment_id=OuterRef("video_comment_id"))
            .order_by("-updated_at", "-id").values("id")[:1]
        )
        video_groups = cls._video_received_reaction_rows(viewer=user).values(
            "video_comment_id"
        ).annotate(
            latest_activity_at=Max("updated_at"),
            latest_reaction_id=Subquery(latest_video_id), source_rows=Count("id"),
        )
        sequences.append(_AdaptiveSequence(
            name="video_reaction_summaries_received",
            fetch=cls._adaptive_group_fetcher(
                queryset=video_groups,
                namespace=cls.ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY,
                object_field="video_comment_id",
            ),
        ))
        return sequences

    @classmethod
    def build_feed_candidate_adaptive(
        cls, *, user, scope: SocialFeedScope, k: int,
        batch_size: int = DEFAULT_ADAPTIVE_BATCH_SIZE,
        max_batches: int = DEFAULT_ADAPTIVE_MAX_BATCHES,
        max_candidate_rows: int = DEFAULT_ADAPTIVE_MAX_CANDIDATE_ROWS,
        fallback_to_legacy: bool = True, return_metadata: bool = False,
        profile_enabled: bool = False,
    ):
        """Return an exact, certified Phase-D top-K or the legacy fallback.

        This internal service is intentionally not called by any view.  A
        sequence expands only when its look-ahead can equal or beat the current
        Kth total key. Payload hydration begins after every frontier certifies.
        """
        metadata = {
            "certified": False, "fallback_reason": None, "batches_by_family": {},
            "source_rows_inspected": 0, "logical_candidates_inspected": 0,
            "hydrated_rows": 0,
        }
        profiler = _CandidateProfile() if profile_enabled else None
        query_context = connection.execute_wrapper(profiler.execute) if profiler else None
        if query_context:
            query_context.__enter__()
        try:
            if not cls.is_valid_scope(scope):
                raise ValueError(f"Unsupported social feed scope: {scope}")
            if k < 0 or batch_size < 1 or max_batches < 1 or max_candidate_rows < 1:
                raise ValueError("Adaptive limits must be positive (k may be zero)")
            if k == 0:
                metadata["certified"] = True
                return ([], metadata) if return_metadata else []
            if scope != cls.SCOPE_ME:
                raise RuntimeError("phase_d_scope_not_supported")
            actor_ids = list(set(cls._get_actor_ids_for_scope(user=user, scope=scope)))
            sequences = cls._adaptive_sequences(
                user=user, actor_ids=actor_ids, profiler=profiler
            )

            def expand(sequence):
                if sequence.batches >= max_batches:
                    raise RuntimeError("max_batches")
                fetch = lambda: sequence.fetch(sequence.after_key, batch_size)
                rows, frontier, exhausted, inspected = (
                    profiler.measure(sequence.name, fetch) if profiler else fetch()
                )
                sequence.batches += 1
                sequence.source_rows += inspected
                metadata["source_rows_inspected"] += inspected
                metadata["logical_candidates_inspected"] += len(rows) + (frontier is not None)
                if metadata["logical_candidates_inspected"] > max_candidate_rows:
                    raise RuntimeError("max_candidate_rows")
                sequence.candidates.extend(rows)
                sequence.frontier = frontier
                sequence.exhausted = exhausted
                cursor_source = frontier or (rows[-1] if rows else None)
                if cursor_source is not None:
                    sequence.after_key = cursor_source.get("_cursor")

            for sequence in sequences:
                expand(sequence)

            while True:
                frontier_started = perf_counter() if profiler else None
                known = sorted(
                    (item for sequence in sequences for item in sequence.candidates),
                    key=cls._candidate_key, reverse=True,
                )
                kth_key = cls._candidate_key(known[k - 1]) if len(known) >= k else None
                unsafe = [sequence for sequence in sequences if not sequence.exhausted and (
                    sequence.frontier is None or kth_key is None
                    or cls._candidate_key(sequence.frontier) >= kth_key
                )]
                if profiler:
                    profiler.frontier_ms += (perf_counter() - frontier_started) * 1000
                certification_started = perf_counter() if profiler else None
                if len(known) >= k and not unsafe:
                    selected = known[:k]
                    if profiler:
                        profiler.certification_ms += (perf_counter() - certification_started) * 1000
                    break
                if not unsafe:
                    # All streams exhausted, so fewer than K is an exact top-K.
                    selected = known
                    if profiler:
                        profiler.certification_ms += (perf_counter() - certification_started) * 1000
                    break
                if profiler:
                    profiler.certification_ms += (perf_counter() - certification_started) * 1000
                for sequence in unsafe:
                    # Promote the look-ahead on expansion without restarting.
                    if sequence.frontier is not None:
                        sequence.candidates.append(sequence.frontier)
                    expand(sequence)

            hydration_started = perf_counter() if profiler else None
            if profiler:
                profiler.phase = "hydration"
            activities = cls._hydrate_adaptive_candidates(
                selected=selected, viewer=user, profiler=profiler
            )
            if profiler:
                profiler.hydration_ms = (perf_counter() - hydration_started) * 1000
            expected_ids = [f'{item["namespace"]}:{item["object_id"]}' for item in selected]
            activities.sort(key=lambda item: (
                cls._activity_sort_timestamp(item), item["_sort_activity_priority"],
                item["_sort_entity_id"], cls.LEGACY_FAMILY_RANK[item["activity_type"]],
            ), reverse=True)
            if [item["id"] for item in activities] != expected_ids:
                raise RuntimeError("hydrated_order_mismatch")
            metadata["certified"] = True
            metadata["hydrated_rows"] = len(activities)
            metadata["batches_by_family"] = {s.name: s.batches for s in sequences}
            if profiler:
                metadata["profile"] = cls._candidate_profile_metadata(
                    profiler=profiler, sequences=sequences
                )
            return (activities, metadata) if return_metadata else activities
        except Exception as exc:
            metadata["fallback_reason"] = str(exc) or exc.__class__.__name__
            if profiler:
                metadata["profile"] = cls._candidate_profile_metadata(
                    profiler=profiler, sequences=locals().get("sequences", [])
                )
            if not fallback_to_legacy:
                return ([], metadata) if return_metadata else []
            legacy = cls.build_feed(user=user, scope=scope)[:k]
            metadata["hydrated_rows"] = len(legacy)
            return (legacy, metadata) if return_metadata else legacy
        finally:
            if query_context:
                query_context.__exit__(None, None, None)

    @classmethod
    def _candidate_profile_metadata(cls, *, profiler, sequences):
        families = {}
        for sequence in sequences:
            elapsed = profiler.family_ms.get(sequence.name, 0.0)
            families[sequence.name] = {
                "batches": sequence.batches,
                "rows": sequence.source_rows,
                "ms": round(elapsed, 3),
                "avg_batch_ms": round(elapsed / sequence.batches, 3) if sequence.batches else 0.0,
                "queries": profiler.queries_by_phase.get(sequence.name, 0),
            }
        hydration_families = {
            phase.removeprefix("hydration_"): {
                "ms": round(elapsed, 3),
                "queries": profiler.queries_by_phase.get(phase, 0),
            }
            for phase, elapsed in profiler.family_ms.items()
            if phase.startswith("hydration_")
        }
        hydration_queries = sum(
            count for phase, count in profiler.queries_by_phase.items()
            if phase == "hydration" or phase.startswith("hydration_")
        )
        return {
            "total_ms": round((perf_counter() - profiler.started) * 1000, 3),
            "candidate_queries_total": profiler.queries_total,
            "families": families,
            "frontier_ms": round(profiler.frontier_ms, 3),
            "certification_ms": round(profiler.certification_ms, 3),
            "hydration_ms": round(profiler.hydration_ms, 3),
            "hydration_queries": hydration_queries,
            "hydration_families": hydration_families,
        }

    @classmethod
    def _hydrate_adaptive_candidates(cls, *, selected, viewer, profiler=None):
        """Hydrate the certified top-K in one evaluated queryset per family.

        Keeping the maps of selected identifiers request-local bounds memory to
        K.  ``profiler`` is deliberately optional so production requests with
        profiling disabled do not pay for clocks or per-family bookkeeping.
        """
        ids = {}
        for item in selected:
            ids.setdefault(item["namespace"], []).append(item["object_id"])
        activities = []
        ordinary = {
            cls.ACTIVITY_RATING: (cls.hydrate_rating_ids, cls.serialize_rating_queryset),
            cls.ACTIVITY_PUBLIC_COMMENT: (cls.hydrate_public_comment_ids, cls.serialize_public_comment_queryset),
            cls.ACTIVITY_PUBLIC_COMMENT_REACTION: (cls.hydrate_public_reaction_ids, cls.serialize_public_reaction_queryset),
            cls.ACTIVITY_PRIVATE_MESSAGE: (cls.hydrate_private_message_ids, cls.serialize_private_message_queryset),
            cls.ACTIVITY_VIDEO_REACTION_CREATED: (cls.hydrate_video_created_ids, cls.serialize_video_reaction_created_queryset),
            cls.ACTIVITY_VIDEO_REACTION_GIVEN: (cls.hydrate_video_reaction_ids, cls.serialize_video_reaction_queryset),
        }
        for namespace, (hydrator, converter) in ordinary.items():
            object_ids = ids.get(namespace, [])
            if object_ids:
                hydrate = lambda: converter(
                    hydrator(object_ids, viewer=viewer), viewer=viewer
                )
                rows = (
                    profiler.measure(f"hydration_{namespace}", hydrate)
                    if profiler else hydrate()
                )
                activities.extend(rows)

        def hydrate_comment_summaries():
            return cls.hydrate_comment_reaction_summaries(
                comment_ids=ids.get(
                    cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY, []
                ),
                viewer=viewer,
            )

        def hydrate_video_summaries():
            return cls.hydrate_video_reaction_summaries(
                video_comment_ids=ids.get(
                    cls.ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY, []
                ),
                viewer=viewer,
            )

        comment_rows = (
            profiler.measure("hydration_comment_summaries", hydrate_comment_summaries)
            if profiler else hydrate_comment_summaries()
        )
        video_rows = (
            profiler.measure("hydration_video_summaries", hydrate_video_summaries)
            if profiler else hydrate_video_summaries()
        )
        activities.extend(comment_rows)
        activities.extend(video_rows)
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
    def build_feed_for_actor(cls, *, viewer, actor, activity_type: str | None = None) -> list[dict]:
        """Build a visited profile feed, optionally querying one public family only.

        ``activity_type`` must be one of ``PROFILE_ACTIVITY_TYPES``. Video
        uploads intentionally live at the dedicated ``/video-reactions/``
        endpoint and are never included here.
        """
        if actor is None:
            return []
        if activity_type is not None and activity_type not in cls.PROFILE_ACTIVITY_TYPES:
            raise ValueError(f"Unsupported profile activity type: {activity_type}")

        actor_ids = [actor.id]
        serializers_by_type = {
            cls.ACTIVITY_RATING: cls._serialize_rating_activities,
            cls.ACTIVITY_PUBLIC_COMMENT: cls._serialize_public_comment_activities,
            cls.ACTIVITY_PUBLIC_COMMENT_REACTION: cls._serialize_public_comment_reaction_activities,
        }
        selected_types = (activity_type,) if activity_type else cls.PROFILE_ACTIVITY_TYPES
        activities = []
        for selected_type in selected_types:
            activities.extend(
                serializers_by_type[selected_type](actor_ids=actor_ids, viewer=viewer)
            )

        if activity_type is None and viewer and actor and viewer.id == actor.id:
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

    # Phase-B selectors.  They intentionally return model querysets rather
    # than values/lists, so callers may further project them without causing
    # evaluation.  ``only`` keeps candidate selection free of payload fields.
    @classmethod
    def rating_candidates_queryset(cls, *, actor_ids, viewer):
        return MovieRating.objects.filter(user_id__in=actor_ids).annotate(
            candidate_activity_at=F("updated_at"),
            candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_RATING]),
        ).only(
            "id", "user_id", "movie_id", "created_at", "updated_at"
        )

    @classmethod
    def public_comment_candidates_queryset(cls, *, actor_ids, viewer):
        return Comment.objects.filter(
            author_id__in=actor_ids, visibility=Comment.VISIBILITY_PUBLIC
        ).annotate(
            candidate_activity_at=F("created_at"),
            candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_PUBLIC_COMMENT]),
        ).only("id", "author_id", "movie_id", "created_at")

    @classmethod
    def public_reaction_candidates_queryset(cls, *, actor_ids, viewer):
        return (
            CommentReaction.objects.filter(comment__visibility=Comment.VISIBILITY_PUBLIC)
            .filter(Q(comment__author_id__in=actor_ids) | Q(user_id__in=actor_ids))
            .exclude(user_id=F("comment__author_id"))
            .exclude(comment__author__visibility_blocks__blocked_user_id=viewer.id)
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
            .annotate(candidate_activity_at=F("updated_at"), candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_PUBLIC_COMMENT_REACTION]))
            .only("id", "user_id", "comment_id", "created_at", "updated_at")
        )

    @classmethod
    def private_message_candidates_queryset(cls, *, actor_ids, viewer):
        # ``has_valid_target_mention`` remains a Python validation after rich
        # hydration; translating its username/token rules to SQL is not proven
        # equivalent and is therefore intentionally deferred.
        return Comment.objects.filter(
            author_id__in=actor_ids,
            visibility=Comment.VISIBILITY_MENTIONED,
            target_user__isnull=False,
        ).annotate(candidate_activity_at=F("created_at"), candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_PRIVATE_MESSAGE])).only("id", "author_id", "target_user_id", "movie_id", "created_at")

    @classmethod
    def private_reaction_candidates_queryset(cls, *, actor_ids, viewer):
        # The parent comment receives the same post-hydration Python validation
        # as the legacy implementation.
        return (
            CommentReaction.objects.filter(
                comment__author_id__in=actor_ids,
                comment__visibility=Comment.VISIBILITY_MENTIONED,
            )
            .exclude(user_id=F("comment__author_id"))
            .annotate(candidate_activity_at=F("updated_at"), candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_PRIVATE_COMMENT_REACTION]))
            .only("id", "user_id", "comment_id", "created_at", "updated_at")
        )

    @classmethod
    def video_created_candidates_queryset(cls, *, actor, viewer):
        return VideoComment.objects.filter(user_id=actor.id).annotate(
            candidate_activity_at=F("created_at"), candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_VIDEO_REACTION_CREATED])
        ).only(
            "id", "user_id", "movie_id", "created_at"
        )

    @classmethod
    def video_reaction_candidates_queryset(cls, *, viewer):
        return (
            VideoCommentReaction.objects.filter(
                Q(user_id=viewer.id) | Q(video_comment__user_id=viewer.id)
            )
            .exclude(user_id=F("video_comment__user_id"))
            .exclude(video_comment__user__visibility_blocks__blocked_user_id=viewer.id)
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
            .annotate(candidate_activity_at=F("updated_at"), candidate_family_rank=Value(cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_VIDEO_REACTION_GIVEN]))
            .only("id", "user_id", "video_comment_id", "created_at", "updated_at")
        )

    @classmethod
    def _public_received_reaction_rows(cls, *, viewer):
        """Authorized public received rows, before any grouping."""
        return (
            CommentReaction.objects.filter(
                comment__visibility=Comment.VISIBILITY_PUBLIC,
                comment__author_id=viewer.id,
            )
            .exclude(user_id=F("comment__author_id"))
            .exclude(comment__author__visibility_blocks__blocked_user_id=viewer.id)
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
        )

    @classmethod
    def _private_received_reaction_rows(cls, *, viewer):
        """Potential private rows; target validation intentionally stays Python."""
        return (
            CommentReaction.objects.filter(
                comment__author_id=viewer.id,
                comment__visibility=Comment.VISIBILITY_MENTIONED,
            )
            .exclude(user_id=F("comment__author_id"))
        )

    @classmethod
    def _video_received_reaction_rows(cls, *, viewer):
        """Authorized video received rows, before any grouping."""
        return (
            VideoCommentReaction.objects.filter(video_comment__user_id=viewer.id)
            .exclude(user_id=F("video_comment__user_id"))
            .exclude(video_comment__user__visibility_blocks__blocked_user_id=viewer.id)
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
        )

    @classmethod
    def comment_received_logical_candidates(cls, *, viewer) -> list[dict]:
        """Return one lightweight logical candidate per authorized comment.

        Public rows are grouped in SQL.  Private rows use a deliberately safe
        hybrid: their parent comments are loaded in one query and checked with
        ``has_valid_target_mention`` before being merged with public groups.
        This preserves the legacy privacy predicate instead of approximating it
        in SQL.  Public and private namespaces are then unioned by comment id.
        """
        latest_public_id = (
            cls._public_received_reaction_rows(viewer=viewer)
            .filter(comment_id=OuterRef("comment_id"))
            .order_by("-updated_at", "-id")
            .values("id")[:1]
        )
        public_groups = cls._public_received_reaction_rows(viewer=viewer).values(
            "comment_id"
        ).annotate(
            latest_activity_at=Max("updated_at"),
            latest_reaction_id=Subquery(latest_public_id),
            source_rows=Count("id"),
        )
        merged = {
            row["comment_id"]: {
                "namespace": cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY,
                "object_id": row["comment_id"],
                "latest_activity_at": row["latest_activity_at"],
                "latest_reaction_id": row["latest_reaction_id"],
                "family_rank": cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY],
                "source_rows": row["source_rows"],
            }
            for row in public_groups
        }

        for row in cls.private_comment_received_logical_candidates(viewer=viewer):
            candidate = merged.get(row["object_id"])
            if candidate is None:
                merged[row["object_id"]] = row
                continue
            candidate["source_rows"] += row["source_rows"]
            if (row["latest_activity_at"], row["latest_reaction_id"]) > (
                candidate["latest_activity_at"], candidate["latest_reaction_id"]
            ):
                candidate["latest_activity_at"] = row["latest_activity_at"]
                candidate["latest_reaction_id"] = row["latest_reaction_id"]
        return list(merged.values())

    @classmethod
    def private_comment_received_logical_candidates(cls, *, viewer) -> list[dict]:
        """Python-validated private groups, isolated for adaptive over-read."""
        merged = {}
        private_rows = cls._private_received_reaction_rows(viewer=viewer).select_related(
            "comment", "comment__target_user"
        ).only(
            "id", "created_at", "updated_at", "comment_id", "comment__visibility",
            "comment__author_id", "comment__target_user_id", "comment__body",
            "comment__target_user__username",
        )
        for reaction in private_rows:
            if not reaction.comment.has_valid_target_mention():
                continue
            activity_at = cls._resolve_activity_at(
                created_at=reaction.created_at, updated_at=reaction.updated_at
            )
            candidate = merged.get(reaction.comment_id)
            if candidate is None:
                candidate = {
                    "namespace": cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY,
                    "object_id": reaction.comment_id,
                    "latest_activity_at": activity_at,
                    "latest_reaction_id": reaction.id,
                    "family_rank": cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_COMMENT_REACTIONS_RECEIVED_SUMMARY],
                    "source_rows": 0,
                }
                merged[reaction.comment_id] = candidate
            candidate["source_rows"] += 1
            if (activity_at, reaction.id) > (
                candidate["latest_activity_at"], candidate["latest_reaction_id"]
            ):
                candidate["latest_activity_at"] = activity_at
                candidate["latest_reaction_id"] = reaction.id
        return list(merged.values())

    @classmethod
    def video_received_logical_candidates(cls, *, viewer) -> list[dict]:
        latest_id = (
            cls._video_received_reaction_rows(viewer=viewer)
            .filter(video_comment_id=OuterRef("video_comment_id"))
            .order_by("-updated_at", "-id")
            .values("id")[:1]
        )
        groups = cls._video_received_reaction_rows(viewer=viewer).values(
            "video_comment_id"
        ).annotate(
            latest_activity_at=Max("updated_at"),
            latest_reaction_id=Subquery(latest_id),
            source_rows=Count("id"),
        )
        return [{
            "namespace": cls.ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY,
            "object_id": row["video_comment_id"],
            "latest_activity_at": row["latest_activity_at"],
            "latest_reaction_id": row["latest_reaction_id"],
            "family_rank": cls.LEGACY_FAMILY_RANK[cls.ACTIVITY_VIDEO_REACTIONS_RECEIVED_SUMMARY],
            "source_rows": row["source_rows"],
        } for row in groups]

    @classmethod
    def count_comment_received_logical_items(cls, *, viewer) -> int:
        """Count the public/private union, not the sum of two distinct counts."""
        return len(cls.comment_received_logical_candidates(viewer=viewer))

    @classmethod
    def count_video_received_logical_items(cls, *, viewer) -> int:
        return cls._video_received_reaction_rows(viewer=viewer).values(
            "video_comment_id"
        ).distinct().count()

    @classmethod
    def count_feed_candidate_logical(cls, *, user, scope: SocialFeedScope) -> int:
        """Count Phase-C logical activities without hydrating feed payloads.

        Phase E only observes this value; DRF continues to count the fully
        materialized legacy list.  The only Python materialization here is for
        mention validation, whose rules intentionally remain outside SQL.
        """
        if scope != cls.SCOPE_ME:
            raise ValueError("candidate_logical_count_scope_not_supported")
        actor_ids = list(set(cls._get_actor_ids_for_scope(user=user, scope=scope)))
        private_messages = cls.private_message_candidates_queryset(
            actor_ids=actor_ids, viewer=user
        ).select_related("target_user").only(
            "id", "body", "author_id", "target_user_id", "target_user__username"
        )
        valid_private_messages = sum(
            comment.has_valid_target_mention() for comment in private_messages
        )
        return sum((
            cls.rating_candidates_queryset(actor_ids=actor_ids, viewer=user).count(),
            cls.public_comment_candidates_queryset(actor_ids=actor_ids, viewer=user).count(),
            cls.public_reaction_candidates_queryset(actor_ids=actor_ids, viewer=user)
            .filter(user_id=user.id).count(),
            valid_private_messages,
            cls.video_created_candidates_queryset(actor=user, viewer=user).count(),
            cls.video_reaction_candidates_queryset(viewer=user).filter(user_id=user.id).count(),
            cls.count_comment_received_logical_items(viewer=user),
            cls.count_video_received_logical_items(viewer=user),
        ))

    @classmethod
    def hydrate_comment_reaction_summaries(cls, *, comment_ids, viewer) -> list[dict]:
        """Hydrate all selected comment groups in two batch queries."""
        comment_ids = list(set(comment_ids))
        if not comment_ids:
            return []
        rows = cls.serialize_public_reaction_queryset(
            cls._public_reaction_activity_queryset(actor_ids=[viewer.id], viewer=viewer)
            .filter(comment_id__in=comment_ids), viewer=viewer
        )
        rows.extend(cls.serialize_private_reaction_queryset(
            cls._private_reaction_activity_queryset(actor_ids=[viewer.id], viewer=viewer)
            .filter(comment_id__in=comment_ids), viewer=viewer
        ))
        return cls._consolidate_received_reactions(rows)

    @classmethod
    def hydrate_video_reaction_summaries(cls, *, video_comment_ids, viewer) -> list[dict]:
        """Hydrate all selected video groups in one batch query."""
        video_comment_ids = list(set(video_comment_ids))
        if not video_comment_ids:
            return []
        rows = cls.serialize_video_reaction_queryset(
            cls._video_reaction_activity_queryset(viewer=viewer).filter(
                video_comment_id__in=video_comment_ids,
                video_comment__user_id=viewer.id,
            ), viewer=viewer
        )
        return cls._consolidate_received_reactions(rows)

    @classmethod
    def hydrate_rating_ids(cls, ids, *, viewer):
        return cls.rating_activity_queryset(actor_ids=None, viewer=viewer).filter(pk__in=ids)

    @classmethod
    def hydrate_public_comment_ids(cls, ids, *, viewer):
        return cls._public_comment_activity_queryset(actor_ids=None, viewer=viewer).filter(pk__in=ids)

    @classmethod
    def hydrate_public_reaction_ids(cls, ids, *, viewer):
        return cls._public_reaction_activity_queryset(actor_ids=None, viewer=viewer).filter(pk__in=ids)

    @classmethod
    def hydrate_private_message_ids(cls, ids, *, viewer):
        return cls._private_message_activity_queryset(actor_ids=None, viewer=viewer).filter(pk__in=ids)

    @classmethod
    def hydrate_private_reaction_ids(cls, ids, *, viewer):
        return cls._private_reaction_activity_queryset(actor_ids=None, viewer=viewer).filter(pk__in=ids)

    @classmethod
    def hydrate_video_created_ids(cls, ids, *, viewer):
        return cls.video_reaction_created_queryset(actor=viewer, viewer=viewer).filter(pk__in=ids)

    @classmethod
    def hydrate_video_reaction_ids(cls, ids, *, viewer):
        return cls._video_reaction_activity_queryset(viewer=viewer).filter(pk__in=ids)

    @classmethod
    def _serialize_rating_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        return cls.serialize_rating_queryset(
            cls.rating_activity_queryset(actor_ids=actor_ids, viewer=viewer)
        )

    @classmethod
    def rating_activity_queryset(cls, *, actor_ids: list[int], viewer):
        """Return the ordered, fully annotated queryset behind rating activities.

        Keeping this as a queryset lets rating-only endpoints apply pagination in
        SQL before activity dictionaries are materialized.
        """
        movie_display_rating_subquery = cls._movie_display_rating_subquery(movie_id_ref="movie_id")
        viewer_rating_subquery = cls._viewer_movie_rating_subquery(
            viewer=viewer,
            movie_id_ref="movie_id",
        )
        queryset = MovieRating.objects.all()
        if actor_ids is not None:
            queryset = queryset.filter(user_id__in=actor_ids)
        return (
            queryset
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

    @classmethod
    def serialize_rating_queryset(cls, queryset, *, viewer=None) -> list[dict]:
        """Convert an already selected rating queryset/page to feed payloads."""
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
        return cls.serialize_public_comment_queryset(
            cls._public_comment_activity_queryset(actor_ids=actor_ids, viewer=viewer),
            viewer=viewer,
        )

    @classmethod
    def _serialize_private_message_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        return cls.serialize_private_message_queryset(
            cls._private_message_activity_queryset(actor_ids=actor_ids, viewer=viewer),
            viewer=viewer,
        )

    @classmethod
    def _serialize_public_comment_reaction_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        return cls.serialize_public_reaction_queryset(
            cls._public_reaction_activity_queryset(actor_ids=actor_ids, viewer=viewer),
            viewer=viewer,
        )

    @classmethod
    def _serialize_private_comment_reaction_activities(cls, *, actor_ids: list[int], viewer) -> Iterable[dict]:
        return cls.serialize_private_reaction_queryset(
            cls._private_reaction_activity_queryset(actor_ids=actor_ids, viewer=viewer),
            viewer=viewer,
        )

    @classmethod
    def _serialize_video_reaction_activities(cls, *, viewer) -> Iterable[dict]:
        """Build current-state video reaction activity for the authenticated user."""
        return cls.serialize_video_reaction_queryset(
            cls._video_reaction_activity_queryset(viewer=viewer), viewer=viewer
        )

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
    def serialize_video_reaction_created_queryset(cls, queryset, *, viewer=None) -> list[dict]:
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
    def _annotate_movie_feed(cls, queryset, *, viewer, movie_id_ref):
        return queryset.annotate(
            movie_display_rating=Subquery(cls._movie_display_rating_subquery(movie_id_ref=movie_id_ref), output_field=FloatField()),
            viewer_movie_rating=Subquery(cls._viewer_movie_rating_subquery(viewer=viewer, movie_id_ref=movie_id_ref), output_field=IntegerField()),
            movie_following_avg_rating=Subquery(cls._viewer_following_avg_rating_subquery(viewer=viewer, movie_id_ref=movie_id_ref), output_field=FloatField()),
            movie_following_ratings_count=Coalesce(
                Subquery(cls._viewer_following_ratings_count_subquery(viewer=viewer, movie_id_ref=movie_id_ref), output_field=IntegerField()),
                Value(0),
            ),
        )

    @classmethod
    def _public_comment_activity_queryset(cls, *, actor_ids, viewer):
        queryset = Comment.objects.filter(visibility=Comment.VISIBILITY_PUBLIC)
        if actor_ids is not None:
            queryset = queryset.filter(author_id__in=actor_ids)
        queryset = queryset.select_related("author", "author__profile", "movie")
        return cls._annotate_movie_feed(queryset, viewer=viewer, movie_id_ref="movie_id").order_by("-created_at", "-id")

    @classmethod
    def serialize_public_comment_queryset(cls, queryset, *, viewer=None):
        return [{
            "id": f"public_comment:{comment.id}", "activity_type": cls.ACTIVITY_PUBLIC_COMMENT,
            "created_at": comment.created_at, "updated_at": comment.updated_at,
            "activity_at": cls._resolve_activity_at(created_at=comment.created_at, updated_at=comment.updated_at),
            "_sort_entity_id": comment.id, "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PUBLIC_COMMENT],
            "actor": cls._serialize_actor(comment.author),
            "movie": cls._serialize_movie(comment.movie, display_rating=comment.movie_display_rating, my_rating=comment.viewer_movie_rating, following_avg_rating=comment.movie_following_avg_rating, following_ratings_count=comment.movie_following_ratings_count),
            "payload": {"comment_id": comment.id, "content": comment.body},
        } for comment in queryset]

    @classmethod
    def _public_reaction_activity_queryset(cls, *, actor_ids, viewer):
        queryset = CommentReaction.objects.filter(comment__visibility=Comment.VISIBILITY_PUBLIC)
        if actor_ids is not None:
            queryset = queryset.filter(Q(comment__author_id__in=actor_ids) | Q(user_id__in=actor_ids))
        queryset = (queryset.exclude(user_id=F("comment__author_id"))
            .exclude(comment__author__visibility_blocks__blocked_user_id=viewer.id)
            .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
            .select_related("user", "user__profile", "comment", "comment__author", "comment__author__profile", "comment__movie"))
        return cls._annotate_movie_feed(queryset, viewer=viewer, movie_id_ref="comment__movie_id").order_by("-created_at", "-id")

    @classmethod
    def serialize_public_reaction_queryset(cls, queryset, *, viewer=None):
        return [cls._serialize_comment_reaction(reaction, viewer=viewer, private=False) for reaction in queryset]

    @classmethod
    def _private_message_activity_queryset(cls, *, actor_ids, viewer):
        queryset = Comment.objects.filter(visibility=Comment.VISIBILITY_MENTIONED, target_user__isnull=False)
        if actor_ids is not None:
            queryset = queryset.filter(author_id__in=actor_ids)
        queryset = queryset.select_related("author", "author__profile", "movie", "target_user")
        return cls._annotate_movie_feed(queryset, viewer=viewer, movie_id_ref="movie_id").order_by("-created_at", "-id")

    @classmethod
    def serialize_private_message_queryset(cls, queryset, *, viewer=None):
        viewer = viewer or getattr(queryset, "_phase_b_viewer", None)
        result = []
        for comment in queryset:
            if not comment.has_valid_target_mention():
                continue
            result.append({
                "id": f"{cls.ACTIVITY_PRIVATE_MESSAGE}:{comment.id}", "activity_type": cls.ACTIVITY_PRIVATE_MESSAGE,
                "created_at": comment.created_at, "updated_at": comment.updated_at,
                "activity_at": cls._resolve_activity_at(created_at=comment.created_at, updated_at=comment.updated_at),
                "_sort_entity_id": comment.id, "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[cls.ACTIVITY_PRIVATE_MESSAGE],
                "actor": cls._serialize_actor(comment.author),
                "movie": cls._serialize_movie(comment.movie, display_rating=comment.movie_display_rating, my_rating=comment.viewer_movie_rating, following_avg_rating=comment.movie_following_avg_rating, following_ratings_count=comment.movie_following_ratings_count),
                "payload": {"comment_id": comment.id, "content": comment.body, "sender": cls._serialize_compact_user(comment.author), "recipient": cls._serialize_compact_user(comment.target_user), "target_user": cls._serialize_compact_user(comment.target_user), "direction": "sent" if viewer and comment.author_id == viewer.id else "received", "counterpart": cls._serialize_compact_user(comment.target_user if viewer and comment.author_id == viewer.id else comment.author)},
            })
        return result

    @classmethod
    def _private_reaction_activity_queryset(cls, *, actor_ids, viewer):
        queryset = CommentReaction.objects.filter(comment__visibility=Comment.VISIBILITY_MENTIONED)
        if actor_ids is not None:
            queryset = queryset.filter(comment__author_id__in=actor_ids)
        return (queryset.exclude(user_id=F("comment__author_id")).select_related(
            "user", "user__profile", "comment", "comment__author",
            "comment__author__profile", "comment__target_user", "comment__movie"
        ).order_by("-created_at", "-id"))

    @classmethod
    def serialize_private_reaction_queryset(cls, queryset, *, viewer=None):
        return [cls._serialize_comment_reaction(reaction, viewer=viewer, private=True) for reaction in queryset if reaction.comment.has_valid_target_mention()]

    @classmethod
    def _serialize_comment_reaction(cls, reaction, *, viewer, private):
        activity_type = cls.ACTIVITY_PRIVATE_COMMENT_REACTION if private else cls.ACTIVITY_PUBLIC_COMMENT_REACTION
        movie_kwargs = {} if private else {"display_rating": reaction.movie_display_rating, "my_rating": reaction.viewer_movie_rating, "following_avg_rating": reaction.movie_following_avg_rating, "following_ratings_count": reaction.movie_following_ratings_count}
        return {
            "id": f"{activity_type}:{reaction.id}", "activity_type": activity_type,
            "created_at": reaction.created_at, "updated_at": reaction.updated_at,
            "activity_at": cls._resolve_activity_at(created_at=reaction.created_at, updated_at=reaction.updated_at),
            "_sort_entity_id": reaction.id, "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[activity_type],
            "actor": cls._serialize_actor(reaction.user), "_comment_text": reaction.comment.body,
            "movie": cls._serialize_movie(reaction.comment.movie, **movie_kwargs),
            "payload": {"comment_id": reaction.comment_id, "reaction_id": reaction.id, "comment_excerpt": cls._truncate_excerpt(reaction.comment.body), "comment_author": cls._serialize_actor(reaction.comment.author), "reaction_value": reaction.reaction_type, "reaction_type": reaction.reaction_type, "is_given_reaction": viewer and reaction.user_id == viewer.id, "is_received_reaction": viewer and reaction.comment.author_id == viewer.id, "object_created_at": reaction.comment.created_at},
        }

    @classmethod
    def _video_reaction_activity_queryset(cls, *, viewer):
        queryset = (VideoCommentReaction.objects.filter(
            Q(user_id=viewer.id) | Q(video_comment__user_id=viewer.id)
        ).exclude(user_id=F("video_comment__user_id"))
          .exclude(video_comment__user__visibility_blocks__blocked_user_id=viewer.id)
          .exclude(user__visibility_blocks__blocked_user_id=viewer.id)
          .select_related("user", "user__profile", "video_comment", "video_comment__user", "video_comment__user__profile", "video_comment__movie"))
        return cls._annotate_movie_feed(queryset, viewer=viewer, movie_id_ref="video_comment__movie_id").order_by("-updated_at", "-id")

    @classmethod
    def serialize_video_reaction_queryset(cls, queryset, *, viewer=None):
        activities = []
        for reaction in queryset:
            is_received = reaction.video_comment.user_id == viewer.id
            activity_type = cls.ACTIVITY_VIDEO_REACTION_RECEIVED if is_received else cls.ACTIVITY_VIDEO_REACTION_GIVEN
            activities.append({
                "id": f"{activity_type}:{reaction.id}", "activity_type": activity_type,
                "created_at": reaction.created_at, "updated_at": reaction.updated_at,
                "activity_at": cls._resolve_activity_at(created_at=reaction.created_at, updated_at=reaction.updated_at),
                "_sort_entity_id": reaction.id, "_sort_activity_priority": cls._ACTIVITY_SORT_PRIORITY[activity_type],
                "actor": cls._serialize_actor(reaction.user),
                "movie": cls._serialize_movie(reaction.video_comment.movie, display_rating=reaction.movie_display_rating, my_rating=reaction.viewer_movie_rating, following_avg_rating=reaction.movie_following_avg_rating, following_ratings_count=reaction.movie_following_ratings_count),
                "payload": {"reaction_id": reaction.id, "reaction_type": reaction.reaction_type, "reaction_value": reaction.reaction_type, "video_comment_id": reaction.video_comment_id, "video_owner": cls._serialize_actor(reaction.video_comment.user), "is_received_reaction": is_received, "is_given_reaction": not is_received, "object_created_at": reaction.video_comment.created_at, "video_url": reaction.video_comment.video.url if reaction.video_comment.video else None},
            })
        return activities

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
