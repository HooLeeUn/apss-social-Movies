from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import timedelta

from django.db import transaction
from django.db.models import F, FloatField, IntegerField, OuterRef, Q, Subquery, Value
from django.db.models.functions import Cast, Coalesce
from django.utils import timezone

from core.models import (
    Movie,
    MovieRating,
    UserDailyFeedCandidate,
    UserDailyFeedPool,
    UserDirectorPreference,
    UserGenrePreference,
    UserTasteProfile,
    UserTypePreference,
)


@dataclass
class FeedPoolPayload:
    pool: UserDailyFeedPool
    ordered_ids: list[int]


class DailyFeedPoolService:
    POOL_SIZE_DEFAULT = 10000
    POOL_SIZE_MIN = 5000
    RETAIN_RATIO = 0.45
    POOL_ALGO_VERSION = "v2_genre_depth_20260423"
    STRONG_GENRE_MAX_GENRES = 6

    SOURCE_TARGETS = {
        "strong_genre": 5600,
        "type": 1400,
        "director": 1100,
        "recent": 1600,
        "exploration": 1700,
        "retained": 1300,
    }

    ROTATION_BAND = 0.25

    def __init__(self, user, *, pool_size: int | None = None):
        self.user = user
        resolved_size = pool_size or self.POOL_SIZE_DEFAULT
        self.pool_size = max(self.POOL_SIZE_MIN, resolved_size)

    def get_daily_pool(self) -> UserDailyFeedPool:
        today = timezone.localdate()
        pool = UserDailyFeedPool.objects.filter(user_id=self.user.id, pool_date=today).first()
        if pool and pool.pool_version == self._current_pool_version():
            return pool
        return self._rebuild_pool(today=today)

    def get_rotated_ids(self, *, rotation_bucket: int) -> FeedPoolPayload:
        pool = self.get_daily_pool()
        candidates = list(
            UserDailyFeedCandidate.objects.filter(pool_id=pool.id)
            .order_by("base_rank", "-base_score", "id")
            .values_list("movie_id", "base_score")
        )

        if not candidates:
            return FeedPoolPayload(pool=pool, ordered_ids=[])

        rotated = list(candidates)
        idx = 0
        while idx < len(rotated):
            start_idx = idx
            anchor_score = float(rotated[start_idx][1] or 0.0)
            idx += 1
            while idx < len(rotated):
                next_score = float(rotated[idx][1] or 0.0)
                if (anchor_score - next_score) > self.ROTATION_BAND:
                    break
                idx += 1

            if idx - start_idx > 1:
                seed = (self.user.id * 1_000_003) + (pool.rotation_seed * 97) + (rotation_bucket * 389) + start_idx
                rng = random.Random(seed)
                chunk = rotated[start_idx:idx]
                rng.shuffle(chunk)
                rotated[start_idx:idx] = chunk

        return FeedPoolPayload(pool=pool, ordered_ids=[movie_id for movie_id, _ in rotated])

    def _rebuild_pool(self, *, today):
        with transaction.atomic():
            UserDailyFeedPool.objects.filter(user_id=self.user.id, pool_date=today).delete()
            pool = UserDailyFeedPool.objects.create(
                user_id=self.user.id,
                pool_date=today,
                pool_version=self._current_pool_version(),
                expires_at=timezone.now() + timedelta(days=1),
                rotation_seed=self._compute_daily_seed(today),
            )

            candidate_ids = self._build_candidate_ids(today)
            if not candidate_ids:
                return pool

            score_by_movie = self._score_candidates(candidate_ids)
            sorted_ids = sorted(
                candidate_ids,
                key=lambda movie_id: (
                    -(score_by_movie.get(movie_id) or 0.0),
                    movie_id,
                ),
            )
            UserDailyFeedCandidate.objects.bulk_create(
                [
                    UserDailyFeedCandidate(
                        pool=pool,
                        movie_id=movie_id,
                        base_rank=position,
                        base_score=score_by_movie.get(movie_id) or 0.0,
                    )
                    for position, movie_id in enumerate(sorted_ids)
                ],
                batch_size=1000,
            )
            return pool

    def _compute_daily_seed(self, day):
        digest = hashlib.sha256(f"{self.user.id}:{day.isoformat()}".encode("utf-8")).hexdigest()
        return int(digest[:8], 16)

    def _current_pool_version(self):
        digest = hashlib.sha256(
            (
                f"{self.POOL_ALGO_VERSION}|"
                f"{self.pool_size}|"
                f"{self.RETAIN_RATIO}|"
                f"{sorted(self.SOURCE_TARGETS.items())}|"
                f"{self.STRONG_GENRE_MAX_GENRES}"
            ).encode("utf-8")
        ).hexdigest()
        return f"{self.POOL_ALGO_VERSION}-{digest[:10]}"

    def _rated_ids_subquery(self):
        return MovieRating.objects.filter(user_id=self.user.id).values("movie_id")

    def _top_preferences(self):
        top_genres = list(
            UserGenrePreference.objects.filter(user_id=self.user.id, ratings_count__gt=0)
            .order_by("-score", "-ratings_count")
            .values_list("genre", flat=True)[: self.STRONG_GENRE_MAX_GENRES]
        )
        top_type = (
            UserTypePreference.objects.filter(user_id=self.user.id, ratings_count__gt=0)
            .order_by("-score", "-ratings_count")
            .values_list("content_type", flat=True)
            .first()
        )
        top_directors = list(
            UserDirectorPreference.objects.filter(user_id=self.user.id, ratings_count__gte=2, score__gte=6)
            .order_by("-score", "-ratings_count")
            .values_list("director", flat=True)[:10]
        )
        return top_genres, top_type, top_directors

    def _build_candidate_ids(self, today):
        rated_ids = self._rated_ids_subquery()
        top_genres, top_type, top_directors = self._top_preferences()
        has_preferences = UserTasteProfile.objects.filter(user_id=self.user.id, ratings_count__gt=0).exists()

        base_qs = Movie.objects.exclude(id__in=Subquery(rated_ids))
        source_buckets = []

        def fetch_ids(queryset, limit):
            if limit <= 0:
                return []
            return list(queryset.values_list("id", flat=True)[:limit])

        if has_preferences and top_genres:
            per_genre_target = max(260, self.SOURCE_TARGETS["strong_genre"] // max(1, len(top_genres)))
            pair_target = max(120, per_genre_target // 2)
            broad_target = max(400, self.SOURCE_TARGETS["strong_genre"] // 3)

            for genre in top_genres:
                source_buckets.append(
                    fetch_ids(
                        base_qs.filter(self._genre_lookup_query(genre)).order_by("-release_year", "-external_votes", "-id"),
                        per_genre_target,
                    )
                )

            for idx, left in enumerate(top_genres):
                for right in top_genres[idx + 1 :]:
                    source_buckets.append(
                        fetch_ids(
                            base_qs.filter(self._genre_lookup_query(left)).filter(self._genre_lookup_query(right)).order_by("-release_year", "-external_votes", "-id"),
                            pair_target,
                        )
                    )

            source_buckets.append(
                fetch_ids(
                    base_qs.filter(self._genres_or_query(top_genres)).order_by("-release_year", "-external_votes", "-id"),
                    broad_target,
                )
            )

        if has_preferences and top_type:
            source_buckets.append(fetch_ids(base_qs.filter(type=top_type).order_by("-release_year", "-external_votes", "-id"), self.SOURCE_TARGETS["type"]))

        if has_preferences and top_directors:
            source_buckets.append(
                fetch_ids(base_qs.filter(director__in=top_directors).order_by("-release_year", "-external_votes", "-id"), self.SOURCE_TARGETS["director"])
            )

        current_year = today.year
        recent_qs = base_qs.filter(release_year__gte=current_year - 3).order_by("-release_year", "-external_votes", "-id")
        if has_preferences and top_genres:
            source_buckets.append(
                fetch_ids(
                    recent_qs.filter(self._genres_or_query(top_genres)),
                    int(self.SOURCE_TARGETS["recent"] * 0.65),
                )
            )
        source_buckets.append(fetch_ids(recent_qs, self.SOURCE_TARGETS["recent"]))

        exploration_bucket = (self._compute_daily_seed(today) % 23) + 3
        exploration_qs = base_qs.annotate(exploration_mod=(F("id") % Value(exploration_bucket))).filter(exploration_mod=0).order_by(
            "-external_votes",
            "-release_year",
            "-id",
        )
        if has_preferences and top_genres:
            source_buckets.append(
                fetch_ids(
                    exploration_qs.filter(self._genres_or_query(top_genres)),
                    int(self.SOURCE_TARGETS["exploration"] * 0.6),
                )
            )
        source_buckets.append(fetch_ids(exploration_qs, self.SOURCE_TARGETS["exploration"]))

        source_buckets.append(
            self._retained_previous_ids(today=today, excluded=set())[: self.SOURCE_TARGETS["retained"]]
        )
        source_buckets.append(fetch_ids(base_qs.order_by("-external_votes", "-release_year", "-id"), self.pool_size))

        return self._merge_source_buckets(source_buckets)

    def _merge_source_buckets(self, source_buckets):
        candidate_ids = []
        seen = set()
        pointers = [0] * len(source_buckets)
        source_exhausted = [False] * len(source_buckets)

        while len(candidate_ids) < self.pool_size:
            added_in_round = False
            for index, bucket in enumerate(source_buckets):
                if source_exhausted[index]:
                    continue
                pointer = pointers[index]
                while pointer < len(bucket) and bucket[pointer] in seen:
                    pointer += 1
                pointers[index] = pointer
                if pointer >= len(bucket):
                    source_exhausted[index] = True
                    continue
                movie_id = bucket[pointer]
                pointers[index] += 1
                seen.add(movie_id)
                candidate_ids.append(movie_id)
                added_in_round = True
                if len(candidate_ids) >= self.pool_size:
                    break
            if not added_in_round:
                break
        return candidate_ids

    def _genre_lookup_query(self, genre):
        return Q(genre_key=genre) | Q(genre_key__startswith=f"{genre}|") | Q(genre_key__endswith=f"|{genre}") | Q(genre_key__contains=f"|{genre}|")

    def _genres_or_query(self, genres):
        query = Q()
        for genre in genres:
            query |= self._genre_lookup_query(genre)
        return query

    def _retained_previous_ids(self, *, today, excluded):
        yesterday = today - timedelta(days=1)
        prev_pool = UserDailyFeedPool.objects.filter(user_id=self.user.id, pool_date=yesterday).first()
        if not prev_pool:
            return []

        retain_limit = max(30, int(self.pool_size * self.RETAIN_RATIO))
        rated_ids = self._rated_ids_subquery()
        prev_ids = list(
            UserDailyFeedCandidate.objects.filter(pool_id=prev_pool.id)
            .exclude(movie_id__in=Subquery(rated_ids))
            .exclude(movie_id__in=excluded)
            .order_by("base_rank")
            .values_list("movie_id", flat=True)[:retain_limit]
        )
        return prev_ids

    def _score_candidates(self, candidate_ids):
        queryset = Movie.objects.filter(id__in=candidate_ids)
        has_preferences = UserTasteProfile.objects.filter(user_id=self.user.id, ratings_count__gt=0).exists()
        scored = queryset.feed_for_user(self.user, include_recommendation_score=has_preferences, include_my_rating=False)
        return {movie_id: float(score or 0.0) for movie_id, score in scored.values_list("id", "recommendation_score")}


def remove_movie_from_active_pool(*, user_id: int, movie_id: int):
    today = timezone.localdate()
    active_pool = UserDailyFeedPool.objects.filter(user_id=user_id, pool_date=today).first()
    if not active_pool:
        return
    UserDailyFeedCandidate.objects.filter(pool_id=active_pool.id, movie_id=movie_id).delete()
