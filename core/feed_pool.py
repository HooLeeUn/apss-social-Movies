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
    POOL_SIZE_DEFAULT = 900
    POOL_SIZE_MIN = 450
    RETAIN_RATIO = 0.35

    SOURCE_TARGETS = {
        "strong_genre": 380,
        "type": 220,
        "director": 180,
        "recent": 180,
        "exploration": 140,
    }

    ROTATION_BAND = 0.10

    def __init__(self, user, *, pool_size: int | None = None):
        self.user = user
        resolved_size = pool_size or self.POOL_SIZE_DEFAULT
        self.pool_size = max(self.POOL_SIZE_MIN, resolved_size)

    def get_daily_pool(self) -> UserDailyFeedPool:
        today = timezone.localdate()
        pool = UserDailyFeedPool.objects.filter(user_id=self.user.id, pool_date=today).first()
        if pool:
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

    def _rated_ids_subquery(self):
        return MovieRating.objects.filter(user_id=self.user.id).values("movie_id")

    def _top_preferences(self):
        top_genres = list(
            UserGenrePreference.objects.filter(user_id=self.user.id, ratings_count__gt=0)
            .order_by("-score", "-ratings_count")
            .values_list("genre", flat=True)[:4]
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
        candidate_ids = []
        seen = set()

        def add_ids(queryset, limit):
            for movie_id in queryset.values_list("id", flat=True)[:limit]:
                if movie_id in seen:
                    continue
                seen.add(movie_id)
                candidate_ids.append(movie_id)
                if len(candidate_ids) >= self.pool_size:
                    break

        if has_preferences and top_genres:
            genre_filter = Q()
            for genre in top_genres:
                genre_filter |= Q(genre_key=genre) | Q(genre_key__startswith=f"{genre}|") | Q(genre_key__endswith=f"|{genre}") | Q(genre_key__contains=f"|{genre}|")
            add_ids(base_qs.filter(genre_filter).order_by("-release_year", "-external_votes", "-id"), self.SOURCE_TARGETS["strong_genre"])

        if has_preferences and top_type:
            add_ids(base_qs.filter(type=top_type).order_by("-release_year", "-external_votes", "-id"), self.SOURCE_TARGETS["type"])

        if has_preferences and top_directors:
            add_ids(base_qs.filter(director__in=top_directors).order_by("-release_year", "-external_votes", "-id"), self.SOURCE_TARGETS["director"])

        current_year = today.year
        add_ids(
            base_qs.filter(release_year__gte=current_year - 3).order_by("-release_year", "-external_votes", "-id"),
            self.SOURCE_TARGETS["recent"],
        )

        exploration_bucket = (self._compute_daily_seed(today) % 23) + 3
        add_ids(
            base_qs.annotate(exploration_mod=(F("id") % Value(exploration_bucket))).filter(exploration_mod=0).order_by("-external_votes", "-release_year", "-id"),
            self.SOURCE_TARGETS["exploration"],
        )

        retained_ids = self._retained_previous_ids(today=today, excluded=set(candidate_ids))
        for movie_id in retained_ids:
            if len(candidate_ids) >= self.pool_size:
                break
            if movie_id in seen:
                continue
            seen.add(movie_id)
            candidate_ids.append(movie_id)

        if len(candidate_ids) < self.pool_size:
            add_ids(base_qs.order_by("-external_votes", "-release_year", "-id"), self.pool_size)

        return candidate_ids[: self.pool_size]

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
