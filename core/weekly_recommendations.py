from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction
from django.db.models import Count, F, FloatField, Q, Sum, Value
from django.db.models.functions import Cast, Coalesce
from django.utils import timezone

from core.models import Movie, WeeklyRecommendationItem, WeeklyRecommendationSnapshot

WEEKLY_RECOMMENDATIONS_LIMIT = 8
WEEKLY_SCORE_PRECISION = Decimal("0.001")


@dataclass(frozen=True)
class ClosedWeekWindow:
    start_at: datetime
    end_at: datetime

    @property
    def start_date(self):
        return self.start_at.date()

    @property
    def end_date(self):
        return self.end_at.date()


def get_previous_closed_week_window(reference_datetime=None):
    current_timezone = timezone.get_current_timezone()
    reference_datetime = reference_datetime or timezone.now()

    if timezone.is_naive(reference_datetime):
        reference_datetime = timezone.make_aware(reference_datetime, current_timezone)
    else:
        reference_datetime = timezone.localtime(reference_datetime, current_timezone)

    current_week_start_date = reference_datetime.date() - timedelta(
        days=reference_datetime.weekday()
    )
    previous_week_start_date = current_week_start_date - timedelta(days=7)

    start_at = timezone.make_aware(
        datetime.combine(previous_week_start_date, time.min), current_timezone
    )
    end_at = timezone.make_aware(
        datetime.combine(current_week_start_date, time.min), current_timezone
    )
    return ClosedWeekWindow(start_at=start_at, end_at=end_at)


def get_weekly_recommendation_candidates(window):
    ratings_filter = Q(
        movie_ratings__updated_at__gte=window.start_at,
        movie_ratings__updated_at__lt=window.end_at,
    )
    return (
        Movie.objects.filter(ratings_filter)
        .annotate(
            week_ratings_count=Count(
                "movie_ratings", filter=ratings_filter, distinct=True
            ),
            week_ratings_sum=Coalesce(
                Sum("movie_ratings__score", filter=ratings_filter),
                Value(0.0),
                output_field=FloatField(),
            ),
            _external_rating_for_weekly_score=Coalesce(
                Cast("external_rating", FloatField()),
                Value(0.0),
                output_field=FloatField(),
            ),
        )
        .filter(week_ratings_count__gt=0)
        .annotate(
            weekly_score=(
                F("_external_rating_for_weekly_score")
                + Cast("week_ratings_sum", FloatField())
            )
            / (Value(1.0) + Cast("week_ratings_count", FloatField()))
        )
        .select_related("author", "author__profile")
        .order_by("-weekly_score", "-week_ratings_count", "id")
    )


def select_weekly_recommendation_movies(window, limit=WEEKLY_RECOMMENDATIONS_LIMIT):
    selected_movies = []
    seen_genres = set()

    for movie in get_weekly_recommendation_candidates(window):
        genre_key = movie.genre if movie.genre is not None else "__none__"
        if genre_key in seen_genres:
            continue

        seen_genres.add(genre_key)
        selected_movies.append(movie)
        if len(selected_movies) == limit:
            break

    return selected_movies


@transaction.atomic
def refresh_weekly_recommendation_snapshot(
    *, reference_datetime=None, limit=WEEKLY_RECOMMENDATIONS_LIMIT
):
    window = get_previous_closed_week_window(reference_datetime=reference_datetime)
    selected_movies = select_weekly_recommendation_movies(window=window, limit=limit)

    snapshot, _ = WeeklyRecommendationSnapshot.objects.update_or_create(
        week_start=window.start_date,
        week_end=window.end_date,
        defaults={"items_count": len(selected_movies)},
    )
    snapshot.items.all().delete()

    items = []
    for position, movie in enumerate(selected_movies, start=1):
        items.append(
            WeeklyRecommendationItem(
                snapshot=snapshot,
                movie=movie,
                position=position,
                genre=movie.genre,
                week_ratings_count=movie.week_ratings_count,
                week_ratings_sum=Decimal(str(movie.week_ratings_sum)).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                ),
                weekly_score=Decimal(str(movie.weekly_score)).quantize(
                    WEEKLY_SCORE_PRECISION, rounding=ROUND_HALF_UP
                ),
            )
        )

    if items:
        WeeklyRecommendationItem.objects.bulk_create(items)

    return snapshot
