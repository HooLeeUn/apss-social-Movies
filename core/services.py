from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

from django.db import transaction
from django.utils import timezone

from core.models import (
    UserDirectorPreference,
    UserGenrePreference,
    UserTasteProfile,
    UserTypePreference,
    build_genre_key,
)


COUNT_FIELDS = [f"count_{score}" for score in range(1, 11)]


def normalize_text(value):
    if value is None:
        return ""
    return str(value).strip()


def recalculate_preference_metrics(preference):
    counts = [getattr(preference, field_name) for field_name in COUNT_FIELDS]
    ratings_count = sum(counts)
    preference.ratings_count = ratings_count

    if ratings_count == 0:
        preference.score = Decimal("0")
        return

    weighted_sum = sum((index + 1) * count for index, count in enumerate(counts))
    raw_score = Decimal(weighted_sum) / Decimal(ratings_count)
    preference.score = raw_score.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def add_score_to_preference(preference, score):
    field_name = f"count_{score}"
    setattr(preference, field_name, getattr(preference, field_name) + 1)
    recalculate_preference_metrics(preference)
    preference.save()


def remove_score_from_preference(preference, score):
    field_name = f"count_{score}"
    current_count = getattr(preference, field_name)
    if current_count == 0:
        return

    setattr(preference, field_name, current_count - 1)
    recalculate_preference_metrics(preference)

    if preference.ratings_count == 0:
        preference.delete()
        return

    preference.save()


def get_or_create_preference(model_class, user, lookup_field, lookup_value):
    preference, _ = model_class.objects.get_or_create(
        user=user,
        **{lookup_field: lookup_value},
    )
    return preference


def get_movie_genres(movie):
    raw_genre = normalize_text(movie.genre)
    if not raw_genre:
        return []

    unique_genres = []
    seen = set()
    for genre_part in raw_genre.split(","):
        genre = normalize_text(genre_part)
        if not genre or genre in seen:
            continue
        seen.add(genre)
        unique_genres.append(genre)
    return unique_genres


def get_movie_preference_targets(movie):
    targets = []
    seen_genre_preferences = set()

    genre_key = build_genre_key(movie.genre)
    if genre_key:
        seen_genre_preferences.add(genre_key)
        targets.append((UserGenrePreference, "genre", genre_key))

    for genre in get_movie_genres(movie):
        if genre in seen_genre_preferences:
            continue
        seen_genre_preferences.add(genre)
        targets.append((UserGenrePreference, "genre", genre))

    content_type = normalize_text(movie.type)
    if content_type:
        targets.append((UserTypePreference, "content_type", content_type))

    director = normalize_text(movie.director)
    if director:
        targets.append((UserDirectorPreference, "director", director))

    return targets


def _apply_score_for_movie_preferences(user, movie, score, *, add):
    for model_class, lookup_field, lookup_value in get_movie_preference_targets(movie):
        filters = {"user": user, lookup_field: lookup_value}

        if add:
            preference = get_or_create_preference(model_class, user, lookup_field, lookup_value)
            add_score_to_preference(preference, score)
            continue

        preference = model_class.objects.filter(**filters).first()
        if preference:
            remove_score_from_preference(preference, score)


@transaction.atomic
def update_user_preferences_for_movie_rating(user, movie, new_score, old_score=None):
    if old_score is not None:
        _apply_score_for_movie_preferences(user, movie, old_score, add=False)

    _apply_score_for_movie_preferences(user, movie, new_score, add=True)

    profile, _ = UserTasteProfile.objects.get_or_create(user=user)
    if old_score is None:
        profile.ratings_count += 1

    profile.last_updated_at = timezone.now()
    profile.save(update_fields=["ratings_count", "last_updated_at"])


@transaction.atomic
def remove_user_preferences_for_movie_rating(user, movie, old_score):
    _apply_score_for_movie_preferences(user, movie, old_score, add=False)

    profile, _ = UserTasteProfile.objects.get_or_create(user=user)
    profile.ratings_count = max(profile.ratings_count - 1, 0)
    profile.last_updated_at = timezone.now()
    profile.save(update_fields=["ratings_count", "last_updated_at"])

