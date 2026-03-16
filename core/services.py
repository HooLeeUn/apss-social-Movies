from django.db import transaction
from django.utils import timezone

from .models import (
    UserDirectorPreference,
    UserGenrePreference,
    UserTasteProfile,
    UserTypePreference,
)


COUNT_FIELD_BY_SCORE = {score: f"count_{score}" for score in range(1, 11)}


def normalize_preference_value(value):
    if value is None:
        return ""
    return str(value).strip()


def extract_movie_genres(movie):
    raw_genres = normalize_preference_value(movie.genre)
    if not raw_genres:
        return []

    genres = []
    seen = set()
    for genre in raw_genres.split(","):
        cleaned = normalize_preference_value(genre)
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        genres.append(cleaned)
    return genres


def iter_movie_preference_targets(movie):
    for genre in extract_movie_genres(movie):
        yield UserGenrePreference, "genre", genre

    movie_type = normalize_preference_value(movie.type)
    if movie_type:
        yield UserTypePreference, "content_type", movie_type

    director = normalize_preference_value(movie.director)
    if director:
        yield UserDirectorPreference, "director", director


def recalculate_preference_metrics(preference):
    counts = [getattr(preference, f"count_{index}") for index in range(1, 11)]
    total = sum(counts)
    preference.ratings_count = total
    if total == 0:
        preference.score = 0
        return

    weighted_sum = sum(index * count for index, count in enumerate(counts, start=1))
    preference.score = round(weighted_sum / total, 2)


def get_or_create_preference(model_class, user, lookup_field, lookup_value):
    return model_class.objects.get_or_create(user=user, **{lookup_field: lookup_value})[0]


def _apply_score_delta_to_preference(preference, score, delta):
    field = COUNT_FIELD_BY_SCORE[score]
    current = getattr(preference, field)
    updated = current + delta
    if updated < 0:
        updated = 0
    setattr(preference, field, updated)
    recalculate_preference_metrics(preference)


def add_score_to_preference(preference, score):
    _apply_score_delta_to_preference(preference, score, 1)
    preference.save()


def remove_score_from_preference(preference, score):
    _apply_score_delta_to_preference(preference, score, -1)
    if preference.ratings_count == 0:
        preference.delete()
        return
    preference.save()


def increment_preference_score(model_class, user, lookup_field, lookup_value, score):
    preference = get_or_create_preference(model_class, user, lookup_field, lookup_value)
    add_score_to_preference(preference, score)


def decrement_preference_score(model_class, user, lookup_field, lookup_value, score):
    filters = {"user": user, lookup_field: lookup_value}
    preference = model_class.objects.filter(**filters).first()
    if not preference:
        return

    remove_score_from_preference(preference, score)


def _get_or_create_locked_profile(user):
    profile = UserTasteProfile.objects.select_for_update().filter(user=user).first()
    if profile:
        return profile
    return UserTasteProfile.objects.create(user=user)


def _update_profile_counters(user, delta):
    profile = _get_or_create_locked_profile(user)
    profile.ratings_count = max(0, profile.ratings_count + delta)
    profile.last_updated_at = timezone.now()
    profile.save(update_fields=["ratings_count", "last_updated_at"])


@transaction.atomic
def update_user_taste_preferences_for_rating(user, movie, new_score, old_score=None):
    targets = list(iter_movie_preference_targets(movie))

    if old_score is not None:
        for model_class, lookup_field, lookup_value in targets:
            decrement_preference_score(model_class, user, lookup_field, lookup_value, old_score)

    for model_class, lookup_field, lookup_value in targets:
        increment_preference_score(model_class, user, lookup_field, lookup_value, new_score)

    ratings_delta = 0 if old_score is not None else 1
    _update_profile_counters(user, ratings_delta)


@transaction.atomic
def remove_user_taste_preferences_for_rating(user, movie, old_score):
    targets = list(iter_movie_preference_targets(movie))
    for model_class, lookup_field, lookup_value in targets:
        decrement_preference_score(model_class, user, lookup_field, lookup_value, old_score)

    _update_profile_counters(user, -1)
