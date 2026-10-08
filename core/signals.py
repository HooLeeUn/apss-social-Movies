from django.contrib.auth.models import User
from django.db.models.signals import post_delete, post_save, pre_delete, pre_save
from django.dispatch import receiver
from django.utils import timezone
from .models import Movie, MovieRating, Profile, MovieRecommendationItem, MovieRecommendationHistory
from .services import (
    remove_user_preferences_for_movie_rating,
    update_user_preferences_for_movie_rating,
)
from .feed_pool import remove_movie_from_active_pool

@receiver(post_save, sender=User)
def create_profile(sender, instance, created, **kwargs):
    if created:
        Profile.objects.create(user=instance)


@receiver(pre_save, sender=MovieRating)
def capture_old_movie_rating_score(sender, instance, **kwargs):
    if not instance.pk:
        instance._old_score = None
        return

    instance._old_score = (
        MovieRating.objects.filter(pk=instance.pk)
        .values_list("score", flat=True)
        .first()
    )


@receiver(post_save, sender=MovieRating)
def sync_preferences_after_movie_rating_save(sender, instance, **kwargs):
    update_user_preferences_for_movie_rating(
        user=instance.user,
        movie=instance.movie,
        new_score=instance.score,
        old_score=getattr(instance, "_old_score", None),
    )
    remove_movie_from_active_pool(user_id=instance.user_id, movie_id=instance.movie_id)


@receiver(post_delete, sender=MovieRating)
def sync_preferences_after_movie_rating_delete(sender, instance, origin, **kwargs):
    # Django's deletion collector removes related objects before it removes the
    # User that started the cascade. Recalculating here would therefore recreate
    # the UserTasteProfile that the collector has just deleted, only for its FK
    # to become orphaned when the collector subsequently deletes the User.
    if isinstance(origin, User) or getattr(origin, "model", None) is User:
        return

    remove_user_preferences_for_movie_rating(
        user=instance.user,
        movie=instance.movie,
        old_score=instance.score,
    )
    remove_movie_from_active_pool(user_id=instance.user_id, movie_id=instance.movie_id)


@receiver(post_save, sender=MovieRecommendationItem)
def open_recommendation_interval(sender, instance, created, **kwargs):
    if created:
        MovieRecommendationHistory.objects.get_or_create(
            user_id=instance.user_id, movie_id=instance.movie_id, ended_at=None,
            defaults={"started_at": instance.created_at},
        )


@receiver(pre_delete, sender=MovieRecommendationItem)
def close_recommendation_interval(sender, instance, origin, **kwargs):
    # Parent deletion intentionally cascades; do not recreate user-owned rows.
    if isinstance(origin, (User, Movie)) or getattr(origin, "model", None) in (User, Movie):
        return
    # Recover an existing item's real start even if removed before the backfill.
    interval, _ = MovieRecommendationHistory.objects.get_or_create(
        user_id=instance.user_id, movie_id=instance.movie_id, ended_at=None,
        defaults={"started_at": instance.created_at},
    )
    MovieRecommendationHistory.objects.filter(pk=interval.pk).update(ended_at=timezone.now())
