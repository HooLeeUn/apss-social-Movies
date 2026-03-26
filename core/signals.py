from django.contrib.auth.models import User
from django.db.models.signals import post_delete, post_save, pre_save
from django.dispatch import receiver
from .models import MovieRating, Profile
from .services import (
    remove_user_preferences_for_movie_rating,
    update_user_preferences_for_movie_rating,
)

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


@receiver(post_delete, sender=MovieRating)
def sync_preferences_after_movie_rating_delete(sender, instance, **kwargs):
    remove_user_preferences_for_movie_rating(
        user=instance.user,
        movie=instance.movie,
        old_score=instance.score,
    )
