from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0023_movie_feed_indexes"),
    ]

    operations = [
        migrations.CreateModel(
            name="ProfileFavoriteMovie",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("slot", models.PositiveSmallIntegerField(choices=[(1, "Slot 1"), (2, "Slot 2"), (3, "Slot 3")])),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("movie", models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="profile_favorite_slots", to="core.movie")),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="profile_favorite_movies",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["slot", "id"],
            },
        ),
        migrations.AddConstraint(
            model_name="profilefavoritemovie",
            constraint=models.UniqueConstraint(fields=("user", "slot"), name="unique_profile_favorite_slot_per_user"),
        ),
        migrations.AddConstraint(
            model_name="profilefavoritemovie",
            constraint=models.UniqueConstraint(fields=("user", "movie"), name="unique_profile_favorite_movie_per_user"),
        ),
    ]
