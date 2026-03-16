from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0014_movie_imdb_id"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="UserTasteProfile",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("ratings_count", models.PositiveIntegerField(default=0)),
                ("last_updated_at", models.DateTimeField(auto_now=True)),
                (
                    "user",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="taste_profile",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="UserGenrePreference",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("count_1", models.PositiveIntegerField(default=0)),
                ("count_2", models.PositiveIntegerField(default=0)),
                ("count_3", models.PositiveIntegerField(default=0)),
                ("count_4", models.PositiveIntegerField(default=0)),
                ("count_5", models.PositiveIntegerField(default=0)),
                ("count_6", models.PositiveIntegerField(default=0)),
                ("count_7", models.PositiveIntegerField(default=0)),
                ("count_8", models.PositiveIntegerField(default=0)),
                ("count_9", models.PositiveIntegerField(default=0)),
                ("count_10", models.PositiveIntegerField(default=0)),
                ("ratings_count", models.PositiveIntegerField(default=0)),
                ("score", models.DecimalField(decimal_places=2, default=0, max_digits=4)),
                ("genre", models.CharField(max_length=100)),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="genre_preferences",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(fields=("user", "genre"), name="unique_user_genre_preference"),
                ],
            },
        ),
        migrations.CreateModel(
            name="UserTypePreference",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("count_1", models.PositiveIntegerField(default=0)),
                ("count_2", models.PositiveIntegerField(default=0)),
                ("count_3", models.PositiveIntegerField(default=0)),
                ("count_4", models.PositiveIntegerField(default=0)),
                ("count_5", models.PositiveIntegerField(default=0)),
                ("count_6", models.PositiveIntegerField(default=0)),
                ("count_7", models.PositiveIntegerField(default=0)),
                ("count_8", models.PositiveIntegerField(default=0)),
                ("count_9", models.PositiveIntegerField(default=0)),
                ("count_10", models.PositiveIntegerField(default=0)),
                ("ratings_count", models.PositiveIntegerField(default=0)),
                ("score", models.DecimalField(decimal_places=2, default=0, max_digits=4)),
                ("content_type", models.CharField(choices=[("movie", "Movie"), ("series", "Series")], max_length=10)),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="type_preferences",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(fields=("user", "content_type"), name="unique_user_type_preference"),
                ],
            },
        ),
        migrations.CreateModel(
            name="UserDirectorPreference",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("count_1", models.PositiveIntegerField(default=0)),
                ("count_2", models.PositiveIntegerField(default=0)),
                ("count_3", models.PositiveIntegerField(default=0)),
                ("count_4", models.PositiveIntegerField(default=0)),
                ("count_5", models.PositiveIntegerField(default=0)),
                ("count_6", models.PositiveIntegerField(default=0)),
                ("count_7", models.PositiveIntegerField(default=0)),
                ("count_8", models.PositiveIntegerField(default=0)),
                ("count_9", models.PositiveIntegerField(default=0)),
                ("count_10", models.PositiveIntegerField(default=0)),
                ("ratings_count", models.PositiveIntegerField(default=0)),
                ("score", models.DecimalField(decimal_places=2, default=0, max_digits=4)),
                ("director", models.CharField(max_length=255)),
                (
                    "user",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="director_preferences",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "constraints": [
                    models.UniqueConstraint(fields=("user", "director"), name="unique_user_director_preference"),
                ],
            },
        ),
    ]
