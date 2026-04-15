from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0025_profile_visibility_uservisibilityblock"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="movierating",
            index=models.Index(fields=["movie", "user"], name="movierating_movie_user_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=models.Index(fields=["genre_key", "type", "release_year", "id"], name="movie_genre_type_year_id_idx"),
        ),
    ]
