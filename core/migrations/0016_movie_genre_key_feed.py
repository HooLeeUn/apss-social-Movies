from django.db import migrations, models


def build_genre_key(value):
    if value is None:
        return None

    genres = sorted({part.strip() for part in str(value).split(",") if part and part.strip()})
    if not genres:
        return None
    return "|".join(genres)


def populate_movie_genre_key(apps, schema_editor):
    Movie = apps.get_model("core", "Movie")
    for movie in Movie.objects.all().iterator(chunk_size=1000):
        genre_key = build_genre_key(movie.genre)
        if movie.genre_key == genre_key:
            continue
        movie.genre_key = genre_key
        movie.save(update_fields=["genre_key"])


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0015_user_taste_preferences"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="genre_key",
            field=models.CharField(blank=True, db_index=True, max_length=100, null=True),
        ),
        migrations.RunPython(populate_movie_genre_key, migrations.RunPython.noop),
    ]
