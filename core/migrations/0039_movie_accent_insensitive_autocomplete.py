import re
import unicodedata

from django.contrib.postgres.indexes import GinIndex, OpClass
from django.db import migrations, models


BATCH_SIZE = 2000
SEARCH_FIELDS = (
    "title_english_search",
    "title_spanish_search",
    "director_search",
    "cast_members_search",
    "genre_search",
    "type_search",
)


def normalize_movie_search_text(value):
    if value is None:
        return ""

    without_accents = "".join(
        char
        for char in unicodedata.normalize("NFKD", str(value))
        if not unicodedata.combining(char)
    )
    lowered = without_accents.lower()
    cleaned = "".join(char if char.isalnum() else " " for char in lowered)
    return re.sub(r"\s+", " ", cleaned).strip()


def backfill_movie_search_fields(apps, schema_editor):
    Movie = apps.get_model("core", "Movie")
    batch = []

    queryset = Movie.objects.order_by("pk").only(
        "pk",
        "title_english",
        "title_spanish",
        "director",
        "cast_members",
        "genre",
        "type",
        *SEARCH_FIELDS,
    )

    for movie in queryset.iterator(chunk_size=BATCH_SIZE):
        movie.title_english_search = normalize_movie_search_text(movie.title_english)
        movie.title_spanish_search = normalize_movie_search_text(movie.title_spanish)
        movie.director_search = normalize_movie_search_text(movie.director)
        movie.cast_members_search = normalize_movie_search_text(movie.cast_members)
        movie.genre_search = normalize_movie_search_text(movie.genre)
        movie.type_search = normalize_movie_search_text(movie.type)
        batch.append(movie)

        if len(batch) >= BATCH_SIZE:
            Movie.objects.bulk_update(batch, SEARCH_FIELDS, batch_size=BATCH_SIZE)
            batch.clear()

    if batch:
        Movie.objects.bulk_update(batch, SEARCH_FIELDS, batch_size=BATCH_SIZE)


class Migration(migrations.Migration):

    atomic = False

    dependencies = [
        ("core", "0038_movie_autocomplete_trigram_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="title_english_search",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="movie",
            name="title_spanish_search",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="movie",
            name="director_search",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="movie",
            name="cast_members_search",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="movie",
            name="genre_search",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="movie",
            name="type_search",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.RunPython(backfill_movie_search_fields, migrations.RunPython.noop),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("title_english_search", name="gin_trgm_ops"), name="movie_title_en_search_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("title_spanish_search", name="gin_trgm_ops"), name="movie_title_es_search_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("director_search", name="gin_trgm_ops"), name="movie_director_search_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("cast_members_search", name="gin_trgm_ops"), name="movie_cast_search_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("genre_search", name="gin_trgm_ops"), name="movie_genre_search_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("type_search", name="gin_trgm_ops"), name="movie_type_search_trgm_idx"),
        ),
    ]
