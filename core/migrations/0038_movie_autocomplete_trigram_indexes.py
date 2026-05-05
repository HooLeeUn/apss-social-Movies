from django.contrib.postgres.indexes import GinIndex, OpClass
from django.contrib.postgres.operations import TrigramExtension
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0037_movie_autocomplete_indexes"),
    ]

    operations = [
        TrigramExtension(),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("title_english", name="gin_trgm_ops"), name="movie_title_en_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("title_spanish", name="gin_trgm_ops"), name="movie_title_es_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("director", name="gin_trgm_ops"), name="movie_director_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("genre", name="gin_trgm_ops"), name="movie_genre_trgm_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=GinIndex(OpClass("cast_members", name="gin_trgm_ops"), name="movie_cast_trgm_idx"),
        ),
    ]
