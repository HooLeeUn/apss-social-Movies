from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0036_movierecommendationitem"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="movie",
            index=models.Index(fields=["title_english", "release_year", "id"], name="movie_title_en_auto_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=models.Index(fields=["title_spanish", "release_year", "id"], name="movie_title_es_auto_idx"),
        ),
        migrations.AddIndex(
            model_name="movie",
            index=models.Index(fields=["release_year", "id"], name="movie_year_auto_idx"),
        ),
    ]
