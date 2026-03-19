from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0016_movie_genre_key_feed"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="external_votes",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="movie",
            name="synopsis",
            field=models.TextField(blank=True, default=""),
        ),
    ]
