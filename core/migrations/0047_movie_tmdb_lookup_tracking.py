from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0046_movie_tmdb_id"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="tmdb_lookup_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="movie",
            name="tmdb_lookup_error",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="movie",
            name="tmdb_lookup_status",
            field=models.CharField(blank=True, default="", max_length=20),
        ),
    ]
