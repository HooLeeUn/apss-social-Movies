from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("core", "0065_movie_public_feed_priority_index"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="movie",
            index=models.Index(
                fields=["-external_votes", "-release_year", "-id"],
                name="movie_votes_year_id_idx",
            ),
        ),
        AddIndexConcurrently(
            model_name="movie",
            index=models.Index(
                fields=["-release_year", "-external_votes", "-id"],
                name="movie_year_votes_id_idx",
            ),
        ),
    ]
