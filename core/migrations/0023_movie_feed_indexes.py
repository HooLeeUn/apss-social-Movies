from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0022_weekly_recommendations"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="movie",
            index=models.Index(fields=["type", "release_year", "id"], name="movie_type_year_id_idx"),
        ),
    ]
