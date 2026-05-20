from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0045_profile_streaming_country"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="tmdb_id",
            field=models.PositiveIntegerField(blank=True, db_index=True, null=True),
        ),
    ]
