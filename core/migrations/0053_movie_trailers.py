# Generated manually because Django is unavailable in the execution environment.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0052_expand_profile_streaming_country_choices"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="trailer_es_key",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="movie",
            name="trailer_en_key",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AddField(
            model_name="movie",
            name="trailer_checked_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
