# Generated manually because Django is not installed in this execution environment.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0049_streaming_provider_link"),
    ]

    operations = [
        migrations.AddField(
            model_name="streamingproviderlink",
            name="landing_url",
            field=models.URLField(blank=True, default="", max_length=1000),
        ),
    ]
