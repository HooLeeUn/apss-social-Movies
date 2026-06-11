from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0050_streaming_provider_link_landing_url"),
    ]

    operations = [
        migrations.AddField(
            model_name="streamingproviderlink",
            name="last_verified_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
