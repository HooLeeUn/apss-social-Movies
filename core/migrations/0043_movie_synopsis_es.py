from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0042_profile_friend_requests_restricted"),
    ]

    operations = [
        migrations.AddField(
            model_name="movie",
            name="synopsis_es",
            field=models.TextField(blank=True, null=True),
        ),
    ]
