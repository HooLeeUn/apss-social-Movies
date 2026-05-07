from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0041_remove_unused_movie_search_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="profile",
            name="friend_requests_restricted",
            field=models.BooleanField(default=False),
        ),
    ]
