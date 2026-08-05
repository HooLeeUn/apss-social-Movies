from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0056_video_comment"),
    ]

    operations = [
        migrations.AlterField(
            model_name="usernotification",
            name="type",
            field=models.CharField(
                choices=[
                    ("private_message", "Private message"),
                    ("public_comment_reaction", "Public comment reaction"),
                    ("private_comment_reaction", "Private comment reaction"),
                    ("friend_request_received", "Friend request received"),
                ],
                max_length=40,
            ),
        ),
    ]
