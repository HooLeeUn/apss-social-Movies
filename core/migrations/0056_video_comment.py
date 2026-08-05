# Generated for VideoComment backend support.

import core.models
import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0055_pendingemailchange"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="VideoComment",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("video", models.FileField(upload_to=core.models.video_comment_upload_to)),
                ("duration_seconds", models.FloatField()),
                ("mime_type", models.CharField(max_length=100)),
                ("file_size", models.PositiveBigIntegerField()),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("movie", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="video_comments", to="core.movie")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="video_comments", to=settings.AUTH_USER_MODEL)),
            ],
            options={"ordering": ["-created_at", "-id"]},
        ),
        migrations.AddIndex(
            model_name="videocomment",
            index=models.Index(fields=["movie", "-created_at", "-id"], name="video_comment_movie_order_idx"),
        ),
        migrations.AddIndex(
            model_name="videocomment",
            index=models.Index(fields=["user", "-created_at"], name="video_comment_user_order_idx"),
        ),
    ]
