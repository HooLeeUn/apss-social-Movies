from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0024_profilefavoritemovie"),
    ]

    operations = [
        migrations.AddField(
            model_name="profile",
            name="visibility",
            field=models.CharField(
                choices=[("public", "Public"), ("private", "Private")],
                default="public",
                max_length=10,
            ),
        ),
        migrations.CreateModel(
            name="UserVisibilityBlock",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "blocked_user",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="blocked_by_visibility_users",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "owner",
                    models.ForeignKey(
                        on_delete=models.deletion.CASCADE,
                        related_name="visibility_blocks",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="uservisibilityblock",
            constraint=models.UniqueConstraint(fields=("owner", "blocked_user"), name="unique_user_visibility_block"),
        ),
        migrations.AddConstraint(
            model_name="uservisibilityblock",
            constraint=models.CheckConstraint(
                condition=~models.Q(owner=models.F("blocked_user")),
                name="user_visibility_block_cannot_block_self",
            ),
        ),
    ]
