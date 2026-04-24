from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0033_appbranding"),
    ]

    operations = [
        migrations.CreateModel(
            name="UserNotification",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "type",
                    models.CharField(
                        choices=[
                            ("private_message", "Private message"),
                            ("public_comment_reaction", "Public comment reaction"),
                            ("private_comment_reaction", "Private comment reaction"),
                        ],
                        max_length=40,
                    ),
                ),
                (
                    "target_tab",
                    models.CharField(
                        choices=[("activity", "Activity"), ("private_inbox", "Private inbox")],
                        max_length=20,
                    ),
                ),
                (
                    "reaction_type",
                    models.CharField(
                        blank=True,
                        choices=[("like", "Like"), ("dislike", "Dislike")],
                        max_length=10,
                        null=True,
                    ),
                ),
                ("is_read", models.BooleanField(default=False)),
                ("read_at", models.DateTimeField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "actor",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=models.CASCADE,
                        related_name="notifications_triggered",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    "comment",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=models.CASCADE,
                        related_name="notifications",
                        to="core.comment",
                    ),
                ),
                (
                    "movie",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=models.CASCADE,
                        related_name="notifications",
                        to="core.movie",
                    ),
                ),
                (
                    "recipient",
                    models.ForeignKey(
                        on_delete=models.CASCADE,
                        related_name="notifications_received",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="usernotification",
            constraint=models.UniqueConstraint(
                fields=("recipient", "actor", "comment", "type"),
                name="unique_user_notification_per_actor_comment_type",
            ),
        ),
    ]
