from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0017_movie_external_votes_synopsis"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="profile",
            name="is_public",
            field=models.BooleanField(default=True),
        ),
        migrations.CreateModel(
            name="Friendship",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("status", models.CharField(choices=[("pending", "Pending"), ("accepted", "Accepted"), ("rejected", "Rejected"), ("cancelled", "Cancelled")], default="pending", max_length=10)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("requester", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="sent_friendship_requests", to=settings.AUTH_USER_MODEL)),
                ("user1", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="friendships_as_user1", to=settings.AUTH_USER_MODEL)),
                ("user2", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="friendships_as_user2", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "ordering": ["-updated_at", "-created_at"],
            },
        ),
        migrations.AddConstraint(
            model_name="follow",
            constraint=models.CheckConstraint(condition=~models.Q(follower=models.F("following")), name="follow_cannot_follow_self"),
        ),
        migrations.AddConstraint(
            model_name="friendship",
            constraint=models.UniqueConstraint(fields=("user1", "user2"), name="unique_friendship_pair"),
        ),
        migrations.AddConstraint(
            model_name="friendship",
            constraint=models.CheckConstraint(condition=~models.Q(user1=models.F("user2")), name="friendship_users_must_differ"),
        ),
    ]
