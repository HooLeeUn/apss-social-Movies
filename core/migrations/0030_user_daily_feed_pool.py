from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0029_comment_is_read"),
    ]

    operations = [
        migrations.CreateModel(
            name="UserDailyFeedPool",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("pool_date", models.DateField(db_index=True)),
                ("expires_at", models.DateTimeField()),
                ("rotation_seed", models.PositiveIntegerField(default=0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "user",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="daily_feed_pools", to=settings.AUTH_USER_MODEL),
                ),
            ],
        ),
        migrations.CreateModel(
            name="UserDailyFeedCandidate",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("base_rank", models.PositiveIntegerField(default=0)),
                ("base_score", models.FloatField(default=0.0)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                (
                    "movie",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="daily_feed_candidates", to="core.movie"),
                ),
                (
                    "pool",
                    models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="candidates", to="core.userdailyfeedpool"),
                ),
            ],
        ),
        migrations.AddConstraint(
            model_name="userdailyfeedpool",
            constraint=models.UniqueConstraint(fields=("user", "pool_date"), name="unique_user_daily_feed_pool"),
        ),
        migrations.AddIndex(
            model_name="userdailyfeedpool",
            index=models.Index(fields=["user", "pool_date"], name="core_userda_user_id_e3c496_idx"),
        ),
        migrations.AddIndex(
            model_name="userdailyfeedpool",
            index=models.Index(fields=["expires_at"], name="core_userda_expires_46e93d_idx"),
        ),
        migrations.AddConstraint(
            model_name="userdailyfeedcandidate",
            constraint=models.UniqueConstraint(fields=("pool", "movie"), name="unique_movie_per_daily_pool"),
        ),
        migrations.AddIndex(
            model_name="userdailyfeedcandidate",
            index=models.Index(fields=["pool", "base_rank"], name="core_userda_pool_id_640629_idx"),
        ),
        migrations.AddIndex(
            model_name="userdailyfeedcandidate",
            index=models.Index(fields=["pool", "-base_score"], name="core_userda_pool_id_5cc5e7_idx"),
        ),
        migrations.AddIndex(
            model_name="userdailyfeedcandidate",
            index=models.Index(fields=["movie"], name="core_userda_movie_i_148044_idx"),
        ),
    ]
