from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0034_usernotification"),
    ]

    operations = [
        migrations.CreateModel(
            name="MovieListItem",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("movie", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="saved_by_users", to="core.movie")),
                ("user", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="movie_list_items", to=settings.AUTH_USER_MODEL)),
            ],
            options={
                "ordering": ["-created_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="movielistitem",
            constraint=models.UniqueConstraint(fields=("user", "movie"), name="unique_movie_list_item_per_user"),
        ),
    ]
