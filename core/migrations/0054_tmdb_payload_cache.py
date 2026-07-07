# Generated manually because the execution environment does not include Django.

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0053_movie_trailers"),
    ]

    operations = [
        migrations.CreateModel(
            name="TMDbPayloadCache",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("tmdb_id", models.PositiveIntegerField(db_index=True)),
                ("content_type", models.CharField(choices=[("movie", "Movie"), ("tv", "TV")], max_length=10)),
                (
                    "payload_type",
                    models.CharField(
                        choices=[
                            ("credits", "Credits"),
                            ("tv_details", "TV details"),
                            ("tv_season_credits", "TV season credits"),
                            ("watch_providers", "Watch providers"),
                        ],
                        max_length=32,
                    ),
                ),
                ("country_code", models.CharField(blank=True, default="", max_length=2)),
                ("season_number", models.PositiveSmallIntegerField(default=0)),
                ("payload", models.JSONField(default=dict)),
                ("source", models.CharField(default="tmdb", max_length=32)),
                ("fetched_at", models.DateTimeField(auto_now=True)),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "movie",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="tmdb_payload_caches",
                        to="core.movie",
                    ),
                ),
            ],
            options={
                "ordering": ["-updated_at", "-id"],
            },
        ),
        migrations.AddConstraint(
            model_name="tmdbpayloadcache",
            constraint=models.UniqueConstraint(
                fields=["tmdb_id", "content_type", "payload_type", "country_code", "season_number"],
                name="unique_tmdb_payload_cache_key",
            ),
        ),
        migrations.AddIndex(
            model_name="tmdbpayloadcache",
            index=models.Index(
                fields=["tmdb_id", "content_type", "payload_type", "country_code"],
                name="tmdb_payload_lookup_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="tmdbpayloadcache",
            index=models.Index(fields=["movie", "payload_type"], name="tmdb_payload_movie_idx"),
        ),
    ]
