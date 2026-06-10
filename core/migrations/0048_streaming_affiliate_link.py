from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0047_movie_tmdb_lookup_tracking"),
    ]

    operations = [
        migrations.CreateModel(
            name="StreamingAffiliateLink",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("provider_id", models.PositiveIntegerField(db_index=True)),
                ("provider_name", models.CharField(max_length=255)),
                ("country_code", models.CharField(db_index=True, max_length=2)),
                ("affiliate_url", models.URLField(max_length=1000)),
                ("is_active", models.BooleanField(default=True)),
                (
                    "monetization_type",
                    models.CharField(
                        choices=[
                            ("none", "None"),
                            ("affiliate", "Affiliate"),
                            ("cpa", "CPA"),
                            ("cpl", "CPL"),
                            ("custom", "Custom"),
                        ],
                        default="affiliate",
                        max_length=20,
                    ),
                ),
                ("notes", models.TextField(blank=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["provider_name", "country_code", "provider_id"],
            },
        ),
        migrations.AddIndex(
            model_name="streamingaffiliatelink",
            index=models.Index(
                fields=["provider_id", "country_code", "is_active"],
                name="saff_prov_ctry_idx",
            ),
        ),
    ]
