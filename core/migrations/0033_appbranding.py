from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0032_userdailyfeedpool_pool_version"),
    ]

    operations = [
        migrations.CreateModel(
            name="AppBranding",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("app_name", models.CharField(default="MiAppSocialMovies", max_length=120)),
                ("default_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("login_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("signup_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("feed_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("movie_detail_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("profile_feed_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("visited_profile_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("personal_data_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("privacy_security_logo", models.ImageField(blank=True, null=True, upload_to="branding/")),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("is_active", models.BooleanField(default=True)),
            ],
            options={
                "verbose_name": "App Branding",
                "verbose_name_plural": "App Branding",
                "ordering": ["-is_active", "-updated_at", "-id"],
                "constraints": [
                    models.UniqueConstraint(
                        condition=models.Q(("is_active", True)),
                        fields=("is_active",),
                        name="unique_active_app_branding",
                    )
                ],
            },
        ),
    ]
