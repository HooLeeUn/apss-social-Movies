from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0026_feed_personalization_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="profile",
            name="birth_date",
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="profile",
            name="birth_date_locked",
            field=models.BooleanField(default=False),
        ),
        migrations.AddField(
            model_name="profile",
            name="birth_date_visible",
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name="profile",
            name="gender_identity",
            field=models.CharField(
                blank=True,
                choices=[
                    ("male", "Hombre"),
                    ("female", "Mujer"),
                    ("non_binary", "No binario"),
                    ("prefer_not_to_say", "Prefiero no decirlo"),
                ],
                max_length=20,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="profile",
            name="gender_identity_visible",
            field=models.BooleanField(default=True),
        ),
    ]
