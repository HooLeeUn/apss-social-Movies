from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0044_pendinguserregistration"),
    ]

    operations = [
        migrations.AddField(
            model_name="profile",
            name="streaming_country",
            field=models.CharField(
                choices=[("CO", "Colombia"), ("US", "Estados Unidos")],
                default="CO",
                max_length=2,
            ),
        ),
    ]
