from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0028_enable_unaccent_extension"),
    ]

    operations = [
        migrations.AddField(
            model_name="comment",
            name="is_read",
            field=models.BooleanField(default=False),
        ),
    ]
