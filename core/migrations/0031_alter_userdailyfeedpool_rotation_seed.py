from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0030_user_daily_feed_pool"),
    ]

    operations = [
        migrations.AlterField(
            model_name="userdailyfeedpool",
            name="rotation_seed",
            field=models.PositiveBigIntegerField(default=0),
        ),
    ]
