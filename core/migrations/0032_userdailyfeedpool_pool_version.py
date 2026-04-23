from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0031_alter_userdailyfeedpool_rotation_seed"),
    ]

    operations = [
        migrations.AddField(
            model_name="userdailyfeedpool",
            name="pool_version",
            field=models.CharField(db_index=True, default="v1", max_length=64),
        ),
    ]
