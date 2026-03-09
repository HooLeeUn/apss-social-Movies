from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0010_movie'),
    ]

    operations = [
        migrations.AddField(
            model_name='movie',
            name='genre',
            field=models.CharField(blank=True, max_length=100, null=True),
        ),
    ]
