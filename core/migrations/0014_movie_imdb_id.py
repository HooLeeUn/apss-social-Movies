from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0013_movierating'),
    ]

    operations = [
        migrations.AddField(
            model_name='movie',
            name='imdb_id',
            field=models.CharField(blank=True, db_index=True, max_length=20, null=True),
        ),
    ]
