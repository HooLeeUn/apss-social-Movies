from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0011_movie_genre'),
    ]

    operations = [
        migrations.AddField(
            model_name='movie',
            name='type',
            field=models.CharField(blank=True, choices=[('movie', 'Movie'), ('series', 'Series')], max_length=10, null=True),
        ),
    ]
