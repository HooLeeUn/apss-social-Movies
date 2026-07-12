# Generated manually because Django is unavailable in the execution environment.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0053_movie_trailers"),
    ]

    operations = [
        migrations.AddField(
            model_name="appbranding",
            name="poster_placeholder",
            field=models.ImageField(
                blank=True,
                null=True,
                upload_to="branding/poster_placeholders/",
                verbose_name="Imagen para poster no disponible",
            ),
        ),
    ]
