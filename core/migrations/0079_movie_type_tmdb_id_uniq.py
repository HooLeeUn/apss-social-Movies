from django.db import migrations, models
from django.db.models import Count


def verify_clean_catalog(apps, schema_editor):
    connection = schema_editor.connection
    if connection.vendor != "postgresql":
        raise RuntimeError("Movie identity migration requires PostgreSQL.")
    with connection.cursor() as cursor:
        cursor.execute("SET LOCAL lock_timeout = '5s'")
        cursor.execute('LOCK TABLE "core_movie" IN ACCESS EXCLUSIVE MODE')
    movie = apps.get_model("core", "Movie")
    duplicates = (movie.objects.using(connection.alias).filter(type__isnull=False, tmdb_id__isnull=False)
                  .order_by().values("type", "tmdb_id").annotate(rows=Count("pk")).filter(rows__gt=1))
    if duplicates.exists():
        raise RuntimeError("Movie identity migration blocked: duplicate non-null keys remain. Run the reviewed cleanup first.")


class Migration(migrations.Migration):
    dependencies = [("core", "0078_comment_individual_dispatch")]
    operations = [
        migrations.RunPython(verify_clean_catalog, migrations.RunPython.noop),
        migrations.AddConstraint(
            model_name="movie",
            constraint=models.UniqueConstraint(fields=["type", "tmdb_id"], name="movie_type_tmdb_id_uniq"),
        ),
    ]
