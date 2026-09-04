from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("core", "0064_contactrecipient_contactmessage"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX CONCURRENTLY IF NOT EXISTS movie_public_feed_priority_idx "
                "ON core_movie (external_rating DESC NULLS LAST, "
                "NULLIF(image, '') DESC NULLS LAST, id DESC)"
            ),
            reverse_sql="DROP INDEX CONCURRENTLY IF EXISTS movie_public_feed_priority_idx",
        ),
    ]
