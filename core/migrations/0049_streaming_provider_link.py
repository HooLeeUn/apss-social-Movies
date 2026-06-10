from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0048_streaming_affiliate_link"),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name="streamingaffiliatelink",
            name="saff_prov_ctry_idx",
        ),
        migrations.RenameModel(
            old_name="StreamingAffiliateLink",
            new_name="StreamingProviderLink",
        ),
        migrations.AddField(
            model_name="streamingproviderlink",
            name="movie",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.CASCADE,
                related_name="streaming_provider_links",
                to="core.movie",
            ),
        ),
        migrations.AddField(
            model_name="streamingproviderlink",
            name="tmdb_id",
            field=models.PositiveIntegerField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name="streamingproviderlink",
            name="imdb_id",
            field=models.CharField(blank=True, db_index=True, max_length=20, null=True),
        ),
        migrations.AddField(
            model_name="streamingproviderlink",
            name="content_type",
            field=models.CharField(
                choices=[("movie", "Movie"), ("tv", "TV")],
                default="movie",
                max_length=10,
            ),
        ),
        migrations.AddField(
            model_name="streamingproviderlink",
            name="direct_url",
            field=models.URLField(blank=True, default="", max_length=1000),
        ),
        migrations.AlterField(
            model_name="streamingproviderlink",
            name="affiliate_url",
            field=models.URLField(blank=True, default="", max_length=1000),
        ),
        migrations.AddIndex(
            model_name="streamingproviderlink",
            index=models.Index(
                fields=["provider_id", "country_code", "is_active"],
                name="spl_prov_ctry_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="streamingproviderlink",
            index=models.Index(
                fields=["provider_id", "country_code", "tmdb_id", "is_active"],
                name="spl_prov_ctry_tmdb_idx",
            ),
        ),
    ]
