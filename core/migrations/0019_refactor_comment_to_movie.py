from django.db import migrations, models
import django.db.models.deletion


def populate_comment_updated_at(apps, schema_editor):
    Comment = apps.get_model("core", "Comment")
    Comment.objects.filter(updated_at__isnull=True).update(updated_at=models.F("created_at"))


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0018_profile_is_public_friendship"),
    ]

    operations = [
        migrations.RenameField(
            model_name="comment",
            old_name="post",
            new_name="movie",
        ),
        migrations.AlterField(
            model_name="comment",
            name="movie",
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="comments", to="core.movie"),
        ),
        migrations.AddField(
            model_name="comment",
            name="updated_at",
            field=models.DateTimeField(auto_now=True, null=True),
        ),
        migrations.RunPython(populate_comment_updated_at, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="comment",
            name="updated_at",
            field=models.DateTimeField(auto_now=True),
        ),
    ]
