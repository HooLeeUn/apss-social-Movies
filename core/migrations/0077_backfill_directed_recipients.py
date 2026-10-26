from django.db import migrations


def backfill(apps, schema_editor):
    Comment = apps.get_model("core", "Comment")
    Recipient = apps.get_model("core", "DirectedCommentRecipient")
    alias = schema_editor.connection.alias
    batch = []
    for comment in Comment.objects.using(alias).filter(
        visibility="mentioned", target_user__isnull=False,
    ).iterator(chunk_size=2000):
        batch.append(Recipient(comment_id=comment.pk, user_id=comment.target_user_id, is_read=comment.is_read))
        if len(batch) == 2000:
            Recipient.objects.using(alias).bulk_create(batch, ignore_conflicts=True)
            batch = []
    if batch:
        Recipient.objects.using(alias).bulk_create(batch, ignore_conflicts=True)


class Migration(migrations.Migration):
    dependencies = [("core", "0076_directedcommentrecipient")]
    operations = [migrations.RunPython(backfill, migrations.RunPython.noop)]
