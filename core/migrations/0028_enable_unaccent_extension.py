from django.contrib.postgres.operations import UnaccentExtension
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0027_profile_personal_data_fields"),
    ]

    operations = [
        UnaccentExtension(),
    ]
