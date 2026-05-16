# Generated manually because Django is not installed in this execution environment.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0043_movie_synopsis_es"),
    ]

    operations = [
        migrations.CreateModel(
            name="PendingUserRegistration",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("username", models.CharField(max_length=150)),
                ("email", models.EmailField(max_length=254)),
                ("first_name", models.CharField(max_length=150)),
                ("last_name", models.CharField(max_length=150)),
                ("birth_date", models.DateField()),
                ("password", models.CharField(max_length=128)),
                ("token", models.CharField(db_index=True, max_length=64, unique=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("expires_at", models.DateTimeField(db_index=True)),
                ("confirmed_at", models.DateTimeField(blank=True, null=True)),
            ],
            options={
                "ordering": ["-created_at", "-id"],
                "indexes": [
                    models.Index(fields=["username", "expires_at"], name="core_pendin_usernam_7af75a_idx"),
                    models.Index(fields=["email", "expires_at"], name="core_pendin_email_002753_idx"),
                ],
            },
        ),
    ]
