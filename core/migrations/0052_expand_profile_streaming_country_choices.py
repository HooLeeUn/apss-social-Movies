# Generated manually because Django is unavailable in the execution environment.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0051_streaming_provider_link_last_verified_at"),
    ]

    operations = [
        migrations.AlterField(
            model_name="profile",
            name="streaming_country",
            field=models.CharField(
                choices=[("AR", "Argentina"), ("BO", "Bolivia"), ("BZ", "Belice"), ("CA", "Canadá"), ("CL", "Chile"), ("CO", "Colombia"), ("CR", "Costa Rica"), ("CU", "Cuba"), ("DO", "República Dominicana"), ("EC", "Ecuador"), ("ES", "España"), ("GT", "Guatemala"), ("HN", "Honduras"), ("MX", "México"), ("NI", "Nicaragua"), ("PA", "Panamá"), ("PE", "Perú"), ("PR", "Puerto Rico"), ("PY", "Paraguay"), ("SV", "El Salvador"), ("UK", "Reino Unido"), ("US", "Estados Unidos"), ("UY", "Uruguay"), ("VE", "Venezuela")],
                default="CO",
                max_length=2,
            ),
        ),
    ]
