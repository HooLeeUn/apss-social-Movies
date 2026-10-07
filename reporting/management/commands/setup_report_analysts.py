from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group, Permission

class Command(BaseCommand):
    help = "Create/update the report analysts group; does not assign users or modify existing permissions."
    def handle(self, **options):
        group, _ = Group.objects.get_or_create(name="Analistas de reportes")
        permissions = Permission.objects.filter(content_type__app_label="reporting", codename__in=["can_view_reports", "can_export_reports"])
        if permissions.count() != 2:
            from django.core.management.base import CommandError
            raise CommandError("Ejecuta migrate antes de configurar el grupo.")
        group.permissions.add(*permissions)
        self.stdout.write("Grupo Analistas de reportes configurado.")
