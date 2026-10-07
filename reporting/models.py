from core.models import Profile

class ReportAccess(Profile):
    class Meta:
        proxy = True
        verbose_name = "Generar reportes"
        verbose_name_plural = "Generar reportes"
        default_permissions = ()
        permissions = [("can_view_reports", "Puede visualizar reportes"), ("can_export_reports", "Puede exportar reportes")]
