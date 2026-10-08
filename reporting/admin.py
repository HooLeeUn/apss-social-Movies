from django.contrib import admin
from django.urls import path
from .models import ReportAccess
from .permissions import can_view
from .views import report_view


@admin.register(ReportAccess)
class ReportAdmin(admin.ModelAdmin):
    def get_urls(self):
        return [path("", self.admin_site.admin_view(self.changelist_view), name="reporting_reportaccess_changelist")]

    def changelist_view(self, request, extra_context=None):
        return report_view(request, self.admin_site)

    def has_module_permission(self, request):
        return can_view(request.user)

    def has_view_permission(self, request, obj=None):
        return can_view(request.user)

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
