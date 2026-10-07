from django.core.exceptions import PermissionDenied
from django.template.response import TemplateResponse
from .forms import ReportForm
from .permissions import can_view, can_export
from .services import generate
from .exports import csv_response, xlsx_response
from .privacy import MESSAGE


def report_view(request, admin_site):
    if not can_view(request.user):
        raise PermissionDenied
    export = request.GET.get("export")
    if export and (export not in {"csv", "xlsx"} or not can_export(request.user)):
        raise PermissionDenied
    form = ReportForm(request.GET or None)
    result, rows = None, []
    page = 1
    try:
        page = max(1, min(100000, int(request.GET.get("page", "1"))))
    except ValueError:
        pass
    if form.is_bound and form.is_valid():
        result = generate(form.cleaned_data)
        if export:
            if result.blocked:
                raise PermissionDenied(MESSAGE)
            return csv_response(result) if export == "csv" else xlsx_response(result)
        rows = list(result.rows((page-1)*50, 51))
    params = request.GET.copy()
    params.pop("page", None)
    params.pop("export", None)
    base = params.urlencode()
    context = {
        **admin_site.each_context(request), "title": "Generar reportes", "form": form,
        "result": result, "rows": rows[:50], "page": page,
        "next_page": page+1 if len(rows)>50 else None,
        "previous_page": page-1 if page>1 else None,
        "query": base, "can_export": can_export(request.user), "privacy_message": MESSAGE,
    }
    return TemplateResponse(request, "admin/reporting/generate.html", context)
