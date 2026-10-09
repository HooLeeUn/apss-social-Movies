from django.core.exceptions import PermissionDenied
from django.template.response import TemplateResponse
from .forms import ReportForm
from .permissions import can_view, can_export, can_view_creator_eligibility
from .services import generate
from .exports import csv_response, xlsx_response
from .privacy import MESSAGE


def report_view(request, admin_site):
    if not can_view(request.user):
        raise PermissionDenied
    if request.GET.get("report") == "creator_eligibility" and not can_view_creator_eligibility(request.user):
        raise PermissionDenied
    export = request.GET.get("export")
    if export and (export not in {"csv", "xlsx"} or not can_export(request.user)):
        raise PermissionDenied
    form = ReportForm(request.GET or None, user=request.user)
    result, rows = None, []
    page = 1
    try:
        page = max(1, min(100000, int(request.GET.get("page", "1"))))
    except ValueError:
        pass
    if form.is_bound and form.is_valid():
        result = generate(form.cleaned_data, user=request.user)
        if export:
            if result.blocked:
                raise PermissionDenied(MESSAGE)
            return csv_response(result) if export == "csv" else xlsx_response(result)
        if result.report == "creator_eligibility":
            page = min(page, max(1, (result.row_count + 49) // 50))
            rows = list(result.rows((page-1)*50, 50))
        else:
            # Preserve the 50-entity page size; periods stay visible on every page.
            page = min(page, max(1, (result.column_count + 49) // 50))
            result = result.for_columns((page-1)*50, 50)
            rows = list(result.rows())
    params = request.GET.copy()
    params.pop("page", None)
    params.pop("export", None)
    base = params.urlencode()
    context = {
        **admin_site.each_context(request), "title": "Generar reportes", "form": form,
        "result": result, "rows": rows, "page": page,
        "next_page": page+1 if result and page*50 < (result.row_count if result.report == "creator_eligibility" else result.column_count) else None,
        "previous_page": page-1 if page>1 else None,
        "query": base, "can_export": can_export(request.user), "privacy_message": MESSAGE,
        "column_start": (page-1)*50+1, "column_end": (page-1)*50+len(result.columns) if result else 0,
    }
    return TemplateResponse(request, "admin/reporting/generate.html", context)
