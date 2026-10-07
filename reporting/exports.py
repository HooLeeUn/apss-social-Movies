import csv
import io
from tempfile import SpooledTemporaryFile
from django.http import FileResponse, StreamingHttpResponse
from openpyxl import Workbook


def safe(value):
    if isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@", "\t", "\r", "\n")):
        return "'" + value
    return value


def export_rows(result):
    yield from result.metadata
    yield []
    yield result.headers
    for row in result.rows():
        yield [safe(value) for value in row]


def csv_response(result):
    def chunks():
        yield '\ufeff'
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        for row in export_rows(result):
            writer.writerow([safe(value) for value in row])
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)
    response = StreamingHttpResponse(chunks(), content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = 'attachment; filename="reccool-report.csv"'
    return response


def xlsx_response(result):
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet("Reporte")
    for row in export_rows(result):
        sheet.append([safe(value) for value in row])
    output = SpooledTemporaryFile(max_size=8*1024*1024, mode="w+b")
    workbook.save(output)
    output.seek(0)
    return FileResponse(output, as_attachment=True, filename="reccool-report.xlsx", content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
