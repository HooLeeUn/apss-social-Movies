from datetime import date
from django import forms
from django.utils import timezone
from core.models import Profile
from .catalog import REPORTS, USER_REPORTS, CONTENT_REPORTS, DIRECT, SOCIAL, AGE_LABELS, MONTH_NAMES
from .periods import REPORT_TZ, month_period


class ReportForm(forms.Form):
    report = forms.ChoiceField(label="Tipo de reporte", choices=[("Usuarios", list(USER_REPORTS.items())), ("Contenido", list(CONTENT_REPORTS.items())), ("Actividad directa", [("direct", REPORTS["direct"]), *DIRECT.items()]), ("Actividad indirecta / social", [("social", REPORTS["social"]), *SOCIAL.items()])])
    countries = forms.MultipleChoiceField(label="País (vacío = Todos)", choices=Profile.StreamingCountry.choices, required=False)
    ages = forms.MultipleChoiceField(label="Rango de edad (vacío = Todos)", choices=list(AGE_LABELS.items()), required=False)
    genders = forms.MultipleChoiceField(label="Identidad de género (vacío = Todos)", choices=Profile.GenderIdentity.choices, required=False)
    content_type = forms.ChoiceField(label="Tipo de contenido", choices=[("", "Todos"), ("movie", "Películas"), ("series", "Series")], required=False)
    total = forms.BooleanField(label="Acumulado total", required=False)
    custom = forms.BooleanField(label="Personalizado", required=False)
    month = forms.TypedChoiceField(label="Mes", choices=[(i+1, name) for i, name in enumerate(MONTH_NAMES)], coerce=int, required=False)
    year = forms.IntegerField(label="Año", min_value=1900, max_value=9998, required=False)
    since = forms.DateField(label="Desde", required=False, widget=forms.DateInput(attrs={"type": "date"}))
    until = forms.DateField(label="Hasta", required=False, widget=forms.DateInput(attrs={"type": "date"}))
    months = forms.MultipleChoiceField(label="Meses a comparar", required=False, choices=[])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        today = timezone.localdate(timezone=REPORT_TZ)
        self.fields["month"].initial = today.month
        self.fields["year"].initial = today.year
        self.fields["months"].initial = [today.strftime("%Y-%m")]
        # No database scan of the catalog; offer every month since first real registration.
        from django.contrib.auth import get_user_model
        from django.db.models import Min
        first = get_user_model().objects.aggregate(first=Min("date_joined"))["first"]
        first_year = min(today.year, first.year) if first else today.year
        self.fields["months"].choices = [(f"{y:04}-{m:02}", f"{name} {y}") for y in range(first_year, today.year+1) for m, name in enumerate(MONTH_NAMES, 1)]

    def clean(self):
        d = super().clean()
        report = d.get("report")
        if report in USER_REPORTS:
            d["content_type"] = ""
            d["months"] = []
            if not d.get("total"):
                if d.get("custom"):
                    if not d.get("since") or not d.get("until"):
                        raise forms.ValidationError("Indica Desde y Hasta.")
                    if d["since"] > d["until"] or d["until"].year == 9999:
                        raise forms.ValidationError("Rango de fechas inválido.")
                elif not d.get("month") or not d.get("year"):
                    raise forms.ValidationError("Selecciona mes y año.")
        else:
            if not d.get("months"):
                raise forms.ValidationError("Selecciona al menos un mes completo.")
            if len(d["months"]) > 24:
                raise forms.ValidationError("Compara como máximo 24 meses por reporte.")
            d.update(total=False, custom=False, since=None, until=None)
            if report == "follows":
                d["content_type"] = ""
            if report == "social" and d.get("content_type"):
                raise forms.ValidationError("El consolidado social incluye follows; selecciona Todos en tipo de contenido.")
        return d
