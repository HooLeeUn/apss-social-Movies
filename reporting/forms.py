from datetime import date
from django import forms
from django.utils import timezone
from core.models import Profile
from .catalog import REPORTS, USER_REPORTS, CONTENT_REPORTS, DIRECT, SOCIAL, INTERNAL_REPORTS, DEFAULT_CREATOR_MIN_FOLLOWERS, AGE_LABELS, MONTH_NAMES
from .periods import REPORT_TZ
from .permissions import can_view_creator_eligibility


class ReportForm(forms.Form):
    report = forms.ChoiceField(label="Tipo de reporte", choices=[("Usuarios", list(USER_REPORTS.items())), ("Contenido", list(CONTENT_REPORTS.items())), ("Actividad directa", [("direct", REPORTS["direct"]), *DIRECT.items()]), ("Actividad indirecta / social", [("social", REPORTS["social"]), *SOCIAL.items()])])
    countries = forms.MultipleChoiceField(label="País (vacío = Todos)", choices=Profile.StreamingCountry.choices, required=False)
    ages = forms.MultipleChoiceField(label="Rango de edad (vacío = Todos)", choices=list(AGE_LABELS.items()), required=False)
    genders = forms.MultipleChoiceField(label="Identidad de género (vacío = Todos)", choices=Profile.GenderIdentity.choices, required=False)
    content_type = forms.ChoiceField(label="Tipo de contenido", choices=[("", "Todos"), ("movie", "Películas"), ("series", "Series")], required=False)
    total = forms.BooleanField(label="Acumulado total", required=False)
    custom = forms.BooleanField(label="Personalizado", required=False, help_text="Un periodo por día; Desde y Hasta incluidos, en America/Bogota.")
    since = forms.DateField(label="Desde", required=False, widget=forms.DateInput(attrs={"type": "date"}))
    until = forms.DateField(label="Hasta", required=False, widget=forms.DateInput(attrs={"type": "date"}))
    months = forms.TypedMultipleChoiceField(label="Meses", coerce=int, required=False, choices=[(i, name) for i, name in enumerate(MONTH_NAMES, 1)], help_text="Selecciona uno o varios meses para comparar.")
    years = forms.TypedMultipleChoiceField(label="Años", coerce=int, required=False, choices=[], help_text="Se combina cada mes con cada año seleccionado (máximo 24 periodos).")

    def __init__(self, *args, user=None, **kwargs):
        super().__init__(*args, **kwargs)
        if can_view_creator_eligibility(user):
            self.fields["report"].choices = [*self.fields["report"].choices, ("Uso interno", list(INTERNAL_REPORTS.items()))]
            self.fields["min_followers"] = forms.IntegerField(label="Mínimo de seguidores", min_value=1, initial=DEFAULT_CREATOR_MIN_FOLLOWERS, required=False,
                help_text="Clasificación analítica sobre seguidores actuales; no activa beneficios.")
        today = timezone.localdate(timezone=REPORT_TZ)
        self.fields["months"].initial = [today.month]
        self.fields["years"].initial = [today.year]
        # No catalog scan; years start with the first registration in report time.
        from django.contrib.auth import get_user_model
        from django.db.models import Min
        first = get_user_model().objects.aggregate(first=Min("date_joined"))["first"]
        first_year = min(today.year, timezone.localdate(first, timezone=REPORT_TZ).year) if first else today.year
        self.fields["years"].choices = [(y, str(y)) for y in range(first_year, today.year+1)]

    def clean(self):
        d = super().clean()
        report = d.get("report")
        if report == "creator_eligibility":
            if any(d.get(field) for field in ("countries", "ages", "genders", "content_type")):
                raise forms.ValidationError("Elegibilidad de creadores solo utiliza periodo y mínimo de seguidores.")
            d["min_followers"] = d.get("min_followers") or DEFAULT_CREATOR_MIN_FOLLOWERS
        else:
            d.pop("min_followers", None)
        if d.get("total") and d.get("custom"):
            raise forms.ValidationError("Acumulado total y Personalizado son mutuamente excluyentes.")
        special = d.get("total") or d.get("custom")
        if d.get("total") and report not in USER_REPORTS:
            raise forms.ValidationError("Acumulado total solo está disponible para reportes de usuarios.")
        if special and (d.get("months") or d.get("years")):
            raise forms.ValidationError("No combines acumulado o personalizado con meses y años.")
        if not d.get("custom") and (d.get("since") or d.get("until")):
            raise forms.ValidationError("Desde y Hasta requieren el modo Personalizado.")
        if d.get("custom"):
            if not d.get("since") or not d.get("until"):
                raise forms.ValidationError("Indica Desde y Hasta.")
            if d["since"] > d["until"] or d["until"] == date.max:
                raise forms.ValidationError("Rango de fechas inválido.")
        if report in USER_REPORTS:
            d["content_type"] = ""
        else:
            if report == "follows":
                d["content_type"] = ""
            if report == "social" and d.get("content_type"):
                raise forms.ValidationError("El consolidado social incluye follows; selecciona Todos en tipo de contenido.")
        if not special:
            if not d.get("months") or not d.get("years"):
                raise forms.ValidationError("Selecciona al menos un mes y un año.")
            if len(set(d["months"])) * len(set(d["years"])) > 24:
                raise forms.ValidationError("Compara como máximo 24 meses por reporte; reduce meses o años.")
            today = timezone.localdate(timezone=REPORT_TZ)
            if any((y, m) > (today.year, today.month) for y in d["years"] for m in d["months"]):
                raise forms.ValidationError("La selección incluye meses futuros; selecciona meses que ya hayan comenzado.")
        return d
