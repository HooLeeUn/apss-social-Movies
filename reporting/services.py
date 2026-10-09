"""One immutable presentation result shared by HTML, CSV and XLSX.

Detailed content aggregation stays in PostgreSQL. Only aggregated entity/period
cells are materialized to pivot; activity records never enter Python.
"""
from dataclasses import dataclass, replace
from collections import Counter
import json
from django.db import connection
from django.db.models import Count, Case, When, Value, CharField, F
from django.utils import timezone
from .catalog import REPORTS, USER_REPORTS, CONTENT_REPORTS, DIRECT, SOCIAL, AGE_RANGES, AGE_LABELS
from .periods import periods, REPORT_TZ
from .privacy import MESSAGE, publishable, segmented, variation, protected_sum
from .queries import source, users, recommendations

WARNINGS = [
    "Usuarios registrados: User con Profile, incluidos inactivos; fecha de alta date_joined. Pendientes excluidos.",
    "País e identidad corresponden al perfil actual; edad histórica calculada en fecha de referencia.",
    "Ratings: score vigente, atribuido a updated_at. No reconstruye scores anteriores ni ratings eliminados.",
    "Reacciones: estado vigente por updated_at; reacciones eliminadas no se reconstruyen.",
    "Follows: relaciones existentes por created_at; unfollows no tienen histórico.",
    "Recomendadas: intervalos solapados con el periodo (mes o día), usuarios únicos por producción. Edad al primer instante del solapamiento de cada intervalo.",
    "Histórico previo al despliegue parcial: solo se recuperan recomendaciones todavía activas, desde su created_at. No se inventan retiros.",
    "Contenido oculto que todavía existe cuenta; comentarios dirigidos excluidos.",
]

@dataclass(frozen=True)
class Column:
    label: str
    cells: list  # (publishable count or None, average) for each requested period


@dataclass(frozen=True)
class Header:
    label: str
    subheaders: tuple = ()

    @property
    def span(self):
        return len(self.subheaders) or 1


def public(value):
    return MESSAGE if value is None else value


@dataclass
class Result:
    title: str
    metadata: list
    population: int | None
    blocked: bool
    methodology: list
    show_population: bool
    report: str
    period_labels: list
    period_heading: str
    columns: list
    period_totals: list
    column_count: int
    include_total: bool

    @property
    def averages(self):
        return self.report in {"productions", "genres", "combinations"}

    @property
    def compares(self):
        return self.report in USER_REPORTS or self.averages

    @property
    def has_total_column(self):
        return self.compares and self.report != "users"

    @property
    def header_groups(self):
        groups = [Header(self.period_heading)]
        for column in self.columns:
            groups.append(Header(column.label, ("No calif.", "Promedio Calif.") if self.averages else ()))
        if self.has_total_column:
            groups.append(Header("TOTAL"))
        if self.compares:
            groups.extend([Header("Dif. Nº calificaciones" if self.averages else "Diferencia"),
                           Header("Var. Nº calificaciones %" if self.averages else "Variación %")])
        return groups

    @property
    def headers(self):
        return [f"{group.label} - {sub}" if sub else group.label
                for group in self.header_groups for sub in (group.subheaders or (None,))]

    @property
    def subheaders(self):
        return [sub for group in self.header_groups for sub in group.subheaders]

    def for_columns(self, offset, limit):
        """Page entities, preserving every period and the full report's totals."""
        return replace(self, columns=self.columns[offset:offset + limit])

    def rows(self):
        if self.blocked:
            return
        for i, label in enumerate(self.period_labels):
            row = [label]
            for column in self.columns:
                n, avg = column.cells[i]
                row.append(public(n))
                if self.averages:
                    row.append(MESSAGE if n is None else round(avg, 2) if avg is not None else "No aplica")
            if self.has_total_column:
                row.append(public(self.period_totals[i]))
            if self.compares:
                row.extend(variation(self.period_totals[i - 1], self.period_totals[i]) if i else ("-", "-"))
            yield row
        if self.include_total:
            row = ["TOTAL"]
            for column in self.columns:
                row.append(public(protected_sum(n for n, _ in column.cells)))
                if self.averages:
                    row.append(None)  # No consolidated average, including protected totals.
            if self.has_total_column:
                row.append(public(protected_sum(self.period_totals)))
            if self.compares:
                row.extend(variation(self.period_totals[0], self.period_totals[-1]))
            yield row


def generate(filters):
    report = filters["report"]
    ps = periods(filters)
    protect = segmented(filters)
    from core.models import Profile
    countries = dict(Profile.StreamingCountry.choices)
    genders = dict(Profile.GenderIdentity.choices)
    metadata = [
        ("Tipo de reporte", REPORTS[report]),
        ("Generado", timezone.localtime(timezone.now(), REPORT_TZ).isoformat()),
        ("Zona horaria", str(REPORT_TZ)),
        ("Periodos", "; ".join(p.label for p in ps)),
        ("Países", ", ".join(countries[k] for k in filters.get("countries", [])) or "Todos"),
        ("Edades", ", ".join(AGE_LABELS[k] for k in filters.get("ages", [])) or "Todos"),
        ("Identidades", ", ".join(genders[k] for k in filters.get("genders", [])) or "Todos"),
        ("Tipo de contenido", {"movie": "Películas", "series": "Series"}.get(filters.get("content_type"), "Todos")),
    ]
    populations, columns = [], []

    if report in USER_REPORTS:
        grouped = []
        for period in ps:
            qs = users(filters, period)
            populations.append(qs.values_list("pk", flat=True))
            if report in {"countries", "genders"}:
                field = "profile__streaming_country" if report == "countries" else "profile__gender_identity"
                groups = qs.values(key=F(field)).annotate(n=Count("pk")).order_by("key")
                labels = countries if report == "countries" else genders
            elif report == "ages":
                cases = [When(report_age__gte=lo, **({"report_age__lte": hi} if hi is not None else {}), then=Value(key)) for key, (lo, hi) in AGE_RANGES.items()]
                groups = qs.annotate(key=Case(*cases, When(report_age__isnull=True, then=Value("unknown")), default=Value("outside"), output_field=CharField())).values("key").annotate(n=Count("pk")).order_by("key")
                labels = {**AGE_LABELS, "unknown": "Sin dato", "outside": "Fuera de rangos definidos"}
            else:
                groups = [{"key": "users", "n": qs.count()}]
                labels = {"users": "Registros"}
            grouped.append({g["key"]: g["n"] for g in groups})
        population = populations[0].union(*populations[1:]).count()
        logical_order = {key: i for i, key in enumerate(labels)}
        keys = sorted(set().union(*(g.keys() for g in grouped)), key=lambda k: (logical_order.get(k, len(labels)), k or ""))
        for key in keys:
            cells = [(g.get(key, 0) if publishable(g.get(key, 0), protect) else None, None) for g in grouped]
            columns.append(Column(labels.get(key, "Sin dato"), cells))
    elif report in CONTENT_REPORTS:
        # Compile narrow ORM source queries; identifiers are fixed here, user
        # supplied filters remain bound query parameters.
        fragments, params = [], []
        for i, period in enumerate(ps):
            if report == "recommended":
                qs = recommendations(filters, period)
            else:
                qs, _ = source("ratings", filters, period)
            populations.append(qs.order_by().values_list("user_id", flat=True))
            title = F("movie__title_spanish")
            from django.db.models.functions import Coalesce, NullIf
            title = Coalesce(NullIf(title, Value("")), F("movie__title_english"))
            if report in {"productions", "recommended"}:
                qs = qs.annotate(rkey=F("movie_id"), rlabel=title, rtype=F("movie__type"))
            else:
                qs = qs.annotate(rkey=Coalesce(F("movie__genre_key"), Value("Sin dato")), rlabel=Coalesce(F("movie__genre_key"), Value("Sin dato")), rtype=Value(""))
            qs = qs.annotate(ractor=F("user_id"), rscore=Value(0) if report == "recommended" else F("score")).values("rkey", "rlabel", "rtype", "ractor", "rscore")
            sql, bound = qs.query.sql_with_params()
            if report == "genres":
                fragment = f"SELECT genre AS key, genre AS label, '' AS type, %s AS period, COUNT(*) AS n, AVG(base.rscore) AS avg, COUNT(DISTINCT base.ractor) AS u FROM ({sql}) base CROSS JOIN LATERAL (SELECT DISTINCT unnest(string_to_array(base.rkey, '|')) AS genre) expanded GROUP BY genre"
            else:
                counter = "COUNT(DISTINCT ractor)" if report == "recommended" else "COUNT(*)"
                fragment = f"SELECT rkey::text AS key, rlabel AS label, rtype AS type, %s AS period, {counter} AS n, AVG(rscore) AS avg, COUNT(DISTINCT ractor) AS u FROM ({sql}) base GROUP BY rkey, rlabel, rtype"
            fragments.append(fragment)
            params.extend([i, *bound])
        population = populations[0].union(*populations[1:]).count()
        order = "SUM(CASE WHEN u >= 10 THEN n ELSE 0 END)" if protect else "SUM(n)"
        ordering = "key" if report == "genres" else f"{order} DESC, key"
        sql = "SELECT key, label, type, jsonb_agg(jsonb_build_object('period',period,'n',n,'avg',avg,'u',u)) FROM (" + " UNION ALL ".join(fragments) + f") cells GROUP BY key,label,type ORDER BY {ordering}"
        if publishable(population, protect):
            # chunked_cursor is a PostgreSQL server-side cursor in autocommit.
            with connection.chunked_cursor() as cursor:
                cursor.execute(sql, params)
                while batch := cursor.fetchmany(200):
                    for key, label, kind, cells in batch:
                        if isinstance(cells, str):
                            cells = json.loads(cells)
                        indexed = {c["period"]: c for c in cells}
                        values = []
                        for i in range(len(ps)):
                            c = indexed.get(i, {"n": 0, "avg": None, "u": 0})
                            values.append((c["n"], c["avg"]) if publishable(c["u"], protect) else (None, None))
                        # Do not disclose titles of entirely suppressed populations.
                        public_label = label if any(n is not None for n, _ in values) else MESSAGE
                        if report in {"productions", "recommended"}:
                            if public_label != MESSAGE:
                                public_label += " (" + {"movie": "Película", "series": "Serie"}.get(kind, "Sin dato") + ")"
                        columns.append((key, Column(public_label, values)))
        # Disambiguate equal public titles without exposing suppressed identifiers.
        duplicates = Counter(column.label for _, column in columns)
        columns = [Column(f"{column.label} [#{key}]", column.cells)
                   if column.label != MESSAGE and duplicates[column.label] > 1 else column
                   for key, column in columns]
    else:
        metrics = list(DIRECT) if report == "direct" else list(SOCIAL) if report == "social" else [report]
        for metric in metrics:
            values = []
            for period in ps:
                qs, actor = source(metric, filters, period)
                populations.append(qs.order_by().values_list(actor + "_id", flat=True))
                stats = qs.aggregate(n=Count("pk"), u=Count(actor, distinct=True))
                values.append((stats["n"] if publishable(stats["u"], protect) else None, None))
            columns.append(Column({**DIRECT, **SOCIAL}[metric], values))
        population = populations[0].union(*populations[1:]).count()

    blocked = not publishable(population, protect)
    if blocked:
        columns = []
    period_totals = [protected_sum(column.cells[i][0] for column in columns) for i in range(len(ps))]
    # Publishing a grand total alongside suppressed cells enables subtraction.
    hide_population = blocked or (protect and (report != "users" or len(ps) > 1))
    # A single registered-user count already describes the entire population.
    show_population = not (report == "users" and len(ps) == 1)
    if show_population:
        metadata.append(("Población analizada (usuarios únicos)", MESSAGE if hide_population else population))
    methodology = [("Fuentes, semántica y limitaciones", warning) for warning in WARNINGS]
    methodology.append(("Periodos", "Personalizado: un periodo diario por fecha Desde/Hasta, ambas inclusivas; cada día usa [00:00, 00:00 del día siguiente) en America/Bogota. Comparación mensual: límites de meses completos; el mes actual puede contener datos parciales."))
    compares = report in USER_REPORTS or report in {"productions", "genres", "combinations"}
    if compares:
        methodology.append(("Comparación", "Diferencia y porcentaje sobre registros/TOTAL frente al periodo seleccionado anterior; primer periodo: -. Base cero: No aplica. En Personalizado se compara el día consecutivo anterior. En la fila TOTAL se compara el último periodo contra el primero, nunca contra la sumatoria."))
    methodology.append(("Totales y privacidad", "Fila TOTAL: sumatoria por entidad/métrica de los periodos mostrados. Un total que contiene alguna celda protegida también se suprime, al igual que comparaciones dependientes. En ratings solo se suman No calif.; Promedio Calif. queda vacío en la fila TOTAL."))
    if report == "genres":
        methodology.append(("TOTAL de géneros", "Suma de contribuciones por género: un rating puede contribuir a varios géneros. No equivale al número de ratings distintos."))
    if report == "recommended":
        methodology.append(("TOTAL de Recomendadas", "Suma de usuarios únicos por producción y periodo; un usuario activo en varios periodos contribuye en cada uno. Sin diferencia, variación ni TOTAL general."))
    elif report not in USER_REPORTS and report not in CONTENT_REPORTS:
        methodology.append(("Actividad", "Periodos en filas y métricas en columnas; sumatoria final por métrica. Sin diferencia ni variación."))
    return Result(REPORTS[report], metadata, None if hide_population else population, blocked,
                  methodology, show_population, report, [p.label for p in ps],
                  "Fecha" if filters["custom"] else "Periodo", columns, period_totals,
                  len(columns), ps[0].start is not None)
