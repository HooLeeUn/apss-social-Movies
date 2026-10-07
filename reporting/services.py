"""One immutable presentation result shared by HTML, CSV and XLSX.

Detailed content aggregation stays in PostgreSQL. Pages are bounded; exports
read server-side cursors rather than materializing activity records in Python.
"""
from dataclasses import dataclass
import json
from django.db import connection
from django.db.models import Count, Case, When, Value, CharField, F
from django.utils import timezone
from .catalog import REPORTS, USER_REPORTS, CONTENT_REPORTS, DIRECT, SOCIAL, AGE_RANGES, AGE_LABELS
from .periods import periods, REPORT_TZ
from .privacy import MESSAGE, publishable, segmented, variation
from .queries import source, users, recommendations

WARNINGS = [
    "Usuarios registrados: User con Profile, incluidos inactivos; fecha de alta date_joined. Pendientes excluidos.",
    "País e identidad corresponden al perfil actual; edad histórica calculada en fecha de referencia.",
    "Ratings: score vigente, atribuido a updated_at. No reconstruye scores anteriores ni ratings eliminados.",
    "Reacciones: estado vigente por updated_at; reacciones eliminadas no se reconstruyen.",
    "Follows: relaciones existentes por created_at; unfollows no tienen histórico.",
    "Recomendadas: intervalos solapados con el mes, usuarios únicos por producción. Edad al primer instante del solapamiento de cada intervalo.",
    "Histórico previo al despliegue parcial: solo se recuperan recomendaciones todavía activas, desde su created_at. No se inventan retiros.",
    "Contenido oculto que todavía existe cuenta; comentarios dirigidos excluidos.",
]

@dataclass
class Result:
    title: str
    headers: list
    metadata: list
    population: int | None
    blocked: bool
    row_factory: object

    def rows(self, offset=0, limit=None):
        if self.blocked:
            return iter(())
        return self.row_factory(offset, limit)


def compare_cells(label, cells, averages=False):
    row = [label]
    previous = None
    for i, cell in enumerate(cells):
        n, avg = cell
        row.append(MESSAGE if n is None else n)
        if averages:
            row.append(MESSAGE if n is None else (round(avg, 2) if avg is not None else "No aplica"))
        if i:
            row.extend(variation(previous, n))
        previous = n
    return row


def generate(filters):
    report = filters["report"]
    ps = periods(filters)
    protect = segmented(filters)
    headers = ["Producción" if report in {"productions", "recommended"} else "Segmento / Métrica"]
    if report in {"productions", "recommended"}:
        headers.append("Tipo")
    average = report in {"productions", "genres", "combinations"}
    for i, period in enumerate(ps):
        headers.append(period.label + (" · Calificaciones" if average else " · Total"))
        if average:
            headers.append(period.label + " · Promedio")
        if i:
            headers.extend([period.label + " · Diferencia", period.label + " · Variación %"])

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
    ] + [("Limitación", warning) for warning in WARNINGS]
    populations = []

    if report in USER_REPORTS:
        qs = users(filters, ps[0])
        population = qs.count()
        if report == "countries":
            groups = qs.values(key=F("profile__streaming_country")).annotate(n=Count("pk")).order_by("key")
            labels = countries
        elif report == "genders":
            groups = qs.annotate(key=F("profile__gender_identity")).values("key").annotate(n=Count("pk")).order_by("key")
            labels = genders
        elif report == "ages":
            cases = [When(report_age__gte=lo, **({"report_age__lte": hi} if hi is not None else {}), then=Value(key)) for key, (lo, hi) in AGE_RANGES.items()]
            groups = qs.annotate(key=Case(*cases, When(report_age__isnull=True, then=Value("unknown")), default=Value("outside"), output_field=CharField())).values("key").annotate(n=Count("pk")).order_by("key")
            labels = {**AGE_LABELS, "unknown": "Sin dato", "outside": "Fuera de rangos definidos"}
        else:
            groups = [{"key": "users", "n": population}]
            labels = {"users": "Usuarios registrados"}
        def rows(offset, limit):
            iterable = groups[offset:None if limit is None else offset+limit]
            for g in iterable:
                valid = publishable(g["n"], protect)
                yield [labels.get(g["key"], "Sin dato"), g["n"] if valid else MESSAGE]
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
        sql = "SELECT key, label, type, jsonb_agg(jsonb_build_object('period',period,'n',n,'avg',avg,'u',u)) FROM (" + " UNION ALL ".join(fragments) + f") cells GROUP BY key,label,type ORDER BY {order} DESC, key"
        def rows(offset, limit):
            query, bound = sql, list(params)
            if limit is not None:
                query += " LIMIT %s"
                bound.append(limit)
            if offset:
                query += " OFFSET %s"
                bound.append(offset)
            # chunked_cursor is a PostgreSQL server-side cursor in autocommit.
            with connection.chunked_cursor() as cursor:
                cursor.execute(query, bound)
                while batch := cursor.fetchmany(200):
                    for key, label, kind, cells in batch:
                        if isinstance(cells, str):
                            cells = json.loads(cells)
                        indexed = {c["period"]: c for c in cells}
                        values = []
                        for i in range(len(ps)):
                            c = indexed.get(i, {"n": 0, "avg": None, "u": 0})
                            values.append((c["n"] if publishable(c["u"], protect) else None, c["avg"]))
                        # Do not disclose titles of entirely suppressed populations.
                        public_label = label if any(n is not None for n, _ in values) else MESSAGE
                        row = compare_cells(public_label, values, average)
                        if report in {"productions", "recommended"}:
                            row.insert(1, {"movie": "Película", "series": "Serie"}.get(kind, "Sin dato") if public_label != MESSAGE else MESSAGE)
                        yield row
    else:
        metrics = list(DIRECT) if report == "direct" else list(SOCIAL) if report == "social" else [report]
        records = []
        for metric in metrics:
            values = []
            for period in ps:
                qs, actor = source(metric, filters, period)
                populations.append(qs.order_by().values_list(actor + "_id", flat=True))
                stats = qs.aggregate(n=Count("pk"), u=Count(actor, distinct=True))
                values.append((stats["n"] if publishable(stats["u"], protect) else None, None))
            records.append(compare_cells({**DIRECT, **SOCIAL}[metric], values))
        population = populations[0].union(*populations[1:]).count()
        def rows(offset, limit):
            return iter(records[offset:None if limit is None else offset+limit])

    blocked = not publishable(population, protect)
    # Publishing a grand total alongside suppressed cells enables subtraction.
    hide_population = blocked or (protect and report != "users")
    metadata.append(("Población analizada (usuarios únicos)", MESSAGE if hide_population else population))
    return Result(REPORTS[report], headers, metadata, None if hide_population else population, blocked, rows)
