from time import perf_counter

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from core.models import Movie
from core.views import (
    build_movie_autocomplete_extended_queryset,
    build_movie_autocomplete_fast_queryset,
)


SCAN_TYPES = (
    "Seq Scan",
    "Bitmap Index Scan",
    "Bitmap Heap Scan",
    "Index Scan",
)


class Command(BaseCommand):
    help = (
        "Diagnostica el rendimiento real del autocomplete de películas con "
        "EXPLAIN (ANALYZE, BUFFERS, VERBOSE) sin modificar datos."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "query",
            type=str,
            help='Texto de búsqueda, por ejemplo: "titanic leonardo".',
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=10,
            help="Límite a simular para /api/movies/?autocomplete=true&q=<query>&limit=10 (default: 10).",
        )

    def handle(self, *args, **options):
        if connection.vendor != "postgresql":
            raise CommandError(
                "Este diagnóstico requiere PostgreSQL porque usa "
                "EXPLAIN (ANALYZE, BUFFERS, VERBOSE)."
            )

        search = options["query"]
        limit = max(1, int(options["limit"]))

        self.stdout.write(f"Consulta autocomplete: {search!r}")
        self.stdout.write(f"Endpoint simulado: /api/movies/?autocomplete=true&q=<query>&limit={limit}")
        self.stdout.write("")

        base_queryset = self._get_autocomplete_base_queryset()
        fast_queryset = build_movie_autocomplete_fast_queryset(base_queryset, search)

        total_wall_seconds = 0.0
        total_database_ms = 0.0
        scan_presence = {scan_type: False for scan_type in SCAN_TYPES}

        self.stdout.write(self.style.MIGRATE_HEADING("FAST QUERYSET"))
        fast_count = fast_queryset.count()
        self.stdout.write(f"Fast queryset count: {fast_count}")
        fast_plan = self._explain_queryset("fast page", fast_queryset[:limit])
        total_wall_seconds += fast_plan["wall_seconds"]
        total_database_ms += fast_plan["execution_ms"] or 0.0
        self._merge_scan_presence(scan_presence, fast_plan["plan_text"])

        if fast_count < limit:
            remaining = limit - fast_count
            extended_queryset = build_movie_autocomplete_extended_queryset(
                base_queryset,
                search,
                fast_queryset=fast_queryset,
            )

            self.stdout.write("")
            self.stdout.write(self.style.MIGRATE_HEADING("EXTENDED QUERYSET"))
            extended_count = extended_queryset.count()
            self.stdout.write(f"Extended queryset count: {extended_count}")
            extended_plan = self._explain_queryset(
                "extended page",
                extended_queryset[:remaining],
            )
            total_wall_seconds += extended_plan["wall_seconds"]
            total_database_ms += extended_plan["execution_ms"] or 0.0
            self._merge_scan_presence(scan_presence, extended_plan["plan_text"])
        else:
            self.stdout.write("")
            self.stdout.write(
                "Extended queryset: no se ejecutaría para la primera página porque "
                f"fast_count ({fast_count}) >= limit ({limit})."
            )

        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING("RESUMEN"))
        self.stdout.write(f"Tiempo total medido en Python para EXPLAIN: {total_wall_seconds:.6f} s")
        self.stdout.write(f"Execution Time total reportado por PostgreSQL: {total_database_ms:.3f} ms")
        self.stdout.write("Tipos de scan detectados en el plan:")
        for scan_type in SCAN_TYPES:
            status = "SI" if scan_presence[scan_type] else "NO"
            self.stdout.write(f"- {scan_type}: {status}")

    def _get_autocomplete_base_queryset(self):
        return Movie.objects.only(
            "id",
            "title_english",
            "title_spanish",
            "type",
            "release_year",
            "image",
            "genre",
            "director",
            "cast_members",
        )

    def _explain_queryset(self, label, queryset):
        sql, params = queryset.query.sql_with_params()
        formatted_sql = self._format_sql(sql, params)

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"SQL generado ({label}):"))
        self.stdout.write(formatted_sql)

        explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE) {sql}"
        started_at = perf_counter()
        with connection.cursor() as cursor:
            cursor.execute(explain_sql, params)
            rows = cursor.fetchall()
        wall_seconds = perf_counter() - started_at

        plan_lines = [row[0] for row in rows]
        plan_text = "\n".join(plan_lines)
        execution_ms = self._extract_execution_time_ms(plan_lines)

        self.stdout.write("")
        self.stdout.write(self.style.NOTICE(f"EXPLAIN ANALYZE ({label}):"))
        self.stdout.write(plan_text)
        self.stdout.write(f"Tiempo medido en Python ({label}): {wall_seconds:.6f} s")
        if execution_ms is not None:
            self.stdout.write(f"Execution Time PostgreSQL ({label}): {execution_ms:.3f} ms")

        return {
            "plan_text": plan_text,
            "wall_seconds": wall_seconds,
            "execution_ms": execution_ms,
        }

    def _format_sql(self, sql, params):
        connection.ensure_connection()
        with connection.cursor() as cursor:
            raw_cursor = getattr(cursor, "cursor", cursor)
            mogrify = getattr(raw_cursor, "mogrify", None)
            if mogrify is None:
                return f"{sql}\nParams: {params!r}"
            formatted = mogrify(sql, params)
        if isinstance(formatted, bytes):
            return formatted.decode(connection.connection.encoding or "utf-8", errors="replace")
        return str(formatted)

    def _extract_execution_time_ms(self, plan_lines):
        for line in reversed(plan_lines):
            stripped = line.strip()
            if stripped.startswith("Execution Time:"):
                value = stripped.removeprefix("Execution Time:").strip().split(" ", 1)[0]
                try:
                    return float(value)
                except ValueError:
                    return None
        return None

    def _merge_scan_presence(self, scan_presence, plan_text):
        for scan_type in SCAN_TYPES:
            scan_presence[scan_type] = scan_presence[scan_type] or scan_type in plan_text
