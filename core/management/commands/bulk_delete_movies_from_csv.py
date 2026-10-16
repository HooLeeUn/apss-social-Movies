"""Set-based deletion with optional, explicitly authorized feed-candidate cleanup."""
import csv
import json
import re
from tempfile import TemporaryFile

from django.core.management.base import BaseCommand, CommandError
from django.db import connections, transaction

from core.models import Movie, UserDailyFeedCandidate


AUDITED_MODELS = frozenset({
    "StreamingProviderLink", "MovieRating", "MovieListItem", "MovieRecommendationItem",
    "MovieRecommendationHistory", "ProfileFavoriteMovie", "WeeklyRecommendationItem",
    "UserDailyFeedCandidate", "Comment", "VideoComment", "UserNotification",
})


def validate_csv(path, output):
    """Validate the whole manifest before opening a connection; spool canonical IDs."""
    seen = set()
    rows = errors = 0
    with open(path, encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source, strict=True)
        if not reader.fieldnames or reader.fieldnames.count("database_id") != 1:
            raise CommandError("Se requiere exactamente una columna database_id.")
        for row in reader:
            rows += 1
            raw = (row.get("database_id") or "").strip()
            if (None in row or not re.fullmatch(r"[1-9][0-9]{0,18}", raw)
                    or int(raw) > 9223372036854775807):
                errors += 1
                continue
            value = int(raw)
            if value in seen:
                errors += 1
                continue
            seen.add(value)
            output.write(f"{value}\n")
    summary = dict(rows_read=rows, unique_ids=len(seen), errors=errors)
    if errors or not rows:
        exc = CommandError(f"CSV rechazado: {summary}. No se ha conectado a la base.")
        exc.validation_summary = summary
        raise exc
    output.seek(0)
    return summary


class Command(BaseCommand):
    help = "Borrado PostgreSQL con COPY y limpieza opcional de candidatos del feed."
    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument("--affected-csv", required=True)
        parser.add_argument("--database", default="default")
        parser.add_argument("--expected-database", required=True)
        parser.add_argument("--delete-feed-candidates", action="store_true",
                            help="Permite borrar candidatos derivados solo para los Movie IDs del CSV.")
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--dry-run", action="store_true")
        mode.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        summary = dict(rows_read=0, unique_ids=0, existing_before=None,
                       related_counts={}, deleted=0, remaining_after=None, errors=0,
                       database=None, mode="apply" if options["apply"] else "dry-run",
                       candidate_rows_before=None, unique_movies_with_candidates=None,
                       affected_pools=None, candidate_rows_deleted=0, candidate_rows_remaining=None)
        try:
            with TemporaryFile(mode="w+t", encoding="ascii", newline="") as spool:
                summary.update(validate_csv(options["affected_csv"], spool))
                db = connections[options["database"]]
                if db.vendor != "postgresql":
                    raise CommandError("Este preflight requiere PostgreSQL.")
                if str(db.settings_dict["NAME"]) != options["expected_database"]:
                    raise CommandError("La base configurada no coincide con --expected-database.")
                summary.update(self.inspect_database(
                    db, spool, options["expected_database"], apply=options["apply"], summary=summary,
                    delete_feed_candidates=options["delete_feed_candidates"],
                ))
        except (OSError, UnicodeError, csv.Error) as exc:
            self._failure(summary)
            raise CommandError(f"CSV no legible: {exc}") from exc
        except CommandError as exc:
            summary.update(getattr(exc, "validation_summary", {}))
            self._failure(summary)
            raise
        except Exception as exc:
            self._failure(summary)
            raise CommandError(str(exc)) from exc
        self.stdout.write(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        self.stdout.write("APPLY: commit completado." if options["apply"] else
                          "DRY-RUN: transacción revertida; sin modificaciones persistentes.")

    def _failure(self, summary):
        summary.update(errors=max(1, summary["errors"]), deleted=0, candidate_rows_deleted=0)
        self.stderr.write(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        self.stderr.write("Operación fallida; no se informa un commit exitoso.")

    def inspect_database(self, db, spool, expected, *, apply=False, summary=None,
                         delete_feed_candidates=False):
        summary = summary if summary is not None else {}
        quote = db.ops.quote_name
        movie_table = quote(Movie._meta.db_table)
        pk = quote(Movie._meta.pk.column)
        candidate_table = quote(UserDailyFeedCandidate._meta.db_table)
        relations = list(Movie._meta.related_objects)
        if ({r.related_model.__name__ for r in relations} != AUDITED_MODELS
                or len(relations) != len(AUDITED_MODELS)
                or any(r.field.name != "movie" or
                       r.field.remote_field.on_delete.__name__ != "CASCADE" for r in relations)):
            raise CommandError("Las relaciones de Movie cambiaron: se requiere nueva auditoría.")
        # Refuse nested transactions so success means this command actually committed.
        if db.in_atomic_block or not db.get_autocommit():
            raise CommandError("Ejecutar fuera de una transacción existente.")
        with transaction.atomic(using=db.alias):
            with db.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL " +
                               ("READ COMMITTED" if apply else "REPEATABLE READ"))
                cursor.execute("SET LOCAL lock_timeout = '5s'")
                cursor.execute("SET LOCAL statement_timeout = '10min'")
                cursor.execute("SELECT current_database()")
                summary["database"] = cursor.fetchone()[0]
                if summary["database"] != expected:
                    raise CommandError("current_database() no coincide con --expected-database.")
                cursor.execute("CREATE TEMP TABLE bulk_movie_ids (id bigint PRIMARY KEY) ON COMMIT DROP")
                with cursor.copy("COPY pg_temp.bulk_movie_ids (id) FROM STDIN") as copy:
                    while chunk := spool.read(1024 * 1024):
                        copy.write(chunk)
                cursor.execute("ANALYZE pg_temp.bulk_movie_ids")
                if apply:
                    # Block INSERT/UPDATE/DELETE on parent and all incoming tables.
                    # Counts use a fresh snapshot after any earlier writers finish.
                    tables = sorted({Movie._meta.db_table} |
                                    {r.related_model._meta.db_table for r in relations})
                    cursor.execute("LOCK TABLE " + ", ".join(quote(t) for t in tables) +
                                   " IN SHARE ROW EXCLUSIVE MODE")
                    self._check_database_fks(cursor, relations)
                    self._check_delete_schema(cursor, delete_feed_candidates)
                cursor.execute(
                    f"SELECT count(*) FROM {movie_table} m "
                    f"JOIN pg_temp.bulk_movie_ids s ON s.id = m.{pk}"
                )
                existing = cursor.fetchone()[0]
                cursor.execute("SELECT count(*) FROM pg_temp.bulk_movie_ids")
                total = cursor.fetchone()[0]
                summary.update(existing_before=existing, not_found=total - existing)
                related = self._count_relations(cursor, relations, quote)
                summary["related_counts"] = related
                cursor.execute(
                    f"SELECT count(*), count(DISTINCT c.movie_id), count(DISTINCT c.pool_id) "
                    f"FROM {candidate_table} c JOIN pg_temp.bulk_movie_ids s ON s.id = c.movie_id"
                )
                candidate_rows, candidate_movies, pools = cursor.fetchone()
                summary.update(candidate_rows_before=candidate_rows,
                               unique_movies_with_candidates=candidate_movies, affected_pools=pools)
                if candidate_rows != related["UserDailyFeedCandidate.movie"]["rows"]:
                    raise CommandError("Conteo de candidatos inconsistente: abortando.")
                candidates_deleted = 0
                candidates_remaining = candidate_rows
                deleted = 0
                remaining = existing
                if apply:
                    if existing != total:
                        raise CommandError("IDs ausentes: apply exige que todos los IDs existan.")
                    if any(item["rows"] for name, item in related.items()
                           if not (delete_feed_candidates and name == "UserDailyFeedCandidate.movie")):
                        raise CommandError("Relaciones no vacías: borrado abortado para preservar señales.")
                    if delete_feed_candidates:
                        cursor.execute(
                            f"DELETE FROM {candidate_table} c USING pg_temp.bulk_movie_ids s "
                            f"WHERE c.movie_id = s.id"
                        )
                        candidates_deleted = cursor.rowcount
                        cursor.execute(
                            f"SELECT count(*) FROM {candidate_table} c "
                            f"JOIN pg_temp.bulk_movie_ids s ON s.id = c.movie_id"
                        )
                        candidates_remaining = cursor.fetchone()[0]
                        summary["candidate_rows_remaining"] = candidates_remaining
                        if candidates_deleted != candidate_rows or candidates_remaining != 0:
                            raise CommandError("Verificación de candidatos incorrecta: rollback completo.")
                        after = self._count_relations(cursor, relations, quote)
                        summary["related_counts_after"] = after
                        if any(item["rows"] for item in after.values()):
                            raise CommandError("Relaciones no vacías tras limpiar candidatos: rollback completo.")
                    cursor.execute(
                        f"DELETE FROM {movie_table} m USING pg_temp.bulk_movie_ids s "
                        f"WHERE m.{pk} = s.id"
                    )
                    deleted = cursor.rowcount
                    cursor.execute(
                        f"SELECT count(*) FROM {movie_table} m "
                        f"JOIN pg_temp.bulk_movie_ids s ON s.id = m.{pk}"
                    )
                    remaining = cursor.fetchone()[0]
                    summary["remaining_after"] = remaining
                    if remaining != 0 or deleted != existing:
                        raise CommandError("Verificación final incorrecta: rollback del borrado.")
                    cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            if not apply:
                transaction.set_rollback(True, using=db.alias)
        # Only report deletions after atomic's __exit__ successfully commits.
        return dict(existing_before=existing, not_found=total - existing,
                    related_counts=related, deleted=deleted, remaining_after=remaining,
                    candidate_rows_before=candidate_rows, unique_movies_with_candidates=candidate_movies,
                    affected_pools=pools, candidate_rows_deleted=candidates_deleted,
                    candidate_rows_remaining=candidates_remaining,
                    database=summary["database"])

    def _count_relations(self, cursor, relations, quote):
        related = {}
        for relation in relations:
            field = relation.field
            table = quote(relation.related_model._meta.db_table)
            column = quote(field.column)
            cursor.execute(
                f"SELECT count(*) FROM {table} r "
                f"JOIN pg_temp.bulk_movie_ids s ON s.id = r.{column}"
            )
            related[f"{relation.related_model.__name__}.{field.name}"] = {
                "rows": cursor.fetchone()[0], "on_delete": field.remote_field.on_delete.__name__,
            }
        return related

    def _check_delete_schema(self, cursor, delete_feed_candidates):
        tables = [Movie._meta.db_table]
        if delete_feed_candidates:
            tables.append(UserDailyFeedCandidate._meta.db_table)
            if UserDailyFeedCandidate._meta.related_objects:
                raise CommandError("Relaciones entrantes inesperadas hacia candidatos: abortando.")
            cursor.execute(
                "SELECT conname FROM pg_constraint WHERE contype = 'f' AND confrelid = %s::regclass",
                [UserDailyFeedCandidate._meta.db_table],
            )
            if cursor.fetchall():
                raise CommandError("FK entrantes inesperadas hacia candidatos: abortando.")
        for table in tables:
            # Internal FK constraint triggers are already covered by the FK audit.
            cursor.execute(
                """SELECT tgname FROM pg_trigger WHERE tgrelid = %s::regclass
                   AND NOT tgisinternal AND tgenabled <> 'D' AND (tgtype & 8) <> 0""",
                [table],
            )
            if cursor.fetchall():
                raise CommandError(f"Triggers DELETE inesperados en {table}: abortando.")

    def _check_database_fks(self, cursor, relations):
        """Reject schema drift, including FKs unknown to Django and SQL cascades."""
        cursor.execute(
            """SELECT c.conrelid::regclass::text, a.attname, c.confdeltype
               FROM pg_constraint c
               JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
               WHERE c.contype = 'f' AND c.confrelid = %s::regclass""",
            [Movie._meta.db_table],
        )
        actual = cursor.fetchall()
        expected = {(r.related_model._meta.db_table, r.field.column) for r in relations}
        if ({(table, column) for table, column, _ in actual} != expected
                or len(actual) != len(expected)
                or any(policy not in {"a", "r"} for _, _, policy in actual)):
            raise CommandError("FK PostgreSQL distintas de las auditadas: abortando.")
