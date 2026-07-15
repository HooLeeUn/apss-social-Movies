import csv
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import ProtectedError

from core.management.commands._csv_utils import open_csv_dict_reader
from core.models import Movie


REPORT_FIELDS = ["database_id", "status", "message", "mode"]


class Command(BaseCommand):
    help = "Delete Movie records explicitly listed by database_id in a CSV file."

    def add_arguments(self, parser):
        parser.add_argument("--affected-csv", required=True, help="CSV file containing database_id values to delete.")
        parser.add_argument("--apply", action="store_true", help="Actually delete records. Defaults to dry-run.")
        parser.add_argument("--batch-size", type=int, default=1000, help="IDs per deletion/check batch. Default: 1000.")
        parser.add_argument(
            "--report-csv",
            default="delete_movies_from_csv_report.csv",
            help="Path for the CSV report. Default: delete_movies_from_csv_report.csv.",
        )

    def handle(self, *args, **options):
        affected_csv = Path(options["affected_csv"])
        report_csv = Path(options["report_csv"])
        apply = options["apply"]
        mode = "apply" if apply else "dry-run"
        batch_size = options["batch_size"]
        if batch_size <= 0:
            raise CommandError("--batch-size debe ser un entero positivo.")

        self._print_database_context()
        self.stdout.write(f"Modo: {'APPLY' if apply else 'DRY-RUN'}")
        self.stdout.write("Identificación: se usará exclusivamente la columna database_id.")

        rows_read, valid_ids, seen_ids, invalid_count, duplicate_count, report_rows = self._read_csv(affected_csv, mode)
        if not valid_ids:
            self._write_report(report_csv, report_rows)
            raise CommandError("El CSV no contiene al menos un database_id válido. No se eliminó nada.")

        existing_ids = set()
        not_found = 0
        for index, batch in enumerate(self._chunks(valid_ids, batch_size), start=1):
            batch_existing = set(Movie.objects.filter(id__in=batch).values_list("id", flat=True))
            existing_ids.update(batch_existing)
            for database_id in batch:
                if database_id not in batch_existing:
                    not_found += 1
                    report_rows.append(self._report_row(database_id, "not_found", "Movie no encontrado.", mode))
                elif not apply:
                    report_rows.append(self._report_row(database_id, "would_delete", "Movie existe y sería eliminado.", mode))
            self._progress(index, batch_size, len(valid_ids), "comprobados")

        deleted = 0
        protected = 0
        errors = 0
        if apply:
            for index, batch in enumerate(self._chunks([movie_id for movie_id in valid_ids if movie_id in existing_ids], batch_size), start=1):
                batch_deleted, batch_protected, batch_errors = self._delete_batch(batch, mode, report_rows)
                deleted += batch_deleted
                protected += batch_protected
                errors += batch_errors
                self._progress(index, batch_size, len(existing_ids), "procesados para eliminación")

        existing = len(existing_ids)
        if not apply:
            deleted = 0
            protected = 0
            errors = 0

        self._write_report(report_csv, report_rows)
        self.stdout.write(f"Reporte CSV: {report_csv}")
        self.stdout.write("Resumen:")
        summary = {
            "filas leídas": rows_read,
            "IDs válidos únicos": len(seen_ids),
            "duplicados dentro del CSV": duplicate_count,
            "inválidos": invalid_count,
            "existentes": existing,
            "no encontrados": not_found,
            "eliminados": deleted,
            "protegidos": protected,
            "errores": errors,
            "modo": mode,
        }
        for label, value in summary.items():
            self.stdout.write(f"- {label}: {value}")
        if apply:
            self.stdout.write(self.style.SUCCESS(f"Movies eliminados: {deleted}"))
        else:
            self.stdout.write(self.style.WARNING("No se realizaron cambios"))
            self.stdout.write(f"Movies que serían eliminados: {existing}")

    def _read_csv(self, affected_csv, mode):
        report_rows = []
        valid_ids = []
        seen_ids = set()
        rows_read = invalid_count = duplicate_count = 0
        with open_csv_dict_reader(affected_csv) as (reader, delimiter):
            self.stdout.write(f"CSV detectado: delimitador {repr(delimiter)}")
            if not reader.fieldnames or "database_id" not in reader.fieldnames:
                raise CommandError("Falta la columna obligatoria: database_id. No se eliminó nada.")
            for row_number, row in enumerate(reader, start=2):
                rows_read += 1
                raw_id = (row.get("database_id") or "").strip()
                try:
                    database_id = int(raw_id)
                    if database_id <= 0 or str(database_id) != raw_id:
                        raise ValueError
                except (TypeError, ValueError):
                    invalid_count += 1
                    report_rows.append(self._report_row(raw_id, "invalid_id", f"Fila {row_number}: database_id vacío o inválido.", mode))
                    continue
                if database_id in seen_ids:
                    duplicate_count += 1
                    report_rows.append(self._report_row(database_id, "duplicate_in_csv", f"Fila {row_number}: database_id repetido en CSV.", mode))
                    continue
                seen_ids.add(database_id)
                valid_ids.append(database_id)
        return rows_read, valid_ids, seen_ids, invalid_count, duplicate_count, report_rows

    def _delete_batch(self, batch, mode, report_rows):
        deleted = protected = errors = 0
        with transaction.atomic():
            for database_id in batch:
                try:
                    with transaction.atomic():
                        movie = Movie.objects.get(id=database_id)
                        movie.delete()
                    deleted += 1
                    report_rows.append(self._report_row(database_id, "deleted", "Movie eliminado.", mode))
                except Movie.DoesNotExist:
                    report_rows.append(self._report_row(database_id, "not_found", "Movie no encontrado al eliminar.", mode))
                except ProtectedError as exc:
                    protected += 1
                    report_rows.append(self._report_row(database_id, "protected_error", str(exc), mode))
                except Exception as exc:  # noqa: BLE001 - command must continue and report real deletion errors.
                    errors += 1
                    report_rows.append(self._report_row(database_id, "error", str(exc), mode))
        return deleted, protected, errors

    def _print_database_context(self):
        db = settings.DATABASES.get(DEFAULT_DB_ALIAS, {})
        safe = {
            "ENGINE": db.get("ENGINE", ""),
            "NAME": db.get("NAME", ""),
            "HOST": db.get("HOST", ""),
            "PORT": db.get("PORT", ""),
        }
        self.stdout.write(f"Base de datos activa ({DEFAULT_DB_ALIAS}): {safe}")
        self.stdout.write(f"Vendor DB: {connections[DEFAULT_DB_ALIAS].vendor}")

    def _write_report(self, report_csv, report_rows):
        report_csv.parent.mkdir(parents=True, exist_ok=True)
        with report_csv.open("w", encoding="utf-8", newline="") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=REPORT_FIELDS)
            writer.writeheader()
            writer.writerows(report_rows)

    def _report_row(self, database_id, status, message, mode):
        return {"database_id": database_id, "status": status, "message": message, "mode": mode}

    def _chunks(self, values, size):
        for index in range(0, len(values), size):
            yield values[index:index + size]

    def _progress(self, index, batch_size, total, action):
        processed = min(index * batch_size, total)
        if processed == total or processed % batch_size == 0:
            self.stdout.write(f"Progreso: {processed}/{total} IDs {action}.")
