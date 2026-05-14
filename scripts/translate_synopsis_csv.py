#!/usr/bin/env python3
"""Translate a semicolon-delimited movie synopsis CSV from English to Spanish.

This script is intentionally standalone: it does not import Django, does not read
project settings, and does not touch the database. It reads an input CSV with
``imdb_id`` and ``synopsis`` columns and writes an output CSV with ``imdb_id``
and ``synopsis_es`` columns.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Callable, Iterable

SOURCE_LANGUAGE = "en"
TARGET_LANGUAGE = "es"
INPUT_COLUMNS = {"imdb_id", "synopsis"}
OUTPUT_COLUMNS = ["imdb_id", "synopsis_es"]
PROGRESS_EVERY_ROWS = 10


class TranslationSetupError(RuntimeError):
    """Raised when Argos Translate cannot be prepared for use."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Traduce un CSV de sinopsis de películas de inglés a español usando "
            "Argos Translate. No usa Django ni modifica la base de datos."
        )
    )
    parser.add_argument(
        "--input",
        required=True,
        type=Path,
        help='Ruta del CSV de entrada separado por ";" con columnas imdb_id y synopsis.',
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help='Ruta del CSV de salida separado por ";" con columnas imdb_id y synopsis_es.',
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Procesa como máximo N filas de datos después de aplicar --start-row.",
    )
    parser.add_argument(
        "--start-row",
        type=int,
        default=1,
        help="Fila de datos desde la que empezar (1 = primera fila después del encabezado).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Salta imdb_id que ya estén presentes en el archivo de salida.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.limit is not None and args.limit < 0:
        raise ValueError("--limit debe ser un número mayor o igual a 0.")
    if args.start_row < 1:
        raise ValueError("--start-row debe ser un número mayor o igual a 1.")
    if not args.input.exists():
        raise FileNotFoundError(f"No existe el archivo CSV de entrada: {args.input}")
    if not args.input.is_file():
        raise ValueError(f"La ruta de entrada no es un archivo: {args.input}")


def import_argos_modules():
    try:
        from argostranslate import package, translate
    except ImportError as exc:
        raise TranslationSetupError(
            "No se encontró argostranslate. Instálalo antes de ejecutar el script: "
            "pip install argostranslate"
        ) from exc
    return package, translate


def ensure_translation_installed() -> Callable[[str], str]:
    package, translate = import_argos_modules()

    translation = translate.get_translation_from_codes(SOURCE_LANGUAGE, TARGET_LANGUAGE)
    if translation is None:
        print(
            "No se encontró el paquete Argos en->es instalado. "
            "Intentando instalarlo desde el repositorio de paquetes de Argos..."
        )
        try:
            package.update_package_index()
            available_packages = package.get_available_packages()
            matching_package = next(
                (
                    available_package
                    for available_package in available_packages
                    if available_package.from_code == SOURCE_LANGUAGE
                    and available_package.to_code == TARGET_LANGUAGE
                ),
                None,
            )
            if matching_package is None:
                raise TranslationSetupError(
                    "No se encontró un paquete Argos disponible para traducir de en a es."
                )

            package_path = matching_package.download()
            package.install_from_path(package_path)
        except Exception as exc:
            if isinstance(exc, TranslationSetupError):
                raise
            raise TranslationSetupError(
                "No se pudo instalar automáticamente el paquete Argos en->es."
            ) from exc

        translation = translate.get_translation_from_codes(SOURCE_LANGUAGE, TARGET_LANGUAGE)

    if translation is None:
        raise TranslationSetupError(
            "El paquete Argos en->es parece instalado, pero la traducción no está disponible."
        )

    return translation.translate


def read_existing_output_imdb_ids(output_path: Path) -> set[str]:
    if not output_path.exists() or output_path.stat().st_size == 0:
        return set()

    with output_path.open("r", encoding="utf-8-sig", newline="") as output_file:
        reader = csv.DictReader(output_file, delimiter=";")
        header = set(reader.fieldnames or [])
        if "imdb_id" not in header:
            raise ValueError(
                f"El archivo de salida existente no contiene la columna imdb_id: {output_path}"
            )
        return {clean_text(row.get("imdb_id")) for row in reader if clean_text(row.get("imdb_id"))}


def iter_input_rows(input_path: Path) -> Iterable[tuple[int, dict[str, str]]]:
    with input_path.open("r", encoding="utf-8-sig", newline="") as input_file:
        reader = csv.DictReader(input_file, delimiter=";")
        header = set(reader.fieldnames or [])
        missing_columns = INPUT_COLUMNS - header
        if missing_columns:
            raise ValueError(
                "El CSV de entrada no contiene todas las columnas requeridas. "
                f"Faltan: {', '.join(sorted(missing_columns))}"
            )

        for data_row_number, row in enumerate(reader, start=1):
            yield data_row_number, row


def clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def print_progress(stats: dict[str, int], final: bool = False) -> None:
    prefix = "Progreso final" if final else "Progreso"
    print(
        f"{prefix}: filas leídas={stats['rows_read']} | "
        f"traducidas={stats['translated']} | "
        f"saltadas por vacío={stats['skipped_empty']} | "
        f"saltadas por resume={stats['skipped_resume']} | "
        f"errores={stats['errors']}"
    )


def translate_csv(args: argparse.Namespace) -> dict[str, int]:
    translate_text = ensure_translation_installed()

    output_exists_with_content = args.output.exists() and args.output.stat().st_size > 0
    existing_imdb_ids = read_existing_output_imdb_ids(args.output) if args.resume else set()
    output_mode = "a" if args.resume and output_exists_with_content else "w"
    write_header = output_mode == "w"

    args.output.parent.mkdir(parents=True, exist_ok=True)

    stats = {
        "rows_read": 0,
        "translated": 0,
        "skipped_empty": 0,
        "skipped_resume": 0,
        "errors": 0,
    }

    print(
        f"Iniciando traducción: input={args.input} | output={args.output} | "
        f"start-row={args.start_row} | limit={args.limit} | resume={args.resume}"
    )
    if args.resume:
        print(f"Resume activo: {len(existing_imdb_ids)} imdb_id ya presentes en output.")

    with args.output.open(output_mode, encoding="utf-8-sig", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=OUTPUT_COLUMNS, delimiter=";")
        if write_header:
            writer.writeheader()

        for data_row_number, row in iter_input_rows(args.input):
            if data_row_number < args.start_row:
                continue
            if args.limit is not None and stats["rows_read"] >= args.limit:
                break

            stats["rows_read"] += 1
            try:
                imdb_id = clean_text(row.get("imdb_id"))
                synopsis = clean_text(row.get("synopsis"))

                if not synopsis:
                    stats["skipped_empty"] += 1
                    continue

                if args.resume and imdb_id in existing_imdb_ids:
                    stats["skipped_resume"] += 1
                    continue

                synopsis_es = translate_text(synopsis).strip()
                writer.writerow({"imdb_id": imdb_id, "synopsis_es": synopsis_es})
                output_file.flush()
                stats["translated"] += 1
                if imdb_id:
                    existing_imdb_ids.add(imdb_id)
            except Exception as exc:  # Keep processing subsequent rows after row-level failures.
                stats["errors"] += 1
                print(f"Error en fila de datos {data_row_number}: {exc}", file=sys.stderr)
            finally:
                if stats["rows_read"] % PROGRESS_EVERY_ROWS == 0:
                    print_progress(stats)

    print_progress(stats, final=True)
    return stats


def main() -> int:
    args = parse_args()
    try:
        validate_args(args)
        translate_csv(args)
    except (FileNotFoundError, TranslationSetupError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrumpido por el usuario.", file=sys.stderr)
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
