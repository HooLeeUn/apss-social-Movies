"""Enrich a catalogue CSV through TMDb without reading or writing the database."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from core.tmdb_catalog import (
    CatalogEnricher,
    normalize_imdb_id,
    normalize_media_type,
    normalize_text,
)
from ._csv_utils import open_csv_dict_reader


CATALOG_COLUMNS = (
    "imdb_id", "title_english", "title_spanish", "type", "genre",
    "release_year", "director", "cast_members", "external_rating",
    "external_votes", "synopsis", "synopsis_es", "tmdb_id", "image",
)
REPORT_COLUMNS = (
    "row_number", "imdb_id", "title", "type", "release_year", "director",
    "status", "tmdb_id", "match_reason", "matched_director",
    "candidate_count", "candidate_tmdb_ids", "poster_found",
    "synopsis_en_found", "synopsis_es_found", "synopsis_en_source",
    "retry_count", "request_count", "notes", "processed_at",
)
FORBIDDEN_COLUMNS = {"movie_id", "database_id", "pk", "author", "author_id"}


def resume_key(row):
    imdb_id = normalize_imdb_id(row.get("imdb_id"))
    if imdb_id:
        return f"imdb:{imdb_id}"
    parts = [row.get(name, "") for name in (
        "type", "title_english", "title_spanish", "release_year", "director"
    )]
    material = "\x1f".join(normalize_text(value) for value in parts)
    return "row:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


class Command(BaseCommand):
    help = "Enriquece un CSV con identidad y metadatos TMDb, sin utilizar el ORM."
    FLUSH_EVERY = 50
    LOG_EVERY = 1000

    def add_arguments(self, parser):
        parser.add_argument("csv_path")
        parser.add_argument("--output", required=True)
        parser.add_argument("--report")
        parser.add_argument("--resume", action="store_true")
        parser.add_argument("--year-tolerance", type=int, default=0)
        parser.add_argument("--requests-per-second", type=float, default=3.0)
        parser.add_argument("--max-retries", type=int, default=4)

    def handle(self, *args, **options):
        source = Path(options["csv_path"])
        output = Path(options["output"])
        if not source.is_file():
            raise CommandError(f"El CSV no existe: {source}")
        report = Path(options["report"]) if options.get("report") else source.with_name(
            f"{source.stem}-enrichment-report.csv"
        )
        if source.resolve() in {output.resolve(), report.resolve()} or output.resolve() == report.resolve():
            raise CommandError("Entrada, salida y reporte deben ser archivos diferentes.")
        output.parent.mkdir(parents=True, exist_ok=True)
        report.parent.mkdir(parents=True, exist_ok=True)
        checkpoint = output.with_suffix(output.suffix + ".checkpoint")

        completed = self._load_completed(checkpoint, output) if options["resume"] else set()
        append = options["resume"] and output.exists()
        report_append = options["resume"] and report.exists()
        enricher = CatalogEnricher(
            year_tolerance=options["year_tolerance"],
            requests_per_second=options["requests_per_second"],
            max_retries=options["max_retries"],
        )
        processed = skipped = 0

        with open_csv_dict_reader(source) as (reader, _delimiter):
            original = [name for name in (reader.fieldnames or []) if name and name not in FORBIDDEN_COLUMNS]
            fields = original + [name for name in CATALOG_COLUMNS if name not in original]
            if append:
                self._validate_existing_header(output, fields)
            with output.open("a" if append else "w", encoding="utf-8", newline="") as out_fh, \
                    report.open("a" if report_append else "w", encoding="utf-8", newline="") as report_fh, \
                    checkpoint.open("a" if options["resume"] else "w", encoding="utf-8") as checkpoint_fh:
                output_writer = csv.DictWriter(out_fh, fieldnames=fields, extrasaction="ignore")
                report_writer = csv.DictWriter(report_fh, fieldnames=REPORT_COLUMNS)
                if not append:
                    output_writer.writeheader()
                if not report_append:
                    report_writer.writeheader()

                for row_number, raw in enumerate(reader, start=2):
                    row = {str(key).strip(): value for key, value in raw.items() if key is not None}
                    key = resume_key(row)
                    if key in completed:
                        skipped += 1
                        report_writer.writerow(self._report_row(row_number, row, status="skipped_resume"))
                        if skipped % self.FLUSH_EVERY == 0:
                            report_fh.flush()
                        continue

                    result = enricher.resolve(row)
                    enriched = {name: row.get(name, "") for name in fields}
                    enriched.update({
                        "tmdb_id": result.tmdb_id or "",
                        "image": result.image,
                        "synopsis": result.synopsis or str(row.get("synopsis") or "").strip(),
                        "synopsis_es": result.synopsis_es,
                    })
                    output_writer.writerow(enriched)
                    report_writer.writerow(self._report_row(row_number, row, result=result))
                    checkpoint_fh.write(json.dumps({"key": key, "row_number": row_number}) + "\n")
                    completed.add(key)
                    processed += 1
                    if processed % self.FLUSH_EVERY == 0:
                        out_fh.flush()
                        report_fh.flush()
                        checkpoint_fh.flush()
                    if processed % self.LOG_EVERY == 0:
                        self.stdout.write(f"Procesadas {processed} filas; omitidas por resume: {skipped}")
                out_fh.flush()
                report_fh.flush()
                checkpoint_fh.flush()
        self.stdout.write(self.style.SUCCESS(
            f"Finalizado: {processed} procesadas, {skipped} omitidas. Salida: {output}; reporte: {report}"
        ))

    def _load_completed(self, checkpoint, output):
        keys = set()
        if checkpoint.exists():
            with checkpoint.open(encoding="utf-8") as fh:
                for line in fh:
                    try:
                        keys.add(json.loads(line)["key"])
                    except (ValueError, KeyError, TypeError):
                        continue
            return keys
        if output.exists():
            with open_csv_dict_reader(output) as (reader, _):
                for row in reader:
                    keys.add(resume_key(row))
        return keys

    def _validate_existing_header(self, output, expected):
        with open_csv_dict_reader(output) as (reader, _):
            if list(reader.fieldnames or []) != list(expected):
                raise CommandError("El encabezado de --output existente no coincide con el CSV de entrada.")

    def _report_row(self, row_number, row, result=None, status=None):
        now = datetime.now(timezone.utc).isoformat()
        if result is None:
            return {
                "row_number": row_number, "imdb_id": normalize_imdb_id(row.get("imdb_id")),
                "title": row.get("title_english") or row.get("title_spanish") or "",
                "type": normalize_media_type(row.get("type")) or row.get("type", ""),
                "release_year": row.get("release_year", ""), "director": row.get("director", ""),
                "status": status, "match_reason": "none", "processed_at": now,
            }
        return {
            "row_number": row_number,
            "imdb_id": normalize_imdb_id(row.get("imdb_id")),
            "title": row.get("title_english") or row.get("title_spanish") or "",
            "type": normalize_media_type(row.get("type")) or row.get("type", ""),
            "release_year": row.get("release_year", ""),
            "director": row.get("director", ""),
            "status": result.status,
            "tmdb_id": result.tmdb_id or "",
            "match_reason": result.match_reason,
            "matched_director": result.matched_director,
            "candidate_count": result.candidate_count,
            "candidate_tmdb_ids": "|".join(map(str, result.candidate_tmdb_ids)),
            "poster_found": bool(result.image),
            "synopsis_en_found": bool(result.synopsis),
            "synopsis_es_found": bool(result.synopsis_es),
            "synopsis_en_source": result.synopsis_en_source,
            "retry_count": result.retry_count,
            "request_count": result.request_count,
            "notes": result.notes,
            "processed_at": now,
        }
