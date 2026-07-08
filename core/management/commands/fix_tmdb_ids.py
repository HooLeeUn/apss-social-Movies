import csv
import gzip
import json
import re
import time
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count
from core.models import Movie
from core.tmdb import TMDbServiceError, get_tmdb_json


class Command(BaseCommand):
    help = "Diagnostica, limpia y repara tmdb_id duplicados sospechosos sin tocar registros no afectados."

    EXPORT_PATTERNS = ("*.json.gz", "*.json", "*.jsonl.gz", "*.jsonl")
    DUPLICATES_REPORT = "tmdb_duplicates_report.csv"
    CLEAR_REPORT = "tmdb_clear_duplicates_report.csv"
    REPAIR_REPORT = "tmdb_repair_cleared_report.csv"
    AFFECTED_REPORTS = (CLEAR_REPORT, DUPLICATES_REPORT)
    DERIVED_FIELDS_TO_CLEAR = (
        "tmdb_id",
        "image",
        "synopsis",
        "synopsis_es",
        "trailer_es_key",
        "trailer_en_key",
        "trailer_checked_at",
    )
    REPORT_COLUMNS = (
        "movie_id",
        "title_en",
        "release_year",
        "director",
        "cast_members",
        "imdb_id",
        "old_tmdb_id",
        "status",
        "notes",
    )
    REPAIR_COLUMNS = (*REPORT_COLUMNS, "new_tmdb_id", "match_reason")

    def add_arguments(self, parser):
        parser.add_argument("--diagnose-duplicates", action="store_true", help="Genera tmdb_duplicates_report.csv.")
        parser.add_argument("--clear-duplicates", action="store_true", help="Limpia sólo el grupo duplicado sospechoso con --apply.")
        parser.add_argument("--repair-cleared", action="store_true", help="Reimporta tmdb_id sólo para movie_id del CSV afectado.")
        parser.add_argument("--apply", action="store_true", help="Guarda cambios. Por defecto sólo dry-run.")
        parser.add_argument("--exports-dir", type=str, default="tmdb_exports", help="Carpeta de exports locales.")
        parser.add_argument("--start-id", type=int, default=None, help="ID inicial opcional.")
        parser.add_argument("--limit", type=int, default=None, help="Cantidad máxima de registros a procesar.")
        parser.add_argument("--year-tolerance", type=int, default=2, help="Tolerancia de año para title_en + año.")
        parser.add_argument("--quiet-warnings", action="store_true", help="Reduce ruido en consola.")
        parser.add_argument("--affected-csv", type=str, default=None, help="CSV con movie_id a reparar; por defecto usa clear/duplicates report.")
        # Opciones heredadas aceptadas por compatibilidad; ya no activan reparación masiva.
        parser.add_argument("--use-local-exports", action="store_true", help="Compatibilidad: los exports locales se usan cuando aplica.")
        parser.add_argument("--only-duplicates", action="store_true", help="Compatibilidad: equivalente a --diagnose-duplicates.")
        parser.add_argument("--include-all", action="store_true", help="Obsoleto: no se permite tocar registros no afectados.")

    def handle(self, *args, **options):
        if options["include_all"]:
            raise CommandError("fix_tmdb_ids ya no permite --include-all: sólo procesa duplicados sospechosos.")
        phases = [options["diagnose_duplicates"] or options["only_duplicates"], options["clear_duplicates"], options["repair_cleared"]]
        if sum(bool(phase) for phase in phases) != 1:
            raise CommandError("Elige exactamente una fase: --diagnose-duplicates, --clear-duplicates o --repair-cleared.")

        exports_dir = self._resolve_path(options["exports_dir"])
        local_index = None
        if options["repair_cleared"]:
            local_index = self._build_local_index(exports_dir, quiet=options["quiet_warnings"])
            if not local_index["rows_loaded"] and not options["quiet_warnings"]:
                self.stdout.write(self.style.WARNING(f"No se cargaron exports locales desde {exports_dir}."))

        if options["repair_cleared"]:
            self._repair_cleared(options, local_index)
        elif options["clear_duplicates"]:
            self._clear_duplicates(options)
        else:
            self._diagnose_duplicates(options)

    def _diagnose_duplicates(self, options):
        movies = list(self._affected_duplicates_queryset(options))
        rows = [self._base_row(movie, "affected_duplicate", "tmdb_id duplicado sospechoso") for movie in movies]
        report_path = Path.cwd() / self.DUPLICATES_REPORT
        self._write_report(report_path, rows, self.REPORT_COLUMNS)
        self._print_summary("Diagnóstico de duplicados", options, movies, report_path, Counter(row["status"] for row in rows))

    def _clear_duplicates(self, options):
        movies = list(self._affected_duplicates_queryset(options))
        rows = []
        updates = []
        for movie in movies:
            rows.append(self._base_row(movie, "cleared" if options["apply"] else "would_clear", self._clear_notes()))
            if options["apply"]:
                movie.tmdb_id = None
                movie.image = None
                movie.synopsis = ""
                movie.synopsis_es = None
                movie.trailer_es_key = None
                movie.trailer_en_key = None
                movie.trailer_checked_at = None
                movie.tmdb_lookup_status = ""
                movie.tmdb_lookup_error = ""
                updates.append(movie)
        if updates:
            Movie.objects.bulk_update(
                updates,
                [*self.DERIVED_FIELDS_TO_CLEAR, "tmdb_lookup_status", "tmdb_lookup_error"],
                batch_size=500,
            )
        report_path = Path.cwd() / self.CLEAR_REPORT
        self._write_report(report_path, rows, self.REPORT_COLUMNS)
        self._print_summary("Limpieza de duplicados", options, movies, report_path, Counter(row["status"] for row in rows))

    def _repair_cleared(self, options, local_index):
        movie_ids = self._load_affected_movie_ids(options.get("affected_csv"))
        qs = Movie.objects.filter(id__in=movie_ids).only(
            "id", "title_english", "release_year", "director", "cast_members", "imdb_id", "tmdb_id", "type",
            "tmdb_lookup_status", "tmdb_lookup_error",
        ).order_by("id")
        if options["start_id"] is not None:
            qs = qs.filter(id__gte=options["start_id"])
        if options["limit"]:
            qs = qs[: options["limit"]]
        movies = list(qs)
        rows = []
        updates = []
        stats = Counter()
        for movie in movies:
            decision = self._repair_decision(movie, local_index, options)
            stats[decision["status"]] += 1
            rows.append({**self._base_row(movie, decision["status"], decision["notes"]), "new_tmdb_id": decision["tmdb_id"] or "", "match_reason": decision["reason"]})
            if options["apply"]:
                movie.tmdb_id = decision["tmdb_id"]
                movie.tmdb_lookup_status = "found" if decision["tmdb_id"] else "not_found"
                movie.tmdb_lookup_error = "" if decision["tmdb_id"] else decision["status"]
                updates.append(movie)
        if updates:
            Movie.objects.bulk_update(updates, ["tmdb_id", "tmdb_lookup_status", "tmdb_lookup_error"], batch_size=500)
        report_path = Path.cwd() / self.REPAIR_REPORT
        self._write_report(report_path, rows, self.REPAIR_COLUMNS)
        self._print_summary("Reparación de duplicados limpiados", options, movies, report_path, stats)

    def _affected_duplicates_queryset(self, options):
        duplicate_ids = self._get_duplicate_tmdb_ids()
        qs = Movie.objects.filter(tmdb_id__in=duplicate_ids or [-1]).only(
            "id", "title_english", "release_year", "director", "cast_members", "imdb_id", "tmdb_id", "type",
            "image", "synopsis", "synopsis_es", "trailer_es_key", "trailer_en_key", "trailer_checked_at",
            "tmdb_lookup_status", "tmdb_lookup_error",
        ).order_by("tmdb_id", "id")
        if options["start_id"] is not None:
            qs = qs.filter(id__gte=options["start_id"])
        if options["limit"]:
            qs = qs[: options["limit"]]
        return qs

    def _get_duplicate_tmdb_ids(self):
        return set(
            Movie.objects.exclude(tmdb_id__isnull=True)
            .values("tmdb_id")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
            .values_list("tmdb_id", flat=True)
        )

    def _repair_decision(self, movie, local_index, options):
        content_matches = lambda rows: self._filter_by_content_kind(movie, rows)
        imdb_id = self._normalize_imdb_id(movie.imdb_id)
        if imdb_id:
            matches = content_matches(local_index["by_imdb"].get(imdb_id, []))
            unique = self._unique_by_tmdb_id(matches)
            if len(unique) == 1:
                return self._repair_result(next(iter(unique)), "imdb_id", "repaired", "Coincidencia exacta por imdb_id en exports locales.")
            if len(unique) > 1:
                return self._repair_result(None, "imdb_id", "skipped_ambiguous", self._ambiguous_note(unique.values()))
            try:
                api_match = self._find_tmdb_by_imdb(movie, imdb_id)
            except TMDbServiceError as exc:
                api_match = None
                if not local_index["rows_loaded"]:
                    return self._repair_result(None, "imdb_id", "skipped_no_match", f"API TMDb no disponible: {exc}")
            if api_match:
                return self._repair_result(api_match, "imdb_id", "repaired", "Coincidencia exacta por imdb_id usando /find.")

        title = self._normalize_text(movie.title_english)
        title_matches = content_matches(local_index["by_title"].get(title, [])) if title else []
        if title_matches and movie.release_year:
            tolerance = max(0, options["year_tolerance"])
            exact = [row for row in title_matches if row.get("release_year") == movie.release_year]
            in_tolerance = [row for row in title_matches if row.get("release_year") and abs(row["release_year"] - movie.release_year) <= tolerance]
            for reason, candidates in (("title_year", exact), ("title_year_tolerance", in_tolerance)):
                unique = self._unique_by_tmdb_id(candidates)
                if len(unique) == 1:
                    return self._repair_result(next(iter(unique)), reason, "repaired", "Coincidencia única y confiable por title_en + año.")
                if len(unique) > 1:
                    return self._repair_result(None, reason, "skipped_ambiguous", self._ambiguous_note(unique.values()))

        first_director = self._first_director(movie.director)
        if title and first_director:
            try:
                api_matches = self._search_tmdb_by_title(movie, title, first_director)
            except TMDbServiceError as exc:
                return self._repair_result(None, "title_director", "skipped_no_match", f"API TMDb no disponible: {exc}")
            unique = self._unique_by_tmdb_id(api_matches)
            if len(unique) == 1:
                return self._repair_result(next(iter(unique)), "title_director", "repaired", "Coincidencia única por title_en + primer director completo.")
            if len(unique) > 1:
                return self._repair_result(None, "title_director", "skipped_ambiguous", self._ambiguous_note(unique.values()))

        return self._repair_result(None, "none", "skipped_no_match", "Sin coincidencia confiable; tmdb_id queda vacío.")

    def _repair_result(self, row, reason, status, notes):
        return {"tmdb_id": row.get("tmdb_id") if row else None, "reason": reason, "status": status, "notes": notes}

    def _find_tmdb_by_imdb(self, movie, imdb_id):
        content_kind = "tv" if movie.type == Movie.SERIES else "movie"
        results_key = "tv_results" if content_kind == "tv" else "movie_results"
        data = get_tmdb_json(f"/find/{imdb_id}", params={"external_source": "imdb_id"})
        candidates = data.get(results_key) or []
        if len(candidates) != 1:
            return None
        candidate = candidates[0]
        return {"tmdb_id": candidate.get("id"), "release_year": self._extract_release_year(candidate), "media_type": content_kind}

    def _search_tmdb_by_title(self, movie, normalized_title, first_director):
        content_kind = "tv" if movie.type == Movie.SERIES else "movie"
        data = get_tmdb_json(f"/search/{content_kind}", params={"query": movie.title_english, "include_adult": "false", "language": "en-US"})
        matches = []
        for candidate in data.get("results") or []:
            candidate_title = self._normalize_text(candidate.get("title") or candidate.get("name") or candidate.get("original_title") or candidate.get("original_name"))
            if candidate_title != normalized_title:
                continue
            tmdb_id = candidate.get("id")
            credits = get_tmdb_json(f"/{content_kind}/{tmdb_id}/credits")
            directors = self._director_names_from_credits(credits)
            if self._normalize_text(first_director) in {self._normalize_text(name) for name in directors}:
                matches.append({"tmdb_id": tmdb_id, "release_year": self._extract_release_year(candidate), "media_type": content_kind})
            time.sleep(0.05)
        return matches

    def _director_names_from_credits(self, credits):
        return [person.get("name") for person in credits.get("crew") or [] if person.get("job") == "Director" and person.get("name")]

    def _load_affected_movie_ids(self, csv_path):
        paths = [self._resolve_path(csv_path)] if csv_path else [Path.cwd() / name for name in self.AFFECTED_REPORTS]
        for path in paths:
            if path.exists():
                with path.open(newline="", encoding="utf-8") as fh:
                    return [int(row["movie_id"]) for row in csv.DictReader(fh) if row.get("movie_id")]
        raise CommandError("No se encontró CSV de afectados. Ejecuta primero --diagnose-duplicates o --clear-duplicates, o usa --affected-csv.")

    def _build_local_index(self, exports_dir, quiet=False):
        index = {"by_imdb": defaultdict(list), "by_title": defaultdict(list), "rows_loaded": 0, "files": []}
        if not exports_dir.exists():
            if not quiet:
                self.stdout.write(self.style.WARNING(f"No se encontraron exports locales en {exports_dir}"))
            return index
        files = []
        for pattern in self.EXPORT_PATTERNS:
            files.extend(exports_dir.rglob(pattern))
        index["files"] = [str(path) for path in sorted(set(files))]
        for file_path in sorted(set(files)):
            media_type = self._media_type_from_export_name(file_path.name)
            for raw in self._iter_json_rows(file_path):
                row = self._normalize_export_row(raw, media_type)
                if not row["tmdb_id"]:
                    continue
                index["rows_loaded"] += 1
                if row["imdb_id"]:
                    index["by_imdb"][row["imdb_id"]].append(row)
                for title in row["titles"]:
                    index["by_title"][title].append(row)
        return index

    def _iter_json_rows(self, file_path):
        opener = gzip.open if file_path.suffix == ".gz" else open
        with opener(file_path, "rt", encoding="utf-8") as fh:
            first = fh.read(1)
            if not first:
                return
            fh.seek(0)
            if first == "[":
                yield from (row for row in json.load(fh) if isinstance(row, dict))
            else:
                for line in fh:
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(row, dict):
                        yield row

    def _normalize_export_row(self, raw, media_type=None):
        tmdb_id = raw.get("tmdb_id") or raw.get("id") or raw.get("movie_id")
        try:
            tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        titles = {self._normalize_text(raw.get(key)) for key in ("title", "original_title", "name", "original_name")}
        return {
            "tmdb_id": tmdb_id,
            "imdb_id": self._normalize_imdb_id(raw.get("imdb_id") or raw.get("imdb")),
            "titles": {title for title in titles if title},
            "release_year": self._extract_release_year(raw),
            "media_type": media_type or raw.get("media_type"),
        }

    def _media_type_from_export_name(self, filename):
        if filename.startswith("movie_ids_"):
            return "movie"
        if filename.startswith("tv_series_ids_"):
            return "tv"
        return None

    def _extract_release_year(self, raw):
        match = re.match(r"^(\d{4})", str(raw.get("release_date") or raw.get("first_air_date") or raw.get("year") or ""))
        return int(match.group(1)) if match else None

    def _filter_by_content_kind(self, movie, matches):
        expected = "movie" if movie.type == Movie.MOVIE else "tv" if movie.type == Movie.SERIES else None
        return [row for row in matches if row.get("media_type") in (None, expected)] if expected else list(matches)

    def _unique_by_tmdb_id(self, rows):
        return {row["tmdb_id"]: row for row in rows if row.get("tmdb_id")}

    def _base_row(self, movie, status, notes):
        return {
            "movie_id": movie.id,
            "title_en": movie.title_english,
            "release_year": movie.release_year,
            "director": movie.director,
            "cast_members": movie.cast_members,
            "imdb_id": movie.imdb_id,
            "old_tmdb_id": movie.tmdb_id,
            "status": status,
            "notes": notes,
        }

    def _write_report(self, path, rows, columns):
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)

    def _print_summary(self, title, options, movies, report_path, stats):
        self.stdout.write(self.style.SUCCESS(f"{title} finalizado."))
        self.stdout.write(f"Modo: {'APPLY' if options['apply'] else 'DRY-RUN'}")
        self.stdout.write(f"Registros del grupo afectado procesados: {len(movies)}")
        for key, value in sorted(stats.items()):
            self.stdout.write(f"{key}: {value}")
        self.stdout.write(f"Campos limpiados por --clear-duplicates --apply: {', '.join(self.DERIVED_FIELDS_TO_CLEAR)}")
        self.stdout.write(f"Reporte CSV: {report_path}")
        if options.get("repair_cleared"):
            self.stdout.write("Los registros sin coincidencia confiable quedan con tmdb_id vacío.")

    def _clear_notes(self):
        return "Se limpian sólo campos contaminables del grupo duplicado sospechoso: " + ", ".join(self.DERIVED_FIELDS_TO_CLEAR)

    def _resolve_path(self, value):
        path = Path(value).expanduser()
        return path.resolve() if path.is_absolute() else (Path.cwd() / path).resolve()

    def _first_director(self, value):
        return next((part.strip() for part in re.split(r"[,;|\n]+", value or "") if part.strip()), "")

    def _normalize_imdb_id(self, value):
        value = str(value or "").strip().lower()
        return value if re.fullmatch(r"tt\d+", value) else ""

    def _normalize_text(self, value):
        normalized = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", " ", normalized.casefold()).strip()

    def _ambiguous_note(self, matches):
        ids = sorted({str(row.get("tmdb_id")) for row in matches if row.get("tmdb_id")})
        return "Coincidencia ambigua: " + ",".join(ids[:10])
