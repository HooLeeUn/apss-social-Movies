import csv
import gzip
import json
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q

from core.models import Movie


class Command(BaseCommand):
    help = "Diagnostica y repara tmdb_id duplicados de Movie usando exports locales de TMDb."

    EXPORT_PATTERNS = ("*.json.gz", "*.json", "*.jsonl.gz", "*.jsonl")
    REPORT_FILENAME = "tmdb_id_fix_report.csv"
    DERIVED_FIELDS_TO_CLEAR = (
        "image",
        "trailer_es_key",
        "trailer_en_key",
        "trailer_checked_at",
        "synopsis",
        "synopsis_es",
    )
    REPORT_COLUMNS = (
        "movie_id",
        "title_en",
        "release_year",
        "director",
        "cast_members",
        "imdb_id",
        "old_tmdb_id",
        "new_tmdb_id",
        "match_reason",
        "status",
        "derived_fields_cleared",
        "notes",
    )

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Guarda los cambios. Por defecto sólo dry-run.")
        parser.add_argument("--start-id", type=int, default=None, help="ID inicial opcional.")
        parser.add_argument("--limit", type=int, default=None, help="Cantidad máxima de registros a revisar.")
        parser.add_argument("--exports-dir", type=str, default="tmdb_exports", help="Carpeta de exports locales.")
        parser.add_argument("--use-local-exports", action="store_true", help="Compatibilidad explícita: este comando siempre usa exports locales.")
        parser.add_argument("--only-duplicates", action="store_true", help="Revisar sólo tmdb_id duplicados sospechosos.")
        parser.add_argument("--include-all", action="store_true", help="Revisar también registros no duplicados.")
        parser.add_argument("--quiet-warnings", action="store_true", help="Reduce ruido en consola.")

    def handle(self, *args, **options):
        if options["only_duplicates"] and options["include_all"]:
            raise CommandError("Usa --only-duplicates o --include-all, no ambos.")

        exports_dir = Path(options["exports_dir"])
        local_index = self._build_local_index(exports_dir, quiet=options["quiet_warnings"])
        duplicate_tmdb_ids = self._get_duplicate_tmdb_ids()
        movies = list(self._get_movies_queryset(options, duplicate_tmdb_ids))
        if not options["apply"] and not options["quiet_warnings"]:
            self._print_dry_run_diagnostics(local_index, movies)
        report_path = Path.cwd() / self.REPORT_FILENAME

        stats = Counter()
        rows = []
        updates = []

        if not options["quiet_warnings"]:
            self._print_duplicate_summary(duplicate_tmdb_ids)

        for movie in movies:
            is_duplicate = movie.tmdb_id in duplicate_tmdb_ids if movie.tmdb_id else False
            decision = self._decide_movie(movie, local_index, is_duplicate)
            stats[decision["status"]] += 1
            rows.append(self._build_report_row(movie, decision))

            if options["apply"] and decision["status"] == "updated":
                self._apply_decision(movie, decision)
                updates.append(movie)

        if updates:
            Movie.objects.bulk_update(
                updates,
                ["tmdb_id", *self.DERIVED_FIELDS_TO_CLEAR, "tmdb_lookup_status", "tmdb_lookup_error"],
                batch_size=500,
            )

        self._write_report(report_path, rows)
        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Modo: {'APPLY' if options['apply'] else 'DRY-RUN'}")
        self.stdout.write(f"Registros revisados: {len(movies)}")
        self.stdout.write(f"tmdb_id duplicados sospechosos: {len(duplicate_tmdb_ids)}")
        self.stdout.write(f"updated: {stats['updated']}")
        self.stdout.write(f"unchanged: {stats['unchanged']}")
        self.stdout.write(f"skipped_ambiguous: {stats['skipped_ambiguous']}")
        self.stdout.write(f"skipped_no_match: {stats['skipped_no_match']}")
        self.stdout.write(f"skipped_not_duplicate: {stats['skipped_not_duplicate']}")
        self.stdout.write(f"Reporte CSV: {report_path}")

    def _get_duplicate_tmdb_ids(self):
        return set(
            Movie.objects.exclude(tmdb_id__isnull=True)
            .values("tmdb_id")
            .annotate(total=Count("id"))
            .filter(total__gt=1)
            .values_list("tmdb_id", flat=True)
        )

    def _get_movies_queryset(self, options, duplicate_tmdb_ids):
        qs = Movie.objects.all().only(
            "id", "title_english", "release_year", "director", "cast_members",
            "imdb_id", "tmdb_id", "type", "image", "trailer_es_key", "trailer_en_key",
            "trailer_checked_at", "synopsis", "synopsis_es", "tmdb_lookup_status", "tmdb_lookup_error",
        ).order_by("id")
        if options["start_id"] is not None:
            qs = qs.filter(id__gte=options["start_id"])
        if options["only_duplicates"] or not options["include_all"]:
            qs = qs.filter(tmdb_id__in=duplicate_tmdb_ids or [-1])
        else:
            qs = qs.filter(Q(tmdb_id__isnull=False) | Q(imdb_id__isnull=False) | Q(title_english__isnull=False))
        if options["limit"]:
            qs = qs[: options["limit"]]
        return qs

    def _decide_movie(self, movie, local_index, is_duplicate):
        if not is_duplicate:
            return self._decision(None, "none", "skipped_not_duplicate", "No tiene tmdb_id duplicado sospechoso.")

        imdb_id = self._normalize_imdb_id(movie.imdb_id)
        if imdb_id:
            imdb_matches = local_index["by_imdb"].get(imdb_id, [])
            if len(imdb_matches) == 1:
                return self._candidate_decision(movie, imdb_matches[0], "imdb_id")
            if len(imdb_matches) > 1:
                return self._decision(None, "imdb_id", "skipped_ambiguous", self._ambiguous_note(imdb_matches))

        title = self._normalize_text(movie.title_english)
        title_matches = self._filter_by_content_kind(movie, local_index["by_title"].get(title, [])) if title else []
        first_director = self._first_director(movie.director)
        if title_matches and first_director:
            director_matches = [row for row in title_matches if self._normalize_text(first_director) in row["director_names"]]
            unique_by_id = {row["tmdb_id"]: row for row in director_matches if row.get("tmdb_id")}
            if len(unique_by_id) == 1:
                return self._candidate_decision(movie, next(iter(unique_by_id.values())), "title_director")
            if len(unique_by_id) > 1:
                return self._decision(None, "title_director", "skipped_ambiguous", self._ambiguous_note(unique_by_id.values()))

        if title_matches and movie.release_year:
            year_matches = [row for row in title_matches if row.get("release_year") == movie.release_year]
            unique_by_id = {row["tmdb_id"]: row for row in year_matches if row.get("tmdb_id")}
            if len(unique_by_id) == 1:
                return self._candidate_decision(movie, next(iter(unique_by_id.values())), "title_year")
            if len(unique_by_id) > 1:
                return self._decision(None, "title_year", "skipped_ambiguous", self._ambiguous_note(unique_by_id.values()))

        return self._decision(None, "none", "skipped_no_match", "Sin coincidencia local confiable por imdb_id, título + director ni título + año.")

    def _candidate_decision(self, movie, candidate, reason):
        new_tmdb_id = candidate.get("tmdb_id")
        if not new_tmdb_id:
            return self._decision(None, reason, "skipped_no_match", "La coincidencia no contiene tmdb_id.")
        if movie.tmdb_id == new_tmdb_id:
            return self._decision(new_tmdb_id, reason, "unchanged", "El tmdb_id actual ya coincide con el export local.")
        return self._decision(new_tmdb_id, reason, "updated", "Se corrige tmdb_id y se limpian campos derivados contaminables.")

    def _decision(self, new_tmdb_id, match_reason, status, notes):
        return {"new_tmdb_id": new_tmdb_id, "match_reason": match_reason, "status": status, "notes": notes}

    def _apply_decision(self, movie, decision):
        movie.tmdb_id = decision["new_tmdb_id"]
        movie.image = None
        movie.trailer_es_key = None
        movie.trailer_en_key = None
        movie.trailer_checked_at = None
        movie.synopsis = ""
        movie.synopsis_es = None
        movie.tmdb_lookup_status = "found"
        movie.tmdb_lookup_error = ""

    def _build_report_row(self, movie, decision):
        cleared = ",".join(self.DERIVED_FIELDS_TO_CLEAR) if decision["status"] == "updated" else ""
        return {
            "movie_id": movie.id,
            "title_en": movie.title_english,
            "release_year": movie.release_year,
            "director": movie.director,
            "cast_members": movie.cast_members,
            "imdb_id": movie.imdb_id,
            "old_tmdb_id": movie.tmdb_id,
            "new_tmdb_id": decision["new_tmdb_id"] or "",
            "match_reason": decision["match_reason"],
            "status": decision["status"],
            "derived_fields_cleared": cleared,
            "notes": decision["notes"],
        }

    def _write_report(self, path, rows):
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=self.REPORT_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def _build_local_index(self, exports_dir, quiet=False):
        if not exports_dir.exists():
            raise CommandError(f"No existe exports-dir: {exports_dir}")
        index = {"by_imdb": defaultdict(list), "by_title": defaultdict(list), "files": [], "rows_loaded": 0}
        files = []
        for pattern in self.EXPORT_PATTERNS:
            files.extend(exports_dir.glob(pattern))
        for file_path in sorted(set(files)):
            index["files"].append(str(file_path))
            media_type = self._media_type_from_export_name(file_path.name)
            for raw in self._iter_json_rows(file_path):
                row = self._normalize_export_row(raw, media_type=media_type)
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
                for row in json.load(fh):
                    if isinstance(row, dict):
                        yield row
            else:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(row, dict):
                        yield row

    def _normalize_export_row(self, raw, media_type=None):
        tmdb_id = raw.get("tmdb_id") or raw.get("id") or raw.get("movie_id")
        titles = {
            self._normalize_text(raw.get(key))
            for key in ("title", "original_title", "name", "original_name")
        }
        directors = self._extract_director_names(raw)
        try:
            tmdb_id = int(tmdb_id) if tmdb_id not in (None, "") else None
        except (TypeError, ValueError):
            tmdb_id = None
        return {
            "tmdb_id": tmdb_id,
            "imdb_id": self._normalize_imdb_id(raw.get("imdb_id") or raw.get("imdb")),
            "titles": {title for title in titles if title},
            "director_names": {self._normalize_text(name) for name in directors if self._normalize_text(name)},
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
        value = raw.get("release_date") or raw.get("first_air_date") or raw.get("year") or ""
        match = re.match(r"^(\d{4})", str(value))
        return int(match.group(1)) if match else None

    def _filter_by_content_kind(self, movie, matches):
        expected = None
        if movie.type == Movie.MOVIE:
            expected = "movie"
        elif movie.type == Movie.SERIES:
            expected = "tv"
        if not expected:
            return matches
        return [row for row in matches if row.get("media_type") in (None, expected)]

    def _print_dry_run_diagnostics(self, local_index, movies):
        self.stdout.write("Diagnóstico dry-run:")
        self.stdout.write(f"  archivos encontrados en tmdb_exports: {len(local_index['files'])}")
        for file_path in local_index["files"][:20]:
            self.stdout.write(f"    {file_path}")
        if len(local_index["files"]) > 20:
            self.stdout.write(f"    ... {len(local_index['files']) - 20} más")
        self.stdout.write(f"  registros locales cargados: {local_index['rows_loaded']}")
        self.stdout.write(f"  imdb_id indexados: {len(local_index['by_imdb'])}")
        self.stdout.write(f"  títulos indexados: {len(local_index['by_title'])}")
        tt_matches = local_index["by_imdb"].get("tt0120338", [])
        self.stdout.write(f"  resultado de buscar tt0120338: {self._diagnostic_matches(tt_matches)}")
        titanic_matches = local_index["by_title"].get(self._normalize_text("Titanic"), [])
        titanic_1997 = [row for row in titanic_matches if row.get("release_year") == 1997]
        self.stdout.write(f"  resultado de buscar título Titanic año 1997: {self._diagnostic_matches(titanic_1997)}")
        self.stdout.write(f"  registros locales cargados para revisar: {len(movies)}")

    def _diagnostic_matches(self, matches):
        if not matches:
            return "0 coincidencias"
        preview = ", ".join(
            f"tmdb_id={row.get('tmdb_id')} year={row.get('release_year')} media_type={row.get('media_type') or '-'}"
            for row in matches[:5]
        )
        suffix = f" ... +{len(matches) - 5}" if len(matches) > 5 else ""
        return f"{len(matches)} coincidencia(s): {preview}{suffix}"

    def _extract_director_names(self, raw):
        values = []
        for key in ("directors", "director", "crew"):
            item = raw.get(key)
            if isinstance(item, str):
                values.extend([part.strip() for part in re.split(r"[,;|\n]+", item) if part.strip()])
            elif isinstance(item, list):
                for person in item:
                    if isinstance(person, str):
                        values.append(person)
                    elif isinstance(person, dict) and (person.get("job") == "Director" or key in ("director", "directors")):
                        name = person.get("name") or person.get("original_name")
                        if name:
                            values.append(name)
        return values

    def _first_director(self, value):
        return next((part.strip() for part in (value or "").split(",") if part.strip()), "")

    def _normalize_imdb_id(self, value):
        value = str(value or "").strip().lower()
        return value if re.fullmatch(r"tt\d+", value) else ""

    def _normalize_text(self, value):
        normalized = unicodedata.normalize("NFKD", str(value or "")).encode("ascii", "ignore").decode("ascii")
        return re.sub(r"[^a-z0-9]+", " ", normalized.casefold()).strip()

    def _ambiguous_note(self, matches):
        ids = sorted({str(row.get("tmdb_id")) for row in matches if row.get("tmdb_id")})
        return "Coincidencia ambigua en exports locales: " + ",".join(ids[:10])

    def _print_duplicate_summary(self, duplicate_tmdb_ids):
        self.stdout.write("tmdb_id duplicados sospechosos:")
        duplicates = (
            Movie.objects.filter(tmdb_id__in=duplicate_tmdb_ids)
            .values("tmdb_id")
            .annotate(total=Count("id"))
            .order_by("tmdb_id")
        )
        for item in duplicates:
            self.stdout.write(f"  tmdb_id={item['tmdb_id']} registros={item['total']}")
        affected = Movie.objects.filter(tmdb_id__in=duplicate_tmdb_ids).order_by("tmdb_id", "id").values(
            "id", "title_english", "release_year", "director", "cast_members", "imdb_id", "tmdb_id"
        )
        for row in affected:
            self.stdout.write(
                "  Movie(id={id}, title_en={title_english!r}, release_year={release_year}, "
                "director={director!r}, cast_members={cast_members!r}, imdb_id={imdb_id!r}, tmdb_id={tmdb_id})".format(**row)
            )
