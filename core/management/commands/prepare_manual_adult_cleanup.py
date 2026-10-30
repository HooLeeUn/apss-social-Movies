"""Prepare a second, explicitly reviewed batch. Never deletes or calls TMDb."""
import csv
import json
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from core.adult_cleanup import digest_file, metadata_fingerprint, target_identity
from core.management.commands.audit_adult_movies import FIELDS
from core.models import Movie


def read_csv(path, delimiter):
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file, delimiter=delimiter, strict=True)
        if not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise CommandError(f"Missing or duplicate CSV header: {path}")
        rows = list(reader)
        if any(None in row or None in row.values() for row in rows):
            raise CommandError(f"Malformed CSV row: {path}")
        return reader.fieldnames, rows


def triple(row):
    try:
        pk, tmdb_id = int(row["database_id"]), int(row["tmdb_id"])
        kind = row["type"]
        if pk <= 0 or tmdb_id <= 0 or kind not in {"movie", "series"}:
            raise ValueError
        return pk, kind, tmdb_id
    except (KeyError, TypeError, ValueError) as exc:
        raise CommandError("Invalid database_id/type/tmdb_id") from exc


def require(condition, message):
    if not condition:
        raise CommandError(message)


def snapshot(movie):
    return {key: getattr(movie, "pk" if key == "database_id" else key) for key in FIELDS}


def check_primary(actual, original, *, internal_id):
    fields = FIELDS if internal_id else [field for field in FIELDS if field != "database_id"]
    require(all(actual[field] == original[field] for field in fields),
            f"Identity changed since audit: {actual['database_id']} ({actual['type']}, {actual['tmdb_id']})")


class Command(BaseCommand):
    help = "Validate 337 ELIMINAR/8 CONSERVAR decisions from review_notes and prepare a database-bound second selection"

    def add_arguments(self, parser):
        parser.add_argument("--audit-dir", required=True, type=Path)
        parser.add_argument("--local-audit-dir", type=Path)
        parser.add_argument("--output-dir", required=True, type=Path)
        parser.add_argument("--target", required=True, choices=("local", "render"))
        parser.add_argument("--expected-database", required=True)
        parser.add_argument("--expected-host", required=True)
        parser.add_argument("--validate-only", action="store_true")
        parser.add_argument("--protect-corrected-525617", action="store_true",
                            help="Protect the user-confirmed correction: 812137, series locally/movie on Render; never eligible for deletion")
        parser.add_argument("--retain-audit-identity", action="append", type=int, default=[],
                            help="Explicitly acknowledge a retained CSV identity discrepancy; protect its audited identity without changing the CSV")

    def handle(self, *args, **options):
        try:
            self.prepare(options)
        except (OSError, UnicodeError, csv.Error, sqlite3.Error, ValueError, KeyError, TypeError) as exc:
            raise CommandError(str(exc)) from exc

    def prepare(self, options):
        audit = options["audit_dir"].resolve()
        output = options["output_dir"].resolve()
        require(not output.is_relative_to(Path(settings.BASE_DIR).resolve()), "Output must be outside the backend repository")
        if not options["validate_only"]:
            require(not output.exists(), "Output directory already exists; use --validate-only, never overwrite a previous batch")
        require(options["expected_database"] == ("socialapp" if options["target"] == "local" else "reccool_postgres_production"),
                "Target does not match the expected database name")
        identity = target_identity(options["expected_database"], options["expected_host"])
        reviewed_path = audit / "adult_movies_review_enriched.csv"
        original_path = audit / "adult_movies_final_review.csv"
        cache_path = audit / "audit.sqlite3"
        old_selection_path = audit / "adult_movies_final_selection.json"
        watched = [reviewed_path, original_path, cache_path, old_selection_path]
        source_hashes = {str(path): digest_file(path) for path in watched}
        fields, reviewed = read_csv(reviewed_path, ";")
        require({"database_id", "tmdb_id", "type", "review_notes", "reccool_title_english", "reccool_release_year"} <= set(fields),
                "Reviewed CSV is missing required fields")
        _, originals = read_csv(original_path, ",")
        require(len(reviewed) == len(originals) == 345, "Expected exactly 345 reviewed and original rows")
        decisions = Counter(row["review_notes"] for row in reviewed)
        require(decisions == {"ELIMINAR": 337, "CONSERVAR": 8}, f"Invalid manual decisions in review_notes: {dict(decisions)}")
        require(len({triple(row)[0] for row in reviewed}) == 345, "Duplicate database_id in reviewed CSV")
        original_map = {triple(row): row for row in originals}
        original_by_id = {triple(row)[0]: row for row in originals}
        require(len(original_map) == len(original_by_id) == 345 and set(original_by_id) == {triple(row)[0] for row in reviewed},
                "Reviewed identities differ from the 345 production candidates")
        raw_reviewed = reviewed
        reviewed, discrepancies = [], []
        acknowledged = set(options["retain_audit_identity"])
        corrected = options["protect_corrected_525617"]
        if corrected:
            special = next((row for row in raw_reviewed if triple(row)[0] == 525617), None)
            require(special is not None and special["review_notes"] == "CONSERVAR", "Correction exception is exclusively for CONSERVAR 525617")
            require(triple(original_by_id[525617]) == (525617, "series", 320832), "Unexpected original identity for corrected retained record")
            acknowledged.add(525617)
        for row in raw_reviewed:
            pk, kind, tmdb_id = triple(row)
            source = original_by_id[pk]
            if triple(row) != triple(source):
                require(row["review_notes"] == "CONSERVAR" and pk in acknowledged,
                        f"Reviewed identities differ from audit for {pk}; no selection generated")
                discrepancies.append({"database_id": pk, "reviewed_type": kind, "reviewed_tmdb_id": tmdb_id,
                                      "audited_type": source["type"], "audited_tmdb_id": int(source["tmdb_id"]), "decision": "CONSERVAR"})
                row = dict(row, type=source["type"], tmdb_id=source["tmdb_id"])
            reviewed.append(row)
        require(acknowledged == {row["database_id"] for row in discrepancies}, "Acknowledged retained IDs must match actual discrepancies exactly")
        require(len({triple(row)[1:] for row in reviewed}) == 345, "Ambiguous duplicate type/TMDb pair in manual review")
        old_selection = json.loads(old_selection_path.read_text(encoding="utf-8"))
        old_ids = {row["database_id"] for row in old_selection["movies"]}
        require(not old_ids.intersection({triple(row)[0] for row in reviewed}), "Second batch overlaps previous internal IDs")
        production_snapshots = {}
        with closing(sqlite3.connect(cache_path.as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("BEGIN")
            meta = dict(db.execute("SELECT key,value FROM metadata"))
            require(old_selection["database"] == meta["database"], "Production cache/selection provenance differs")
            if options["target"] == "render":
                require(identity == meta["database"], "Connection differs from the audited production database")
            for row in reviewed:
                pk, kind, tmdb_id = triple(row)
                item = db.execute("SELECT snapshot FROM candidates WHERE database_id=?", (pk,)).fetchone()
                require(item is not None, f"Production candidate missing from cache: {pk}")
                cached = json.loads(item[0])
                require(triple(cached) == triple(row), f"Cached identity differs: {pk}")
                source = original_map[triple(row)]
                require(cached["title_english"] == source["title_english"] == row["reccool_title_english"], f"Reviewed title altered: {pk}")
                year = str(cached["release_year"]) if cached["release_year"] is not None else ""
                require(source["release_year"] == year, f"Original year differs: {pk}")
                require(row["reccool_release_year"] == year or (not year and row["reccool_release_year"] == "[NO DISPONIBLE EN LA CACHÉ]"),
                        f"Reviewed ReCCool year altered: {pk}")
                response = db.execute("SELECT status,evidence FROM responses WHERE type=? AND id=?", (kind, tmdb_id)).fetchone()
                require(response is not None and response[0] == "complete", f"Completed API evidence missing: {pk}")
                data = json.loads(response[1])
                require(type(data.get("id")) is int and data["id"] == tmdb_id and data.get("adult") is True,
                        f"API evidence is inconsistent: {pk}")
                require(db.execute("SELECT 1 FROM exports WHERE type=? AND id=?", (kind, tmdb_id)).fetchone(),
                        f"Adult export membership missing: {pk}")
                production_snapshots[(kind, tmdb_id)] = cached

        local_snapshots = {}
        if options["target"] == "local":
            require(options["local_audit_dir"] is not None, "--local-audit-dir is required for local identity validation")
            local_cache = (options["local_audit_dir"] / "audit.sqlite3").resolve()
            local_selection_path = (options["local_audit_dir"] / "adult_movies_final_selection.json").resolve()
            watched += [local_cache, local_selection_path]
            source_hashes.update({str(path): digest_file(path) for path in (local_cache, local_selection_path)})
            with closing(sqlite3.connect(local_cache.as_uri() + "?mode=ro", uri=True)) as db:
                db.execute("BEGIN")
                require(dict(db.execute("SELECT key,value FROM metadata"))["database"] == identity, "Local cache belongs to a different database")
                old_local = json.loads(local_selection_path.read_text(encoding="utf-8"))
                require(old_local["database"] == identity, "Local previous selection belongs to another database")
                old_local_ids = {r["database_id"] for r in old_local["movies"]}
                for cached_row, in db.execute("SELECT snapshot FROM candidates"):
                    cached = json.loads(cached_row)
                    pair = (cached["type"], cached["tmdb_id"])
                    if (pair in production_snapshots and cached["database_id"] not in old_local_ids
                            and all(cached[field] == production_snapshots[pair][field] for field in FIELDS if field != "database_id")):
                        require(pair not in local_snapshots, f"Ambiguous audited local identity: {pair}")
                        local_snapshots[pair] = cached
            require(set(local_snapshots) == set(production_snapshots), "Local audit is missing some reviewed type/TMDb pairs")

        # Query all reviewed identities, including the eight protected records, only by the appropriate keys.
        if options["target"] == "render":
            movies = list(Movie.objects.filter(pk__in=[triple(row)[0] for row in reviewed]).order_by("pk"))
        else:
            condition = Q()
            for kind in ("movie", "series"):
                condition |= Q(type=kind, tmdb_id__in=[pair[1] for pair in production_snapshots if pair[0] == kind])
            if corrected:
                require(local_snapshots[("series", 320832)]["database_id"] == 525617, "Corrected local record ID differs from user confirmation")
                condition |= Q(pk=525617)
            movies = list(Movie.objects.filter(condition).order_by("pk"))
        current = {}
        for movie in movies:
            pair = (movie.type, movie.tmdb_id)
            special = corrected and movie.pk == 525617
            if special:
                pair = ("series", 320832)
            require(pair in production_snapshots and pair not in current, f"Missing, changed or ambiguous current identity: {pair}")
            original = production_snapshots[pair]
            if special:
                require(movie.title_english == "Power Rangers Jungle Fury" and movie.release_year == 2008,
                        "Corrected retained title/year differ from explicit user confirmation")
                original = dict(original, type="series" if options["target"] == "local" else "movie", tmdb_id=812137)
            check_primary(snapshot(movie), original, internal_id=options["target"] == "render")
            if options["target"] == "local":
                baseline = local_snapshots[pair]
                if special:
                    baseline = dict(baseline, type="series", tmdb_id=812137)
                check_primary(snapshot(movie), baseline, internal_id=True)
            current[pair] = movie
        require(set(current) == set(production_snapshots), "At least one reviewed production is missing or its identity changed")
        approved = [row for row in raw_reviewed if row["review_notes"] == "ELIMINAR"]
        retained = [row for row in raw_reviewed if row["review_notes"] == "CONSERVAR"]

        def protected_snapshot(row):
            movie = current[triple(original_by_id[triple(row)[0]])[1:]]
            return dict(snapshot(movie), identity_metadata_sha256=metadata_fingerprint(movie))

        selected = sorted((protected_snapshot(row) for row in approved), key=lambda row: row["database_id"])
        kept = sorted((protected_snapshot(row) for row in retained), key=lambda row: row["database_id"])
        require(not {r["database_id"] for r in selected}.intersection(r["database_id"] for r in kept), "Protected IDs entered deletion selection")
        require(all(digest_file(path) == source_hashes[str(path)] for path in watched), "An input changed during preparation")
        manifest = output / "adult_movies_final_to_delete.csv"
        selection_path = output / "adult_movies_final_selection.json"
        proof = {"version": 2, "database": identity, "movies": selected, "protected_movies": kept,
                 "manual_review": {"decision_column": "review_notes", "approved": 337, "retain": 8,
                                   "source_sha256": source_hashes[str(reviewed_path)], "source_files": source_hashes,
                                   "retained_identity_discrepancies": discrepancies}}
        if corrected:
            proof["manual_review"]["user_confirmed_correction"] = {
                "database_id": 525617, "decision": "CONSERVAR", "audited_type": "series", "audited_tmdb_id": 320832,
                "protected_tmdb_id": 812137, "local_type": "series", "render_type": "movie",
                "reason": "Intentional manual correction after audit, confirmed by user; local/production type difference remains unchanged",
            }
        if options["validate_only"]:
            existing = json.loads(selection_path.read_text(encoding="utf-8"))
            require(existing == dict(proof, manifest_sha256=digest_file(manifest)), "Selection/provenance changed since preparation")
            _, manifest_rows = read_csv(manifest, ",")
            require([int(row["database_id"]) for row in manifest_rows] == [row["database_id"] for row in selected], "Manifest IDs differ")
            for name, expected in (("approved_manual_review.csv", approved), ("retain_manual_review.csv", retained)):
                actual_fields, actual = read_csv(output / name, ";")
                require(actual_fields == fields and actual == expected, f"Manual decisions changed: {name}")
        else:
            output.mkdir(parents=True, exist_ok=False)
            for name, rows in (("approved_manual_review.csv", approved), ("retain_manual_review.csv", retained)):
                with (output / name).open("x", encoding="utf-8-sig", newline="") as file:
                    writer = csv.DictWriter(file, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_ALL)
                    writer.writeheader()
                    writer.writerows(rows)
            with manifest.open("x", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=["database_id"])
                writer.writeheader()
                writer.writerows({"database_id": row["database_id"]} for row in selected)
            proof["manifest_sha256"] = digest_file(manifest)
            with selection_path.open("x", encoding="utf-8") as file:
                json.dump(proof, file, ensure_ascii=False, indent=2)
        self.stdout.write(json.dumps({"target": options["target"], "approved": len(selected), "retained": len(kept),
                                      "mode": "validate-only" if options["validate_only"] else "prepared", "output": str(output)}, ensure_ascii=True))
