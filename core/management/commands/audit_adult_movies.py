"""Read-only catalog audit. SQLite stores local evidence, never catalog changes."""
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import threading
import time
import unicodedata
from contextlib import closing, contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from django.core.management.base import BaseCommand, CommandError
from django.db import connections

from core.models import Movie
from core.tmdb import TMDbServiceError, get_tmdb_json


FIELDS = ["database_id", "tmdb_id", "type", "title_english", "release_year"]
DETAIL_FIELDS = FIELDS + ["adult", "validation_status"]
REVIEW_FIELDS = DETAIL_FIELDS + ["reason", "api_evidence"]


def normalize_title(value):
    return "".join(c for c in unicodedata.normalize("NFKD", value.casefold())
                   if c.isalnum())


def classify(movie, data):
    """Fail closed: missing evidence or identity differences require review."""
    if not isinstance(data, dict) or type(data.get("id")) is not int or data["id"] != movie["tmdb_id"]:
        return "review", "API ID missing or different"
    if type(data.get("adult")) is not bool:
        return "review", "API adult must be a boolean"
    if data["adult"] is False:
        return "review", "Export/API contradiction: adult=false"
    is_movie = movie["type"] == Movie.MOVIE
    title_key, original_key, date_key = (
        ("title", "original_title", "release_date") if is_movie
        else ("name", "original_name", "first_air_date")
    )
    titles = [normalize_title(data[k]) for k in (title_key, original_key)
              if isinstance(data.get(k), str) and data[k].strip()]
    title = normalize_title(movie["title_english"] or "")
    if not title or title not in titles:
        return "review", "Title differs or is missing; manual review required"
    date = data.get(date_key)
    try:
        year = datetime.strptime(date, "%Y-%m-%d").year
    except (ValueError, TypeError):
        return "review", "API date missing or invalid"
    if movie["release_year"] is None or movie["release_year"] != year:
        return "review", "Year differs or is missing; manual review required"
    return "confirmed", "Export, type, API adult and identity agree"


@contextmanager
def local_lock(directory):
    path = directory / "audit.lock"
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise CommandError(f"Audit locked: {path}. Check for another process before removing a stale lock.") from exc
    try:
        os.write(fd, str(os.getpid()).encode("ascii"))
        os.close(fd)
        yield
    finally:
        path.unlink(missing_ok=True)


def database_identity(alias):
    config = connections[alias].settings_dict
    safe = {k: str(config.get(k, "")) for k in ("ENGINE", "HOST", "PORT", "NAME")}
    return hashlib.sha256(json.dumps(safe, sort_keys=True).encode()).hexdigest()


class Command(BaseCommand):
    help = "Audit adult TMDb exports against Movie, then verify candidates via API. Never deletes or updates catalog rows."

    def add_arguments(self, parser):
        parser.add_argument("phase", choices=("audit", "verify"))
        parser.add_argument("--movie-export")
        parser.add_argument("--tv-export")
        parser.add_argument("--output-dir", default="tmdb_exports/adult/audit")
        parser.add_argument("--database", default="default")
        parser.add_argument("--batch-size", type=int, default=1000)
        parser.add_argument("--interval", type=float, default=0.3)
        parser.add_argument("--attempts", type=int, default=3)
        parser.add_argument("--workers", type=int, default=1)
        parser.add_argument("--response-cache", help="Reuse completed type/ID API evidence from an earlier audit SQLite file")

    def handle(self, *args, **options):
        if options["batch_size"] < 1 or options["attempts"] < 1 or not 1 <= options["workers"] <= 8:
            raise CommandError("batch-size and attempts must be positive; workers must be between 1 and 8")
        if not math.isfinite(options["interval"]) or options["interval"] < 0:
            raise CommandError("interval must be finite and nonnegative")
        directory = Path(options["output_dir"])
        directory.mkdir(parents=True, exist_ok=True)
        state = directory / "audit.sqlite3"
        try:
            with local_lock(directory):
                if options["phase"] == "audit":
                    self.audit(state, directory, options)
                else:
                    if not state.exists():
                        raise CommandError("Run audit first")
                    with closing(sqlite3.connect(state)) as db:
                        db.row_factory = sqlite3.Row
                        meta = dict(db.execute("SELECT key, value FROM metadata"))
                        if meta.get("database") != database_identity(options["database"]):
                            raise CommandError("State belongs to a different database")
                        self.verify(db, directory, options)
        except (OSError, EOFError, UnicodeError, ValueError, sqlite3.Error) as exc:
            raise CommandError(str(exc)) from exc

    def audit(self, state, directory, options):
        if state.exists():
            raise CommandError("Audit state already exists; use verify to resume or a new output directory")
        if not options["movie_export"] or not options["tv_export"]:
            raise CommandError("Both --movie-export and --tv-export are required")
        temp = directory / "audit.building.sqlite3"
        temp.unlink(missing_ok=True)
        try:
            with closing(sqlite3.connect(temp)) as db:
                db.execute("CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
                db.execute("CREATE TABLE exports (type TEXT, id INTEGER, PRIMARY KEY(type,id))")
                db.execute("CREATE TABLE candidates (database_id INTEGER PRIMARY KEY, snapshot TEXT NOT NULL)")
                db.execute("CREATE TABLE responses (type TEXT, id INTEGER, status TEXT, evidence TEXT, checked_at TEXT, PRIMARY KEY(type,id))")
                for kind, path in ((Movie.MOVIE, options["movie_export"]), (Movie.SERIES, options["tv_export"])):
                    total = 0
                    digest = hashlib.sha256()
                    # Validate the entire gzip (including CRC) before querying PostgreSQL.
                    with gzip.open(path, "rb") as source:
                        for line_number, line in enumerate(source, 1):
                            digest.update(line)
                            try:
                                row = json.loads(line)
                                if not isinstance(row, dict) or type(row.get("id")) is not int or not 0 < row["id"] <= 2147483647:
                                    raise ValueError("expected a positive integer id")
                                if "adult" in row and row["adult"] is not True:
                                    raise ValueError("adult export contains a contradictory adult flag")
                            except (ValueError, UnicodeError) as exc:
                                raise CommandError(f"{path}:{line_number}: {exc}") from exc
                            db.execute("INSERT OR IGNORE INTO exports VALUES (?,?)", (kind, row["id"]))
                            total += 1
                    if not total:
                        raise CommandError(f"Empty export: {path}")
                    db.execute("INSERT INTO metadata VALUES (?,?)", (kind + "_export", json.dumps({
                        "path": str(Path(path).resolve()), "sha256_jsonl": digest.hexdigest(), "lines": total,
                    })))
                    self.stdout.write(f"Validated {kind}: {total} export rows")
                # Bounded IN queries use tmdb_id's existing index; no full catalog load.
                for kind in (Movie.MOVIE, Movie.SERIES):
                    cursor = db.execute("SELECT id FROM exports WHERE type=? ORDER BY id", (kind,))
                    while batch := cursor.fetchmany(options["batch_size"]):
                        ids = [r[0] for r in batch]
                        rows = Movie.objects.using(options["database"]).filter(
                            type=kind, tmdb_id__in=ids,
                        ).order_by().values_list("id", "tmdb_id", "type", "title_english", "release_year")
                        for row in rows.iterator(chunk_size=options["batch_size"]):
                            snapshot = dict(zip(FIELDS, row))
                            db.execute("INSERT OR IGNORE INTO candidates VALUES (?,?)",
                                       (row[0], json.dumps(snapshot, ensure_ascii=False)))
                if options.get("response_cache"):
                    cache_path = Path(options["response_cache"])
                    if not cache_path.is_file():
                        raise CommandError("Response cache does not exist")
                    with closing(sqlite3.connect(cache_path)) as cache:
                        for response in cache.execute("SELECT type,id,status,evidence,checked_at FROM responses WHERE status='complete'"):
                            # IDs and type identify TMDb resources globally; internal database IDs never transfer.
                            db.execute("INSERT OR IGNORE INTO responses VALUES (?,?,?,?,?)", response)
                db.execute("INSERT INTO metadata VALUES ('database',?)", (database_identity(options["database"]),))
                db.execute("INSERT INTO metadata VALUES ('created_at',?)", (datetime.now(timezone.utc).isoformat(),))
                db.commit()
            temp.replace(state)
        except BaseException:
            temp.unlink(missing_ok=True)
            raise
        with closing(sqlite3.connect(state)) as db:
            count = db.execute("SELECT count(*) FROM candidates").fetchone()[0]
            self.write_csv(directory / "adult_movies_candidates.csv", FIELDS,
                           (json.loads(r[0]) for r in db.execute("SELECT snapshot FROM candidates ORDER BY database_id")))
        self.stdout.write(f"Candidates: {count}. State: {state}. No catalog changes; run verify next.")

    def verify(self, db, directory, options):
        # An interrupted verification must not leave an older deletion manifest usable.
        (directory / "adult_movies_to_delete.csv").unlink(missing_ok=True)
        pending = db.execute("""SELECT DISTINCT e.type,e.id FROM candidates c
            JOIN exports e ON e.type=json_extract(c.snapshot,'$.type') AND e.id=json_extract(c.snapshot,'$.tmdb_id')
            LEFT JOIN responses r ON r.type=e.type AND r.id=e.id
            WHERE r.status IS NULL OR r.status='error' ORDER BY e.type,e.id""")
        next_request = [0.0]
        rate_lock = threading.Lock()

        def fetch(key):
            kind, tmdb_id = key
            data = None
            status = "error"
            for attempt in range(options["attempts"]):
                # All workers share one request rate and Retry-After cooldown.
                with rate_lock:
                    time.sleep(max(0, next_request[0] - time.monotonic()))
                    next_request[0] = time.monotonic() + options["interval"]
                try:
                    data = get_tmdb_json(f"/{'movie' if kind == Movie.MOVIE else 'tv'}/{tmdb_id}", params={"language": "en-US"})
                    status = "complete"
                    break
                except TMDbServiceError as exc:
                    data = {"error": str(exc), "status_code": exc.status_code}
                    if not exc.retryable or attempt + 1 == options["attempts"]:
                        break
                    try:
                        delay = float(exc.retry_after)
                        if not math.isfinite(delay) or delay < 0:
                            raise ValueError
                    except (TypeError, ValueError):
                        delay = 2 ** attempt
                    with rate_lock:
                        next_request[0] = max(next_request[0], time.monotonic() + delay)
            return kind, tmdb_id, status, json.dumps(data, ensure_ascii=False), datetime.now(timezone.utc).isoformat()

        completed = 0
        with ThreadPoolExecutor(max_workers=options["workers"]) as executor:
            # Only one bounded group is scheduled; SQLite writes stay in the main thread.
            while batch := pending.fetchmany(options["workers"]):
                for result in executor.map(fetch, batch):
                    db.execute("INSERT OR REPLACE INTO responses VALUES (?,?,?,?,?)", result)
                    db.commit()  # Resume after each completed key, shared by duplicate Movie rows.
                    completed += 1
                    if completed % 100 == 0:
                        self.stdout.write(f"API keys checked this run: {completed}")
        self.export_results(db, directory, options)

    def export_results(self, db, directory, options):
        confirmed_path = directory / "adult_movies_confirmed.csv"
        review_path = directory / "adult_movies_review.csv"
        manifest_path = directory / "adult_movies_to_delete.csv"
        paths = (confirmed_path, review_path, manifest_path)
        files = []
        confirmed = reviewed = 0
        try:
            for path in paths:
                files.append(path.with_suffix(".csv.tmp").open("w", encoding="utf-8", newline=""))
            writers = [csv.DictWriter(file, fieldnames=fields) for file, fields in zip(
                files, (DETAIL_FIELDS, REVIEW_FIELDS, ["database_id"]))]
            for writer in writers:
                writer.writeheader()
            cursor = db.execute("SELECT database_id,snapshot FROM candidates ORDER BY database_id")
            while batch := cursor.fetchmany(options["batch_size"]):
                current = {r[0]: dict(zip(FIELDS, r)) for r in Movie.objects.using(options["database"]).filter(
                    pk__in=[r[0] for r in batch],
                ).order_by().values_list("id", "tmdb_id", "type", "title_english", "release_year")}
                for database_id, snapshot in batch:
                    movie = json.loads(snapshot)
                    response = db.execute("SELECT status,evidence FROM responses WHERE type=? AND id=?",
                                          (movie["type"], movie["tmdb_id"])).fetchone()
                    data = json.loads(response["evidence"]) if response else {}
                    if current.get(database_id) != movie:
                        status, reason = "review", "Catalog row changed or disappeared since audit"
                    elif response is None or response["status"] == "error":
                        status, reason = "error", "API validation unavailable; never interpreted as adult=false"
                    else:
                        status, reason = classify(movie, data)
                    row = dict(movie, adult=data.get("adult", "") if isinstance(data, dict) else "", validation_status=status)
                    if status == "confirmed":
                        writers[0].writerow(row)
                        writers[2].writerow({"database_id": database_id})
                        confirmed += 1
                    else:
                        writers[1].writerow(dict(row, reason=reason, api_evidence=json.dumps(data, ensure_ascii=False)))
                        reviewed += 1
        finally:
            for file in files:
                file.close()
        # Publish manifest last, only after all rows were classified successfully.
        for path in paths:
            path.with_suffix(".csv.tmp").replace(path)
        self.stdout.write(f"Confirmed: {confirmed}; review/errors: {reviewed}; CSV directory: {directory}")

    @staticmethod
    def write_csv(path, fields, rows):
        temp = path.with_suffix(".csv.tmp")
        with temp.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        temp.replace(path)
