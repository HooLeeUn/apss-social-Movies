"""Disk-bounded checkpoint and recoverable concatenated-GZIP output journal."""
from contextlib import contextmanager
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from core.tmdb_enrichment_client import strict_json

STATUSES = ("accepted", "excluded", "incomplete", "invalid")
OUTPUTS = (*STATUSES, "duplicates")


class StorageError(Exception):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_bytes(path, data):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


@contextmanager
def run_lock(directory):
    """OS lock releases even on process death; no stale PID-file override needed."""
    stream = (Path(directory) / "run.lock").open("a+b")
    if os.name == "nt":
        import msvcrt
        if stream.seek(0, 2) == 0:
            stream.write(b"0")
            stream.flush()
        stream.seek(0)
        try:
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            stream.close()
            raise StorageError("Run is already locked") from None
    else:
        import fcntl
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            stream.close()
            raise StorageError("Run is already locked") from None
    try:
        yield
    finally:
        if os.name == "nt":
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
        stream.close()


class CheckpointStore:
    def __init__(self, directory, identity, configuration, *, resume=False, now=utc_now):
        self.directory, self.now = Path(directory), now
        self.connection = None
        self.hashes = {}
        db_path = self.directory / "checkpoint.sqlite3"
        if resume and not db_path.is_file():
            raise StorageError("Resume requires an existing checkpoint")
        if not resume and db_path.exists():
            raise StorageError("Checkpoint already exists")
        # This is a local SQLite file, never a Django/remote database connection.
        initializing = db_path.with_name(db_path.name + ".initializing")
        if not resume and initializing.exists():
            raise StorageError("Unfinished initialization exists; use a new run directory")
        self.connection = sqlite3.connect(db_path if resume else initializing)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA synchronous=FULL")
        try:
            if not resume:
                self._initialize(identity, configuration)
                self.close()
                os.replace(initializing, db_path)
                self.connection = sqlite3.connect(db_path)
                self.connection.row_factory = sqlite3.Row
                self.connection.execute("PRAGMA synchronous=FULL")
            elif self.meta("input") != identity or self.meta("configuration") != configuration:
                raise StorageError("Input, fixture, configuration or normalizer version does not match")
            self._recover_outputs()
        except BaseException:
            self.close()
            raise

    def _initialize(self, identity, configuration):
        self.connection.executescript("""
            CREATE TABLE metadata (name TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE candidates (
              position INTEGER PRIMARY KEY, type TEXT, tmdb_id INTEGER,
              source TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
              error TEXT, UNIQUE(type, tmdb_id));
            CREATE INDEX candidate_status ON candidates(status, position);
            CREATE TABLE cache (type TEXT, tmdb_id INTEGER, language TEXT, payload TEXT NOT NULL,
              PRIMARY KEY(type, tmdb_id, language));
            CREATE TABLE outputs (name TEXT PRIMARY KEY, offset INTEGER NOT NULL,
              sha256 TEXT NOT NULL, records INTEGER NOT NULL);
            CREATE TABLE counters (name TEXT PRIMARY KEY, value INTEGER NOT NULL);
        """)
        for name, value in {"input": identity, "configuration": configuration,
                            "started_at": self.now(), "state": "created", "cursor": 0,
                            "input_exhausted": False, "cooldown_until": 0,
                            "last_invocation": None}.items():
            self._set_meta(name, value)
        for name in ("examined", "duplicates", "candidate_attempts", "http_requests",
                     "retries", "transient_errors"):
            self.connection.execute("INSERT INTO counters VALUES (?, 0)", (name,))
        empty = gzip.compress(b"", mtime=0)
        for name in OUTPUTS:
            with self.path(name).open("xb") as stream:
                stream.write(empty)
                stream.flush()
                os.fsync(stream.fileno())
            self.connection.execute("INSERT INTO outputs VALUES (?, ?, ?, 0)",
                                    (name, len(empty), hashlib.sha256(empty).hexdigest()))
        self.connection.commit()

    def path(self, name):
        return self.directory / f"{name}.jsonl.gz"

    def _set_meta(self, name, value):
        self.connection.execute("INSERT OR REPLACE INTO metadata VALUES (?, ?)",
                                (name, strict_json(value)))

    def meta(self, name):
        row = self.connection.execute("SELECT value FROM metadata WHERE name=?", (name,)).fetchone()
        if row is None:
            raise StorageError("Checkpoint initialization is incomplete; no confirmed run can be resumed")
        return json.loads(row[0])

    def _increment(self, name, value=1):
        self.connection.execute("UPDATE counters SET value=value+? WHERE name=?", (value, name))

    def event(self, name, value):
        with self.connection:
            if name == "cooldown":
                self._set_meta("cooldown_until", value)
            else:
                self._increment(name, value)

    def state(self, value):
        with self.connection:
            self._set_meta("state", value)

    def invocation(self, options):
        with self.connection:
            self._set_meta("last_invocation", {"started_at": self.now(), **options})

    def _recover_outputs(self):
        for row in self.connection.execute("SELECT * FROM outputs"):
            path = self.path(row["name"])
            if not path.is_file() or path.stat().st_size < row["offset"]:
                raise StorageError("Confirmed output is missing or truncated")
            if path.stat().st_size > row["offset"]:
                with path.open("r+b") as stream:
                    stream.truncate(row["offset"])
                    stream.flush()
                    os.fsync(stream.fileno())
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            if digest.hexdigest() != row["sha256"]:
                raise StorageError("Confirmed output checksum mismatch")
            self.hashes[row["name"]] = digest

    def find_identity(self, kind, tmdb_id):
        return self.connection.execute("SELECT position FROM candidates WHERE type=? AND tmdb_id=?",
                                       (kind, tmdb_id)).fetchone()

    def add(self, position, kind, tmdb_id, source):
        with self.connection:
            self.connection.execute("INSERT INTO candidates(position,type,tmdb_id,source,error) VALUES (?,?,?,?,?)",
                (position, kind, tmdb_id, strict_json(source), strict_json({"code": "not_started", "retryable": True})))
            self._increment("examined")
            self._set_meta("cursor", position)

    def _append(self, name, record):
        blob = gzip.compress((strict_json(record) + "\n").encode("utf-8"), mtime=0)
        previous = self.connection.execute("SELECT offset FROM outputs WHERE name=?", (name,)).fetchone()[0]
        with self.path(name).open("r+b") as stream:
            stream.seek(0, 2)
            if stream.tell() != previous:
                raise StorageError("Uncommitted output requires recovery before continuing")
            stream.write(blob)
            stream.flush()
            os.fsync(stream.fileno())
        digest = self.hashes[name].copy()
        digest.update(blob)
        return previous + len(blob), digest

    def _confirm_output(self, name, offset, digest):
        self.connection.execute("UPDATE outputs SET offset=?,sha256=?,records=records+1 WHERE name=?",
                                (offset, digest.hexdigest(), name))

    def duplicate(self, position, source, original_position):
        record = {"source": source, "original_position": original_position,
                  "code": "duplicate_input", "position": position}
        offset, digest = self._append("duplicates", record)
        with self.connection:
            self._confirm_output("duplicates", offset, digest)
            self._increment("examined")
            self._increment("duplicates")
            self._set_meta("cursor", position)
        self.hashes["duplicates"] = digest

    def candidate(self, position):
        row = dict(self.connection.execute("SELECT * FROM candidates WHERE position=?", (position,)).fetchone())
        row["source"] = json.loads(row["source"])
        return row

    def pending_positions(self):
        after = 0
        while True:
            row = self.connection.execute(
                "SELECT position FROM candidates WHERE status='pending' AND position>? ORDER BY position LIMIT 1",
                (after,)).fetchone()
            if row is None:
                break
            after = row[0]
            yield after

    def pending_error(self, position, error):
        with self.connection:
            self.connection.execute("UPDATE candidates SET error=? WHERE position=?", (strict_json(error), position))

    def cached(self, kind, tmdb_id, language):
        row = self.connection.execute("SELECT payload FROM cache WHERE type=? AND tmdb_id=? AND language=?",
                                       (kind, tmdb_id, language)).fetchone()
        return json.loads(row[0]) if row else None

    def cache(self, kind, tmdb_id, language, payload):
        with self.connection:
            self.connection.execute("INSERT OR REPLACE INTO cache VALUES (?,?,?,?)",
                (kind, tmdb_id, language, strict_json(payload)))

    def terminal(self, position, record):
        name = record["status"]
        if name not in STATUSES:
            raise StorageError("Unknown terminal status")
        if self.candidate(position)["status"] != "pending":
            raise StorageError("Candidate is already confirmed")
        offset, digest = self._append(name, record)
        # Only this transaction confirms the GZIP member. Any uncommitted suffix
        # is truncated on resume (including a torn member after process death).
        with self.connection:
            self._confirm_output(name, offset, digest)
            self.connection.execute("UPDATE candidates SET status=?,error=NULL WHERE position=?", (name, position))
            row = self.candidate(position)
            self.connection.execute("DELETE FROM cache WHERE type=? AND tmdb_id=?", (row["type"], row["tmdb_id"]))
        self.hashes[name] = digest

    def exhausted(self):
        with self.connection:
            self._set_meta("input_exhausted", True)

    def manifest(self):
        counts = {name: 0 for name in (*STATUSES, "pending")}
        counts.update(dict(self.connection.execute("SELECT status,count(*) FROM candidates GROUP BY status")))
        counters = dict(self.connection.execute("SELECT name,value FROM counters"))
        terminal = sum(counts[name] for name in STATUSES)
        if counters["examined"] != terminal + counts["pending"] + counters["duplicates"]:
            raise StorageError("Checkpoint counters do not reconcile")
        manifest = {"schema_version": 1, "started_at": self.meta("started_at"),
            "updated_at": self.now(), "input": self.meta("input"),
            "configuration": self.meta("configuration"), "state": self.meta("state"),
            "last_invocation": self.meta("last_invocation"),
            "complete": self.meta("state") == "completed", "counts": counts,
            "terminal_results": terminal, "counters": counters, "cursor": self.meta("cursor"),
            "input_exhausted": self.meta("input_exhausted"), "cooldown_until": self.meta("cooldown_until"),
            "publication_policy": "pending_approval_no_record_is_auto_importable",
            "outputs": {row["name"]: {"file": self.path(row["name"]).name, "bytes": row["offset"],
                        "sha256": row["sha256"], "records": row["records"]}
                        for row in self.connection.execute("SELECT * FROM outputs")}}
        # Pending is a replaceable snapshot, not an append-only attempt log.
        temporary = self.path("pending").with_suffix(".gz.tmp")
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as stream:
                for position in self.pending_positions():
                    row = self.candidate(position)
                    stream.write((strict_json({"type": row["type"], "tmdb_id": row["tmdb_id"],
                        "source": row["source"], "status": "pending", "error": json.loads(row["error"])}) + "\n").encode("utf-8"))
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary, self.path("pending"))
        manifest["outputs"]["pending"] = {"file": self.path("pending").name,
            "bytes": self.path("pending").stat().st_size, "sha256": file_digest(self.path("pending")),
            "records": counts["pending"]}
        atomic_bytes(self.directory / "manifest.json", (strict_json(manifest) + "\n").encode("utf-8"))
        return manifest

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
