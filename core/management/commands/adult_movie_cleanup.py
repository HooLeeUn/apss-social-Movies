"""Guarded adapter for the existing ORM deletion command and durable media retries."""
import csv
import hashlib
import json
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

from django.core.management.base import CommandError
from django.apps import apps
from django.db import connection, transaction
from django.db.models.deletion import CASCADE

from core.adult_cleanup import digest_file, metadata_fingerprint, target_identity
from core.management.commands.audit_adult_movies import FIELDS, local_lock
from core.management.commands.delete_movies_from_csv import Command as DeleteCommand
from core.models import Comment, ContentReport, Movie, VideoComment, WeeklyRecommendationItem, WeeklyRecommendationSnapshot
from core.protected_metadata_approval import load_approval, validate_protected_rows


def storage_identity():
    storage = VideoComment._meta.get_field("video").storage
    safe = {"backend": f"{type(storage).__module__}.{type(storage).__name__}"}
    for key in ("bucket_name", "endpoint_url", "location"):
        safe[key] = str(getattr(storage, key, ""))
    return hashlib.sha256(json.dumps(safe, sort_keys=True).encode()).hexdigest()


class Command(DeleteCommand):
    help = "Plan, dry-run/apply, verify or retry media cleanup for a database-bound adult selection"

    def add_arguments(self, parser):
        parser.add_argument("phase", choices=("plan", "delete", "inspect", "verify", "clean-media"))
        parser.add_argument("--selection", required=True)
        parser.add_argument("--journal", required=True)
        parser.add_argument("--expected-database", required=True)
        parser.add_argument("--expected-host", required=True)
        parser.add_argument("--batch-size", type=int, default=100)
        mode = parser.add_mutually_exclusive_group()
        mode.add_argument("--apply", action="store_true")
        mode.add_argument("--dry-run", action="store_true")
        parser.add_argument("--check-media", action="store_true", help="Read storage to verify recorded keys are absent")
        parser.add_argument("--protected-metadata-approval", type=Path,
                            help="Explicit immutable approval sidecar for protected 525617 metadata only")

    def handle(self, *args, **options):
        self.options = options
        if options["batch_size"] < 1:
            raise CommandError("batch-size must be positive")
        if connection.in_atomic_block or not connection.get_autocommit():
            raise CommandError("Run outside an enclosing transaction")
        identity = target_identity(options["expected_database"], options["expected_host"])
        selection_path = Path(options["selection"])
        proof = json.loads(selection_path.read_text(encoding="utf-8"))
        if proof.get("version") != 2 or proof.get("database") != identity:
            raise CommandError("Selection belongs to a different database or unsupported format")
        manifest = selection_path.parent / "adult_movies_final_to_delete.csv"
        if proof.get("manifest_sha256") != digest_file(manifest):
            raise CommandError("Manifest checksum differs from selection")
        with manifest.open(encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file))
        self.snapshots = {row["database_id"]: row for row in proof["movies"]}
        ids = [int(row["database_id"]) for row in rows]
        if not ids or ids != sorted(set(ids)) or ids != sorted(self.snapshots):
            raise CommandError("Manifest and selection IDs differ, are empty or duplicated")
        protected = proof.get("protected_movies", [])
        protected_ids = [row["database_id"] for row in protected]
        if len(set(protected_ids)) != len(protected_ids) or set(protected_ids).intersection(ids):
            raise CommandError("Protected IDs overlap selection or are duplicated")
        approval = None
        if options["protected_metadata_approval"]:
            if options["phase"] == "plan":
                raise CommandError("Metadata approval reuses an existing journal; do not rebuild plan")
            approval = load_approval(options["protected_metadata_approval"], proof, selection_path, options["journal"], identity)
        if protected:
            present = {movie.pk: movie for movie in Movie.objects.filter(pk__in=protected_ids)}
            validate_protected_rows(proof, present, approval)
        if approval:
            self.stdout.write(json.dumps({"protected_metadata_approval": {"database_id": 525617,
                "expected_sha256": approval["expected_metadata_sha256"], "accepted_sha256": approval["approved_metadata_sha256"],
                "approval_file_sha256": digest_file(options["protected_metadata_approval"])}}))
        journal = Path(options["journal"])
        self.readonly = options["phase"] in {"inspect", "verify"} or (
            options["phase"] != "plan" and not options["apply"]
        )
        if options["phase"] in {"inspect", "verify"} and options["apply"]:
            raise CommandError("inspect/verify are read-only; omit --apply")
        if self.readonly:
            if not journal.is_file():
                raise CommandError("Journal missing")
            # Do not create lock directories, CSVs, SQLite side effects or reconcile
            # prepared states during diagnosis. The operator must stop any writer.
            with closing(sqlite3.connect(journal.resolve().as_uri() + "?mode=ro", uri=True)) as db:
                db.row_factory = sqlite3.Row
                db.execute("BEGIN")
                self.journal = db
                self.validate_journal(identity, selection_path)
                if options["phase"] == "delete":
                    self.delete(ids, options)
                elif options["phase"] == "clean-media":
                    self.inspect_media()
                self.verify(ids)
            return
        journal.parent.mkdir(parents=True, exist_ok=True)
        # Use a separate lock directory per journal, avoiding interference with audit.lock.
        lock_dir = journal.parent / (journal.name + ".lockdir")
        lock_dir.mkdir(exist_ok=True)
        if options["phase"] != "plan" and not journal.is_file():
            raise CommandError("Run plan before delete/verify/clean-media")
        with local_lock(lock_dir), closing(sqlite3.connect(journal)) as db:
            self.journal = db
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA synchronous=FULL")
            if options["phase"] == "plan":
                self.plan(db, proof, identity)
            else:
                self.validate_journal(identity, selection_path)
                if options["phase"] == "delete":
                    self.delete(ids, options)
                elif options["phase"] == "clean-media":
                    self.clean_media(options["apply"])
                self.verify(ids)
            self.write_reports(journal)

    def validate_journal(self, identity, selection_path):
        meta = dict(self.journal.execute("SELECT key,value FROM metadata"))
        if (meta.get("database") != identity or meta.get("selection") != digest_file(selection_path)
                or meta.get("storage") != storage_identity()):
            raise CommandError("Journal database, selection or storage does not match this environment")
        states = dict(self.journal.execute("SELECT id,status FROM movies"))
        if set(states) != set(self.snapshots) or not set(states.values()) <= {"planned", "prepared", "deleted", "error"}:
            raise CommandError("Journal IDs/states differ from the approved selection")

    def plan(self, db, proof, identity):
        if db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchone():
            raise CommandError("Journal already exists; reuse it for delete/verify/clean-media")
        db.executescript("""
            CREATE TABLE metadata (key TEXT PRIMARY KEY,value TEXT);
            CREATE TABLE movies (id INTEGER PRIMARY KEY,status TEXT,error TEXT);
            CREATE TABLE media (movie_id INTEGER,name TEXT,status TEXT,error TEXT,PRIMARY KEY(movie_id,name));
            CREATE TABLE related (model TEXT,id INTEGER,PRIMARY KEY(model,id));
        """)
        for key, value in {"database": identity, "storage": storage_identity(),
                           "selection": digest_file(self.options["selection"])}.items():
            db.execute("INSERT INTO metadata VALUES (?,?)", (key, value))
        for batch in self._chunks(list(self.snapshots), self.options["batch_size"]):
            movies = {m.pk: m for m in Movie.objects.filter(pk__in=batch)}
            for pk in batch:
                if pk not in movies:
                    raise CommandError(f"Movie {pk} missing during plan; generate a fresh selection")
                self.check_snapshot(movies[pk])
                db.execute("INSERT INTO movies VALUES (?,'planned','')", (pk,))
            self.record_media(batch)
            self.record_relations(batch)
        db.commit()
        self.stdout.write(json.dumps({"planned": len(self.snapshots), "video_keys": db.execute("SELECT count(*) FROM media").fetchone()[0]}))

    def check_snapshot(self, movie):
        expected = self.snapshots[movie.pk]
        actual = {key: getattr(movie, "pk" if key == "database_id" else key) for key in FIELDS}
        if actual != {key: expected[key] for key in FIELDS} or metadata_fingerprint(movie) != expected.get("identity_metadata_sha256"):
            raise CommandError(f"Movie {movie.pk} differs from the approved selection")

    def record_media(self, ids):
        videos = VideoComment.objects.filter(movie_id__in=ids).exclude(video="")
        if connection.in_atomic_block:
            videos = videos.select_for_update()
        for pk, name in videos.values_list("movie_id", "video"):
            self.journal.execute("INSERT OR IGNORE INTO media VALUES (?,?,'pending','')", (pk, name))

    def record_relations(self, ids):
        queue, seen = [(Movie, ids)], set()
        collected = {}
        while queue:
            parent, parent_ids = queue.pop()
            for relation in parent._meta.related_objects:
                if relation.field.remote_field.on_delete is not CASCADE:
                    continue
                model = relation.related_model
                children = list(model.objects.filter(**{relation.field.name + "__in": parent_ids}).order_by().values_list("pk", flat=True))
                fresh = [pk for pk in children if (model._meta.label, pk) not in seen]
                for pk in fresh:
                    seen.add((model._meta.label, pk))
                    self.journal.execute("INSERT OR IGNORE INTO related VALUES (?,?)", (model._meta.label, pk))
                if fresh:
                    collected.setdefault(model, set()).update(fresh)
                    queue.append((model, fresh))
        for kind, model in (("comment", Comment), ("video_comment", VideoComment)):
            children = collected.get(model, set())
            if not children:
                continue
            for pk in ContentReport.objects.filter(content_type=kind, object_id__in=children).values_list("pk", flat=True):
                self.journal.execute("INSERT OR IGNORE INTO related VALUES (?,?)", (ContentReport._meta.label, pk))
        return collected

    def _delete_movie(self, movie):
        self.check_snapshot(movie)
        # Parent row lock is already held by the reused ORM command; FK insertions
        # cannot commit against it while deletion runs. Persist keys BEFORE DELETE.
        self.record_media([movie.pk])
        collected = self.record_relations([movie.pk])
        self.journal.execute("UPDATE movies SET status='prepared',error='' WHERE id=?", (movie.pk,))
        self.journal.commit()
        # Numeric moderation references are not Django FKs and require explicit cleanup.
        for kind, model in (("comment", Comment), ("video_comment", VideoComment)):
            if collected.get(model):
                ContentReport.objects.filter(content_type=kind, object_id__in=collected[model]).delete()
        snapshots = []
        if collected.get(WeeklyRecommendationItem):
            snapshot_ids = list(WeeklyRecommendationSnapshot.objects.filter(items__movie=movie).order_by("pk").values_list("pk", flat=True))
            snapshots = list(WeeklyRecommendationSnapshot.objects.select_for_update().filter(pk__in=snapshot_ids).order_by("pk"))
        super()._delete_movie(movie)
        for snapshot in snapshots:
            snapshot.items_count = snapshot.items.count()
            snapshot.save(update_fields=["items_count"])

    def delete(self, ids, options):
        states = dict(self.journal.execute("SELECT id,status FROM movies"))
        deleted_ids = [pk for pk in ids if states[pk] == "deleted"]
        # Never feed a confirmed deletion back to the ORM, even if an ID has been
        # unexpectedly recreated. Refuse that inconsistency instead.
        for batch in self._chunks(deleted_ids, 1000):
            if Movie.objects.filter(pk__in=batch).exists():
                raise CommandError("A journal-deleted Movie exists again; stop for manual review")
        active_ids = [pk for pk in ids if states[pk] != "deleted"]
        if not options["apply"]:
            for movie in Movie.objects.filter(pk__in=active_ids).iterator():
                self.check_snapshot(movie)
            self.stdout.write("DRY-RUN: no PostgreSQL or storage deletions")
            return
        for batch in self._chunks(active_ids, options["batch_size"]):
            remaining = []
            existing = set(Movie.objects.filter(pk__in=batch).values_list("pk", flat=True))
            for pk in batch:
                if pk not in existing and states[pk] == "prepared":
                    self.journal.execute("UPDATE movies SET status='deleted',error='' WHERE id=?", (pk,))
                elif pk not in existing:
                    self.journal.execute("UPDATE movies SET status='error',error='Missing before prepared deletion' WHERE id=?", (pk,))
                else:
                    remaining.append(pk)
            report = []
            try:
                if remaining:
                    self._delete_batch(remaining, "apply", report)
            except Exception as exc:
                # Batch commit failure: never publish its tentative successes.
                report = [self._report_row(pk, "error", str(exc), "apply") for pk in remaining]
            for row in report:
                status = "deleted" if row["status"] == "deleted" else "error"
                self.journal.execute("UPDATE movies SET status=?,error=? WHERE id=?",
                                     (status, "" if status == "deleted" else row["message"], row["database_id"]))
            self.journal.commit()
            self.stdout.write(f"Batch processed: {len(batch)}; committed statuses recorded in journal")

    def verify(self, ids):
        remaining = Movie.objects.filter(pk__in=ids).count()
        prepared = [r[0] for r in self.journal.execute("SELECT id FROM movies WHERE status='prepared'")]
        prepared_present = set()
        for batch in self._chunks(prepared, 1000):
            prepared_present.update(Movie.objects.filter(pk__in=batch).values_list("pk", flat=True))
        for pk in set(prepared) - prepared_present:
            if not self.readonly:
                self.journal.execute("UPDATE movies SET status='deleted',error='' WHERE id=?", (pk,))
        if not self.readonly:
            self.journal.commit()
        deleted_ids = [r[0] for r in self.journal.execute("SELECT id FROM movies WHERE status='deleted'")]
        deleted_present = 0
        for batch in self._chunks(deleted_ids, 1000):
            deleted_present += Movie.objects.filter(pk__in=batch).count()
        relation_remaining = {}
        for label, in self.journal.execute("SELECT DISTINCT model FROM related"):
            model = apps.get_model(label)
            tracked_ids = [r[0] for r in self.journal.execute("SELECT id FROM related WHERE model=?", (label,))]
            count = 0
            for batch in self._chunks(tracked_ids, 1000):
                count += model.objects.filter(pk__in=batch).count()
            relation_remaining[label] = count
        states = dict(Counter(r[0] for r in self.journal.execute("SELECT status FROM movies")))
        media = dict(Counter(r[0] for r in self.journal.execute("SELECT status FROM media")))
        objects_remaining, media_check_errors = None, []
        if self.options["check_media"]:
            objects_remaining = 0
            storage = VideoComment._meta.get_field("video").storage
            for name, in self.journal.execute("SELECT DISTINCT name FROM media"):
                try:
                    objects_remaining += int(storage.exists(name))
                except Exception as exc:
                    media_check_errors.append({"name": name, "error": str(exc)})
        summary = {"movies_remaining": remaining, "tracked_related_remaining": relation_remaining,
                   "movie_statuses": states, "media_statuses": media,
                   "objects_remaining": objects_remaining, "media_check_errors": media_check_errors,
                   "prepared_present": len(prepared_present), "prepared_absent": len(prepared) - len(prepared_present),
                   "deleted_present": deleted_present}
        self.stdout.write(json.dumps(summary, sort_keys=True))
        destructive = self.options["apply"] and self.options["phase"] in {"delete", "clean-media"}
        failed = (remaining or deleted_present or any(relation_remaining.values()) or any(status != "deleted" for status in states)
                  or objects_remaining or media_check_errors)
        if self.options["phase"] == "clean-media" or self.options["check_media"]:
            failed = failed or any(status != "deleted" for status in media)
        if ((destructive or self.options["phase"] == "verify") and failed) or deleted_present:
            # Reports must survive a nonzero exit.
            if not self.readonly:
                self.write_reports(Path(self.options["journal"]))
            raise CommandError("Verification incomplete; inspect journal reports. No claim of full success.")

    def inspect_media(self):
        """Read-only counterpart of clean-media dry-run; never updates media errors."""
        counts = Counter()
        for pk, name in self.journal.execute("SELECT movie_id,name FROM media WHERE status!='deleted'"):
            state = self.journal.execute("SELECT status FROM movies WHERE id=?", (pk,)).fetchone()[0]
            if state not in {"prepared", "deleted"} or Movie.objects.filter(pk=pk).exists():
                counts["parent_not_confirmed"] += 1
            elif VideoComment.objects.filter(video=name).exists():
                counts["retained_reference"] += 1
            else:
                counts["eligible_after_parent_commit"] += 1
        self.stdout.write("DRY-RUN: no storage objects deleted; " + json.dumps(dict(counts)))

    def clean_media(self, apply):
        storage = VideoComment._meta.get_field("video").storage
        rows = self.journal.execute("SELECT movie_id,name FROM media WHERE status!='deleted'")
        for pk, name in rows:
            try:
                state = self.journal.execute("SELECT status FROM movies WHERE id=?", (pk,)).fetchone()[0]
                if state not in {"prepared", "deleted"} or Movie.objects.filter(pk=pk).exists():
                    raise CommandError("Parent deletion not confirmed")
                # Avoid deleting a key concurrently reused by another retained video.
                with transaction.atomic():
                    with connection.cursor() as cursor:
                        cursor.execute("SET LOCAL lock_timeout = '5s'")
                        cursor.execute('LOCK TABLE "core_videocomment" IN SHARE ROW EXCLUSIVE MODE')
                    if VideoComment.objects.filter(video=name).exists():
                        raise CommandError("Object still referenced by a retained VideoComment")
                    if not apply:
                        continue
                    storage.delete(name)
                    if storage.exists(name):
                        raise CommandError("Object still exists after storage.delete")
                self.journal.execute("UPDATE media SET status='deleted',error='' WHERE movie_id=? AND name=?", (pk, name))
            except Exception as exc:
                self.journal.execute("UPDATE media SET status='error',error=? WHERE movie_id=? AND name=?", (str(exc), pk, name))
            self.journal.commit()
        if not apply:
            self.stdout.write("DRY-RUN: no storage objects deleted")

    def write_reports(self, path):
        for table in ("movies", "media"):
            rows = self.journal.execute(f"SELECT * FROM {table}")
            with path.with_suffix(f".{table}.csv").open("w", encoding="utf-8", newline="") as file:
                writer = csv.writer(file)
                writer.writerow([c[0] for c in rows.description])
                writer.writerows(rows)
