"""Reassess cached evidence; never calls TMDb or mutates PostgreSQL."""
import csv
import json
import sqlite3
from collections import Counter
from contextlib import closing
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from core.adult_cleanup import assess_identity, digest_file, metadata_fingerprint, target_identity, write_json
from core.management.commands.audit_adult_movies import FIELDS, Command as AuditCommand, classify, local_lock, normalize_title
from core.models import Movie


class Command(BaseCommand):
    help = "Generate final adult manifest using cached API evidence only"

    def add_arguments(self, parser):
        parser.add_argument("--audit-dir", required=True)
        parser.add_argument("--expected-database", required=True)
        parser.add_argument("--expected-host", required=True)

    def handle(self, *args, **options):
        identity = target_identity(options["expected_database"], options["expected_host"])
        directory = Path(options["audit_dir"])
        state = directory / "audit.sqlite3"
        if not state.is_file():
            raise CommandError("Audit state missing")
        with local_lock(directory), closing(sqlite3.connect(state)) as db:
            db.row_factory = sqlite3.Row
            if dict(db.execute("SELECT key,value FROM metadata")).get("database") != identity:
                raise CommandError("Audit belongs to another database")
            selected, review, reconsidered, proof_movies = [], [], [], []
            counts = Counter()
            overview_counts = Counter()
            for response, in db.execute("SELECT evidence FROM responses WHERE status='complete'"):
                overview = json.loads(response).get("overview")
                if isinstance(overview, str) and overview:
                    overview_counts[normalize_title(overview)] += 1
            cursor = db.execute("SELECT snapshot FROM candidates ORDER BY database_id")
            while batch := cursor.fetchmany(500):
                snapshots = [json.loads(row[0]) for row in batch]
                current = {row["id"]: row for row in Movie.objects.filter(
                    pk__in=[m["database_id"] for m in snapshots],
                ).order_by().values("id", "tmdb_id", "type", "title_english", "release_year",
                                    "title_spanish", "synopsis", "synopsis_es")}
                for movie in snapshots:
                    key = (movie["type"], movie["tmdb_id"])
                    response = db.execute("SELECT status,evidence FROM responses WHERE type=? AND id=?", key).fetchone()
                    member = db.execute("SELECT 1 FROM exports WHERE type=? AND id=?", key).fetchone()
                    data = json.loads(response["evidence"]) if response else {}
                    row = current.get(movie["database_id"])
                    original_confirmed = classify(movie, data)[0] == "confirmed" if response and response["status"] == "complete" else False
                    if not member or not row or any(row["id" if k == "database_id" else k] != movie[k] for k in FIELDS):
                        accepted, category, reason = False, "catalog_changed", "Snapshot differs or export membership missing"
                    elif not response or response["status"] != "complete":
                        accepted, category, reason = False, "api_unavailable", "No completed cached API response"
                    elif original_confirmed:
                        accepted, category, reason = True, "original_confirmed", "Original strict confirmation preserved"
                    else:
                        overview = data.get("overview")
                        row["_overview_unique"] = isinstance(overview, str) and overview_counts[normalize_title(overview)] == 1
                        accepted, category, reason = assess_identity(movie, data, row)
                    result = dict(movie, adult=data.get("adult", ""), validation_status="confirmed" if accepted else "review",
                                  category=category, reason=reason)
                    if not original_confirmed:
                        reconsidered.append(result)
                    counts[category] += 1
                    if accepted:
                        selected.append(result)
                        proof_movies.append(dict(movie, identity_metadata_sha256=metadata_fingerprint(row)))
                    else:
                        review.append(result)
            fields = FIELDS + ["adult", "validation_status", "category", "reason"]
            AuditCommand.write_csv(directory / "adult_movies_final_confirmed.csv", fields, selected)
            AuditCommand.write_csv(directory / "adult_movies_final_review.csv", fields, review)
            AuditCommand.write_csv(directory / "adult_movies_reassessment.csv", fields, reconsidered)
            manifest = directory / "adult_movies_final_to_delete.csv"
            AuditCommand.write_csv(manifest, ["database_id"], ({"database_id": r["database_id"]} for r in selected))
            proof = {"version": 2, "database": identity, "manifest_sha256": digest_file(manifest),
                     "counts": dict(counts), "movies": proof_movies}
            write_json(directory / "adult_movies_final_selection.json", proof)
        self.stdout.write(json.dumps({"selected": len(selected), "review": len(review), "categories": dict(counts)}))
