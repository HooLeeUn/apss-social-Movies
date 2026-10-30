"""Narrow, opt-in authorization for one immutable protected metadata transition."""
import hashlib
import json
import re
import sqlite3
from contextlib import closing
from pathlib import Path

from django.core.management.base import CommandError

from core.adult_cleanup import digest_file, metadata_fingerprint
from core.management.commands.audit_adult_movies import FIELDS


IDENTITY = {"database_id": 525617, "type": "movie", "tmdb_id": 812137,
            "title_english": "Power Rangers Jungle Fury", "release_year": 2008}
METADATA_FIELDS = ("title_spanish", "synopsis", "synopsis_es")


def recover_known_metadata(expected_hash):
    # Identified from existing title/default-value candidates and verified against
    # the original selection digest. Never return a guessed, non-matching value.
    candidate = {"title_spanish": "Power Rangers Jungle Fury", "synopsis": "", "synopsis_es": ""}
    return candidate if metadata_fingerprint(candidate) == expected_hash else None


def require(condition, message):
    if not condition:
        raise CommandError(message)


def journal_binding(path, proof, selection_path):
    path = Path(path)
    require(path.is_file(), "Existing journal required; no plan is created or rebuilt")
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
            db.execute("BEGIN")
            meta = dict(db.execute("SELECT key,value FROM metadata"))
            ids = [r[0] for r in db.execute("SELECT id FROM movies ORDER BY id")]
    except sqlite3.Error as exc:
        raise CommandError(f"Journal cannot be read: {exc}") from exc
    require(meta.get("database") == proof["database"] and meta.get("selection") == digest_file(selection_path),
            "Existing journal database/selection binding differs")
    require(ids == sorted(row["database_id"] for row in proof["movies"]), "Journal IDs differ from selection")
    # Deliberately exclude progress states: commits/recovery must not invalidate authorization.
    return hashlib.sha256(json.dumps({"metadata": meta, "movie_ids": ids}, sort_keys=True).encode()).hexdigest()


def validate_approval(document, proof, selection_path, journal_path, identity):
    require(isinstance(document, dict), "Metadata approval must be an object")
    protected = proof.get("protected_movies", [])
    expected = next((row for row in protected if row.get("database_id") == 525617), None)
    require(proof.get("version") == 2 and len(proof.get("movies", [])) == 337 and len(protected) == 8,
            "Approval is restricted to the second 337/8 batch")
    selected_ids = [row["database_id"] for row in proof["movies"]]
    protected_ids = [row["database_id"] for row in protected]
    require(len(set(selected_ids)) == 337 and len(set(protected_ids)) == 8
            and not set(selected_ids).intersection(protected_ids), "Approval requires disjoint unique selected/protected IDs")
    require(expected is not None and {key: expected.get(key) for key in FIELDS} == IDENTITY,
            "Protected selection identity is not the confirmed Render movie 525617/812137")
    require(all(row["database_id"] != 525617 for row in proof["movies"]), "Protected ID appears in deletion selection")
    require(document.get("version") == 1 and document.get("identity") == IDENTITY,
            "Metadata approval has an unsupported identity/version")
    require(document.get("database") == proof["database"] == identity, "Metadata approval database differs")
    require(document.get("selection_sha256") == digest_file(selection_path), "Metadata approval selection checksum differs")
    manifest = Path(selection_path).parent / "adult_movies_final_to_delete.csv"
    require(document.get("manifest_sha256") == proof["manifest_sha256"] == digest_file(manifest),
            "Metadata approval manifest checksum differs")
    require(document.get("journal_binding_sha256") == journal_binding(journal_path, proof, selection_path),
            "Metadata approval journal binding differs")
    old = document.get("expected_metadata_sha256", "")
    new = document.get("approved_metadata_sha256", "")
    require(isinstance(old, str) and isinstance(new, str) and re.fullmatch(r"[0-9a-f]{64}", old)
            and re.fullmatch(r"[0-9a-f]{64}", new) and old != new, "Approval requires two different exact SHA-256 hashes")
    require(old == expected.get("identity_metadata_sha256"), "Approval expected hash differs from selection")
    values = document.get("approved_metadata")
    require(isinstance(values, dict) and set(values) == set(METADATA_FIELDS)
            and all(value is None or isinstance(value, str) for value in values.values()), "Approval must retain all three exact metadata values")
    require(metadata_fingerprint(values) == new, "Approved metadata text/hash differs")
    recovered = recover_known_metadata(old)
    if recovered is not None:
        require(document.get("expected_metadata") == recovered, "Approval must preserve the hash-verified original metadata")
        require(values["title_spanish"] == recovered["title_spanish"], "title_spanish changed outside the reviewed synopsis enrichment")
    return document


def load_approval(path, proof, selection_path, journal_path, identity):
    try:
        document = json.loads(Path(path).read_text(encoding="utf-8"))
        return validate_approval(document, proof, selection_path, journal_path, identity)
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise CommandError(f"Metadata approval unreadable/invalid: {exc}") from exc


def validate_protected_rows(proof, present, approval=None):
    for expected in proof.get("protected_movies", []):
        pk = expected["database_id"]
        movie = present.get(pk)
        require(movie is not None, "A protected Movie is missing; stop for manual review")
        actual = {key: getattr(movie, "pk" if key == "database_id" else key) for key in FIELDS}
        require(actual == {key: expected[key] for key in FIELDS}, "A protected Movie changed; stop for manual review")
        expected_hash = expected.get("identity_metadata_sha256")
        if approval is not None and pk == 525617:
            require(actual == IDENTITY, "Metadata approval cannot authorize an identity change")
            expected_hash = approval["approved_metadata_sha256"]
        require(metadata_fingerprint(movie) == expected_hash, "A protected Movie changed; stop for manual review")
