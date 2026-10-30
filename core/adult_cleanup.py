"""Local evidence and target checks shared by the adult cleanup commands."""
import hashlib
import json
import re
from datetime import datetime

from django.core.management.base import CommandError
from django.db import connection

from core.management.commands.audit_adult_movies import database_identity, normalize_title


def target_identity(expected_database, expected_host):
    if connection.vendor != "postgresql":
        raise CommandError("PostgreSQL required")
    if str(connection.settings_dict["HOST"]) != expected_host:
        raise CommandError("Configured HOST differs from --expected-host")
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        actual = cursor.fetchone()[0]
    if actual != expected_database or str(connection.settings_dict["NAME"]) != expected_database:
        raise CommandError("Database differs from --expected-database")
    return database_identity("default")


def digest_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def metadata_fingerprint(movie):
    fields = ("title_spanish", "synopsis", "synopsis_es")
    data = {key: movie.get(key) if isinstance(movie, dict) else getattr(movie, key) for key in fields}
    return hashlib.sha256(json.dumps(data, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def canonical_title(value):
    # Only known formatting variants; never drop installment numbers or use fuzzy matching.
    words = re.findall(r"\w+", value.casefold(), flags=re.UNICODE)
    words = [w for w in words if w not in {"the", "a", "an", "il", "la", "le", "una", "un", "o", "vol", "volume"}]
    return normalize_title(" ".join(words))


def assess_identity(movie, data, extra):
    if not isinstance(data, dict) or type(data.get("id")) is not int or data["id"] != movie["tmdb_id"]:
        return False, "identity_conflict", "API ID differs or missing"
    if data.get("adult") is False:
        return False, "adult_false", "API adult=false"
    if data.get("adult") is not True:
        return False, "api_unavailable", "API adult not a true boolean"
    is_movie = movie["type"] == "movie"
    keys = ("title", "original_title") if is_movie else ("name", "original_name")
    titles = [data[k] for k in keys if isinstance(data.get(k), str) and data[k].strip()]
    local_titles = [movie["title_english"], extra.get("title_spanish") or ""]
    exact = any(normalize_title(a) == normalize_title(b) and len(normalize_title(a)) >= 6
                for a in local_titles if a for b in titles)
    variant = any(canonical_title(a) == canonical_title(b) and len(canonical_title(a)) >= 8
                  for a in local_titles if a for b in titles)
    date = data.get("release_date" if is_movie else "first_air_date")
    try:
        api_year = datetime.strptime(date, "%Y-%m-%d").year
    except (ValueError, TypeError):
        api_year = None
    year = movie["release_year"]
    gap = abs(year - api_year) if year and api_year else None
    # A large year disagreement stays unresolved even for identical titles (remakes).
    if gap is not None and gap > 1:
        return False, "identity_conflict", "Year gap >1; possible remake or incorrect TMDb assignment"
    if exact:
        return True, "metadata_difference", "Exact English/Spanish/original title; year agrees, differs <=1, or date missing"
    if variant and gap is not None:
        return True, "metadata_difference", "Article/volume formatting variant with agreeing year (tolerance 1)"
    overview = data.get("overview") if isinstance(data.get("overview"), str) else ""
    synopsis_match = any(
        len(normalize_title(text)) >= 120 and normalize_title(text) == normalize_title(overview)
        for text in (extra.get("synopsis") or "", extra.get("synopsis_es") or "")
    )
    if synopsis_match and gap is not None and extra.get("_overview_unique") is True:
        return True, "metadata_difference", "Exact substantial synopsis and agreeing year; translated/alternative title"
    return False, "identity_unresolved", "adult=true, but cached evidence cannot establish title identity safely"


def write_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    with temp.open("w", encoding="utf-8") as file:
        json.dump(value, file, ensure_ascii=False, indent=2)
        file.flush()
        import os
        os.fsync(file.fileno())
    temp.replace(path)
