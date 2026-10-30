"""Independent read-only diagnosis. Does not invoke cleanup or open its journal."""
import json
import unicodedata
from pathlib import Path

from django.core.exceptions import ValidationError
from django.core.management.base import BaseCommand, CommandError

from core.adult_cleanup import digest_file, metadata_fingerprint, target_identity
from core.management.commands.audit_adult_movies import FIELDS
from core.models import Movie


EXTRA_FIELDS = ("title_spanish", "synopsis", "synopsis_es")


def classify_difference(field, expected, actual):
    model_field = Movie._meta.pk if field == "database_id" else Movie._meta.get_field(field)
    try:
        if model_field.to_python(expected) == model_field.to_python(actual):
            return "comparison_representation", "Different Python/JSON representations of the same field value; no automatic correction"
    except (ValueError, TypeError, ValidationError):
        pass
    if isinstance(expected, str) and isinstance(actual, str):
        def normalize(value):
            return unicodedata.normalize("NFC", value.replace("\r\n", "\n").replace("\r", "\n"))
        if normalize(expected) == normalize(actual):
            return "text_encoding_or_line_endings", "Text differs in Unicode normalization/line endings; exact comparison still fails"
    return "field_value_difference", "Current field value differs from the selection snapshot; cause and author are not inferred"


class Command(BaseCommand):
    help = "Compare protected snapshots with current Movies without invoking cleanup validation or changing any data/files"

    def add_arguments(self, parser):
        parser.add_argument("--selection", required=True, type=Path)
        parser.add_argument("--expected-database", required=True)
        parser.add_argument("--expected-host", required=True)
        # Accept the existing renderArgs2 array, but never read/write these paths.
        parser.add_argument("--journal", help="Accepted for compatibility; never opened")
        parser.add_argument("--batch-size", type=int, default=25, help="Accepted for compatibility")

    def handle(self, *args, **options):
        path = options["selection"]
        try:
            before = digest_file(path)
            proof = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise CommandError(f"Selection unreadable: {exc}") from exc
        identity = target_identity(options["expected_database"], options["expected_host"])
        if proof.get("database") != identity:
            raise CommandError("Selection database fingerprint differs from the explicitly verified destination")
        protected = proof.get("protected_movies", [])
        if not isinstance(protected, list) or not protected:
            raise CommandError("No protected_movies snapshots available")
        ids = [row.get("database_id") for row in protected]
        if any(type(pk) is not int or pk <= 0 for pk in ids) or len(set(ids)) != len(ids):
            raise CommandError("Protected internal IDs are invalid or duplicated")
        actual = {movie.pk: movie for movie in Movie.objects.filter(pk__in=ids).only(
            "id", "tmdb_id", "type", "title_english", "release_year", *EXTRA_FIELDS,
        ).order_by("pk")}
        results = []
        for expected in protected:
            pk = expected["database_id"]
            movie = actual.get(pk)
            differences = []
            result = {"database_id": pk, "expected_title": expected.get("title_english"),
                      "current_title": movie.title_english if movie else None, "differences": differences}
            if movie is None:
                differences.append({"field": "record_exists", "expected": True, "actual": False,
                                    "classification": "record_missing", "explanation": "Protected ID is absent in the destination"})
            else:
                for field in FIELDS:
                    current = getattr(movie, "pk" if field == "database_id" else field)
                    if field not in expected or expected[field] != current:
                        category, reason = classify_difference(field, expected.get(field), current)
                        if field not in expected:
                            category, reason = "selection_field_missing", "Expected value is not stored in the selection"
                        differences.append({"field": field, "expected": expected.get(field), "actual": current,
                                            "expected_available": field in expected, "expected_python_type": type(expected.get(field)).__name__,
                                            "actual_python_type": type(current).__name__, "classification": category, "explanation": reason})
                expected_hash = expected.get("identity_metadata_sha256")
                current_hash = metadata_fingerprint(movie)
                if expected_hash != current_hash:
                    differences.append({"field": "identity_metadata_sha256", "expected": expected_hash, "actual": current_hash,
                                        "classification": "undetermined_hash_only",
                                        "explanation": "Hash covers title_spanish, synopsis, synopsis_es. A hash alone cannot recover expected text or prove which field changed or whether historical serialization differed."})
                    # Newer/external snapshots may include raw metadata; use it if supplied.
                    for field in EXTRA_FIELDS:
                        current = getattr(movie, field)
                        if field in expected and expected[field] == current:
                            continue
                        if field in expected:
                            category, reason = classify_difference(field, expected[field], current)
                        else:
                            category, reason = "expected_value_unavailable", "Only the combined hash was retained; this field is a candidate, not a proven individual discrepancy"
                        differences.append({"field": field, "expected": expected.get(field), "actual": current,
                                            "expected_available": field in expected, "classification": category, "explanation": reason})
            result["matches_existing_guard"] = not differences
            results.append(result)
        if digest_file(path) != before:
            raise CommandError("Selection changed concurrently while diagnosing")
        report = {"read_only": True, "journal_opened": False, "selection_unchanged": True,
                  "checked": len(results), "records_with_differences": sum(not row["matches_existing_guard"] for row in results),
                  "records": results,
                  "note": "This report neither authorizes deletion nor changes/weakens protected_movies validation."}
        # ASCII escaping makes output portable to Windows terminals without losing text.
        self.stdout.write(json.dumps(report, ensure_ascii=True, indent=2))
