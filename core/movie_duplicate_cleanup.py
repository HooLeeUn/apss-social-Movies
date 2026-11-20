"""Execute a reviewed phase 5D plan, one identity per transaction."""
from collections import Counter, defaultdict
from contextlib import contextmanager
from decimal import Decimal
import hashlib
import json
import uuid

from django.core.management.base import CommandError
from django.db import router, transaction
from django.db.models import Count
from django.db.models.deletion import Collector

from core.management.commands.diagnose_local_movie_duplicates import is_loopback, json_text, require_local
from core.models import Movie, MovieRating, UserDailyFeedCandidate, UserTasteProfile
from core.movie_duplicate_cleanup_plan import (
    POLICY, choose_survivor, dependency_counts, dependency_paths, django_dependencies, read_schema, write_plan,
)
from core.movie_duplicate_diagnosis import selected_fields
from core.movie_duplicate_preferences import (
    PREFERENCES, REVIEW_KEY, assert_reviewed_preferences, build_preference_review,
    capture_preferences,
)
from core.movie_duplicate_resume import group_descriptor, update_progress, verify_execution_preferences
from core.services import COUNT_FIELDS, get_movie_preference_targets
from core.tmdb_enrichment_store import atomic_bytes
from core.tmdb_import_apply import check_session as check_local_session
from core.movie_duplicate_execution_scope import execution_alias, supported_dependencies, session_validator
from django.apps import apps
from core import movie_duplicate_weekly as weekly


def check_session(cursor, target, read_only):
    validator = session_validator()
    if validator is None:
        return check_local_session(cursor, target, read_only)
    return validator(cursor, target, read_only)


LOCK_ID = 734917923001
SUPPORTED = {MovieRating._meta.label, UserDailyFeedCandidate._meta.label}


def publish(path, data):
    atomic_bytes(path, (json_text(data) + "\n").encode("utf-8"))


def read_rows(path):
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def load_plan(directory, target, approved_digest):
    manifest_bytes = (directory / "cleanup_manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    plan = (directory / "cleanup_plan.jsonl").read_bytes()
    schema = (directory / "cleanup_dependencies.json").read_bytes()
    if (manifest.get("status") != "complete" or manifest.get("policy") != POLICY
            or manifest.get("target") != target or not manifest.get("dependency_estimate_complete")
            or manifest.get("schema_blockers") or manifest.get("protected_groups")
            or manifest.get("excluded_nonnull_duplicate_groups") != 0
            or manifest.get("plan_sha256") != hashlib.sha256(plan).hexdigest()
            or manifest.get("dependencies_sha256") != hashlib.sha256(schema).hexdigest()
            or (approved_digest is not None and manifest["plan_sha256"] != approved_digest)):
        raise CommandError("Plan is incomplete, blocked, changed, or belongs to a different target.")
    groups = [json.loads(line) for line in plan.decode("utf-8").split("\n") if line]
    if len(groups) != manifest["groups"] or any(row["status"] != "planned" for row in groups):
        raise CommandError("Plan groups are incomplete or blocked.")
    return manifest, groups, json.loads(schema), hashlib.sha256(manifest_bytes).hexdigest()


@contextmanager
def cleanup_session(connection, target, apply):
    require_local(connection)
    if not is_loopback(target["host"]) or target["alias"] != "default" or router.routers:
        raise CommandError("Cleanup requires a literal loopback address, the default alias and no database routers.")
    try:
        if not connection.get_autocommit():
            raise CommandError("Cleanup requires autocommit and an independent connection.")
        if apply:
            with connection.cursor() as cursor:
                check_session(cursor, target, read_only=False)
                cursor.execute("SELECT pg_try_advisory_lock(%s)", [LOCK_ID])
                if not cursor.fetchone()[0]:
                    raise CommandError("Another cleanup is already running.")
        yield
    finally:
        connection.close()


def verify_schema(cursor, schema, edges):
    derived = sorted({UserTasteProfile._meta.db_table, *(model._meta.db_table for model in PREFERENCES)})
    tables = sorted({Movie._meta.db_table, *(edge["table"] for edge in edges), *derived})
    cursor.execute("""
        SELECT bool_and(to_regclass(name) IS NOT NULL
                        AND to_regclass(name) = to_regclass('public.' || name))
        FROM unnest(%s::text[]) AS name
    """, [tables])
    if not cursor.fetchone()[0] or json_text({"django": edges, "postgresql": read_schema(cursor, edges)}) != json_text(schema):
        raise CommandError("Live schema differs from the reviewed cleanup schema.")
    cursor.execute("""
        SELECT EXISTS (
            SELECT 1 FROM pg_trigger WHERE NOT tgisinternal AND tgenabled <> 'D'
              AND tgrelid IN (SELECT to_regclass(name) FROM unnest(%s::text[]) AS name)
            UNION ALL
            SELECT 1 FROM pg_constraint WHERE contype = 'f'
              AND confrelid IN (SELECT to_regclass(name) FROM unnest(%s::text[]) AS name)
            UNION ALL
            SELECT 1 FROM pg_rewrite
              WHERE ev_class IN (SELECT to_regclass(name) FROM unnest(%s::text[]) AS name)
        )
    """, [derived, derived, tables])
    if cursor.fetchone()[0]:
        raise CommandError("Cleanup blocked by triggers or references on derived preferences, or table rewrite rules.")


def group_movies(plan):
    return Movie.objects.filter(type=plan["type"], tmdb_id=plan["tmdb_id"]).order_by("pk")


def survivor_digest(plan):
    row = group_movies(plan).filter(pk=plan["keep_id"]).values().get()
    return digest(row)


def digest(value):
    return hashlib.sha256(json_text(value).encode()).hexdigest()


def validate_group_metadata(plan, rows, *, index=None):
    expected = {key: plan[key] for key in ("keep_id", "remove_ids", "scores", "reason", "metadata_sha256")}
    # JSON object keys are strings on disk. Normalize before canonical comparison
    # so both sides sort strings, while preserving every value and its JSON type.
    actual = json.loads(json_text(choose_survivor(rows))) if rows else None
    if len(rows) < 2 or json_text(actual) != json_text(expected):
        error = CommandError("Group metadata or membership changed.")
        error.cleanup_group = group_descriptor(index, plan)
        error.cleanup_details = {"expected_member_ids": sorted([plan["keep_id"], *plan["remove_ids"]]),
            "actual_member_ids": sorted(row["id"] for row in rows),
            "expected_metadata_sha256": plan["metadata_sha256"],
            "actual_metadata_sha256": actual["metadata_sha256"] if actual else None}
        raise error
    return actual


def group_key(plan):
    return f"{plan['type']}:{plan['tmdb_id']}"


def snapshot_entries(groups):
    """Fingerprint every Movie column and the exact supported dependent rows."""
    roots = {}
    dependencies = defaultdict(list)
    all_ids = [pk for group in groups for pk in (group["keep_id"], *group["remove_ids"])]
    removed = [pk for group in groups for pk in group["remove_ids"]]
    for row in Movie.objects.filter(pk__in=all_ids).order_by("pk").values():
        roots[row["id"]] = row
    for model in (MovieRating, UserDailyFeedCandidate, *([apps.get_model("core.WeeklyRecommendationItem")] if "core.WeeklyRecommendationItem" in supported_dependencies() else [])):
        for row in model.objects.filter(movie_id__in=removed).order_by("pk").values():
            dependencies[row["movie_id"]].append((model._meta.label, row))
    return {
        group_key(group): {
            "survivor_sha256": digest(roots[group["keep_id"]]),
            "full_sha256": digest([(roots[pk], dependencies[pk])
                                   for pk in (group["keep_id"], *group["remove_ids"])]),
        }
        for group in groups
    }


def verify_applied(plan, receipt, paths):
    if (list(group_movies(plan).values_list("pk", flat=True)) != [plan["keep_id"]]
            or Movie.objects.filter(pk__in=plan["remove_ids"]).exists()
            or survivor_digest(plan) != receipt["survivor_sha256"]
            or dependency_counts(execution_alias(), paths, plan["remove_ids"])):
        raise CommandError("Completed group cannot be reconciled with its survivor and dependencies.")


def assert_preferences(user_ids, *, review=None, removed_movie_ids=()):
    if review is not None:
        assert_reviewed_preferences(capture_preferences(user_ids), review, removed_movie_ids)
        return
    for user_id in sorted(user_ids):
        ratings = list(MovieRating.objects.filter(user_id=user_id).select_related("movie"))
        profile_count = UserTasteProfile.objects.filter(user_id=user_id).values_list("ratings_count", flat=True).first()
        if profile_count != len(ratings):
            raise CommandError("Taste profile disagrees with remaining ratings.")
        expected = Counter((model, value, f"count_{rating.score}")
                           for rating in ratings for model, _, value in get_movie_preference_targets(rating.movie))
        actual = Counter()
        for model, field in PREFERENCES.items():
            for row in model.objects.filter(user_id=user_id).values(field, *COUNT_FIELDS, "ratings_count", "score"):
                count = sum(row[key] for key in COUNT_FIELDS)
                weighted = sum(score * row[key] for score, key in enumerate(COUNT_FIELDS, 1))
                score = Decimal(str(round(weighted / count, 2))) if count else Decimal(0)
                if row["ratings_count"] != count or row["score"] != score:
                    raise CommandError("Preference total or score disagrees with its distribution.")
                for key in COUNT_FIELDS:
                    if row[key]:
                        actual[model, row[field], key] = row[key]
        if actual != expected:
            raise CommandError("Preference distribution disagrees with remaining ratings.")


def delete_group(connection, target, schema, edges, paths, plan, reviewed, receipt_path, *,
                 previously_removed=(), previous_preferences_sha256=None, index=None, confirmed=None):
    with transaction.atomic(using=execution_alias(), durable=True):
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            check_session(cursor, target, read_only=False)
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            cursor.execute("SET LOCAL synchronous_commit = 'on'")
            tables = sorted({Movie._meta.db_table, *(edge["table"] for edge in edges),
                             UserTasteProfile._meta.db_table, *(model._meta.db_table for model in PREFERENCES)})
            if "core.WeeklyRecommendationItem" in supported_dependencies():
                tables = sorted(set(tables) | {"core_weeklyrecommendationsnapshot"})
            quoted = ", ".join(f'public.{connection.ops.quote_name(table)}' for table in tables)
            cursor.execute(f"LOCK TABLE {quoted} IN SHARE ROW EXCLUSIVE MODE")
            verify_schema(cursor, schema, edges)
        if snapshot_entries([plan]).get(group_key(plan)) != reviewed[group_key(plan)]:
            raise CommandError("A Movie or dependent row changed since the reviewed dry-run.")
        rows = list(group_movies(plan).values(*selected_fields()))
        validate_group_metadata(plan, rows, index=index)
        ids = plan["remove_ids"]
        impacts = dependency_counts(execution_alias(), paths, ids)
        live = [dict(item, movie_id=pk) for pk in ids for item in impacts.get(pk, ())]
        if live != plan["dependencies"] or any(item["model"] not in supported_dependencies() or item["on_delete"] != "CASCADE" for item in live):
            raise CommandError("Group dependencies changed or need specific handling.")
        candidates = group_movies(plan).filter(pk__in=ids)
        collector = Collector(using=execution_alias(), origin=candidates)
        collector.collect(candidates)
        allowed = supported_dependencies() | {Movie._meta.label}
        collected = Counter({model._meta.label: len(rows) for model, rows in collector.data.items()})
        for queryset in collector.fast_deletes:
            collected[queryset.model._meta.label] += queryset.count()
        expected = Counter({Movie._meta.label: len(ids)})
        for item in live:
            expected[item["model"]] += item["rows"]
        if collector.field_updates or any(count and model not in allowed for model, count in collected.items()) or collected != expected:
            raise CommandError("Django deletion collector differs from the approved dependency scope.")
        preference_review = reviewed[REVIEW_KEY]
        users = {int(user_id) for user_id in preference_review["baseline"]["users"]}
        assert_preferences(users, review=preference_review, removed_movie_ids=previously_removed)
        if previous_preferences_sha256 != digest(capture_preferences(users)):
            raise CommandError("Preferences changed since the last reviewed/confirmed group.")
        receipt = {"state": "pending_commit", "survivor_sha256": survivor_digest(plan),
                   "keep_id": plan["keep_id"], "remove_ids": ids, "deleted_by_model": dict(expected)}
        if weekly.KEY in reviewed:
            weekly.assert_state(reviewed[weekly.KEY], previously_removed)
        _, removed = collector.delete()
        if weekly.KEY in reviewed:
            weekly.decrement(reviewed[weekly.KEY], ids)
            weekly.assert_state(reviewed[weekly.KEY], (*previously_removed, *ids))
        if Counter(removed) != expected:
            raise CommandError("Post-delete row counts differ from the approved scope.")
        verify_applied(plan, receipt, paths)
        assert_preferences(users, review=preference_review, removed_movie_ids=(*previously_removed, *ids))
        receipt["preference_scope_sha256"] = digest(preference_review)
        receipt["preferences_after_sha256"] = digest(capture_preferences(users))
        publish(receipt_path, receipt)
    receipt["state"] = "committed"
    if confirmed is not None:
        confirmed(receipt)  # The durable transaction committed, even if publishing its receipt fails.
    publish(receipt_path, receipt)
    return receipt


def preflight(connection, target, schema, edges, paths, groups, output, receipts, applied, reviewed, *, progress=None):
    validation = output / ("validation-" + uuid.uuid4().hex[:12])
    validation.mkdir()
    with transaction.atomic(using=execution_alias(), durable=True):
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("SET LOCAL lock_timeout = '5s'")
            check_session(cursor, target, read_only=True)
            verify_schema(cursor, schema, edges)
        verified = set()
        update_progress(progress, groups, receipts, verified, complete=False)
        for index in sorted(applied):
            if reviewed is None or receipts[index]["survivor_sha256"] != reviewed[group_key(groups[index])]["survivor_sha256"]:
                raise CommandError("Execution receipt disagrees with the reviewed survivor.")
            verify_applied(groups[index], receipts[index], paths)
            verified.add(index)
            update_progress(progress, groups, receipts, verified, complete=False)
        update_progress(progress, groups, receipts, verified, complete=True)
        write_plan(execution_alias(), validation, paths, estimate_dependencies=True)
        remaining = read_rows(validation / "cleanup_plan.jsonl")
        if remaining != [plan for index, plan in enumerate(groups) if index not in applied]:
            error = CommandError("The live cleanup plan differs from the approved pending groups.")
            by_key = {group_key(group): group for group in remaining}
            error.cleanup_group = next((group_descriptor(i, group) for i, group in enumerate(groups)
                if i not in applied and by_key.get(group_key(group)) != group), None)
            raise error
        snapshots = snapshot_entries(remaining)
        if reviewed is not None and any(item != reviewed.get(key) for key, item in snapshots.items()):
            raise CommandError("A Movie or dependent row changed since the reviewed dry-run.")
        all_removed = [pk for plan in groups for pk in plan["remove_ids"]]
        if reviewed is None:
            users = set(MovieRating.objects.filter(movie_id__in=all_removed).values_list("user_id", flat=True))
            preference_review = build_preference_review(capture_preferences(users), all_removed)
        else:
            preference_review = reviewed.get(REVIEW_KEY)
            compatibility = verify_execution_preferences(preference_review, all_removed)
            if progress is not None:
                progress["preference_review_compatibility"] = compatibility
            users = {int(user_id) for user_id in preference_review["baseline"]["users"]}
        completed_ids = [pk for index in applied for pk in groups[index]["remove_ids"]]
        assert_preferences(users, review=preference_review, removed_movie_ids=completed_ids)
        for index in applied:
            if receipts[index].get("preference_scope_sha256") != digest(preference_review):
                raise CommandError("Execution receipt disagrees with the reviewed preference scope.")
        if applied and receipts[max(applied)].get("preferences_after_sha256") != digest(capture_preferences(users)):
            raise CommandError("Preferences changed since the last confirmed group.")
        snapshots[REVIEW_KEY] = preference_review
        if "core.WeeklyRecommendationItem" in supported_dependencies():
            weekly_review = reviewed[weekly.KEY] if reviewed is not None else weekly.capture(all_removed)
            weekly.assert_state(weekly_review, completed_ids)
            snapshots[weekly.KEY] = weekly_review
    return snapshots


def load_receipts(output, groups):
    receipts = {}
    for path in sorted(output.glob("group-*.json")):
        index = int(path.stem.removeprefix("group-"))
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if (path.name != f"group-{index:06d}.json" or index != len(receipts) or index >= len(groups)
                or receipt.get("state") not in ("pending_commit", "committed")
                or (index > 0 and receipts[index - 1]["state"] != "committed")
                or any(receipt.get(key) != groups[index][key] for key in ("keep_id", "remove_ids"))):
            raise CommandError("Invalid execution receipts.")
        expected = Counter({Movie._meta.label: len(groups[index]["remove_ids"])})
        for item in groups[index]["dependencies"]:
            expected[item["model"]] += item["rows"]
        if (receipt.get("deleted_by_model") != dict(expected) or
                any(type(value) is not int or value < 0 for value in receipt["deleted_by_model"].values()) or any(
                not isinstance(receipt.get(key), str) or len(receipt[key]) != 64
                for key in ("survivor_sha256", "preference_scope_sha256", "preferences_after_sha256"))):
            raise CommandError("Execution receipt counts or verification digests are invalid.")
        receipts[index] = receipt
    return receipts


def execute_cleanup(connection, target, schema, groups, output, *, apply, limit, resume, reviewed, progress=None):
    if progress is None:
        progress = {}
    progress.update(groups=len(groups), remove_movies=sum(len(group["remove_ids"]) for group in groups),
                    committed_groups=None, pending_groups=None, progress_verification_complete=False,
                    active_group=None, last_committed_group=None, removed_movies=0)
    edges = django_dependencies()
    paths = dependency_paths(edges)
    unsupported = sorted({item["model"] for plan in groups for item in plan["dependencies"]
                          if item["model"] not in supported_dependencies() or item["on_delete"] != "CASCADE"})
    if apply and unsupported:
        raise CommandError(f"Apply blocked by dependencies that need explicit handling: {unsupported}")
    receipts = load_receipts(output, groups) if resume else {}
    update_progress(progress, groups, receipts, set(), complete=False)
    applied = {index for index, receipt in receipts.items() if receipt["state"] == "committed"}
    pending = next((index for index in receipts if index not in applied), None)
    if pending is not None and not Movie.objects.filter(pk__in=groups[pending]["remove_ids"]).exists():
        applied.add(pending)  # Verified in the same fresh snapshot as the remaining plan.
    snapshots = preflight(connection, target, schema, edges, paths, groups, output, receipts, applied, reviewed, progress=progress)
    summary = {"state": "validated", "target": target, "groups": len(groups),
               "committed_groups": len(applied), "unsupported_dependencies": unsupported,
               "remove_movies": sum(len(group["remove_ids"]) for group in groups)}
    dependencies = Counter()
    for group in groups:
        for item in group["dependencies"]:
            dependencies[item["model"]] += item["rows"]
    summary["dependencies"] = dict(dependencies)
    diagnosis = snapshots[REVIEW_KEY]["diagnosis"]
    summary["preference_validation"] = {
        "policy": snapshots[REVIEW_KEY]["policy"],
        "preexisting_unrelated_director_discrepancies": len(diagnosis["preexisting_unrelated_director_discrepancies"]),
        "historical_rows_reconciled": 0,
    }
    publish(output / "preference_diagnosis.json", diagnosis)
    if not apply:
        publish(output / "review_snapshot.json", snapshots)
        summary["review_sha256"] = hashlib.sha256((output / "review_snapshot.json").read_bytes()).hexdigest()
        return {**progress, **summary}
    if pending in applied:
        receipts[pending]["state"] = "committed"
        publish(output / f"group-{pending:06d}.json", receipts[pending])
    for index in range(len(applied), min(len(groups), len(applied) + limit)):
        progress["active_group"] = group_descriptor(index, groups[index])
        previously_removed = [pk for done in applied for pk in groups[done]["remove_ids"]]
        previous_preferences_sha256 = (receipts[max(applied)]["preferences_after_sha256"] if applied
                                       else digest(reviewed[REVIEW_KEY]["baseline"]))
        def confirmed(receipt, index=index):
            receipts[index] = receipt
            applied.add(index)
            update_progress(progress, groups, receipts, applied, complete=True)

        receipts[index] = delete_group(connection, target, schema, edges, paths, groups[index], reviewed,
            output / f"group-{index:06d}.json", previously_removed=previously_removed,
            previous_preferences_sha256=previous_preferences_sha256, index=index, confirmed=confirmed)
        applied.add(index)
        progress["active_group"] = None
    if len(applied) == len(groups):
        with transaction.atomic(using=execution_alias(), durable=True):
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                check_session(cursor, target, read_only=True)
            duplicates = (Movie.objects.filter(type__isnull=False, tmdb_id__isnull=False)
                          .order_by().values("type", "tmdb_id").annotate(rows=Count("pk")).filter(rows__gt=1).count())
            if duplicates:
                raise CommandError("Duplicate identities remain after processing the plan.")
        summary.update(state="complete", remaining_duplicate_groups=duplicates)
    else:
        summary["state"] = "limited"
    summary["committed_groups"] = len(applied)
    return {**progress, **summary}
