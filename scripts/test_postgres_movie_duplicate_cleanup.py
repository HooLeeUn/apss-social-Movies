"""Run the cleanup lifecycle against a new, disposable loopback PostgreSQL cluster."""
import argparse
from contextlib import contextmanager
from datetime import timedelta
import io
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

import psycopg


@contextmanager
def isolated_server(binaries, root):
    data = root / "pgdata"
    password_path = root / "password"
    password = secrets.token_urlsafe(32)
    password_path.write_text(password + "\n", encoding="ascii")
    # Ignore ambient routing and credentials. All clients target this fresh cluster.
    env = {key: value for key, value in os.environ.items() if not key.upper().startswith("PG")}
    for key in list(os.environ):
        if key.upper().startswith("PG"):
            del os.environ[key]
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)

    def executable(name):
        return str(binaries / (name + (".exe" if os.name == "nt" else "")))

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    subprocess.run([executable("initdb"), "--pgdata", str(data), "--username", "cleanup_fixture",
                    "--auth", "scram-sha-256", "--pwfile", str(password_path),
                    "--no-locale", "--encoding", "UTF8"], check=True, env=env, creationflags=flags,
                   stdout=subprocess.DEVNULL)
    password_path.unlink()
    with (root / "postgres.log").open("w+", encoding="utf-8") as log:
        server = subprocess.Popen([executable("postgres"), "-D", str(data), "-h", "127.0.0.1",
                                   "-p", str(port), "-c", "unix_socket_directories="],
                                  env=env, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            credentials = {"host": "127.0.0.1", "port": port, "user": "cleanup_fixture",
                           "password": password, "connect_timeout": 2}
            for _ in range(100):
                if server.poll() is not None:
                    break
                try:
                    with psycopg.connect(**credentials, dbname="postgres", autocommit=True) as conn:
                        actual_directory, address, actual_port = conn.execute(
                            "SELECT current_setting('data_directory'), host(inet_server_addr()), inet_server_port()"
                        ).fetchone()
                        if (Path(actual_directory).resolve() != data.resolve()
                                or (address, actual_port) != ("127.0.0.1", port)):
                            raise RuntimeError("Disposable PostgreSQL identity did not match. No fixture database created.")
                        conn.execute("CREATE DATABASE reccool_cleanup_test")
                    break
                except psycopg.OperationalError:
                    time.sleep(0.1)
            else:
                raise RuntimeError("Disposable PostgreSQL did not respond.")
            if server.poll() is not None:
                log.seek(0)
                raise RuntimeError("Disposable PostgreSQL exited:\n" + log.read())
            yield {**credentials, "dbname": "reccool_cleanup_test"}
        finally:
            if server.poll() is None:
                subprocess.run([executable("pg_ctl"), "-D", str(data), "-m", "fast", "-w", "stop"],
                               check=True, env=env, creationflags=flags, stdout=subprocess.DEVNULL)
                server.wait(timeout=30)


def create_fixture_schema_without_migrations():
    """Create disposable test tables directly; never invoke a migration command."""
    from django.apps import apps
    from django.db import connection
    from core.models import Movie

    with connection.cursor() as cursor:
        cursor.execute("CREATE EXTENSION pg_trgm")
        cursor.execute("CREATE EXTENSION unaccent")
    historical_constraints = [constraint for constraint in Movie._meta.constraints
                              if constraint.name != "movie_type_tmdb_id_uniq"]
    with patch.object(Movie._meta, "constraints", historical_constraints):
        with connection.schema_editor() as editor:
            for model in apps.get_models():
                editor.create_model(model)
    # Real search-vector behavior in the fixture, without executing migrations.
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE FUNCTION core_movie_search_vector_update() RETURNS trigger AS $$
            BEGIN
              NEW.search_vector :=
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.title_spanish, ''))), 'A') ||
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.title_english, ''))), 'A') ||
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.release_year::text, ''))), 'A') ||
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.director, ''))), 'B') ||
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.genre, ''))), 'C') ||
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.cast_members, ''))), 'C') ||
                setweight(to_tsvector('simple', unaccent(coalesce(NEW.type, ''))), 'D');
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            CREATE TRIGGER core_movie_search_vector_update_trigger
              BEFORE INSERT OR UPDATE ON core_movie FOR EACH ROW
              EXECUTE FUNCTION core_movie_search_vector_update();
        """)


def exercise(root, credentials, *, no_migrations=False):
    import django
    from django.conf import settings

    assert not settings.configured
    settings.configure(
        SECRET_KEY="synthetic-cleanup-only",
        BASE_DIR=Path(__file__).resolve().parent.parent,
        INSTALLED_APPS=["django.contrib.auth", "django.contrib.contenttypes", "django.contrib.postgres", "core"],
        DATABASES={"default": {"ENGINE": "django.db.backends.postgresql",
            **{key.upper(): value for key, value in credentials.items() if key not in ("dbname", "connect_timeout")},
            "NAME": credentials["dbname"]}},
        DEFAULT_AUTO_FIELD="django.db.models.BigAutoField", USE_TZ=True,
    )
    django.setup()
    from django.contrib.auth import get_user_model
    from django.core.management import call_command
    from django.core.management.base import CommandError
    from django.db import IntegrityError, connection, transaction
    from django.utils import timezone
    from core import movie_duplicate_cleanup as cleanup
    from core.models import Movie, MovieRating, Profile, UserDailyFeedCandidate, UserDailyFeedPool, UserTasteProfile
    from core.tmdb_importer import read_json

    if no_migrations:
        create_fixture_schema_without_migrations()
    else:
        call_command("migrate", "auth", verbosity=0)
        call_command("migrate", "core", "0078", verbosity=0)
    user = get_user_model().objects.create_user(username="synthetic")
    keep = Movie.objects.create(type="movie", tmdb_id=82, title_english="A complete title",
                                genre="Drama, Comedy", synopsis="A synopsis", author=user)
    discard = Movie.objects.create(type="movie", tmdb_id=82, title_english="Incomplete", genre="Drama")
    unrelated = Movie.objects.create(type="movie", tmdb_id=83, title_english="Unrelated", genre="Drama")
    other_keep = Movie.objects.create(type="series", tmdb_id=82, title_english="A series", release_year=2020)
    other_discard = Movie.objects.create(type="series", tmdb_id=82, title_english="Incomplete")
    MovieRating.objects.create(user=user, movie=discard, score=4)
    MovieRating.objects.create(user=user, movie=unrelated, score=8)
    pool = UserDailyFeedPool.objects.create(user=user, pool_date=timezone.localdate(),
                                            expires_at=timezone.now() + timedelta(days=1))
    for movie in (keep, discard, unrelated):
        UserDailyFeedCandidate.objects.create(pool=pool, movie=movie)
    original = Movie.objects.filter(pk=keep.pk).values().get()
    assert original["search_vector"] and original["genre_key"] == "Comedy|Drama"

    plan_dir = root / "plan"
    output = root / "apply"
    target = {"confirm_database": credentials["dbname"], "stdout": io.StringIO()}
    if not no_migrations:
        try:
            call_command("migrate", "core", "0079", verbosity=0)
        except RuntimeError as exc:
            assert "duplicate non-null keys remain" in str(exc)
        else:
            raise AssertionError("Migration accepted duplicates.")
    connection.close()
    call_command("plan_local_movie_duplicate_cleanup", output_dir=str(plan_dir), **target)
    call_command("cleanup_local_movie_duplicates", plan_dir=str(plan_dir), output_dir=str(root / "dry-run"), **target)
    assert Movie.objects.count() == 5
    assert read_json(root / "dry-run" / "execution_summary.json")["committed_groups"] == 0
    connection.close()
    digest = read_json(plan_dir / "cleanup_manifest.json")["plan_sha256"]
    options = {**target, "plan_dir": str(plan_dir), "output_dir": str(output), "apply": True,
               "confirm_maintenance": True, "limit_groups": 1, "confirm_plan_sha256": digest,
               "review_dir": str(root / "dry-run")}
    Movie.objects.filter(pk=keep.pk).update(author_id=None)
    connection.close()
    try:
        call_command("cleanup_local_movie_duplicates", **{**options, "output_dir": str(root / "changed")})
    except CommandError as exc:
        assert "changed since the reviewed dry-run" in str(exc)
    else:
        raise AssertionError("An unreviewed author change was accepted.")
    Movie.objects.filter(pk=keep.pk).update(author_id=user.pk)
    connection.close()
    with psycopg.connect(**credentials, autocommit=True) as other:
        other.execute("SELECT pg_advisory_lock(%s)", [cleanup.LOCK_ID])
        try:
            call_command("cleanup_local_movie_duplicates", **{**options, "output_dir": str(root / "locked")})
        except CommandError as exc:
            assert "already running" in str(exc)
        else:
            raise AssertionError("Concurrent cleanup was accepted.")

    original_assert = cleanup.assert_preferences

    def reject_after_delete(users, **kwargs):
        original_assert(users, **kwargs)
        if not Movie.objects.filter(pk=discard.pk).exists():
            raise CommandError("synthetic failure after delete")

    with patch.object(cleanup, "assert_preferences", reject_after_delete):
        try:
            call_command("cleanup_local_movie_duplicates", **options)
        except CommandError as exc:
            assert "synthetic failure" in str(exc)
        else:
            raise AssertionError("Injected delete failure did not roll back.")
    assert Movie.objects.count() == 5 and MovieRating.objects.count() == 2
    assert UserDailyFeedCandidate.objects.count() == 3
    assert UserTasteProfile.objects.get(user=user).ratings_count == 2
    connection.close()

    def interrupt_after_commit(path, payload):
        if path.name == "group-000000.json" and payload["state"] == "committed":
            raise CommandError("synthetic interruption after commit")
        cleanup_publish(path, payload)

    cleanup_publish = cleanup.publish
    with patch.object(cleanup, "publish", interrupt_after_commit):
        try:
            call_command("cleanup_local_movie_duplicates", resume=True, **options)
        except CommandError as exc:
            assert "interruption after commit" in str(exc)
        else:
            raise AssertionError("Commit gap was not exercised.")
    assert read_json(output / "group-000000.json")["state"] == "pending_commit"
    assert not Movie.objects.filter(pk=discard.pk).exists()
    assert UserTasteProfile.objects.get(user=user).ratings_count == 1
    assert set(UserDailyFeedCandidate.objects.values_list("movie_id", flat=True)) == {keep.pk, unrelated.pk}
    assert UserDailyFeedPool.objects.filter(pk=pool.pk).exists()
    connection.close()

    call_command("cleanup_local_movie_duplicates", resume=True, **options)
    call_command("cleanup_local_movie_duplicates", resume=True, **options)
    report = read_json(output / "execution_summary.json")
    assert report["state"] == "complete" and report["remaining_duplicate_groups"] == 0
    assert set(Movie.objects.values_list("pk", flat=True)) == {keep.pk, other_keep.pk, unrelated.pk}
    assert not Movie.objects.filter(pk=other_discard.pk).exists()
    assert Movie.objects.filter(pk=keep.pk).values().get() == original
    assert get_user_model().objects.filter(pk=user.pk).exists()
    assert Profile.objects.filter(user=user).exists()
    assert MovieRating.objects.filter(movie=unrelated).exists()
    cleanup.assert_preferences({user.pk})
    exercise_preference_drift(root, credentials)
    exercise_resume_lifecycle(root, credentials)
    if not no_migrations:
        call_command("migrate", "core", "0079", verbosity=0)
        from core.tmdb_import_apply import check_schema
        with connection.cursor() as cursor:
            check_schema(cursor, '"core_movie"')
        try:
            with transaction.atomic():
                Movie.objects.create(type="movie", tmdb_id=82, title_english="Duplicate rejected")
        except IntegrityError:
            pass
        else:
            raise AssertionError("Unique constraint did not reject a duplicate.")
        Movie.objects.create(type="movie", tmdb_id=None, title_english="Missing TMDb 1")
        Movie.objects.create(type="movie", tmdb_id=None, title_english="Missing TMDb 2")
        call_command("makemigrations", "core", check=True, dry_run=True, verbosity=0)
    connection.close()
    print("PASS: isolated PostgreSQL cleanup, rollback, commit-gap recovery, idempotence and exact preference deltas.")
    print("Migration commands executed: " + ("0 (fixture schema mode)." if no_migrations else "legacy migration tests enabled."))


def exercise_preference_drift(root, credentials):
    """Real PostgreSQL signals, historical director renames, rollback and resume."""
    from django.contrib.auth import get_user_model
    from django.core.management import call_command
    from django.core.management.base import CommandError
    from django.db import connection
    from django.db.models import Count
    from core import movie_duplicate_cleanup as cleanup
    from core import signals
    from core.models import Movie, MovieRating, UserDirectorPreference
    from core.movie_duplicate_preferences import capture_preferences, REVIEW_KEY
    from core.tmdb_importer import read_json

    legacy_user = get_user_model().objects.create_user(username="synthetic_director_drift")
    others = set(get_user_model().objects.exclude(pk=legacy_user.pk).values_list("pk", flat=True))
    other_preferences = capture_preferences(others)
    other_movies = list(Movie.objects.order_by("pk").values())
    keep_a = Movie.objects.create(type="movie", tmdb_id=90001, title_english="Keep A", synopsis="Full", genre="Drama")
    delete_a = Movie.objects.create(type="movie", tmdb_id=90001, title_english="Discard A", director="Target", genre="Drama")
    keep_b = Movie.objects.create(type="series", tmdb_id=90002, title_english="Keep B", synopsis="Full", genre="Drama")
    delete_b = Movie.objects.create(type="series", tmdb_id=90002, title_english="Discard B", director="Target", genre="Drama")
    # Ensure the reviewed existing policy chooses the intended fixture survivors.
    for keep in (keep_a, keep_b):
        keep.title_spanish = "More complete metadata"
        keep.image = "https://example.test/poster.jpg"
        keep.save()
    first = Movie.objects.create(type="movie", tmdb_id=90003, title_english="Unrelated A", director="Old A", genre="Drama")
    second = Movie.objects.create(type="movie", tmdb_id=90004, title_english="Unrelated B", director="Old B", genre="Drama")
    MovieRating.objects.create(user=legacy_user, movie=delete_a, score=8)
    MovieRating.objects.create(user=legacy_user, movie=delete_b, score=4)
    MovieRating.objects.create(user=legacy_user, movie=first, score=5)
    MovieRating.objects.create(user=legacy_user, movie=second, score=7)
    first.director = "Old A, Co A"
    first.save(update_fields=["director"])
    second.director = "Old B, Co B"
    second.save(update_fields=["director"])
    # Demonstrates the actual historical mechanism: Movie metadata changes do
    # not emit MovieRating signals and therefore leave old preference keys.
    assert not UserDirectorPreference.objects.filter(user=legacy_user, director=first.director).exists()
    assert UserDirectorPreference.objects.filter(user=legacy_user, director="Old A", count_5=1).exists()
    before = capture_preferences({legacy_user.pk})
    survivors = {movie.pk: Movie.objects.filter(pk=movie.pk).values().get() for movie in (keep_a, keep_b)}
    target = {"confirm_database": credentials["dbname"], "stdout": io.StringIO()}
    plan = root / "drift-plan"
    review_dir = root / "drift-review"
    connection.close()
    call_command("plan_local_movie_duplicate_cleanup", output_dir=str(plan), **target)
    call_command("cleanup_local_movie_duplicates", plan_dir=str(plan), output_dir=str(review_dir), **target)
    report = read_json(review_dir / "execution_summary.json")
    assert report["state"] == "validated"
    assert report["preference_validation"]["preexisting_unrelated_director_discrepancies"] == 4
    assert capture_preferences({legacy_user.pk}) == before  # dry-run never repairs.
    reviewed = read_json(review_dir / "review_snapshot.json")[REVIEW_KEY]
    digest = read_json(plan / "cleanup_manifest.json")["plan_sha256"]
    options = {**target, "plan_dir": str(plan), "review_dir": str(review_dir), "apply": True,
        "confirm_maintenance": True, "confirm_plan_sha256": digest, "limit_groups": 1}

    def blocked(options, contains):
        connection.close()
        try:
            call_command("cleanup_local_movie_duplicates", **options)
        except CommandError as exc:
            assert contains in str(exc), str(exc)
        else:
            raise AssertionError("Unsafe preference transition was accepted")

    # Even internally consistent changes after review must invalidate the review.
    unrelated = UserDirectorPreference.objects.get(user=legacy_user, director="Old A")
    unrelated.count_5 = 2
    unrelated.save()
    blocked({**options, "output_dir": str(root / "drift-changed")}, "exact reviewed deletion delta")
    unrelated.count_5 = 1
    unrelated.save()
    assert capture_preferences({legacy_user.pk}) == before

    # A discrepancy on an actually affected director must still block preflight.
    touched = UserDirectorPreference.objects.get(user=legacy_user, director="Target")
    touched.count_8 += 1
    touched.save()
    blocked({**target, "plan_dir": str(plan), "output_dir": str(root / "drift-touched-blocked")}, "deletion target")
    touched.count_8 -= 1
    touched.save()

    # Bad signal side effects on unrelated or related preferences roll back every
    # deletion and every preference change, even with historical drift present.
    original_remove = signals.remove_user_preferences_for_movie_rating
    for field, value in (("Old A", "count_5"), ("Target", "count_8")):
        def corrupt_after_removal(*args, _director=field, _bucket=value, **kwargs):
            original_remove(*args, **kwargs)
            preference = UserDirectorPreference.objects.get(user=legacy_user, director=_director)
            setattr(preference, _bucket, getattr(preference, _bucket) + 1)
            preference.save()

        with patch.object(signals, "remove_user_preferences_for_movie_rating", corrupt_after_removal):
            blocked({**options, "output_dir": str(root / ("drift-rollback-" + value))}, "exact reviewed deletion delta")
        assert Movie.objects.filter(pk__in=[delete_a.pk, delete_b.pk]).count() == 2
        assert capture_preferences({legacy_user.pk}) == before

    output = root / "drift-apply"
    options["output_dir"] = str(output)
    original_publish = cleanup.publish

    def interrupted_receipt(path, payload):
        if path.name == "group-000000.json" and payload["state"] == "committed":
            raise CommandError("synthetic drift interruption after commit")
        original_publish(path, payload)

    with patch.object(cleanup, "publish", interrupted_receipt):
        blocked(options, "drift interruption after commit")
    assert read_json(output / "group-000000.json")["state"] == "pending_commit"
    assert not Movie.objects.filter(pk=delete_a.pk).exists()
    assert Movie.objects.filter(pk=delete_b.pk).exists()
    # Resume also protects historical keys, not merely Movie/dependency snapshots.
    unrelated.refresh_from_db()
    unrelated.count_5 = 2
    unrelated.save()
    blocked({**options, "resume": True}, "exact reviewed deletion delta")
    unrelated.count_5 = 1
    unrelated.save()
    connection.close()
    call_command("cleanup_local_movie_duplicates", resume=True, **options)
    call_command("cleanup_local_movie_duplicates", resume=True, **options)
    final = read_json(output / "execution_summary.json")
    assert final["state"] == "complete" and final["remaining_duplicate_groups"] == 0
    cleanup.assert_preferences({legacy_user.pk}, review=reviewed, removed_movie_ids=[delete_a.pk, delete_b.pk])
    assert capture_preferences(others) == other_preferences
    for original in other_movies:
        assert Movie.objects.filter(pk=original["id"]).values().get() == original
    for pk, original in survivors.items():
        assert Movie.objects.filter(pk=pk).values().get() == original
    assert UserDirectorPreference.objects.filter(user=legacy_user, director="Old A", count_5=1).exists()
    assert UserDirectorPreference.objects.filter(user=legacy_user, director="Old B", count_7=1).exists()
    assert not UserDirectorPreference.objects.filter(user=legacy_user, director__in=["Old A, Co A", "Old B, Co B", "Target"]).exists()
    assert not (Movie.objects.filter(type__isnull=False, tmdb_id__isnull=False).order_by()
        .values("type", "tmdb_id").annotate(rows=Count("pk")).filter(rows__gt=1).exists())
    print("PASS: historical metadata drift reproduced; four keys preserved; touched/new drift blocked; rollback/resume; zero duplicates.")


def exercise_resume_lifecycle(root, credentials):
    """Ten confirmed groups, old serialization failure, and safe same-run resume."""
    import json
    import shutil
    from django.core.management import call_command
    from django.core.management.base import CommandError
    from django.db import connection
    from django.db.models import Count
    from core.models import Movie
    from core import movie_duplicate_cleanup as cleanup
    from core.management.commands import cleanup_local_movie_duplicates as command
    from core.management.commands.verify_local_movie_duplicate_resume import artifact_hashes
    from core.management.commands.diagnose_local_movie_duplicates import json_text
    from core.movie_duplicate_preferences import REVIEW_KEY
    from core.movie_duplicate_resume import LEGACY_RULES
    from core.tmdb_importer import read_json

    survivors = {}
    for i in range(16):
        keep_id, remove_id = ((800, 10000) if i == 10 else
                             (1000 + i * 2, 1001 + i * 2))
        for pk in (keep_id, remove_id):
            Movie.objects.create(pk=pk, type="movie", tmdb_id=100000 + i, title_english="Resume fixture")
        survivors[keep_id] = Movie.objects.filter(pk=keep_id).values().get()
    plan = root / "resume-plan"
    review = root / "resume-review"
    output = root / "resume-apply"
    target = {"confirm_database": credentials["dbname"], "stdout": io.StringIO()}
    connection.close()
    call_command("plan_local_movie_duplicate_cleanup", output_dir=str(plan), **target)
    # Reproduce an already-approved legacy review, not a new substitute plan.
    with patch("core.movie_duplicate_preferences.rules_digest", return_value=LEGACY_RULES):
        call_command("cleanup_local_movie_duplicates", plan_dir=str(plan), output_dir=str(review), **target)
    groups = [json.loads(line) for line in (plan / "cleanup_plan.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(groups) == 16
    assert groups[10]["keep_id"] == 800 and groups[10]["remove_ids"] == [10000]
    source_hashes = artifact_hashes((plan, review))
    options = {**target, "plan_dir": str(plan), "review_dir": str(review), "output_dir": str(output),
        "apply": True, "confirm_maintenance": True, "limit_groups": 10,
        "confirm_plan_sha256": read_json(plan / "cleanup_manifest.json")["plan_sha256"]}
    connection.close()
    call_command("cleanup_local_movie_duplicates", **options)
    first = read_json(output / "execution_summary.json")
    assert first["state"] == "limited" and first["committed_groups"] == 10 and first["pending_groups"] == 6
    first_ten = {path.name: path.read_bytes() for path in output.glob("group-*.json")}
    assert len(first_ten) == 10
    original_config = (output / "execution_config.json").read_bytes()
    audit_index = 0

    def audit(*, execution=output, succeeds=True):
        nonlocal audit_index
        audit_index += 1
        folder = root / f"resume-audit-{audit_index}"
        original = artifact_hashes((plan, review, execution))
        connection.close()
        try:
            call_command("verify_local_movie_duplicate_resume", plan_dir=str(plan), review_dir=str(review),
                execution_dir=str(execution), confirm_plan_sha256=options["confirm_plan_sha256"],
                report_dir=str(folder), **target)
        except CommandError:
            assert not succeeds
        else:
            assert succeeds
        assert artifact_hashes((plan, review, execution)) == original
        result = read_json(folder / "recovery_summary.json")
        assert result["read_only"] and result["original_artifacts_unchanged"] and result["sql"]["mutations"] == 0
        return result

    assert audit()["committed_groups"] == 10  # limited is recoverable.

    def legacy_compare(group, rows, **kwargs):
        actual = cleanup.choose_survivor(rows)
        expected = {key: group[key] for key in ("keep_id", "remove_ids", "scores", "reason", "metadata_sha256")}
        if json_text(actual) != json_text(expected):
            raise CommandError("Group metadata or membership changed.")

    def legacy_failure(execution, progress, error):
        return {**execution, "state": "failed", "error_type": type(error).__name__, "reason": str(error)}

    connection.close()
    with patch.object(cleanup, "validate_group_metadata", legacy_compare), patch.object(command, "failure_report", legacy_failure):
        try:
            call_command("cleanup_local_movie_duplicates", **{**options, "resume": True, "limit_groups": 100})
        except CommandError as error:
            assert "metadata or membership changed" in str(error)
        else:
            raise AssertionError("The original JSON-order regression was not reproduced")
    assert "committed_groups" not in read_json(output / "execution_summary.json")
    recovered = audit()
    assert recovered["original_execution_state"] == "failed" and recovered["committed_groups"] == 10
    assert recovered["pending_groups"] == 6 and recovered["next_pending_group"]["index"] == 10
    assert recovered["preference_review_compatibility"] == "legacy-serialization-fix-v1"

    # Existing JSON is not proof of a commit: malformed counts and a forged
    # committed receipt for an undeleted group are rejected against PostgreSQL.
    corrupt = root / "resume-corrupt-receipt"
    shutil.copytree(output, corrupt)
    bad = read_json(corrupt / "group-000000.json")
    bad["deleted_by_model"]["core.Movie"] = 99
    cleanup.publish(corrupt / "group-000000.json", bad)
    audit(execution=corrupt, succeeds=False)
    forged = root / "resume-forged-commit"
    shutil.copytree(output, forged)
    scope = read_json(review / "review_snapshot.json")
    fake = {"state": "committed", "keep_id": groups[10]["keep_id"], "remove_ids": groups[10]["remove_ids"],
        "deleted_by_model": {"core.Movie": 1},
        "survivor_sha256": scope[cleanup.group_key(groups[10])]["survivor_sha256"],
        "preference_scope_sha256": cleanup.digest(scope[REVIEW_KEY]),
        "preferences_after_sha256": cleanup.digest(scope[REVIEW_KEY]["baseline"])}
    cleanup.publish(forged / "group-000010.json", fake)
    audit(execution=forged, succeeds=False)

    # External metadata changes to either a survivor or a pending group block.
    original_title = Movie.objects.get(pk=1000).title_english
    Movie.objects.filter(pk=1000).update(title_english="Unapproved survivor change")
    audit(succeeds=False)
    Movie.objects.filter(pk=1000).update(title_english=original_title)
    Movie.objects.filter(pk=800).update(title_english="Unapproved pending change")
    blocked = audit(succeeds=False)
    assert blocked["failed_group"]["index"] == 10 and blocked["committed_groups"] == 10
    connection.close()
    try:
        call_command("cleanup_local_movie_duplicates", **{**options, "resume": True, "limit_groups": 2})
    except CommandError:
        pass
    else:
        raise AssertionError("Changed pending metadata was accepted")
    assert len(list(output.glob("group-*.json"))) == 10
    Movie.objects.filter(pk=800).update(title_english=original_title)
    connection.close()
    call_command("cleanup_local_movie_duplicates", **{**options, "resume": True, "limit_groups": 2})
    limited = read_json(output / "execution_summary.json")
    assert (limited["committed_groups"], limited["pending_groups"]) == (12, 4)  # additional, not cumulative.
    assert not Movie.objects.filter(pk=10000).exists() and Movie.objects.filter(pk=800).exists()

    # A publication failure after commit must report the newly committed group,
    # retain pending_commit on disk and remain read-only-verifiable/recoverable.
    original_publish = cleanup.publish

    def fail_after_commit(path, payload):
        if path.name == "group-000012.json" and payload["state"] == "committed":
            raise CommandError("synthetic resume checkpoint publication failure")
        original_publish(path, payload)

    connection.close()
    with patch.object(cleanup, "publish", fail_after_commit):
        try:
            call_command("cleanup_local_movie_duplicates", **{**options, "resume": True, "limit_groups": 100})
        except CommandError as error:
            assert "publication failure" in str(error)
        else:
            raise AssertionError("The post-commit gap was not exercised")
    failed = read_json(output / "execution_summary.json")
    assert (failed["committed_groups"], failed["pending_groups"], failed["removed_movies"]) == (13, 3, 13)
    assert failed["last_committed_group"]["index"] == 12 and failed["failed_group"]["index"] == 12
    assert read_json(output / "group-000012.json")["state"] == "pending_commit"
    assert audit()["committed_groups"] == 13
    assert read_json(output / "group-000012.json")["state"] == "pending_commit"  # audit never edits.
    histories = [read_json(path) for path in output.glob("attempt-*.json")]
    assert any((item.get("previous_summary") or {}).get("state") == "limited" for item in histories)
    assert any((item.get("previous_summary") or {}).get("state") == "failed" for item in histories)
    connection.close()
    call_command("cleanup_local_movie_duplicates", **{**options, "resume": True, "limit_groups": 100})
    connection.close()
    call_command("cleanup_local_movie_duplicates", **{**options, "resume": True, "limit_groups": 100})
    complete = read_json(output / "execution_summary.json")
    assert complete["state"] == "complete" and complete["remaining_duplicate_groups"] == 0
    assert (complete["committed_groups"], complete["pending_groups"], complete["removed_movies"]) == (16, 0, 16)
    for name, content in first_ten.items():
        assert (output / name).read_bytes() == content
    for pk, original in survivors.items():
        assert Movie.objects.filter(pk=pk).values().get() == original
    assert artifact_hashes((plan, review)) == source_hashes
    assert (output / "execution_config.json").read_bytes() == original_config
    assert not (Movie.objects.filter(type__isnull=False, tmdb_id__isnull=False).order_by()
        .values("type", "tmdb_id").annotate(rows=Count("pk")).filter(rows__gt=1).exists())
    print("PASS: ten-group limit; limited/failed/legacy resume; JSON-order regression; immutable checkpoints; corrupt/forged receipts; external drift; commit-gap counters; zero duplicates.")


def main():
    if not __debug__:
        raise RuntimeError("Run the integration with assertions enabled.")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--postgres-bin", type=Path, required=True)
    parser.add_argument("--no-migrations", action="store_true", help="Create only a disposable fixture schema; skip migration-specific assertions")
    args = parser.parse_args()
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    with tempfile.TemporaryDirectory(prefix="reccool-cleanup-pg-") as directory:
        root = Path(directory).resolve()
        assert root.parent == Path(tempfile.gettempdir()).resolve()
        with isolated_server(args.postgres_bin.resolve(strict=True), root) as credentials:
            from django.db.backends.base.base import BaseDatabaseWrapper
            original_connect = BaseDatabaseWrapper.ensure_connection

            def only_disposable(connection):
                cfg = connection.settings_dict
                if (connection.vendor != "postgresql" or cfg["HOST"] != credentials["host"]
                        or str(cfg["PORT"]) != str(credentials["port"]) or cfg["NAME"] != credentials["dbname"]):
                    raise AssertionError("Only the newly verified disposable cluster is allowed")
                return original_connect(connection)

            with patch.object(BaseDatabaseWrapper, "ensure_connection", only_disposable):
                exercise(root, credentials, no_migrations=args.no_migrations)


if __name__ == "__main__":
    main()
