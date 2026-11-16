"""Offline only: python -m unittest core.test_tmdb_enrichment core.test_tmdb_normalizer -v."""
from contextlib import ExitStack
import builtins
import copy
import csv
from datetime import datetime, timezone
from email.utils import format_datetime
import gzip
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import requests

from core.tmdb_enrich import (CSV_FIELDS, FixtureClient, candidates, configuration,
                              inspect_input, main, outside_repository, run_batch)
from core.tmdb_enrichment_client import QueryError, TMDbMetadataClient, strict_json
from core.tmdb_enrichment_store import CheckpointStore, StorageError, file_digest, run_lock
from core.tmdb_enrich_verify import main as verify_main, verify_run
from core.tmdb_normalizer import INTERNAL_GENRES, normalize_tmdb_metadata
from scripts.create_tmdb_enrichment_demo import create_demo, fixture_pair

SECRET = "SYNTHETIC_TOKEN_NEVER_A_REAL_CREDENTIAL"


class Clock:
    def __init__(self):
        self.value = 0
        self.waits = []

    def now(self):
        return self.value

    def wall(self):
        return 1700000000 + self.value

    def sleep(self, value):
        self.waits.append(value)
        self.value += value


class Response:
    def __init__(self, status=200, payload=None, headers=None):
        self.status_code, self.payload = status, payload
        self.headers, self.closed = headers or {}, False

    def json(self, **kwargs):
        if isinstance(self.payload, Exception):
            raise self.payload
        return copy.deepcopy(self.payload)

    def close(self):
        self.closed = True


def http_client(responses, **options):
    clock = Clock()
    session = Mock()
    session.get.side_effect = responses
    events = []
    client = TMDbMetadataClient(SECRET, session=session, sleep=clock.sleep,
        clock=clock.now, wall_clock=clock.wall, jitter=lambda a, b: 0,
        event=lambda *values: events.append(values), **options)
    return client, session, clock, events


def read_gzip(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line, parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
                for line in stream]


class ClientTests(unittest.TestCase):
    def test_locales_endpoints_and_appended_credits(self):
        client, session, clock, events = http_client([Response(payload={"id": 12}) for _ in range(4)])
        for kind in ("movie", "series"):
            for language in ("en-US", "es-ES"):
                client.fetch_part(kind, 12, language)
        calls = session.get.call_args_list
        self.assertEqual([call.args[0] for call in calls], [
            "https://api.themoviedb.org/3/movie/12"] * 2 + ["https://api.themoviedb.org/3/tv/12"] * 2)
        self.assertEqual(calls[0].kwargs["params"], {"language": "en-US", "append_to_response": "credits"})
        self.assertEqual(calls[1].kwargs["params"], {"language": "es-ES"})
        for call in calls:
            self.assertEqual(call.kwargs["timeout"], (3.05, 15))
            self.assertFalse(call.kwargs["allow_redirects"])
            self.assertTrue(call.kwargs["verify"])
        self.assertFalse(session.trust_env)
        self.assertEqual(client.request_count, 4)
        self.assertTrue(all(wait >= .5 for wait in clock.waits))

    def test_429_retry_after_seconds_and_http_date(self):
        for header in ("4", format_datetime(datetime.fromtimestamp(1700000004, timezone.utc), usegmt=True)):
            with self.subTest(header=header):
                client, session, clock, events = http_client([
                    Response(429, headers={"Retry-After": header}), Response(payload={"id": 1})])
                self.assertEqual(client.fetch_part("movie", 1, "en-US"), {"id": 1})
                self.assertGreaterEqual(clock.waits[0], 4)
                self.assertEqual(client.retry_count, 1)
                self.assertIn(("transient_errors", 1), events)

    def test_server_errors_and_connection_errors_retry(self):
        errors = [Response(status) for status in (500, 502, 503, 504)] + [
            requests.Timeout(SECRET), requests.ConnectionError(SECRET),
            requests.exceptions.ChunkedEncodingError(SECRET)]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                client, session, clock, events = http_client([error, Response(payload={"id": 1})])
                client.fetch_part("series", 1, "es-ES")
                self.assertEqual(session.get.call_count, 2)
                self.assertEqual(client.retry_count, 1)
                self.assertGreaterEqual(clock.waits[0], 1)

    def test_exponential_backoff_jitter_and_retry_bound(self):
        client, session, clock, events = http_client([Response(503)] * 4, max_retries=3)
        client.jitter = lambda low, high: high
        with self.assertRaises(QueryError) as caught:
            client.fetch_part("movie", 1, "en-US")
        self.assertTrue(caught.exception.retryable)
        self.assertEqual(clock.waits, [1.25, 2.5, 5.0])
        self.assertEqual((client.request_count, client.retry_count), (4, 3))

    def test_exhausted_429_cooldown_applies_to_next_candidate(self):
        client, session, clock, events = http_client([
            Response(429, headers={"Retry-After": "5"}), Response(payload={"id": 2})], max_retries=0)
        with self.assertRaises(QueryError):
            client.fetch_part("movie", 1, "en-US")
        client.fetch_part("movie", 2, "en-US")
        self.assertGreaterEqual(clock.waits[0], 5)

    def test_very_long_retry_after_defers_without_long_sleep(self):
        client, session, clock, events = http_client([Response(429, headers={"Retry-After": "3600"})])
        with self.assertRaises(QueryError) as caught:
            client.fetch_part("movie", 1, "en-US")
        self.assertEqual(caught.exception.halt, "waiting")
        self.assertEqual(session.get.call_count, 1)
        self.assertEqual(clock.waits, [])
        self.assertIn(("cooldown", 1700003600), events)

    def test_invalid_retry_after_is_finite_and_safe(self):
        client, _, _, _ = http_client([])
        for value in (None, "nonsense", "NaN", "Infinity", "-4"):
            self.assertEqual(client._retry_after(value), 0)

    def test_404_is_not_transient_and_authentication_stops(self):
        for status in (404, 401, 403):
            client, session, _, _ = http_client([Response(status)])
            with self.assertRaises(QueryError) as caught:
                client.fetch_part("movie", 1, "en-US")
            error = caught.exception
            self.assertEqual(error.status, status)
            self.assertFalse(error.retryable)
            self.assertEqual(error.halt, None if status == 404 else "blocked_auth")
            self.assertEqual(session.get.call_count, 1)

    def test_redirects_and_tls_errors_stop_without_secrets(self):
        for response in [Response(302), requests.exceptions.SSLError(SECRET)]:
            client, session, _, _ = http_client([response])
            with self.assertRaises(QueryError) as caught:
                client.fetch_part("movie", 1, "en-US")
            self.assertEqual(caught.exception.halt, "blocked_security")
            self.assertNotIn(SECRET, str(caught.exception))
            self.assertEqual(session.get.call_count, 1)

    def test_budget_counts_attempts_including_retries(self):
        client, session, _, _ = http_client([Response(503)], max_requests=1)
        with self.assertRaises(QueryError) as caught:
            client.fetch_part("movie", 1, "en-US")
        self.assertEqual(caught.exception.halt, "budget_exhausted")
        self.assertEqual(client.request_count, 1)
        self.assertEqual(client.retry_count, 0)

    def test_unsafe_urls_invalid_options_and_ids_never_request(self):
        for url in ("http://api.themoviedb.org/3", "https://evil.test/3", "https://api.themoviedb.org/3?key=x",
                    "https://token@api.themoviedb.org/3", "https://api.themoviedb.org.evil/3"):
            with self.assertRaises(ValueError):
                http_client([], base_url=url)
        for kwargs in ({"requests_per_second": 0}, {"read_timeout": float("nan")},
                       {"max_retries": 11}, {"max_requests": 0}):
            with self.assertRaises(ValueError):
                http_client([], **kwargs)
        client, session, _, _ = http_client([])
        for kind, tmdb_id in (("tv", 1), ("movie", True), ("series", -1), ("movie", 2147483648)):
            with self.assertRaises(ValueError):
                client.fetch_part(kind, tmdb_id, "en-US")
        session.get.assert_not_called()

    def test_json_objects_finiteness_and_redaction(self):
        for payload in (ValueError(SECRET), [], {"unexpected": float("nan")}):
            client, session, _, _ = http_client([Response(payload=payload)])
            with self.assertRaises(QueryError) as caught:
                client.fetch_part("movie", 1, "en-US")
            self.assertEqual(caught.exception.code, "invalid_json")
            self.assertNotIn(SECRET, str(caught.exception))
        client, _, _, _ = http_client([Response(payload={"title": SECRET})])
        self.assertEqual(client.fetch_part("movie", 1, "en-US")["title"], "[REDACTED]")


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="reccool-enrichment-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.demo = create_demo(self.root / "demo")
        self.input = self.demo / "candidates.csv"
        self.fixtures = self.demo / "fixtures"
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        self.stack.enter_context(patch("requests.Session.get", side_effect=AssertionError("Real network forbidden")))
        self.stack.enter_context(patch("socket.socket.connect", side_effect=AssertionError("Network/PostgreSQL forbidden")))

    def batch(self, name="run", *, client=None, resume=False, limit=0, **kwargs):
        return run_batch(self.input, self.root / name, client or FixtureClient(self.fixtures),
                         resume=resume, limit=limit, now=lambda: "2026-11-12T00:00:00+00:00", **kwargs)

    def input_rows(self, rows):
        with self.input.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
            writer.writeheader()
            for kind, tmdb_id in rows:
                writer.writerow(dict(type=kind, tmdb_id=tmdb_id, original_title="Title", popularity="0", adult="False", video="False"))

    def test_all_statuses_counts_unicode_genres_credits_and_sources(self):
        summary = self.batch()
        self.assertTrue(summary["complete"])
        self.assertEqual(summary["counts"], {"accepted": 2, "excluded": 1, "incomplete": 1, "invalid": 3, "pending": 0})
        self.assertEqual(summary["counters"]["examined"], 8)
        self.assertEqual(summary["counters"]["duplicates"], 1)
        self.assertEqual(summary["counters"]["http_requests"], 0)
        self.assertEqual(summary["terminal_results"], 7)
        accepted = read_gzip(self.root / "run/accepted.jsonl.gz")
        movie, series = accepted
        self.assertEqual(set(movie["normalization"]["data"]["genre"].split(", ")), INTERNAL_GENRES)
        self.assertEqual(movie["normalization"]["data"]["external_rating"], "8.3")
        self.assertEqual(movie["normalization"]["data"]["external_votes"], 123)
        self.assertEqual(movie["normalization"]["sources"]["title_spanish"], "es")
        self.assertIn("española", movie["normalization"]["data"]["title_spanish"])
        self.assertEqual(movie["normalization"]["data"]["cast_members"], "Zoë 日本")
        self.assertEqual(series["normalization"]["sources"]["director"], "details.created_by")
        self.assertFalse(movie["publication"]["auto_importable"])
        self.assertTrue(movie["publication"]["requires_destination_identity_check"])
        incomplete = read_gzip(self.root / "run/incomplete.jsonl.gz")[0]
        self.assertIsNone(incomplete["normalization"]["data"]["image"])
        self.assertIn("field_states", incomplete["normalization"])
        self.assertEqual(read_gzip(self.root / "run/duplicates.jsonl.gz")[0]["original_position"], 1)

    def test_incremental_reader_does_not_read_all_records(self):
        class Incremental(io.StringIO):
            reads = 0

            def __next__(self):
                self.reads += 1
                return super().__next__()

            def read(self, *args):
                raise AssertionError("Whole CSV read forbidden")
        stream = Incremental(self.input.read_text(encoding="utf-8"))
        with patch.object(Path, "open", return_value=stream):
            iterator = candidates(self.input)
            first = next(iterator)
            self.assertEqual(first[:3], (1, "movie", 101))
            self.assertEqual(stream.reads, 2)
            iterator.close()

    def test_headers_duplicate_headers_and_bom(self):
        for contents in ("id,type\n1,movie\n", "type,tmdb_id,type,original_title,popularity,adult,video\n"):
            self.input.write_text(contents, encoding="utf-8")
            with self.assertRaises(ValueError):
                inspect_input(self.input)
        self.input_rows([("movie", 101)])
        self.input.write_bytes(b"\xef\xbb\xbf" + self.input.read_bytes())
        self.assertEqual(inspect_input(self.input)["columns"], list(CSV_FIELDS))

    def test_invalid_ids_types_extra_values_and_quoted_newlines(self):
        self.input_rows([(kind, value) for kind, value in [("tv", 1), ("Movie", 1), ("movie", 0),
            ("movie", -1), ("series", "1.2"), ("movie", "https://evil.test"), ("series", 2147483648), ("series", 2147483647)]])
        values = list(candidates(self.input))
        self.assertTrue(all(row[1] is None for row in values[:-1]))
        self.assertEqual(values[-1][1:3], ("series", 2147483647))
        self.input.write_text(",".join(CSV_FIELDS) + '\nmovie,101,"First\nsecond",0,False,False\nmovie,101,x,0,False,False,extra\n', encoding="utf-8")
        values = list(candidates(self.input))
        self.assertEqual(values[0][3]["line_end"], 3)
        self.assertEqual(values[1][1], None)

    def test_direct_normalizer_reuse(self):
        self.input_rows([("movie", 101)])
        with patch("core.tmdb_enrich.normalize_tmdb_metadata", wraps=normalize_tmdb_metadata) as normalizer:
            self.batch()
        self.assertEqual(normalizer.call_count, 1)
        self.assertEqual(normalizer.call_args.args[:2], ("movie", 101))
        self.assertEqual(normalizer.call_args.kwargs["spanish"]["title"], "Órbita española 日本")

    def test_same_tmdb_id_different_type_is_not_duplicate(self):
        self.input_rows([("movie", 101), ("series", 101), ("movie", 101)])
        (self.fixtures / "series_101.json").write_text(strict_json(fixture_pair("series", 101)), encoding="utf-8")
        summary = self.batch()
        self.assertEqual(summary["counts"]["accepted"], 2)
        self.assertEqual(summary["counters"]["duplicates"], 1)

    def test_low_popularity_and_export_flags_are_not_arbitrary_filters(self):
        self.input_rows([("movie", 101)])
        text = self.input.read_text(encoding="utf-8").replace(",0,False,False", ",-999,True,True")
        self.input.write_text(text, encoding="utf-8")
        self.assertEqual(self.batch()["counts"]["accepted"], 1)
        result = read_gzip(self.root / "run/accepted.jsonl.gz")[0]
        self.assertEqual(result["source"]["adult"], "True")
        self.assertFalse(result["publication"]["auto_importable"])

    def test_limit_resume_is_idempotent_and_checksums_reconcile(self):
        first = self.batch(limit=2)
        self.assertFalse(first["complete"])
        self.assertEqual(first["state"], "limited")
        final = self.batch(resume=True)
        repeated = self.batch(resume=True)
        self.assertEqual(final["counts"], repeated["counts"])
        self.assertEqual(final["counters"]["examined"], 8)
        for entry in final["outputs"].values():
            path = self.root / "run" / entry["file"]
            self.assertEqual(path.stat().st_size, entry["bytes"])
            self.assertEqual(file_digest(path), entry["sha256"])
            self.assertEqual(len(read_gzip(path)), entry["records"])
        identities = [(record["type"], record["tmdb_id"]) for status in ("accepted", "excluded", "incomplete")
                      for record in read_gzip(self.root / "run" / f"{status}.jsonl.gz")]
        self.assertEqual(len(identities), len(set(identities)))

    def test_changed_input_or_configuration_refuses_resume(self):
        self.batch(limit=1)
        original = self.input.read_bytes()
        self.input.write_bytes(original + b"\n")
        with self.assertRaises(StorageError):
            self.batch(resume=True)
        self.input.write_bytes(original)
        original_configuration = configuration(FixtureClient(self.fixtures))
        with patch("core.tmdb_enrich.configuration", return_value={**original_configuration, "normalizer_sha256": "different"}):
            with self.assertRaises(StorageError):
                self.batch(resume=True)
        (self.fixtures / "movie_101.json").write_text("{}", encoding="utf-8")
        with self.assertRaises(StorageError):
            self.batch(resume=True)

    def test_missing_offline_fixture_is_pending_and_never_live(self):
        self.input_rows([("movie", 909)])
        result = self.batch()
        self.assertEqual(result["state"], "pending")
        self.assertFalse(result["complete"])
        self.assertEqual(result["counts"]["pending"], 1)
        self.assertEqual(read_gzip(self.root / "run/pending.jsonl.gz")[0]["error"]["code"], "offline_fixture_missing")

    def test_http_404_is_terminal_but_temporary_errors_are_pending(self):
        self.input_rows([("movie", 101), ("movie", 102)])
        client, _, _, _ = http_client([Response(404), Response(503)], max_retries=0)
        result = self.batch(client=client)
        self.assertEqual((result["counts"]["invalid"], result["counts"]["pending"]), (1, 1))
        self.assertEqual(result["counters"]["http_requests"], 2)
        self.assertEqual(result["counters"]["transient_errors"], 1)
        self.assertEqual(read_gzip(self.root / "run/invalid.jsonl.gz")[0]["lookup_error"]["code"], "tmdb_not_found")

    def test_authentication_stops_the_batch_without_reading_other_rows(self):
        client, session, _, _ = http_client([Response(401)])
        result = self.batch(client=client)
        self.assertEqual(result["state"], "blocked_auth")
        self.assertEqual(result["counters"]["examined"], 1)
        self.assertEqual(session.get.call_count, 1)
        self.assertEqual(result["counts"]["pending"], 1)

    def test_partial_success_is_cached_and_retry_does_not_duplicate_en_requests(self):
        self.input_rows([("movie", 101)])
        pair = fixture_pair("movie", 101)
        first_client, first_session, _, _ = http_client([
            Response(payload=pair["english"]), Response(503)], max_retries=0)
        first = self.batch(client=first_client)
        self.assertEqual(first["counts"]["pending"], 1)
        second_client, second_session, _, _ = http_client([Response(payload=pair["spanish"])])
        final = self.batch(client=second_client, resume=True, wall_clock=lambda: 2000000000)
        self.assertTrue(final["complete"])
        self.assertEqual(second_session.get.call_count, 1)
        self.assertEqual(second_session.get.call_args.kwargs["params"], {"language": "es-ES"})
        self.assertEqual(final["counters"]["http_requests"], 3)
        self.assertEqual(read_gzip(self.root / "run/pending.jsonl.gz"), [])

    def test_request_budget_resume_uses_cached_english(self):
        self.input_rows([("movie", 101)])
        pair = fixture_pair("movie", 101)
        client, session, _, _ = http_client([Response(payload=pair["english"])], max_requests=1)
        first = self.batch(client=client)
        self.assertEqual(first["state"], "budget_exhausted")
        client, session, _, _ = http_client([Response(payload=pair["spanish"])])
        final = self.batch(client=client, resume=True)
        self.assertEqual(final["counts"]["accepted"], 1)
        self.assertEqual(final["counters"]["http_requests"], 2)

    def test_long_cooldown_is_persisted_and_resume_does_not_bypass_it(self):
        client, session, _, _ = http_client([Response(429, headers={"Retry-After": "3600"})])
        first = self.batch(client=client)
        self.assertEqual(first["state"], "waiting")
        client, session, _, _ = http_client([])
        second = self.batch(client=client, resume=True, wall_clock=lambda: 1700000001)
        self.assertEqual(second["state"], "waiting")
        session.get.assert_not_called()

    def test_interruption_after_output_fsync_recovers_partial_member_and_reuses_cache(self):
        self.input_rows([("movie", 101)])
        with patch.object(CheckpointStore, "_confirm_output", side_effect=KeyboardInterrupt):
            interrupted = self.batch()
        self.assertEqual(interrupted["state"], "interrupted")
        self.assertFalse(interrupted["complete"])
        self.assertEqual(interrupted["terminal_results"], 0)
        with (self.root / "run/accepted.jsonl.gz").open("ab") as stream:
            stream.write(b"\x1f\x8bpartial-gzip")
        with patch.object(FixtureClient, "fetch_part", side_effect=AssertionError("Confirmed cache must be reused")):
            final = self.batch(resume=True)
        self.assertTrue(final["complete"])
        self.assertEqual(len(read_gzip(self.root / "run/accepted.jsonl.gz")), 1)

    def test_interruption_after_checkpoint_commit_does_not_refetch_terminal_record(self):
        self.input_rows([("movie", 101)])
        terminal = CheckpointStore.terminal

        def interrupt(store, position, record):
            terminal(store, position, record)
            raise KeyboardInterrupt

        with patch.object(CheckpointStore, "terminal", interrupt):
            result = self.batch()
        self.assertEqual(result["counts"]["accepted"], 1)
        with patch.object(FixtureClient, "fetch_part", side_effect=AssertionError("Terminal record must not be fetched")):
            final = self.batch(resume=True)
        self.assertEqual(final["counts"]["accepted"], 1)
        self.assertTrue(final["complete"])

    def test_uncommitted_suffix_is_removed_but_confirmed_corruption_is_refused(self):
        self.batch(limit=1)
        path = self.root / "run/accepted.jsonl.gz"
        with path.open("ab") as stream:
            stream.write(b"partial")
        self.batch(resume=True, limit=1)
        self.assertEqual(len(read_gzip(path)), 1)
        with path.open("r+b") as stream:
            stream.seek(12)
            stream.write(b"CORRUPT")
        with self.assertRaises(StorageError):
            self.batch(resume=True)

    def test_truncated_confirmed_output_is_refused(self):
        self.batch(limit=1)
        (self.root / "run/accepted.jsonl.gz").write_bytes(b"")
        with self.assertRaises(StorageError):
            self.batch(resume=True)

    def test_pending_snapshot_can_be_rebuilt_after_interruption(self):
        self.input_rows([("movie", 909)])
        self.batch()
        (self.root / "run/pending.jsonl.gz").write_bytes(b"partial")
        self.batch(resume=True)
        self.assertEqual(len(read_gzip(self.root / "run/pending.jsonl.gz")), 1)

    def test_gzip_results_are_deterministic_for_fixed_fixtures(self):
        self.batch("first")
        self.batch("second")
        for status in ("accepted", "excluded", "incomplete", "invalid", "duplicates", "pending"):
            self.assertEqual((self.root / "first" / f"{status}.jsonl.gz").read_bytes(),
                             (self.root / "second" / f"{status}.jsonl.gz").read_bytes())

    def test_strict_serialization_does_not_confirm_bad_json(self):
        self.input_rows([("movie", 101)])
        client, _, _, _ = http_client([Response(payload={"id": 101, "extra": float("inf")})])
        result = self.batch(client=client)
        self.assertEqual(result["counts"]["pending"], 1)
        self.assertEqual(result["terminal_results"], 0)
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.assertRaises(ValueError):
                strict_json({"value": value})

    def test_credentials_never_enter_checkpoint_outputs_or_console(self):
        self.input_rows([("movie", 101)])
        pair = fixture_pair("movie", 101)
        pair["english"]["overview"] = SECRET
        self.input.write_text(self.input.read_text(encoding="utf-8").replace("Title", SECRET), encoding="utf-8")
        client, _, _, _ = http_client([Response(payload=pair["english"]), Response(payload=pair["spanish"])])
        self.batch(client=client)
        for path in (self.root / "run").iterdir():
            self.assertNotIn(SECRET.encode(), path.read_bytes())
            if path.suffix == ".gz":
                self.assertNotIn(SECRET, strict_json(read_gzip(path)))
        with patch.dict("os.environ", {"TMDB_READ_ACCESS_TOKEN": SECRET, "TMDB_BASE_URL": "https://evil.test/3"}), \
                patch("sys.stderr", new_callable=io.StringIO) as stderr:
            self.assertEqual(main(["--live", "--input", str(self.input), "--output-dir", str(self.root / "cli")]), 3)
            self.assertNotIn(SECRET, stderr.getvalue())

    def test_no_django_or_postgresql_import_or_connection(self):
        original_import = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name.startswith(("django", "psycopg", "core.models", "config.settings")):
                raise AssertionError("ORM/PostgreSQL forbidden: " + name)
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", guarded):
            self.assertTrue(self.batch()["complete"])

    def test_output_paths_default_mode_and_run_lock(self):
        with self.assertRaises(ValueError):
            outside_repository(Path(__file__).parent / "output")
        with patch("sys.stderr", new_callable=io.StringIO):
            self.assertEqual(main(["--input", str(self.input), "--output-dir", str(self.root / "unused")]), 3)
        self.assertFalse((self.root / "unused").exists())
        lock_dir = self.root / "lock"
        lock_dir.mkdir()
        with run_lock(lock_dir):
            with self.assertRaises(StorageError):
                with run_lock(lock_dir):
                    self.fail("Second run acquired the same lock")

    def test_verifier_checks_completed_and_explicitly_incomplete_runs(self):
        self.batch(limit=1)
        directory = self.root / "run"
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(verify_main([str(directory)]), 2)
            self.assertEqual(verify_main([str(directory), "--allow-incomplete"]), 0)
        self.batch(resume=True)
        self.assertTrue(verify_run(directory, input_path=self.input)["verified"])
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(verify_main([str(directory)]), 0)

    def test_verifier_detects_duplicate_records_even_with_recomputed_digest(self):
        self.batch()
        directory = self.root / "run"
        records = read_gzip(directory / "accepted.jsonl.gz")
        data = (strict_json(records[0]) + "\n") * 2
        path = directory / "accepted.jsonl.gz"
        path.write_bytes(gzip.compress(data.encode("utf-8"), mtime=0))
        manifest_path = directory / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["outputs"]["accepted"].update(bytes=path.stat().st_size, sha256=file_digest(path))
        manifest_path.write_text(strict_json(manifest), encoding="utf-8")
        with patch("sys.stdout", new_callable=io.StringIO):
            self.assertEqual(verify_main([str(directory)]), 3)

    def test_verifier_rejects_bad_counts_unsafe_paths_and_nonfinite_json(self):
        self.batch()
        directory = self.root / "run"
        path = directory / "manifest.json"
        original = json.loads(path.read_text(encoding="utf-8"))
        for change in ("counts", "path", "nonfinite"):
            manifest = copy.deepcopy(original)
            if change == "counts":
                manifest["counts"]["accepted"] += 1
            elif change == "path":
                manifest["outputs"]["accepted"]["file"] = "../other.jsonl.gz"
            else:
                manifest["extra"] = float("nan")
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(verify_main([str(directory)]), 3)

    def test_authentication_can_be_fixed_on_explicit_resume(self):
        self.input_rows([("movie", 101)])
        first_client, _, _, _ = http_client([Response(401)])
        self.batch(client=first_client)
        pair = fixture_pair("movie", 101)
        next_client, session, _, _ = http_client([Response(payload=pair["english"]), Response(payload=pair["spanish"])])
        final = self.batch(client=next_client, resume=True)
        self.assertTrue(final["complete"])
        self.assertEqual(final["counts"]["pending"], 0)
        self.assertEqual(final["counters"]["http_requests"], 3)

    def test_interrupted_initialization_is_not_published_as_a_valid_checkpoint(self):
        original = CheckpointStore._initialize

        def interrupt(store, identity, config):
            original(store, identity, config)
            raise KeyboardInterrupt

        with patch.object(CheckpointStore, "_initialize", interrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.batch()
        self.assertFalse((self.root / "run/checkpoint.sqlite3").exists())
        self.assertFalse((self.root / "run/manifest.json").exists())
        with self.assertRaises(StorageError):
            self.batch(resume=True)


if __name__ == "__main__":
    unittest.main()
