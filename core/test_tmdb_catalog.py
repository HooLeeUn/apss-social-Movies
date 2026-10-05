import csv
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.core.management import call_command
from django.test import SimpleTestCase

from core.tmdb import TMDbServiceError
from core.tmdb_catalog import CatalogEnricher, CatalogResult, poster_url, split_directors


class FakeTMDb:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, path, params=None):
        self.calls.append((path, params))
        response = self.responses[path]
        if isinstance(response, list):
            response = response.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def details(media_type, tmdb_id, director="Jane Doe", en="English", es="Español", poster="/p.jpg"):
    return {
        f"/{media_type}/{tmdb_id}": [
            {"credits": {"crew": [{"job": "Director", "name": director}]}, "created_by": []},
            {"overview": en, "poster_path": poster},
            {"overview": es},
        ]
    }


class CatalogEnricherTests(SimpleTestCase):
    def test_imdb_movie_resolves_before_details_and_does_not_search(self):
        fake = FakeTMDb({"/find/tt123": {"movie_results": [{"id": 7}]},
                         "/movie/7": [{"overview": "English", "poster_path": "/p.jpg"},
                                      {"overview": "Español"}]})
        result = CatalogEnricher(request=fake, requests_per_second=0).resolve(
            {"type": "movie", "imdb_id": " TT123 ", "synopsis": "old"}
        )
        self.assertEqual((result.tmdb_id, result.match_reason, result.status), (7, "imdb_id", "enriched"))
        self.assertFalse(any(path.startswith("/search") for path, _ in fake.calls))
        self.assertEqual([path for path, _ in fake.calls][:2], ["/find/tt123", "/movie/7"])

    def test_imdb_tv(self):
        fake = FakeTMDb({"/find/tt9": {"tv_results": [{"id": 8}]},
                         "/tv/8": [{"overview": "English", "poster_path": "/p.jpg"},
                                   {"overview": "Español"}]})
        result = CatalogEnricher(request=fake, requests_per_second=0).resolve({"type": "series", "imdb_id": "tt9"})
        self.assertEqual((result.tmdb_id, result.match_reason), (8, "imdb_id"))

    def test_imdb_miss_falls_back_and_second_csv_director_matches(self):
        responses = {
            "/find/tt1": {"movie_results": []},
            "/search/movie": {"results": [{"id": 4, "title": "A Film", "release_date": "2003-01-01"}]},
            **details("movie", 4, director="Sean Archer"),
        }
        fake = FakeTMDb(responses)
        result = CatalogEnricher(request=fake, requests_per_second=0).resolve({
            "type": "movie", "imdb_id": "tt1", "title_english": "A Film",
            "release_year": "2003", "director": "Castor Troy, Sean Archer",
        })
        self.assertEqual((result.tmdb_id, result.matched_director), (4, "Sean Archer"))
        self.assertEqual(split_directors("Castor Troy, Sean Archer"), ["Castor Troy", "Sean Archer"])

    def test_single_intersecting_director_is_enough_and_cast_is_ignored(self):
        responses = {
            "/search/movie": {"results": [{"id": 4, "title": "A", "release_date": "2000"}]},
            **details("movie", 4, director="CSV Director"),
        }
        result = CatalogEnricher(request=FakeTMDb(responses), requests_per_second=0).resolve({
            "type": "movie", "title_english": "A", "release_year": 2000,
            "director": "CSV Director, Someone Else", "cast_members": "Wrong Person",
        })
        self.assertEqual(result.tmdb_id, 4)

    def test_no_director_match(self):
        responses = {"/search/movie": {"results": [{"id": 4, "title": "A", "release_date": "2000"}]},
                     "/movie/4": {"credits": {"crew": [{"job": "Director", "name": "Other"}]}}}
        result = CatalogEnricher(request=FakeTMDb(responses), requests_per_second=0).resolve(
            {"type": "movie", "title_english": "A", "release_year": 2000, "director": "Nobody"})
        self.assertEqual(result.status, "not_found")
        self.assertIsNone(result.tmdb_id)

    def test_two_matching_candidates_are_ambiguous(self):
        search = {"results": [{"id": i, "title": "A", "release_date": "2000"} for i in (4, 5)]}
        responses = {"/search/movie": search,
                     "/movie/4": {"credits": {"crew": [{"job": "Director", "name": "Same"}]}},
                     "/movie/5": {"credits": {"crew": [{"job": "Director", "name": "Same"}]}}}
        result = CatalogEnricher(request=FakeTMDb(responses), requests_per_second=0).resolve(
            {"type": "movie", "title_english": "A", "release_year": 2000, "director": "Same"})
        self.assertEqual((result.status, result.candidate_tmdb_ids), ("ambiguous", [4, 5]))

    def test_metadata_language_fallback_and_missing_poster(self):
        fake = FakeTMDb({"/find/tt2": {"movie_results": [{"id": 2}]},
                         "/movie/2": [{"overview": "", "poster_path": None}, {"overview": "Resumen"}]})
        result = CatalogEnricher(request=fake, requests_per_second=0).resolve(
            {"type": "movie", "imdb_id": "tt2", "synopsis": "Original"})
        self.assertEqual((result.synopsis, result.synopsis_en_source), ("Original", "original_csv"))
        self.assertEqual(result.synopsis_es, "Resumen")
        self.assertEqual(result.image, "")
        self.assertEqual(poster_url("/abc.jpg"), "https://image.tmdb.org/t/p/w500/abc.jpg")

    def test_retry_after_and_timeout_retry(self):
        retry = TMDbServiceError("limited", status_code=429, retry_after="3", retryable=True)
        timeout = TMDbServiceError("timeout", retryable=True)
        fake = FakeTMDb({"/find/tt3": [retry, timeout, {"movie_results": []}]})
        sleeps = []
        result = CatalogEnricher(request=fake, requests_per_second=0, max_retries=2, sleep=sleeps.append).resolve(
            {"type": "movie", "imdb_id": "tt3", "title_english": "", "release_year": "", "director": ""})
        self.assertEqual(result.retry_count, 2)
        self.assertEqual(sleeps, [3.0, 2])


class EnrichCatalogCommandTests(SimpleTestCase):
    def test_preserves_original_columns_while_using_tmdb_media_kinds(self):
        original_columns = (
            "imdb_id", "title_english", "title_spanish", "type", "genre",
            "release_year", "director", "cast_members", "external_rating", "external_votes",
        )
        input_rows = [
            {
                "imdb_id": "TT9", "title_english": "A Series", "title_spanish": "Una serie",
                "type": "series", "genre": "Drama", "release_year": "2020",
                "director": "Jane Doe", "cast_members": "Actor One, Actor Two",
                "external_rating": "8.10", "external_votes": "00123", "synopsis": "Old series",
            },
            {
                "imdb_id": "TT7", "title_english": "A Movie", "title_spanish": "Una película",
                "type": "movie", "genre": "Comedy", "release_year": "2019",
                "director": "John Doe", "cast_members": "Actor Three",
                "external_rating": "7.00", "external_votes": "00456", "synopsis": "Old movie",
            },
        ]
        fake = FakeTMDb({
            "/find/tt9": {"tv_results": [{"id": 9}]},
            "/tv/9": [
                {"overview": "English series", "poster_path": "/series.jpg"},
                {"overview": "Serie en español"},
            ],
            "/find/tt7": {"movie_results": [{"id": 7}]},
            "/movie/7": [
                {"overview": "English movie", "poster_path": "/movie.jpg"},
                {"overview": "Película en español"},
            ],
        })

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output = root / "in.csv", root / "out.csv"
            with source.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=(*original_columns, "synopsis"))
                writer.writeheader()
                writer.writerows(input_rows)

            with patch("core.tmdb_catalog.get_tmdb_json", new=fake):
                call_command("enrich_catalog_csv", str(source), output=str(output), requests_per_second=0)

            with output.open(encoding="utf-8") as fh:
                enriched_rows = list(csv.DictReader(fh))

        self.assertIn("/tv/9", [path for path, _ in fake.calls])
        self.assertIn("/movie/7", [path for path, _ in fake.calls])
        self.assertEqual([row["type"] for row in enriched_rows], ["series", "movie"])
        for original, enriched in zip(input_rows, enriched_rows):
            for column in original_columns:
                self.assertEqual(enriched[column], original[column], column)
        self.assertEqual(
            [row["synopsis"] for row in enriched_rows],
            ["English series", "English movie"],
        )

    def test_incremental_output_report_resume_and_no_orm(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, output, report = root / "in.csv", root / "out.csv", root / "report.csv"
            source.write_text("imdb_id,title_english,type,release_year,director,cast_members\n"
                              "tt1,A,movie,2000,Jane,Actor\n", encoding="utf-8")
            resolved = CatalogResult(status="matched_partial", tmdb_id=10, match_reason="imdb_id",
                                     synopsis="English", synopsis_en_source="tmdb")
            with patch("core.management.commands.enrich_catalog_csv.CatalogEnricher.resolve", return_value=resolved) as mock:
                call_command("enrich_catalog_csv", str(source), output=str(output), report=str(report), requests_per_second=0)
                call_command("enrich_catalog_csv", str(source), output=str(output), report=str(report), resume=True,
                             requests_per_second=0)
            self.assertEqual(mock.call_count, 1)
            with output.open(encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["tmdb_id"], "10")
            self.assertNotIn("movie_id", rows[0])
            with report.open(encoding="utf-8") as fh:
                statuses = [row["status"] for row in csv.DictReader(fh)]
            self.assertEqual(statuses, ["matched_partial", "skipped_resume"])
