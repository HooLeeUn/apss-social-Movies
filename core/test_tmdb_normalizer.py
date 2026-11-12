"""Offline tests: python -m unittest core.test_tmdb_normalizer -v"""
import copy
import json
import unittest
from unittest.mock import patch

from core.tmdb_normalizer import normalize_tmdb_metadata


class NormalizerTests(unittest.TestCase):
    def movie(self, **changes):
        payload = dict(id=12, title="English title", original_title="日本語",
                       release_date="2020-02-29", overview="English overview",
                       poster_path="/poster.jpg", vote_average=8.26, vote_count=123,
                       genres=[dict(id=28), dict(id=35)], credits=dict(
                           crew=[dict(id=1, name="Ana", job="Director"),
                                 dict(id=2, name="Writer", job="Writer")],
                           cast=[dict(id=4, name="Second", order=1),
                                 dict(id=3, name="First", order=0)]))
        payload.update(changes)
        return payload

    def run_movie(self, **changes):
        return normalize_tmdb_metadata("movie", 12, self.movie(**changes))

    def test_valid_movie(self):
        r = self.run_movie()
        self.assertTrue(r["valid"])
        self.assertEqual(r["data"]["external_rating"], "8.3")
        self.assertEqual(r["data"]["external_votes"], 123)
        self.assertEqual(r["data"]["release_year"], 2020)
        self.assertEqual(r["data"]["director"], "Ana")
        self.assertEqual(r["data"]["cast_members"], "First, Second")
        self.assertEqual(r["data"]["image"], "https://image.tmdb.org/t/p/w500/poster.jpg")

    def test_valid_series(self):
        r = normalize_tmdb_metadata("series", 12, dict(
            id=12, name="Serie", original_name="原作", first_air_date="2001-03-04",
            created_by=[dict(id=2, name="Creadora")], genre_ids=[10759]))
        self.assertTrue(r["valid"])
        self.assertEqual(r["data"]["type"], "series")
        self.assertEqual(r["data"]["release_year"], 2001)
        self.assertEqual(r["creators"], ["Creadora"])
        self.assertEqual(r["data"]["director"], "Creadora")
        self.assertEqual(r["director_entries"], [dict(name="Creadora", job="Creator")])
        self.assertEqual(r["sources"]["director"], "details.created_by")

    def test_ids(self):
        for bad in (0, -1, True, "12", 12.0, None, 2147483648):
            with self.subTest(bad=bad):
                self.assertFalse(normalize_tmdb_metadata("movie", bad, self.movie())["valid"])

    def test_types(self):
        for bad in ("tv", "film", None, ""):
            self.assertFalse(normalize_tmdb_metadata(bad, 12, self.movie())["valid"])

    def test_identity(self):
        for changes in (dict(id=13), dict(id=True), dict(media_type="tv")):
            self.assertFalse(self.run_movie(**changes)["valid"])
        self.assertFalse(normalize_tmdb_metadata("movie", 12, dict(id=12, name="Series"))["valid"])

    def test_payload(self):
        for bad in (None, [], "invalid"):
            self.assertFalse(normalize_tmdb_metadata("movie", 12, bad)["valid"])

    def test_localized_titles_and_synopses(self):
        r = normalize_tmdb_metadata("movie", 12, self.movie(), spanish=self.movie(
            title="Título español ñ", overview="Descripción española"))
        self.assertEqual(r["data"]["title_spanish"], "Título español ñ")
        self.assertEqual(r["data"]["title_english"], "English title")
        self.assertEqual(r["data"]["synopsis_es"], "Descripción española")
        self.assertEqual(r["sources"]["title_spanish"], "es")

    def test_localized_identity_must_match(self):
        self.assertFalse(normalize_tmdb_metadata("movie", 12, self.movie(),
                                               spanish=self.movie(id=99))["valid"])

    def test_original_is_not_assumed_english(self):
        r = normalize_tmdb_metadata("movie", 12, self.movie(title="Español"), language="es-ES")
        self.assertEqual(r["data"]["title_english"], "日本語")
        self.assertEqual(r["sources"]["title_english"], "details.original")
        self.assertIsNone(r["data"]["synopsis"])

    def test_title_fallback(self):
        r = self.run_movie(original_title="", title="Fallback")
        self.assertEqual(r["data"]["title_spanish"], "Fallback")
        self.assertEqual(r["sources"]["title_spanish"], "details.localized")

    def test_minimal_and_missing_title(self):
        self.assertTrue(normalize_tmdb_metadata("movie", 12, dict(id=12, title="Only"))["valid"])
        self.assertFalse(normalize_tmdb_metadata("movie", 12, dict(id=12))["valid"])

    def test_approved_mapping_is_activated(self):
        r = self.run_movie()
        self.assertEqual(r["data"]["genre"], "Action, Comedy")
        self.assertEqual(r["field_states"]["genre"], "valid")
        self.assertEqual(r["update_data"]["genre"], "Action, Comedy")

    def test_explicit_mapping_order_dedupe_and_unknown(self):
        mapping = {("movie", 28): "Action", ("movie", 35): "Comedy", ("movie", 12): "Action"}
        r = normalize_tmdb_metadata("movie", 12, self.movie(genre_ids=[28],
            genres=[dict(id=35), dict(id=28), dict(id=12), dict(id=999)]), genre_mapping=mapping)
        self.assertEqual(r["data"]["genre"], "Action, Comedy")
        self.assertEqual(r["field_states"]["genre"], "partial")
        self.assertNotIn("genre", r["update_data"])

    def test_movie_series_mapping_separation(self):
        mapping = {("movie", 28): "Action", ("series", 10759): "Action"}
        r = normalize_tmdb_metadata("series", 12, dict(id=12, name="Series", genre_ids=[28]),
                                    genre_mapping=mapping)
        self.assertIsNone(r["data"]["genre"])
        r = normalize_tmdb_metadata("series", 12, dict(id=12, name="Series", genre_ids=[10759]),
                                    genre_mapping=mapping)
        self.assertEqual(r["data"]["genre"], "Action")

    def test_all_allowed_configured_categories(self):
        from core.tmdb_normalizer import INTERNAL_GENRES
        for category in INTERNAL_GENRES:
            r = normalize_tmdb_metadata("movie", 12, self.movie(genres=[dict(id=28)]),
                                        genre_mapping={("movie", 28): category})
            self.assertEqual(r["data"]["genre"], category)

    def test_no_name_based_mapping(self):
        self.assertIsNone(self.run_movie(genres=[dict(name="Drama")])["data"]["genre"])

    def test_duplicate_cast_and_empty_names(self):
        r = self.run_movie(credits=dict(cast=[dict(id=1, name="Ána", order=2),
            dict(id=1, name="Ána", order=1), dict(name=""), dict(name="Zoë", order=None)]))
        self.assertEqual(r["data"]["cast_members"], "Ána, Zoë")

    def test_multiple_directors(self):
        r = self.run_movie(credits=dict(crew=[dict(name="A", job="Director"),
            dict(name="A", job="Director"), dict(name="B", job="Director")]))
        self.assertEqual(r["data"]["director"], "A, B")

    def test_separate_credits(self):
        r = normalize_tmdb_metadata("movie", 12, self.movie(), credits=dict(id=12,
            cast=[dict(name="Other")]))
        self.assertEqual(r["data"]["cast_members"], "Other")
        self.assertFalse(normalize_tmdb_metadata("movie", 12, self.movie(), credits=dict(id=13))["valid"])

    def test_empty_synopsis_and_update_policy(self):
        r = self.run_movie(overview="  ", poster_path=None, credits=dict(cast=[], crew=[]), genres=[])
        for field in ("synopsis", "synopsis_es", "image", "director", "cast_members", "genre"):
            self.assertIsNone(r["data"][field])
            self.assertNotIn(field, r["update_data"])
        self.assertEqual(r["field_states"]["synopsis"], "empty")
        self.assertEqual(r["field_states"]["synopsis_es"], "missing")

    def test_bad_poster(self):
        for path in ("https://evil.test/a.jpg", "//evil/a.jpg", "/../a.jpg", 123):
            self.assertEqual(self.run_movie(poster_path=path)["field_states"]["image"], "invalid")

    def test_bad_ratings(self):
        for rating in (-1, 10.1, float("nan"), float("inf"), True, "8", []):
            self.assertFalse(self.run_movie(vote_average=rating)["valid"])

    def test_bad_votes(self):
        for votes in (-1, 1.1, True, "10", 2147483648):
            self.assertFalse(self.run_movie(vote_count=votes)["valid"])

    def test_zero_and_missing_ratings(self):
        r = self.run_movie(vote_count=0, vote_average=0)
        self.assertEqual(r["update_data"]["external_votes"], 0)
        r = self.run_movie(vote_count=None, vote_average=None)
        self.assertIsNone(r["data"]["external_rating"])
        self.assertNotIn("external_votes", r["update_data"])

    def test_inconsistent_pair_is_withheld(self):
        for changes in (dict(vote_count=0), dict(vote_count=None), dict(vote_average=None)):
            r = self.run_movie(**changes)
            self.assertNotIn("external_rating", r["update_data"])
            self.assertNotIn("external_votes", r["update_data"])

    def test_invalid_dates(self):
        for value in ("2020", "2020-02", "2021-02-29", "0000-01-01", 2020):
            r = self.run_movie(release_date=value)
            self.assertIsNone(r["data"]["release_year"])
            self.assertEqual(r["field_states"]["release_year"], "invalid")

    def test_lengths(self):
        self.assertFalse(self.run_movie(title="x" * 256)["valid"])
        self.assertFalse(self.run_movie(credits=dict(crew=[dict(name="x" * 256, job="Director")]))["valid"])

    def test_json_determinism_and_no_mutation(self):
        payload = self.movie()
        before = copy.deepcopy(payload)
        with patch("socket.socket", side_effect=AssertionError("No network")):
            a = normalize_tmdb_metadata("movie", 12, payload)
            b = normalize_tmdb_metadata("movie", 12, payload)
        self.assertEqual(payload, before)
        self.assertEqual(a, b)
        self.assertEqual(a, json.loads(json.dumps(a, ensure_ascii=False, allow_nan=False)))
        for forbidden in ("id", "author", "real_rating", "display_rating", "ratings"):
            self.assertNotIn(forbidden, a["data"])


class HistoricalGenreTests(unittest.TestCase):
    def normalize(self, ids, production_type="movie", **extra):
        payload = dict(id=12, genre_ids=ids)
        payload["title" if production_type == "movie" else "name"] = "Title"
        payload.update(extra)
        return normalize_tmdb_metadata(production_type, 12, payload)

    def test_every_movie_equivalence(self):
        expected = {
            28: "Action", 12: "Action", 16: "Animation", 35: "Comedy",
            80: "Action, Drama", 99: "Documentary", 18: "Drama",
            10751: "Comedy, Drama", 14: "Animation, Sci-Fi",
            36: "Documentary, Drama", 27: "Horror", 10402: "Musical",
            9648: "Drama", 10749: "Drama", 878: "Sci-Fi",
            53: "Drama, Horror", 10770: "Drama", 10752: "Action", 37: "Action",
        }
        for gid, genres in expected.items():
            with self.subTest(gid=gid):
                r = self.normalize([gid])
                self.assertEqual(r["data"]["genre"], genres)
                self.assertEqual(r["field_states"]["genre"], "valid")
                self.assertTrue(r["valid"])

    def test_every_series_equivalence(self):
        expected = {
            10759: "Action", 16: "Animation", 35: "Comedy", 80: "Action, Drama",
            99: "Documentary", 18: "Drama", 10751: "Comedy, Drama",
            10762: "Action, Animation, Comedy", 9648: "Drama",
            10765: "Animation, Sci-Fi", 10766: "Drama",
            10768: "Action, Drama", 37: "Action",
        }
        for gid, genres in expected.items():
            with self.subTest(gid=gid):
                r = self.normalize([gid], "series")
                self.assertEqual(r["data"]["genre"], genres)
                self.assertEqual(r["field_states"]["genre"], "valid")
                self.assertTrue(r["valid"])

    def test_tv_movie_alone_and_duplicates(self):
        for ids in ([10770], [10770, 10770]):
            self.assertEqual(self.normalize(ids)["data"]["genre"], "Drama")

    def test_tv_movie_ignored_with_others(self):
        for ids, expected in (([10770, 35], "Comedy"), ([10770, 14], "Animation, Sci-Fi")):
            self.assertEqual(self.normalize(ids)["data"]["genre"], expected)
        r = self.normalize([10770, 999])
        self.assertIsNone(r["data"]["genre"])
        self.assertEqual(r["status"], "incomplete")

    def test_news_talk_excluded(self):
        for ids in ([10763], [10767], [10763, 10767], [10767, 10763, 10763]):
            with self.subTest(ids=ids):
                r = self.normalize(ids, "series")
                self.assertEqual(r["status"], "excluded")
                self.assertEqual(r["exclusion_reasons"], ["news_talk_only"])
                self.assertTrue(r["valid"])
                self.assertEqual(r["errors"], [])
                self.assertEqual(r["update_data"], {})

    def test_news_talk_ignored_with_other_genres(self):
        for ids in ([10763, 18], [10767, 18], [10763, 10767, 18]):
            r = self.normalize(ids, "series")
            self.assertEqual(r["data"]["genre"], "Drama")
            self.assertEqual(r["exclusion_reasons"], [])

    def test_reality_always_excluded(self):
        for ids in ([10764], [10764, 18], [10764, 999], [10764, 10763, 10767]):
            r = self.normalize(ids, "series")
            self.assertEqual(r["status"], "excluded")
            self.assertEqual(r["exclusion_reasons"], ["reality"])
            self.assertTrue(r["valid"])
            self.assertEqual(r["update_data"], {})

    def test_unknown_does_not_mean_excluded(self):
        for ids in ([999], [10763, 999], [10767, None]):
            r = self.normalize(ids, "series")
            self.assertEqual(r["status"], "incomplete")
            self.assertEqual(r["exclusion_reasons"], [])
            self.assertEqual(r["errors"], [])
            self.assertIn("unmapped_genre", [w["code"] for w in r["warnings"]])

    def test_ids_do_not_cross_types(self):
        for gid in (28, 12, 14, 36, 27, 10402, 10749, 878, 53, 10770, 10752):
            r = self.normalize([gid], "series")
            self.assertIsNone(r["data"]["genre"])
            self.assertEqual(r["exclusion_reasons"], [])
        for gid in (10759, 10762, 10763, 10764, 10765, 10766, 10767, 10768):
            r = self.normalize([gid])
            self.assertIsNone(r["data"]["genre"])
            self.assertEqual(r["exclusion_reasons"], [])

    def test_deduplication(self):
        r = self.normalize([28, 12, 80, 80, 18, 37])
        self.assertEqual(r["data"]["genre"], "Action, Drama")
        self.assertFalse(r["requires_review"])

    def test_three_genres_do_not_need_tiebreak(self):
        r = self.normalize([10762], "series")
        self.assertEqual(r["data"]["genre"], "Action, Animation, Comedy")
        self.assertFalse(r["requires_review"])
        self.assertIsNone(r["genre_selection"])

    def test_all_generated_categories_are_preserved(self):
        r = self.normalize([14, 80, 35, 99, 27])
        self.assertEqual(r["data"]["genre"], "Action, Animation, Comedy, Documentary, Drama, Horror, Sci-Fi")
        self.assertEqual(r["genre_candidates"], ["Action", "Animation", "Comedy",
                         "Documentary", "Drama", "Horror", "Sci-Fi"])
        self.assertFalse(r["requires_review"])
        self.assertEqual(r["status"], "incomplete")
        self.assertEqual(r["update_data"]["genre"], r["data"]["genre"])
        self.assertNotIn("genre_tiebreak_pending_approval", [w["code"] for w in r["warnings"]])

    def test_five_categories_are_preserved(self):
        r = self.normalize([28, 16, 99, 27, 878])
        self.assertEqual(r["data"]["genre"], "Action, Animation, Documentary, Horror, Sci-Fi")
        self.assertFalse(r["requires_review"])

    def test_four_categories_are_preserved(self):
        for ids, expected in (([18, 28, 16, 99], "Action, Animation, Documentary, Drama"),
                              ([35, 28, 16, 99], "Action, Animation, Comedy, Documentary")):
            r = self.normalize(ids)
            self.assertEqual(r["data"]["genre"], expected)
            self.assertFalse(r["requires_review"])

    def test_determinism_across_genre_permutations(self):
        ids = [14, 80, 35, 99, 27]
        a = self.normalize(ids)
        b = self.normalize(list(reversed(ids)))
        self.assertEqual(a, b)
        self.assertEqual(a, json.loads(json.dumps(a, allow_nan=False)))

    def test_every_catalog_combination_preserves_all_categories(self):
        from itertools import combinations
        from core.tmdb_normalizer import MOVIE_GENRES, SERIES_GENRES
        for production_type, mapping in (("movie", MOVIE_GENRES), ("series", SERIES_GENRES)):
            for ids in combinations(mapping, 3):
                r = self.normalize(list(ids), production_type)
                value = r["data"]["genre"]
                expected = sorted({category for gid in ids for category in mapping[gid]})
                self.assertEqual(value, ", ".join(expected) or None)
                self.assertLessEqual(len(value or ""), 100)

    def test_statuses_and_precedence(self):
        r = normalize_tmdb_metadata("movie", 12, dict(id=12, title="EN", overview="EN",
            release_date="2020-01-01", poster_path="/poster.jpg", genre_ids=[18],
            vote_average=8, vote_count=10, credits=dict(crew=[dict(name="Ana", job="Director")],
            cast=[dict(name="Bob")])), spanish=dict(id=12, title="ES", overview="ES"))
        self.assertEqual(r["status"], "accepted")
        self.assertEqual(self.normalize([])["status"], "incomplete")
        r = self.normalize([10764], "series", id=99)
        self.assertEqual(r["status"], "invalid")
        self.assertEqual(r["exclusion_reasons"], ["reality"])
        self.assertEqual(r["update_data"], {})

    def test_creators_fallback_and_director_priority(self):
        creators = [dict(id=1, name="Ana"), dict(id=1, name="Ana"), dict(name="Zoë")]
        r = self.normalize([18], "series", created_by=creators)
        self.assertEqual(r["data"]["director"], "Ana, Zoë")
        self.assertEqual(r["director_entries"], [dict(name="Ana", job="Creator"), dict(name="Zoë", job="Creator")])
        self.assertEqual(r["update_data"]["director"], "Ana, Zoë")
        r = self.normalize([18], "series", created_by=creators,
                           credits=dict(crew=[dict(name="Director", job="Director")]))
        self.assertEqual(r["data"]["director"], "Director")
        self.assertEqual(r["director_entries"], [dict(name="Director", job="Director")])
        self.assertEqual(r["sources"]["director"], "credits")

    def test_missing_creators_and_no_movie_fallback(self):
        r = self.normalize([18], "series")
        self.assertIsNone(r["data"]["director"])
        self.assertEqual(r["director_entries"], [])
        r = self.normalize([18], created_by=[dict(name="Not a movie director")])
        self.assertIsNone(r["data"]["director"])

    def test_details_and_genre_ids_formats_match(self):
        a = self.normalize([80, 14])
        b = self.normalize([], genres=[dict(id=80, name="Ignore translation"), dict(id=14)])
        self.assertEqual(a, b)

    def complete(self, ids, production_type="movie"):
        payload = dict(id=12, overview="EN", poster_path="/poster.jpg", genre_ids=ids,
                       vote_average=8, vote_count=20, credits=dict(
                           crew=[dict(name="Ana", job="Director")], cast=[dict(name="Bob")]))
        title = "title" if production_type == "movie" else "name"
        payload[title] = "EN"
        payload["release_date" if production_type == "movie" else "first_air_date"] = "2020-01-01"
        return normalize_tmdb_metadata(production_type, 12, payload,
                                       spanish={"id": 12, title: "ES", "overview": "ES"})

    def test_examples_a_and_d_accepted(self):
        for production_type in ("movie", "series"):
            r = self.complete([80, 10751], production_type)
            self.assertEqual(r["data"]["genre"], "Action, Comedy, Drama")
            self.assertEqual(r["status"], "accepted")

    def test_example_b_accepted_and_updatable(self):
        r = self.complete([10762, 10765], "series")
        self.assertEqual(r["data"]["genre"], "Action, Animation, Comedy, Sci-Fi")
        self.assertEqual(r["update_data"]["genre"], "Action, Animation, Comedy, Sci-Fi")
        self.assertEqual(r["status"], "accepted")
        self.assertEqual(r["warnings"], [])
        self.assertFalse(r["requires_review"])
        self.assertEqual(r["genre_candidates"], ["Action", "Animation", "Comedy", "Sci-Fi"])
        self.assertEqual(r, self.complete([10765, 10762], "series"))

    def test_example_c_accepted_and_updatable(self):
        r = self.complete([14, 18, 35])
        self.assertEqual(r["data"]["genre"], "Animation, Comedy, Drama, Sci-Fi")
        self.assertEqual(r["update_data"]["genre"], "Animation, Comedy, Drama, Sci-Fi")
        self.assertEqual(r["status"], "accepted")
        self.assertEqual(r["warnings"], [])

    def test_example_e_direct_scifi_is_preserved(self):
        r = self.complete([878, 28, 18, 35])
        self.assertEqual(r["data"]["genre"], "Action, Comedy, Drama, Sci-Fi")
        self.assertEqual(r["status"], "accepted")

    def test_fantasy_preserves_every_category(self):
        for production_type, fantasy in (("movie", 14), ("series", 10765)):
            r = self.complete([fantasy, 18, 35, 99, 80], production_type)
            self.assertEqual(r["data"]["genre"], "Action, Animation, Comedy, Documentary, Drama, Sci-Fi")
            self.assertEqual(r["status"], "accepted")

    def test_mapping_only_for_matching_type_and_assignment(self):
        for production_type, wrong_id in (("movie", 10765), ("series", 14)):
            r = self.normalize([wrong_id], production_type)
            self.assertIsNone(r["data"]["genre"])
        r = normalize_tmdb_metadata("movie", 12, dict(id=12, title="EN", genre_ids=[14]),
                                    genre_mapping={("movie", 14): "Animation"})
        self.assertEqual(r["data"]["genre"], "Animation")

    def test_duplicate_scifi_sources(self):
        r = self.normalize([14, 878, 14])
        self.assertEqual(r["data"]["genre"], "Animation, Sci-Fi")
        self.assertIsNone(r["genre_selection"])

    def test_four_categories_with_unknown_are_still_partial(self):
        r = self.complete([14, 18, 35, 999])
        self.assertEqual(r["data"]["genre"], "Animation, Comedy, Drama, Sci-Fi")
        self.assertEqual(r["field_states"]["genre"], "partial")
        self.assertEqual(r["status"], "incomplete")
        self.assertNotIn("genre", r["update_data"])
        self.assertIn(dict(field="genre", code="unmapped_genre", genre_id=999), r["warnings"])

    def test_all_genre_permutations_and_no_side_effects(self):
        from itertools import permutations
        for production_type, ids in (("movie", [14, 18, 35, 28]),
                                     ("series", [10765, 18, 35, 10762])):
            original = copy.deepcopy(ids)
            with patch("socket.socket", side_effect=AssertionError("No network")):
                expected = self.complete(ids, production_type)
                for order in permutations(ids):
                    self.assertEqual(self.complete(list(order), production_type), expected)
            self.assertEqual(ids, original)
            self.assertEqual(expected, json.loads(json.dumps(expected, allow_nan=False)))

    def test_one_three_four_five_and_eight_categories(self):
        from core.tmdb_normalizer import INTERNAL_GENRES
        cases = (
            ([18], "Drama"),
            ([80, 10751], "Action, Comedy, Drama"),
            ([14, 18, 35], "Animation, Comedy, Drama, Sci-Fi"),
            ([28, 16, 99, 27, 878], "Action, Animation, Documentary, Horror, Sci-Fi"),
            ([28, 16, 35, 99, 18, 27, 10402, 878], ", ".join(sorted(INTERNAL_GENRES))),
        )
        for ids, expected in cases:
            with self.subTest(ids=ids):
                r = self.complete(ids)
                self.assertEqual(r["data"]["genre"], expected)
                self.assertEqual(r["update_data"]["genre"], expected)
                self.assertEqual(r["status"], "accepted")
                self.assertEqual(r["genre_candidates"], expected.split(", "))
                self.assertIsNone(r["genre_selection"])
                self.assertNotIn("scifi_protected", r)

    def test_maximum_genre_length_and_every_internal_subset(self):
        from itertools import combinations
        from core.tmdb_normalizer import INTERNAL_GENRES
        ids = [28, 16, 35, 99, 18, 27, 10402, 878]
        maximum = ", ".join(sorted(INTERNAL_GENRES))
        self.assertEqual(len(maximum), 70)
        self.assertEqual(len("|".join(sorted(INTERNAL_GENRES))), 63)
        for size in range(9):
            for subset in combinations(ids, size):
                r = self.complete(list(subset))
                self.assertLessEqual(len(r["data"]["genre"] or ""), len(maximum))
                self.assertEqual(r, self.complete(list(reversed(subset))))
                self.assertEqual(r, json.loads(json.dumps(r, allow_nan=False)))


if __name__ == "__main__":
    unittest.main()
