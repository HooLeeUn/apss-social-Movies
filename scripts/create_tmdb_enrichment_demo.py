"""Create synthetic CSV/fixtures only. Never uses TMDb, Django or PostgreSQL."""
import argparse
import csv
from pathlib import Path

from core.tmdb_enrich import CSV_FIELDS, outside_repository
from core.tmdb_enrichment_client import strict_json


def metadata(kind, tmdb_id):
    payload = {"id": tmdb_id, "overview": "An English synthetic synopsis.",
        "poster_path": "/synthetic.jpg", "vote_average": 8.26, "vote_count": 123,
        "adult": False, "video": False,
        "genres": [{"id": value} for value in ([28, 16, 35, 99, 18, 27, 10402, 878]
                                              if kind == "movie" else [10762, 10765])],
        "credits": {"crew": [{"id": 1, "name": "Ána Directora", "job": "Director"}],
                    "cast": [{"id": 2, "name": "Zoë 日本", "order": 0}]}}
    if kind == "movie":
        payload.update(title="Synthetic orbit", original_title="Órbita 日本", release_date="2020-02-29")
    else:
        payload.update(name="Synthetic series", original_name="Série 日本", first_air_date="2021-02-28",
                       created_by=[{"id": 3, "name": "Creator"}])
    return payload


def fixture_pair(kind, tmdb_id):
    english = metadata(kind, tmdb_id)
    spanish = {**english, "overview": "Una sinopsis sintética en español."}
    spanish["title" if kind == "movie" else "name"] = "Órbita española 日本"
    spanish.pop("credits")
    return {"english": english, "spanish": spanish}


def create_demo(directory):
    directory = outside_repository(directory)
    directory.mkdir(parents=True, exist_ok=False)
    fixtures = directory / "fixtures"
    fixtures.mkdir()
    pairs = {("movie", 101): fixture_pair("movie", 101),
             ("series", 202): fixture_pair("series", 202),
             ("movie", 303): fixture_pair("movie", 303),
             ("movie", 404): fixture_pair("movie", 404),
             ("series", 505): fixture_pair("series", 505)}
    pairs[("series", 202)]["english"]["genres"] = [{"id": 10764}]
    pairs[("movie", 303)]["english"]["poster_path"] = None
    pairs[("movie", 303)]["spanish"]["overview"] = ""
    pairs[("movie", 404)]["english"]["id"] = 999  # Deliberate response identity mismatch.
    pairs[("series", 505)]["english"]["credits"]["crew"] = []  # Creator fallback.
    for (kind, tmdb_id), payload in pairs.items():
        (fixtures / f"{kind}_{tmdb_id}.json").write_text(strict_json(payload) + "\n", encoding="utf-8")
    with (directory / "candidates.csv").open("x", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for kind, tmdb_id in [*pairs, ("movie", 101), ("movie", "bad-id"), ("tv", 202)]:
            writer.writerow(dict(type=kind, tmdb_id=tmdb_id, original_title="Síntesis 日本",
                                 popularity="0.01", adult="False", video="False"))
    return directory


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    print(create_demo(parser.parse_args().directory))
