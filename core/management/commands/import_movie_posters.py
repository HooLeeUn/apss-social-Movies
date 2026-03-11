import os
import time
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import quote, unquote

import requests
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q

from core.models import Movie


WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIMEDIA_FILEPATH_BASE = "https://commons.wikimedia.org/wiki/Special:FilePath/"
FANART_BASE_URL = "https://webservice.fanart.tv/v3"
DEFAULT_BATCH_SIZE = 400
DEFAULT_TIMEOUT = 15


@dataclass
class MovieCandidate:
    movie_id: int
    imdb_id: str
    movie_type: Optional[str]


class Command(BaseCommand):
    help = (
        "Importa posters para Movie.image desde Wikidata/Wikimedia y Fanart.tv "
        "usando Movie.imdb_id."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Cantidad máxima de películas a procesar.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help="Cantidad de imdb_id por consulta SPARQL (máximo recomendado: 400).",
        )
        parser.add_argument(
            "--only-empty",
            action="store_true",
            help="Procesa solo películas sin image (comportamiento recomendado).",
        )
        parser.add_argument(
            "--pause",
            type=float,
            default=0.1,
            help="Pausa en segundos entre requests externas.",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Muestra detalles de depuración para consultas a Wikidata y fallback a Fanart.",
        )

    def handle(self, *args, **options):
        limit = options["limit"]
        batch_size = min(max(1, options["batch_size"]), 400)
        only_empty = options["only_empty"]
        pause = max(0.0, options["pause"])
        debug = options["debug"]

        fanart_api_key = self._get_fanart_api_key()
        if not fanart_api_key:
            self.stdout.write(
                self.style.WARNING(
                    "FANART_API_KEY no está configurada; se omitirá Fanart.tv y solo se usará Wikidata/Wikimedia."
                )
            )

        base_qs = Movie.objects.filter(imdb_id__isnull=False).exclude(imdb_id="")
        # Regla: no sobrescribir imágenes existentes.
        empty_image_filter = Q(image__isnull=True) | Q(image="")
        base_qs = base_qs.filter(empty_image_filter)

        if not only_empty:
            self.stdout.write(
                self.style.WARNING(
                    "--only-empty no fue enviado; por seguridad solo se procesarán registros con image vacío."
                )
            )

        if limit:
            base_qs = base_qs.order_by("id")[:limit]
        else:
            base_qs = base_qs.order_by("id")

        total_candidates = base_qs.count()
        if total_candidates == 0:
            self.stdout.write(self.style.SUCCESS("No hay películas candidatas para procesar."))
            return

        self.stdout.write(
            self.style.NOTICE(
                f"Iniciando importación de posters para {total_candidates} películas "
                f"(batch-size={batch_size}, pause={pause}s)."
            )
        )

        stats = {
            "processed": 0,
            "wikidata": 0,
            "fanart": 0,
            "without_poster": 0,
            "errors": 0,
        }

        session = requests.Session()
        session.headers.update(
            {
                "Accept": "application/sparql-results+json",
                "User-Agent": "apss-social-movies/1.0 (Django management command)",
            }
        )

        candidate_iter = base_qs.values_list("id", "imdb_id", "type").iterator(chunk_size=5000)

        batch: List[MovieCandidate] = []
        for movie_id, imdb_id, movie_type in candidate_iter:
            clean_imdb = self._clean_imdb(imdb_id)
            if not clean_imdb:
                continue

            batch.append(MovieCandidate(movie_id=movie_id, imdb_id=clean_imdb, movie_type=movie_type))
            if len(batch) >= batch_size:
                self._process_batch(batch, session, fanart_api_key, pause, stats, debug=debug)
                batch = []

        if batch:
            self._process_batch(batch, session, fanart_api_key, pause, stats, debug=debug)

        self.stdout.write(self.style.SUCCESS("Proceso finalizado."))
        self.stdout.write(f"Procesadas: {stats['processed']}")
        self.stdout.write(f"Posters desde Wikidata/Wikimedia: {stats['wikidata']}")
        self.stdout.write(f"Posters desde Fanart.tv: {stats['fanart']}")
        self.stdout.write(f"Sin poster: {stats['without_poster']}")
        self.stdout.write(f"Errores: {stats['errors']}")

    def _process_batch(self, batch, session, fanart_api_key, pause, stats, debug=False):
        imdb_ids = [item.imdb_id for item in batch]
        wikidata_map = self._fetch_wikidata_posters(session, imdb_ids, pause, stats, debug=debug)

        to_update = []
        missing_for_fanart: List[MovieCandidate] = []

        for item in batch:
            poster_url = wikidata_map.get(item.imdb_id)
            if poster_url:
                to_update.append(Movie(id=item.movie_id, image=poster_url))
                stats["wikidata"] += 1
            else:
                missing_for_fanart.append(item)

        for item in missing_for_fanart:
            if debug:
                self.stdout.write(f"[DEBUG] Wikidata sin poster para {item.imdb_id}; probando fallback Fanart...")
            poster_url = self._fetch_fanart_poster(session, item, fanart_api_key, pause, stats, debug=debug)
            if poster_url:
                to_update.append(Movie(id=item.movie_id, image=poster_url))
                stats["fanart"] += 1
            else:
                stats["without_poster"] += 1

        if to_update:
            with transaction.atomic():
                Movie.objects.bulk_update(to_update, ["image"], batch_size=1000)

        stats["processed"] += len(batch)
        self.stdout.write(
            self.style.NOTICE(
                "Progreso -> "
                f"procesadas={stats['processed']}, "
                f"wikidata={stats['wikidata']}, "
                f"fanart={stats['fanart']}, "
                f"sin_poster={stats['without_poster']}, "
                f"errores={stats['errors']}"
            )
        )

    def _fetch_wikidata_posters(self, session, imdb_ids, pause, stats, debug=False):
        if not imdb_ids:
            return {}

        values = " ".join(f'"{imdb}"' for imdb in imdb_ids)
        query = f"""
SELECT ?imdb_id ?poster WHERE {{
  VALUES ?imdb_id {{ {values} }}
  ?item wdt:P345 ?imdb_id .
  ?item wdt:P3383 ?poster .
}}
""".strip()

        if debug:
            self.stdout.write(f"[DEBUG] imdb_id enviados a Wikidata ({len(imdb_ids)}): {imdb_ids}")
            self.stdout.write("[DEBUG] Consulta SPARQL final:")
            self.stdout.write(query)

        try:
            response = session.get(
                WIKIDATA_SPARQL_URL,
                params={"query": query, "format": "json"},
                timeout=DEFAULT_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
        except (requests.RequestException, ValueError) as exc:
            stats["errors"] += 1
            self.stdout.write(self.style.ERROR(f"Wikidata error: {exc}"))
            return {}
        finally:
            if pause:
                time.sleep(pause)

        results = {}
        bindings = data.get("results", {}).get("bindings", [])
        if debug:
            self.stdout.write(f"[DEBUG] Resultados Wikidata recibidos: {len(bindings)}")
            self.stdout.write(f"[DEBUG] Primer resultado: {bindings[0] if bindings else None}")

        for row in bindings:
            imdb = row.get("imdb_id", {}).get("value")
            poster_value = row.get("poster", {}).get("value")
            if not imdb or not poster_value:
                continue
            filename = poster_value.replace("http://commons.wikimedia.org/wiki/Special:FilePath/", "")
            filename = filename.replace("https://commons.wikimedia.org/wiki/Special:FilePath/", "")
            filename = filename.replace("https://commons.wikimedia.org/wiki/File:", "")
            filename = filename.replace("http://commons.wikimedia.org/wiki/File:", "")
            filename = filename.replace("File:", "", 1)
            filename = unquote(filename).strip()
            if not filename:
                continue
            results[imdb] = f"{WIKIMEDIA_FILEPATH_BASE}{quote(filename, safe='')}"

        return results

    def _fetch_fanart_poster(self, session, item, fanart_api_key, pause, stats, debug=False):
        if not fanart_api_key:
            if debug:
                self.stdout.write(f"[DEBUG] FANART_API_KEY ausente; fallback no disponible para {item.imdb_id}")
            return None

        endpoint = "movies" if item.movie_type == Movie.MOVIE else "tv"
        url = f"{FANART_BASE_URL}/{endpoint}/{item.imdb_id}"
        if debug:
            self.stdout.write(f"[DEBUG] Fanart request -> endpoint={endpoint}, imdb_id={item.imdb_id}")

        try:
            response = session.get(url, params={"api_key": fanart_api_key}, timeout=DEFAULT_TIMEOUT)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            stats["errors"] += 1
            self.stdout.write(self.style.ERROR(f"Fanart error [{item.imdb_id}]: {exc}"))
            return None
        finally:
            if pause:
                time.sleep(pause)

        poster_keys = ["movieposter", "tvposter", "seasonposter", "hdmovieclearart"]
        for key in poster_keys:
            posters = payload.get(key)
            if not isinstance(posters, list) or not posters:
                continue
            first = posters[0]
            if isinstance(first, dict) and first.get("url"):
                return first["url"]

        return None

    @staticmethod
    def _clean_imdb(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        imdb = str(value).strip()
        if not imdb:
            return None
        if imdb.isdigit():
            imdb = f"tt{imdb}"
        elif imdb.lower().startswith("tt"):
            imdb = f"tt{imdb[2:]}"
        return imdb

    @staticmethod
    def _get_fanart_api_key() -> Optional[str]:
        return getattr(settings, "FANART_API_KEY", None) or os.environ.get("FANART_API_KEY")
