import re
from urllib.parse import urlparse, urlunparse

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from core.models import Movie


AMAZON_IMAGE_HOST = "m.media-amazon.com"
DEFAULT_TARGET_SIZE = 600
DEFAULT_BATCH_SIZE = 1000

SIZE_TOKEN_RE = re.compile(r"(?:^|_)(?:U[XY]\d+|S[XY]\d+)(?:_|$)", re.IGNORECASE)
AMAZON_SIZE_SUFFIX_RE = re.compile(
    r"\._V1_(?P<directives>[^/]*?)(?P<extension>\.(?:jpg|jpeg|png|webp))$",
    re.IGNORECASE,
)


class Command(BaseCommand):
    help = (
        "Actualiza Movie.image para reemplazar sufijos de tamaño de m.media-amazon.com "
        "por una versión de mayor resolución."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Muestra qué URLs se actualizarían sin guardar cambios.",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Procesa como máximo N películas con image no vacío.",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=DEFAULT_BATCH_SIZE,
            help=f"Cantidad de películas a actualizar por lote (default: {DEFAULT_BATCH_SIZE}).",
        )
        parser.add_argument(
            "--size",
            type=int,
            default=DEFAULT_TARGET_SIZE,
            help=f"Alto objetivo para el sufijo UY (default: {DEFAULT_TARGET_SIZE}).",
        )
        parser.add_argument(
            "--clean",
            action="store_true",
            help="Elimina el sufijo ._V1_... y deja la URL base con su extensión original.",
        )

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        limit = options["limit"]
        batch_size = max(1, options["batch_size"])
        size = options["size"]
        clean = options["clean"]

        if limit is not None and limit < 0:
            raise CommandError("--limit debe ser un número mayor o igual a 0.")
        if size <= 0:
            raise CommandError("--size debe ser un número mayor a 0.")

        queryset = Movie.objects.exclude(image__isnull=True).exclude(image="").only("id", "image").order_by("id")
        if limit is not None:
            queryset = queryset[:limit]

        stats = {"processed": 0, "updated": 0, "skipped": 0}
        movies_to_update = []

        self.stdout.write(
            self.style.NOTICE(
                "Revisando URLs de posters de películas "
                f"(limit={limit}, batch-size={batch_size}, size={size}, clean={clean}, dry-run={dry_run})."
            )
        )

        for movie in queryset.iterator(chunk_size=batch_size):
            stats["processed"] += 1
            new_url = build_amazon_high_resolution_url(movie.image, size=size, clean=clean)

            if not new_url or new_url == movie.image:
                stats["skipped"] += 1
                continue

            if dry_run:
                stats["updated"] += 1
                self.stdout.write(f"[dry-run] Movie {movie.id}: {movie.image} -> {new_url}")
                continue

            movie.image = new_url
            movies_to_update.append(movie)
            if len(movies_to_update) >= batch_size:
                self._bulk_update(movies_to_update)
                stats["updated"] += len(movies_to_update)
                movies_to_update.clear()

        if not dry_run and movies_to_update:
            self._bulk_update(movies_to_update)
            stats["updated"] += len(movies_to_update)

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry-run activo: no se guardaron cambios."))

        self.stdout.write(self.style.SUCCESS(f"Procesadas: {stats['processed']}"))
        self.stdout.write(self.style.SUCCESS(f"Actualizadas: {stats['updated']}"))
        self.stdout.write(self.style.SUCCESS(f"Saltadas: {stats['skipped']}"))

    @staticmethod
    def _bulk_update(movies):
        with transaction.atomic():
            Movie.objects.bulk_update(movies, ["image"], batch_size=len(movies))


def build_amazon_high_resolution_url(url, size=DEFAULT_TARGET_SIZE, clean=False):
    if not url:
        return None

    parsed = urlparse(str(url).strip())
    if parsed.hostname != AMAZON_IMAGE_HOST:
        return None

    match = AMAZON_SIZE_SUFFIX_RE.search(parsed.path)
    if not match or not SIZE_TOKEN_RE.search(match.group("directives")):
        return None

    extension = match.group("extension")
    replacement = extension if clean else f"._V1_UY{size}_{extension}"
    new_path = f"{parsed.path[:match.start()]}{replacement}"

    return urlunparse(parsed._replace(path=new_path, query="", fragment=""))
