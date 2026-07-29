import json
from pathlib import Path

from django.contrib.auth import get_user_model
from django.core import serializers
from django.core.management.base import BaseCommand, CommandError
from django.core.management.color import no_style
from django.core.serializers.base import DeserializationError
from django.core.exceptions import ValidationError
from django.db import DatabaseError, connection, transaction

from core.models import Movie


class Command(BaseCommand):
    help = "Import the 400 movie/100 series catalog produced by export_staging_catalog."

    SCHEMA_VERSION = 1
    TOTAL_COUNT = 500
    MOVIE_COUNT = 400
    SERIES_COUNT = 100

    def add_arguments(self, parser):
        parser.add_argument("file", help="Path to the JSON catalog to import.")
        parser.add_argument(
            "--author-username",
            required=True,
            help="Username of the existing user to assign to every imported movie.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Validate and report changes without writing to the database.",
        )

    def handle(self, *args, **options):
        author_username = options["author_username"]
        user_model = get_user_model()
        try:
            author = user_model.objects.get(username=author_username)
        except user_model.DoesNotExist as exc:
            raise CommandError(
                f'El usuario "{author_username}" no existe en la base de datos destino.'
            ) from exc

        document = self._read_document(options["file"])
        objects = self._validate_document(document, author)
        ids = [obj.pk for obj in objects]
        existing_ids = set(Movie.objects.filter(pk__in=ids).values_list("pk", flat=True))
        created = len(ids) - len(existing_ids)
        updated = len(existing_ids)

        if not options["dry_run"]:
            try:
                with transaction.atomic():
                    for obj in objects:
                        values = {
                            field.attname: getattr(obj, field.attname)
                            for field in Movie._meta.concrete_fields
                            if not field.primary_key
                        }
                        if obj.pk in existing_ids:
                            Movie.objects.filter(pk=obj.pk).update(**values)
                        else:
                            # bulk_create bypasses auto_now/auto_now_add and therefore
                            # retains every value and the explicit PK from the export.
                            Movie.objects.bulk_create([obj])
                    self._reset_sequence()
            except DatabaseError as exc:
                raise CommandError(f"No se pudo importar el catálogo: {exc}") from exc

        self._write_summary(
            objects, created, updated, options["dry_run"], author_username
        )

    @staticmethod
    def _read_document(filename):
        path = Path(filename).expanduser()
        try:
            with path.open("r", encoding="utf-8") as source:
                return json.load(source)
        except json.JSONDecodeError as exc:
            raise CommandError(
                f"JSON inválido en {path} (línea {exc.lineno}, columna {exc.colno})."
            ) from exc
        except (OSError, UnicodeError) as exc:
            raise CommandError(f"No se pudo leer {path}: {exc}") from exc

    def _validate_document(self, document, author):
        if not isinstance(document, dict):
            raise CommandError("El JSON debe contener un objeto en el nivel superior.")
        if document.get("schema_version") != self.SCHEMA_VERSION:
            raise CommandError(
                f"schema_version incompatible: se esperaba {self.SCHEMA_VERSION}."
            )
        if document.get("model_label") != Movie._meta.label_lower:
            raise CommandError(
                f"model_label inválido: se esperaba {Movie._meta.label_lower}."
            )
        raw_objects = document.get("objects")
        if not isinstance(raw_objects, list):
            raise CommandError("El campo objects debe ser una lista.")
        if document.get("total") != len(raw_objects):
            raise CommandError("El total declarado no coincide con el número de objetos.")
        if len(raw_objects) != self.TOTAL_COUNT:
            raise CommandError("El catálogo debe contener exactamente 500 registros.")
        if document.get("movies_count") != self.MOVIE_COUNT:
            raise CommandError("movies_count debe ser exactamente 400.")
        if document.get("series_count") != self.SERIES_COUNT:
            raise CommandError("series_count debe ser exactamente 100.")

        expected_fields = {
            field.name for field in Movie._meta.concrete_fields if not field.primary_key
        }
        for index, item in enumerate(raw_objects, start=1):
            if not isinstance(item, dict) or set(item) != {"model", "pk", "fields"}:
                raise CommandError(f"El objeto {index} no tiene el formato exportado esperado.")
            if item.get("model") != Movie._meta.label_lower:
                raise CommandError(f"El objeto {index} no corresponde al modelo Movie.")
            if not isinstance(item.get("pk"), int) or isinstance(item["pk"], bool) or item["pk"] <= 0:
                raise CommandError(f"El objeto {index} tiene un ID inválido.")
            fields = item.get("fields")
            if not isinstance(fields, dict) or set(fields) != expected_fields:
                raise CommandError(
                    f"Los campos del objeto con ID {item['pk']} no son compatibles con Movie."
                )

        ids = [item["pk"] for item in raw_objects]
        if len(ids) != len(set(ids)):
            raise CommandError("El catálogo contiene IDs duplicados.")

        try:
            deserialized = list(serializers.deserialize("json", json.dumps(raw_objects)))
        except (DeserializationError, TypeError, ValueError) as exc:
            raise CommandError(f"Hay campos incompatibles con Movie: {exc}") from exc
        objects = [item.object for item in deserialized]

        # The exported foreign key belongs to the source database. Replace it
        # before model validation so it never has to exist in the destination.
        for obj in objects:
            obj.author = author

        movie_count = sum(obj.type == Movie.MOVIE for obj in objects)
        series_count = sum(obj.type == Movie.SERIES for obj in objects)
        if movie_count != self.MOVIE_COUNT or series_count != self.SERIES_COUNT:
            raise CommandError("Los objetos deben contener exactamente 400 películas y 100 series.")
        for obj in objects:
            if not isinstance(obj.tmdb_id, int) or isinstance(obj.tmdb_id, bool) or obj.tmdb_id <= 0:
                raise CommandError(f"El registro con ID {obj.pk} no tiene un tmdb_id válido.")
            try:
                obj.full_clean(validate_unique=False, validate_constraints=False)
            except ValidationError as exc:
                raise CommandError(f"El registro con ID {obj.pk} no es válido: {exc}") from exc
        return objects

    @staticmethod
    def _reset_sequence():
        statements = connection.ops.sequence_reset_sql(no_style(), [Movie])
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def _write_summary(self, objects, created, updated, dry_run, author_username):
        prefix = "Dry-run completado" if dry_run else "Importación completada"
        self.stdout.write(self.style.SUCCESS(prefix))
        self.stdout.write(f"Registros procesados: {len(objects)}")
        self.stdout.write(f"Creados: {created}")
        self.stdout.write(f"Actualizados: {updated}")
        self.stdout.write(f"Autor asignado: {author_username}")
        self.stdout.write(f"Películas: {sum(obj.type == Movie.MOVIE for obj in objects)}")
        self.stdout.write(f"Series: {sum(obj.type == Movie.SERIES for obj in objects)}")
        self.stdout.write(f"IDs mínimo y máximo: {min(obj.pk for obj in objects)}–{max(obj.pk for obj in objects)}")
        self.stdout.write(f"Con image: {sum(bool(obj.image) for obj in objects)}")
        self.stdout.write(f"Con synopsis: {sum(bool(obj.synopsis) for obj in objects)}")
        self.stdout.write(f"Con synopsis_es: {sum(bool(obj.synopsis_es) for obj in objects)}")
        self.stdout.write(
            f"Con trailer: {sum(bool(obj.trailer_es_key or obj.trailer_en_key) for obj in objects)}"
        )
        if not dry_run:
            self.stdout.write("Secuencia de Movie ajustada correctamente.")
