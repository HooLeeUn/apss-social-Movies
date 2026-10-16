# Retirada de Movie.imdb_id

## Alcance

Se elimina el campo de Movie y su uso en runtime, búsqueda social y MovieAdmin.
StreamingProviderLink.imdb_id se conserva, junto con su admin, normalización y
condición IS NULL para clasificar enlaces generales. No se modifica
LEGAL_POLICIES_PAYLOAD ni las migraciones históricas.

La auditoría de producción aportada por el usuario encontró 504 enlaces generales,
cero IMDb y cero coincidencias perdidas: no se necesitan conversiones de enlaces.
El matching interno conserva Movie FK, TMDB, país, tipo, plataforma, prioridad
específico/general y orden updated_at/id. No se cambia la consulta externa TMDB.

## Comandos

| Comando | Decisión |
| --- | --- |
| fix_movie_imdb_ids | Retirado: solo corrige el campo que desaparece. |
| fix_tmdb_ids | Retirado: reparación histórica del catálogo anterior. |
| import_movie_posters | Retirado: matching IMDb con Wikidata/Fanart, sustituido operativamente por imágenes TMDB. |
| import_movies | Retirado: cargador histórico con identidad/matching IMDb. No se conserva una vía de altas ambigua por títulos. |
| import_movie_posters_csv | Retirado: contrato histórico de CSV identificado por IMDb. |
| import_movie_synopsis_csv | Retirado: contrato histórico de CSV identificado por IMDb. |
| import_movie_synopsis_es_csv | Retirado: contrato histórico de CSV identificado por IMDb. |
| enrich_movies_tmdb | Conservado: usa exclusivamente tmdb_id ya asignado; mantiene ratings/votes, imágenes, sinopsis EN/ES, filtros, dry-run, retries de API, reportes de errores, verificación de persistencia y affected-csv/resume. |
| export_movies_without_tmdb | Conservado como diagnóstico sin columna IMDb. |
| fix_movie_directors | Conservado sin exigir columna IMDb en CSV; identidad y matching restantes no cambian. |
| clear_movie_tmdb_content | Conservado; solo se actualiza ayuda. |
| enrich_catalog_csv | Sin cambios: usa identificadores de un archivo externo. |
| generate_tmdb_review_candidates | Sin cambios: usa archivos externos y respuestas TMDB. |

El futuro cargador TMDB Daily ID Exports + API NO se implementa en esta tarea.
Los scripts que invocaban comandos retirados deben actualizarse antes de usarlos.
Los importadores/exportadores de ratings TMDB pendientes en la rama se conservan.

enrich_movies_tmdb ya no asigna ni resuelve tmdb_id: los faltantes/ceros se omiten.
Se retiran las opciones de resolución --only-missing-tmdb-id, --retry-not-found,
--retry-errors, --use-local-exports, --exports-dir, --local-only,
--second-pass-relaxed-match, --year-tolerance y --min-match-score.
--use-existing-tmdb-id-only permanece como alias de compatibilidad del modo actual.
No se retiran los retries de errores transitorios al consultar detalles de TMDB.
Los CSV de affected-csv mantienen su contrato movie_id/status/new_tmdb_id para
reutilizar reportes históricos sin reintroducir resolución de identidad.

## Referencias IMDb restantes

* Campo independiente de enlaces: core/models.py (StreamingProviderLink),
  core/admin.py (StreamingProviderLinkAdmin), core/watch_providers.py (enlaces
  generales y link.imdb_id), core/streaming_provider_links_seed.py (generales).
* Herramientas CSV externas: core/tmdb_catalog.py,
  core/management/commands/enrich_catalog_csv.py,
  core/management/commands/generate_tmdb_review_candidates.py,
  scripts/translate_synopsis_csv.py. No leen Movie.imdb_id.
* Migraciones históricas 0014, dependencia 0015 y 0049: intactas.
* Migración nueva 0073_remove_movie_imdb_id: RemoveField del Movie actual.
* Tests legítimos: core/test_tmdb_catalog.py prueba archivos externos;
  core/test_movie_imdb_removal.py comprueba ausencia del campo, preservación del
  campo independiente, contrato API y exclusión de enlaces exclusivamente IMDb.
* Textos legales de core/views.py: intactos por instrucción expresa; su actualización
  de procedencia de datos queda pendiente en otro bloque.

No debe quedar un acceso operativo a movie.imdb_id ni una consulta ORM de Movie
que seleccione/filtre/actualice esa columna.

## Migración y despliegue

0073_remove_movie_imdb_id depende de 0072_movierecommendationhistory y contiene
solo RemoveField(model_name='movie', name='imdb_id'). SQL obtenido con sqlmigrate:

```sql
BEGIN;
ALTER TABLE "core_movie" DROP COLUMN "imdb_id";
COMMIT;
```

PostgreSQL retira automáticamente el índice asociado a la columna. No se añade
CASCADE ni se modifica core_streamingproviderlink. Se prueba el esquema resultante
en una base temporal PostgreSQL, incluida la presencia de IMDb en enlaces y su
ausencia en Movie.

La migración no se aplica a socialapp, staging ni producción en esta tarea.
Antes del DROP real deben estar retirados todos los procesos con el modelo viejo:
sus SELECT de Movie todavía intentarían leer la columna eliminada. El código nuevo
puede operar con la columna nullable aún presente mientras se coordina el despliegue.
Eliminar la columna pierde sus valores; una reversión de esquema no los recupera.

## Verificación

Se usa un clúster temporal PostgreSQL 18 en 127.0.0.1:55439, inicializado aparte de
los servidores existentes. La base test_reccool_imdb_removal se crea desde cero
con todas las migraciones, incluida 0073. No se usan datos ni credenciales reales.

check: sin incidencias. makemigrations --check: sin cambios pendientes.
sqlmigrate: únicamente ALTER TABLE core_movie DROP COLUMN imdb_id, entre BEGIN/COMMIT.

Verificación final: 122 tests OK en PostgreSQL temporal. Incluye 15 pruebas nuevas
de retirada/modelo/admin/API/enlaces/esquema/comandos y pruebas conservadas de
enriquecimiento, ratings/votes, import/export TMDB, bulk-delete, trailers, créditos,
ratings y pool diario. La tanda final usa MD5PasswordHasher solo en el runner
temporal para acelerar creación de usuarios; no se modifica la configuración del
proyecto. Las APIs externas se simulan en tests.

Una tanda más amplia ejecutó 196 tests y obtuvo 11 fallos en 10 métodos. Se repitieron
los mismos métodos en una copia temporal de git HEAD sin cambios, con otra base
temporal, y se reprodujeron los mismos 11 fallos:

* MovieWatchProvidersEndpointTests.test_does_not_use_provider_direct_url_when_tmdb_supplies_one
* MovieWatchProvidersEndpointTests.test_returns_country_watch_providers_grouped_by_monetization_type
* StreamingProviderLinkCommandTests.test_refresh_streaming_provider_links_adds_claro_video_global_fallbacks
* StreamingProviderLinkCommandTests.test_seed_streaming_provider_links_includes_colombia_provider_variants
  (dos fallos, incluido subcaso provider_id=207)
* MovieListViewSearchAndFiltersTests.test_autocomplete_requires_all_terms_across_metadata
* MovieListViewSearchAndFiltersTests.test_autocomplete_still_uses_autocomplete_serializer_without_search_vector
* FeedMoviesEndpointTests.test_feed_orders_null_release_years_last
* WeeklyRecommendationsTests.test_endpoint_refreshes_snapshot_when_missing
* WeeklyRecommendationsTests.test_previous_closed_week_uses_last_complete_monday_to_monday_window
* WeeklyRecommendationsTests.test_weekly_endpoint_top_user_avatar_uses_media_url_prefix

Son fallos preexistentes de expectativas de enlaces generales/seed, autocomplete,
orden de feed, snapshots, zona horaria y avatar; no se modifican ni se silencian.
La suite amplia no está completamente verde. No se detectaron regresiones nuevas
en los casos ejecutados. El clúster y la copia HEAD temporales se retiran al terminar.
