# Diagnóstico de `GET /api/feed/movies/`

## Flujo comprobado

* **Anónimo:** consulta el catálogo con campos mínimos, ordena por `external_rating`, presencia de imagen e ID, pagina y recién entonces hidrata la página con `display_rating`, contadores y autor. No lee perfil, ratings personales ni pool. Este camino no fue modificado.
* **Autenticado sin ratings:** resuelve un `UserDailyFeedPool`. Sin preferencias, el pool se forma con recientes, exploración, retenidos y títulos con más votos; el score usa calidad, confianza y recencia.
* **Autenticado con ratings:** las señales se actualizan al guardar/eliminar `MovieRating`; se mantiene `UserTasteProfile.ratings_count` y distribuciones 1–10 para cada combinación normalizada de géneros, tipo y director. El pool excluye IDs calificados y toma candidatos adicionales de esas preferencias. El ranking se calcula al construir el pool diario y se persiste como `UserDailyFeedCandidate(base_rank, base_score)`.

No existe `MovieCandidatePool`; los modelos reales son `UserDailyFeedPool` y `UserDailyFeedCandidate`. Tampoco existe una preferencia de país ni país participa en afinidad.

## Ranking vigente

Afinidad: género (0,72), tipo (0,18) y director (0,10), modulados por confianza basada en cantidad de ratings. Calidad elige el promedio real desde 5000 ratings; en caso contrario el rating externo desde 5000 votos; las fuentes pequeñas se atenúan. Confianza y recencia permanecen como señales separadas. `display_rating` conserva su umbral visual independiente de 100 ratings.

## Causa raíz medida por estructura de consulta

El pool ya limitaba el ranking a un máximo nominal de 10 000 candidatos, pero cada página reconstruía un `CASE WHEN id=…` con todos esos IDs y luego añadía agregados de ratings y subqueries correlacionadas (`listas`, `recomendaciones` y ratings de seguidos) antes de paginar. PostgreSQL debía agrupar y ordenar nuevamente todo el pool para devolver unas pocas filas. El costo aparecía después de calificar porque entonces se activaban además las subqueries de afinidad durante la primera construcción del pool. No se encontró N+1 en serialización, pero sí materialización duplicada de los 10 000 pares/IDs y un `sorted()` de 10 000 elementos durante la regeneración; esto último queda acotado y se ejecuta una vez por versión del perfil, no por página.

Sin acceso a una copia del catálogo PostgreSQL de producción no es responsable inventar tiempos absolutos. La instrumentación `profile_feed=1` registra ahora tiempos reales por etapa (`taste_profile_load`, lookup/rebuild/lectura del pool, selección, scoring, sort, filtro, count, hidratación y serializer), tamaños y total de queries. `profile_explain=1` se reserva para diagnóstico controlado.

## Cambio mínimo

1. Se pagina la lista de IDs ya ordenada y se hidrata/anota sólo la página solicitada. Antes se evaluaban hasta 10 000 filas con el gran `CASE`; ahora esa consulta evalúa como máximo `page_size` (50 por configuración).
2. Los IDs rotados del pool se cachean tres horas por pool, versión y bucket, evitando releer/materializar los 10 000 candidatos en cada scroll.
3. La versión del pool incorpora cantidad y fecha de actualización del perfil. Una calificación que cambie gustos fuerza una regeneración; las páginas posteriores reutilizan el resultado persistido/cacheado.
4. El count usa la longitud del subconjunto de IDs y su caché existente; no agrega ni recorre las ~900k producciones.

La primera regeneración todavía hace consultas acotadas de selección contra el catálogo para obtener los 10 000 candidatos. Los índices y planes reales deben validarse en staging mediante la instrumentación añadida. No se tocó el endpoint semanal ni el frontend.

## Diagnóstico de reconstrucción en catálogo productivo

La traza de preview del 7 de septiembre de 2026 acotó el timeout a `fetch_ids()` dentro de `_build_candidate_ids()`, antes del scoring. El SQL generado para cada preferencia combinaba cuatro predicados de límites de token sobre `genre_key` (`=`, `LIKE 'valor|%'`, `LIKE '%|valor'` y `LIKE '%|valor|%'`) con `NOT IN` de ratings y `ORDER BY release_year DESC, external_votes DESC, id DESC LIMIT n`. Los dos patrones con comodín inicial no pueden usar el B-tree de `genre_key`. Con seis preferencias, el código emitía 6 consultas individuales, 15 de pares y una broad —hasta 22 escaneos/sorts de género— además de recent/exploration y fallback por volumen.

Los índices declarados antes de este cambio eran `genre_key`, `(genre_key, type, release_year, id)`, `(type, release_year, id)` y `(release_year, id)`; no existía uno que satisficiera el fallback `ORDER BY external_votes DESC, release_year DESC, id DESC`. Los índices trigram existentes cubren `genre`, no `genre_key`, y no solucionan a la vez el orden solicitado.

No hay credenciales de la base productiva en este workspace, por lo que no se presenta un `EXPLAIN ANALYZE` inventado. La evidencia disponible demuestra que PostgreSQL seguía ejecutando una consulta de bucket al producirse el aborto; el logging incremental ahora emite `start` con el SQL antes de cada cursor y `done` con duración/filas inmediatamente después. Esto permite identificar el bucket exacto aun si el worker muere. En una base de staging debe ejecutarse `EXPLAIN (ANALYZE, BUFFERS)` sobre el SQL registrado.

### Optimización de selección

Los buckets individuales, de pares y broad ahora reutilizan un solo universo ligero de hasta 10.000 pares `(id, genre_key)`. Las mismas comprobaciones de límites de token distribuyen ese universo en memoria entre los buckets, eliminando hasta 21 consultas repetidas sin eliminar ningún tipo de bucket. Recent y exploration continúan siendo fuentes independientes para diversidad, y el fallback completa el pool hasta 10.000.

Se añaden concurrentemente dos índices de orden: `(external_votes DESC, release_year DESC, id DESC)`, que coincide exactamente con el fallback por volumen, y `(release_year DESC, external_votes DESC, id DESC)`, que coincide con los buckets personalizados/recent. `CREATE INDEX CONCURRENTLY` evita bloquear escrituras durante el despliegue. No se añadieron índices para cada combinación de género: el cuello repetitivo se eliminó consolidando consultas en lugar de crear índices grandes para patrones con comodín inicial.
