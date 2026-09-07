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
