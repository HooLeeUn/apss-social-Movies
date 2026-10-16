# Borrado masivo seguro de Movie

## Estado y evidencia

APPLY exige cero relaciones salvo una excepción explícita:
--delete-feed-candidates permite limpiar UserDailyFeedCandidate de los IDs del CSV.
Sin ese flag los candidatos siguen provocando aborto. El usuario aportó un preflight
real de socialapp_render_clone: 561579 filas e IDs únicos, 561579 existentes, cero
ausentes/errores, 282721 candidatos y cero filas en las otras diez FK auditadas.
Ese resultado no se reutiliza como autorización técnica: apply repite todas las
comprobaciones bajo bloqueos dentro de la transacción que borra.

Durante esta implementación no se conectó a ninguna base ni se ejecutó dry-run
o apply. Los tests usan cursores y transacciones simulados, sin base de tests.

## Auditoría conservada

Las once FK entrantes a Movie tienen on_delete=CASCADE:

| Modelo | Campo | Política |
| --- | --- | --- |
| StreamingProviderLink | movie (nullable) | CASCADE |
| MovieRating | movie | CASCADE |
| MovieListItem | movie | CASCADE |
| MovieRecommendationItem | movie | CASCADE |
| MovieRecommendationHistory | movie | CASCADE |
| ProfileFavoriteMovie | movie | CASCADE |
| WeeklyRecommendationItem | movie | CASCADE |
| UserDailyFeedCandidate | movie | CASCADE |
| Comment | movie | CASCADE |
| VideoComment | movie | CASCADE |
| UserNotification | movie (nullable) | CASCADE |

No hay PROTECT, RESTRICT, SET_NULL, DO_NOTHING ni M2M entrantes en los modelos
actuales. Movie.author es saliente; borrar Movie no borra User. Hay cascadas
indirectas a CommentReaction, VideoCommentReaction y UserNotification por sus
comentarios/vídeos. Al exigir cero comentarios/vídeos tampoco hay hijos indirectos
afectados por este borrado.

CoreConfig.ready registra core.signals. Movie no tiene receptores de borrado.
MovieRating.post_delete ejecuta sync_preferences_after_movie_rating_delete:
decrementa histogramas count_1..count_10 por género (incluida clave combinada), tipo
y director; recalcula score y ratings_count; elimina preferencias vacías; actualiza
UserTasteProfile.ratings_count/last_updated_at y limpia el pool activo. SQL directo
no lo hace: por ello una sola valoración, de cualquier usuario, aborta apply.
No se usa estado TMDB ni actividad de usuarios como sustituto de esta condición.

MovieRecommendationItem.pre_delete cierra intervalos salvo si el origen es Movie
o QuerySet de Movie. En ese origen omite el cierre porque el historial cae por
cascada. Aquí tanto items como historial deben estar vacíos. Las señales de save
no se disparan en un borrado normal de Movie.

VideoComment.delete limpia storage al borrar la instancia; el Collector de Movie
no llama ese override en hijos. ContentReport.content_type/object_id tiene enlaces
lógicos a comentarios/vídeos sin FK ni limpieza automática. El contador
WeeklyRecommendationSnapshot.items_count tampoco tiene señal de ajuste. La
condición de cero vídeos/comentarios/items evita nuevos efectos de estos tipos;
no repara huérfanos preexistentes. El trigger de búsqueda de Movie atiende
INSERT/UPDATE, no DELETE.

Django CASCADE se implementa en Collector, no implica ON DELETE CASCADE en SQL.
Apply consulta pg_constraint y exige exactamente las once FK/tablas/columnas
esperadas, sin cascadas SQL (solo NO ACTION/RESTRICT). Rechaza divergencia del
modelo, nuevas relaciones o drift del catálogo. No intenta cascadas SQL ni
reproducir señales: exige cero relaciones antes del DELETE de Movie.

UserDailyFeedCandidate guarda id, pool_id, movie_id, base_rank, base_score y
created_at, con unicidad (pool,movie). Ambas FK salientes usan CASCADE. No tiene
relaciones entrantes en Django ni receptores de pre/post_delete o pre/post_save,
ni overrides de save/delete. DailyFeedPoolService lo genera con bulk_create a
partir del catálogo, preferencias y candidatos retenidos. No es actividad permanente.
Al usar el flag, apply verifica también que no existan FK entrantes reales hacia
candidatos. En ambos modos de apply rechaza triggers DELETE no internos habilitados
en Movie; con el flag hace lo mismo para candidatos. Triggers internos de integridad
referencial se cubren con la auditoría de FK.

## Transacción y concurrencia

1. Valida todo el CSV antes de acceder a conexiones: positivos canónicos, únicos,
   dentro de bigint; rechaza todo el archivo por cualquier error. reason se ignora.
2. Comprueba vendor PostgreSQL y NAME configurado contra --expected-database.
3. Abre una única transacción; verifica también current_database() exactamente.
4. Crea tabla temporal con PK, carga el flujo normalizado con COPY FROM STDIN y
   ejecuta ANALYZE. No necesita que el servidor tenga acceso al archivo local.
5. En apply bloquea Movie y las once tablas relacionadas, en orden fijo, con
   SHARE ROW EXCLUSIVE. Bloquea escrituras concurrentes e impide nuevas referencias
   hasta commit/rollback; permite SELECT normales. Se requiere permiso de bloqueo
   apropiado sobre esas tablas. Un fallo de permisos aborta sin borrar.
6. Usa READ COMMITTED en apply para que los conteos vean los commits de escritores
   anteriores que terminaron mientras se esperaban bloqueos. Un snapshot anterior
   a los bloqueos podría omitir referencias recién confirmadas. Dry-run conserva
   REPEATABLE READ, sin estos bloqueos de escritura.
7. Audita FK/triggers reales, cuenta existentes/ausentes y las once relaciones.
   Cuenta también filas de candidatos, count(DISTINCT movie_id) y
   count(DISTINCT pool_id). Apply exige todos los IDs existentes y las otras diez
   relaciones vacías. Sin el flag exige vacíos también los candidatos.
8. Con --delete-feed-candidates ejecuta DELETE de core_userdailyfeedcandidate USING
   pg_temp.bulk_movie_ids con igualdad movie_id=id. Exige rowcount igual al conteo
   inicial y candidatos restantes cero. Recuenta las once relaciones y exige cero
   en todas. No borra ni modifica UserDailyFeedPool ni ninguna de las otras diez
   relaciones; tampoco candidatos de películas fuera del CSV.
9. Ejecuta un solo DELETE FROM core_movie USING pg_temp.bulk_movie_ids con igualdad
   de PK. Cuenta de nuevo los objetivos: exige cero restantes y rowcount igual a
   existing_before. Fuerza constraints diferidas con SET CONSTRAINTS ALL IMMEDIATE.
10. Commit al salir de atomic; informa ambos conteos eliminados solo tras commit.
   Cualquier excepción revierte todo. Dry-run siempre revierte, sin DELETE.

No permite invocación dentro de otra transacción ni con autocommit desactivado:
un mensaje de éxito significa
que el comando salió de su propia transacción. No hay .delete() por objeto.
--expected-database sigue siendo obligatorio también para dry-run.

561579 películas y los candidatos derivados es un volumen razonable para una
transacción PostgreSQL;
el tamaño por sí solo no exige lotes. Esta elección conserva todo-o-nada. Produce
WAL y tuplas muertas y retiene bloqueos hasta terminar: ejecutar en ventana de
mantenimiento con capacidad de WAL/disco suficiente. No se midió duración ni
capacidad del servidor. Lock timeout 5 s; statement timeout 10 minutos; un timeout
revierte todo. Una ejecución por lotes necesitaría persistir progreso y permitir
estado parcial: no se implementa porque debilitaría esta garantía.

## Reporte

JSON: database, mode, rows_read, unique_ids, existing_before, candidate_rows_before,
unique_movies_with_candidates, affected_pools (número de pools distintos),
candidate_rows_deleted, candidate_rows_remaining, related_counts (conteos ANTES,
rows/on_delete por relación), deleted, remaining_after, errors y not_found.
Apply con flag añade related_counts_after con las once relaciones revalidadas
tras limpiar candidatos. No sobrescribe la evidencia inicial.

Dry-run: deleted=candidate_rows_deleted=0, remaining_after=existing_before y
candidate_rows_remaining=candidate_rows_before. Los fallos emiten errors>=1,
deleted=candidate_rows_deleted=0 y CommandError con salida no cero; los campos
no medidos son null. En fallos después de DELETE, los conteos restantes representan
la comprobación intentada,
no el estado tras rollback. Como en cualquier cliente PostgreSQL, una pérdida de
conexión al confirmar puede dejar el resultado del commit indeterminado: verificar
con dry-run antes de reintentar; no asumir que un error de transporte prueba rollback.

## Cachés y pools: sin invalidación implementada

No se limpia caché, modifica pool_version, borra pools ni regenera candidatos.

| Caché | Clave | TTL configurado |
| --- | --- | --- |
| IDs rotados | daily_feed_rotated:v1:{pool.id}:{pool.pool_version}:{rotation_bucket} | 3 horas |
| Conteo del feed | feed_movies_count_v3, usuario/fecha/versión/filtros | 120 segundos |
| Página serializada | feed_movies_v3, usuario/fecha/versión/página/filtros/rotación/perfil | 120 segundos |

La rotación cambia cada dos horas, lo que puede dejar de usar una clave de IDs
antes de su expiración. Los TTL se cuentan desde cada escritura de caché, no desde
el commit; una petición concurrente con datos anteriores podría escribir después.
Al reconstruir desde SQL, el feed filtra IDs contra Movie existentes y descarta
los borrados, pero una página serializada cacheada se devuelve directamente:
puede mostrar películas eliminadas durante su vigencia. El conteo puede ser stale.
Reconstruir el pool cambia su ID, pero las claves de página/conteo no lo incluyen;
si se mantiene la misma versión no garantiza invalidación de esas claves.

get_daily_pool reutiliza el pool de hoy si su versión coincide; no comprueba
cantidad de candidatos ni expires_at para decidir reconstruir. El pool puede quedar
reducido o vacío durante el resto del día, hasta cambio de versión/fecha o una
regeneración explícita. La retención del día anterior también pierde esos IDs.
No se reajustan base_rank/base_score de los supervivientes; los huecos de rank son
compatibles con order_by y con la eliminación individual que ya hace el servicio.
No debe confundirse expiración de caché con reposición del pool. La invalidación
o regeneración, si se decide, será una tarea separada.

## Comandos manuales (no ejecutados)

PowerShell desde la raíz del proyecto. DATABASE_URL=' ' impide que dotenv restaure
la URL del .env y selecciona la configuración DB_*. Conserva DB_HOST/USER/PASSWORD/
PORT existentes: deben corresponder al servidor local deseado. Cambia estas variables
solo en la sesión actual. Para Render usar su DATABASE_URL y el nombre real esperado.

```powershell
$env:DATABASE_URL = ' '
$env:DB_NAME = 'socialapp_render_clone'
.\venv\Scripts\python.exe manage.py bulk_delete_movies_from_csv --affected-csv movies_to_delete_remaining.csv --expected-database socialapp_render_clone --delete-feed-candidates --dry-run
.\venv\Scripts\python.exe manage.py bulk_delete_movies_from_csv --affected-csv movies_to_delete_remaining.csv --expected-database socialapp_render_clone --delete-feed-candidates --apply
```

El apply esperado para este CSV informa rows_read=unique_ids=existing_before=deleted=
561579, remaining_after=0, errors=0, database=socialapp_render_clone, mode=apply.
Si el estado no cambió: candidate_rows_before=candidate_rows_deleted=282721,
candidate_rows_remaining=0. unique_movies_with_candidates y affected_pools se
obtienen del nuevo preflight; no pueden deducirse del total de filas. related_counts
conserva 282721 candidatos y cero en las otras diez; related_counts_after tiene once
ceros. La salida termina con 'APPLY: commit completado.'.

Después ejecutar exactamente el mismo dry-run:

```powershell
.\venv\Scripts\python.exe manage.py bulk_delete_movies_from_csv --affected-csv movies_to_delete_remaining.csv --expected-database socialapp_render_clone --delete-feed-candidates --dry-run
```

Exigir rows_read=unique_ids=not_found=561579, existing_before=remaining_after=deleted=0,
errors=0, database=socialapp_render_clone, mode=dry-run y once relaciones en cero.
Todos los campos candidate_* y unique_movies_with_candidates/affected_pools son cero.
Ese dry-run relee el mismo CSV y vuelve a consultar los IDs desde una conexión nueva.
No ejecutar apply de nuevo: todos ausentes provoca aborto deliberado.

Comprobación independiente opcional en psql conectado a socialapp_render_clone
(sustituir ruta absoluta del archivo; no usar una conexión de producción):

```sql
SELECT current_database();
BEGIN;
CREATE TEMP TABLE verify_movie_ids (database_id bigint PRIMARY KEY, reason text);
\copy verify_movie_ids FROM 'C:/dev/apss-social-Movies/movies_to_delete_remaining.csv' WITH (FORMAT csv, HEADER true)
SELECT count(*) AS manifest_rows FROM verify_movie_ids;
SELECT count(*) AS remaining_targets
FROM core_movie m JOIN verify_movie_ids s ON m.id = s.database_id;
SELECT count(*) AS remaining_candidates
FROM core_userdailyfeedcandidate c JOIN verify_movie_ids s ON c.movie_id = s.database_id;
ROLLBACK;
```

Resultados exigidos: socialapp_render_clone, manifest_rows=561579,
remaining_targets=remaining_candidates=0. No demuestra por sí solo preservación histórica de todas las
otras filas; el DELETE está acotado al JOIN por PK en el código.

## Tests

27 tests unitarios OK (con subcasos para las once relaciones), sin conexiones reales:
validación CSV/cabeceras/duplicados, modo predeterminado, dry-run sin DELETE y con
rollback, apply sin relaciones, bloqueo de cada relación incluida MovieRating,
base configurada y base real incorrectas, opción expected obligatoria, ausentes,
conteo final incorrecto, rowcount incorrecto, drift de FK, timeout de bloqueo,
reporte de apply y fallo de commit.
Incluye candidatos presentes con/sin flag, película en varios pools, aborto por
cada una de las otras diez relaciones con flag, conteos de candidatos incorrectos,
revalidación de relaciones, dry-run con candidatos y FK/triggers inesperados.
Un simulador de estado transaccional verifica preservación de películas/candidatos
fuera del CSV, pools y otros datos de usuario, y restauración de ambos conjuntos
si falla DELETE Movie o su conteo final. El simulador no es un motor SQL.
Comprueban orden de bloqueo/conteos/DELETE y propagación del error a atomic.
No verifican el motor PostgreSQL real ni concurrencia real por la instrucción de
no ejecutar contra ninguna base.
