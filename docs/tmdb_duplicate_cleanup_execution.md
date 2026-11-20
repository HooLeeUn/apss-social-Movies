# Fase 5E — ejecución revisada de la depuración local

## Reanudación de la ejecución parcial

El caso `apply-local-20261118` y la corrección del comparador JSON, progreso y
compatibilidad están documentados en `docs/tmdb_duplicate_cleanup_resume.md`.
El diagnóstico real encontró 74 grupos confirmados (no solo diez); la ejecución
existente fue validada como recuperable sin alterar sus archivos ni socialapp.
La reanudación real queda pendiente de autorización. Se agregó un comando de
verificación exclusivamente de lectura, separado del ejecutor apply.

## Diagnóstico anterior y resolución del bloqueo

El preflight de solo lectura reprodujo los 1.295 grupos y las 1.298 filas a
descartar del plan de fase 5D. El alcance FK incluye una calificación de prueba
y 4.953 candidatos de feed. Se detectaron cuatro discrepancias previas entre
las distribuciones de preferencias de director de la cuenta afectada y sus
calificaciones actuales. Las cuatro corresponden a directores ajenos a los
campos afectados por la calificación que se propone eliminar. El diagnóstico
anterior bloqueó la validación global. La corrección acotada se documenta en
`docs/tmdb_duplicate_preferences_blocker.md`: el nuevo dry-run fija una línea
base de esas discrepancias y exige conservarlas exactamente, mientras valida
el delta completo de la eliminación. No las reconcilia. Cualquier corrección
histórica sigue requiriendo autorización separada. La auditoría usa READ ONLY.

## Validar el plan sin escrituras

```powershell
Set-Location 'C:\dev\apss-social-Movies'
$plan = 'C:\Users\USUARIO\AppData\Local\Temp\reccool-duplicate-cleanup-e1d1bcc556344035ba34334119e13493'
$review = Join-Path ([System.IO.Path]::GetTempPath()) ('reccool-cleanup-review-' + [guid]::NewGuid().ToString('N'))
.\venv\Scripts\python.exe -B manage.py cleanup_local_movie_duplicates --dry-run --confirm-database socialapp --plan-dir $plan --output-dir $review
```

El modo predeterminado es dry-run. Se comprueba el destino, el manifiesto y
los SHA-256 del plan y su inventario. Se vuelve a ejecutar el algoritmo de
5D en un snapshot PostgreSQL `REPEATABLE READ READ ONLY` y se compara el plan
completo. Tras validar preferencias, el dry-run publica `review_snapshot.json`
con huellas de todas las columnas de cada Movie y de las filas concretas de
calificaciones y candidatos de feed asociadas a los descartes. Se usa esa
instantánea para detectar cambios en campos que la puntuación de 5D no utilizaba,
como el autor, y cambios de dependencias que mantienen el mismo conteo.
Los informes previos se conservan.

El snapshot incorpora `__preference_review__`: política y huella del código,
preferencias/perfil de gustos y entradas técnicas de las calificaciones de los
usuarios afectados. `preference_diagnosis.json` expone los valores actuales y
esperados y el aporte de las calificaciones a descartar, sin datos de cuenta.
Se toleran exclusivamente discrepancias de distribución de director anteriores
y ajenas a todos los aportes previstos. Los totales/puntuaciones internas y el
conteo del perfil deben seguir siendo correctos; discrepancias en géneros/tipos
o en cualquier clave afectada bloquean. Un review anterior sin esta política
debe regenerarse; no se omiten comprobaciones por compatibilidad.

## Alcance y garantías de apply

La implementación de esta fase admite Movie y las dependencias CASCADE de
`MovieRating` y `UserDailyFeedCandidate`. Si aparecen comentarios, videos,
notificaciones, streaming, recomendaciones, una relación protegida o un cambio
en el grafo esperado, se exige otro tratamiento revisado. El esquema real de
FKs, triggers y reglas se vuelve a verificar en cada transacción.

- Se exige PostgreSQL primario en loopback literal, alias `default` sin routers,
  confirmación del nombre de base, mantenimiento, huella explícita del plan,
  un dry-run validado y un límite positivo de grupos por ejecución.
- Un advisory lock de sesión excluye otros procesos de esta herramienta. Cada
  identidad utiliza una transacción con bloqueos de escritura sobre las tablas
  involucradas y `lock_timeout` de cinco segundos. Bajo esos bloqueos se valida
  el esquema, se recalculan ganador y dependencias y se verifica la huella
  completa de la revisión.
- El colector de Django programa la eliminación. Antes de ejecutarla se
  comprueban los modelos y conteos exactos, y se rechazan actualizaciones por FK.
  Se mantiene `origin` de Movie y se ejecutan las señales normales. El
  `post_delete` de la calificación resta sus aportes de preferencias y actualiza
  el perfil de gustos. Se comprueban sus histogramas, puntuaciones y conteos
  contra el delta exacto revisado. Las claves afectadas parten de distribuciones
  consistentes y deben corresponder a las calificaciones restantes; las claves
  históricas ajenas conservan todas sus filas, IDs, histogramas y métricas,
  incluidas las ausencias de filas. Se conserva el mismo residuo histórico,
  sin añadir errores nuevos. Los usuarios y perfiles de cuenta se
  conservan. La única modificación derivada permitida es la de gustos
  correspondiente a la calificación descartada.
- Los candidatos de feed se eliminan por su FK a los Movie descartados. Se
  conservan pools y candidatos de otros Movie, con sus posiciones existentes.
  El servicio lee las filas presentes y no guarda un contador en el pool.
  Los Movie supervivientes se releen y se comparan completos, incluido el
  vector de búsqueda. Las restricciones diferidas se comprueban al confirmar.

El comando no escribe en la caché de la aplicación. Las respuestas anónimas
de detalle, resultados y conteos de feed y listas rotadas pueden vivir en los
procesos web tras un cambio de catálogo. Mantener lectores y escritores de
la aplicación detenidos durante el mantenimiento y expirar o invalidar la caché
en el entorno antes de reabrir. En local con LocMem, reiniciar todos los procesos
que la tienen en memoria. Para una caché compartida, coordinar invalidación del
namespace correspondiente o esperar la expiración sin repoblar entradas. La
rotación de feed tiene una vigencia máxima de tres horas en el código actual.

Se publica un recibo por grupo con IDs, huella del superviviente, conteos por
modelo y huellas del alcance y estado de preferencias posterior. Se graba
`pending_commit` antes del commit y `committed` después. Si
el proceso termina en ese intervalo, `--resume` verifica que quede exactamente
el superviviente esperado, que los IDs descartados y dependencias ya no existan
y que el plan pendiente coincida con el catálogo. Si la transacción se revirtió,
se vuelve a comprobar y procesar ese grupo. Un estado parcial o alterado
bloquea la ejecución. Conservar los recibos en almacenamiento local y no
alterarlos entre invocaciones. La reanudación proyecta desde el baseline todos
los grupos confirmados y compara también la huella del último estado confirmado;
un cambio ajeno en preferencias o calificaciones bloquea la continuación.
El resumen final cuenta todos los grupos
duplicados no nulos y exige cero.

## Comandos futuros

Primero revisar la política diferencial y sus cuatro discrepancias preservadas,
generar un dry-run válido, validar un respaldo restaurable y
obtener la autorización de ejecución. Los siguientes comandos son propuestos.

```powershell
$run = Join-Path ([System.IO.Path]::GetTempPath()) ('reccool-cleanup-apply-' + [guid]::NewGuid().ToString('N'))
$approved = 'cab809623bb71b3e9c248dc8a33914d64f7e2ac45f17eca94c311b47e273925a'
.\venv\Scripts\python.exe -B manage.py cleanup_local_movie_duplicates --apply --confirm-database socialapp --confirm-maintenance --confirm-plan-sha256 $approved --plan-dir $plan --review-dir $review --output-dir $run --limit-groups 10
```

Continuar desde el mismo `$run`, repetir el comando y añadir `--resume`.
Usar la huella revisada si se aprueba un plan nuevo.

## Transición a la restricción

1. Congelar escrituras al catálogo y sus dependencias antes de apply. Detener
   procesos web, importadores y tareas programadas y controlar conexiones
   manuales. Mantener la congelación hasta verificar la restricción. Si hay
   otros roles escritores, coordinar que queden sin permisos efectivos de
   escritura o sin acceso durante la ventana, incluidos permisos heredados.
   El advisory lock de limpieza sólo coordina instancias del comando.
2. Completar el plan y comprobar `remaining_duplicate_groups = 0`. Verificar
   que el resumen corresponde a la conexión local autorizada.
3. En una operación autorizada posterior, aplicar `0079_movie_type_tmdb_id_uniq`
   sobre el mismo destino, tras confirmar en la sesión que `default` resuelve
   al PostgreSQL local previsto.

   ```powershell
   .\venv\Scripts\python.exe -B manage.py migrate core 0079 --database default
   ```

   La migración es atómica. Toma `ACCESS EXCLUSIVE` con una espera máxima de
   cinco segundos, vuelve a comprobar que no existen pares duplicados y crea la
   restricción bajo el mismo bloqueo. Durante esa operación se bloquean
   lecturas y escrituras de Movie. El tiempo de construcción del índice se
   debe estimar con volumen representativo. Una escritura que reaparezca
   antes del bloqueo puede hacer fallar la migración, que se revierte.
4. Comprobar que `movie_type_tmdb_id_uniq` está validada, que su índice asociado
   está válido e inmediato y que siguen sin existir duplicados. La restricción
   conserva el comportamiento de NULL de PostgreSQL. [Bloqueos de ALTER TABLE](https://www.postgresql.org/docs/18/sql-altertable.html)
5. Reabrir la aplicación tras la gestión de caché. Verificar el destino, el
   trigger, el autor y el dry-run de importación antes de una autorización
   distinta para el importador apply. Al cambiar `models.py` cambia su huella
   de reglas. Regenerar `import_ready` desde el enriquecimiento intacto hacia
   un directorio nuevo y verificarlo de nuevo. Los preparados previos se
   conservan como evidencia y el verificador sigue siendo estricto.

## Ensayo aislado

```powershell
.\venv\Scripts\python.exe -B scripts\test_postgres_movie_duplicate_cleanup.py --postgres-bin 'C:/Program Files/PostgreSQL/18/bin' --no-migrations
```

El ejecutor crea un clúster temporal con datos sintéticos, puerto loopback
independiente y credenciales efímeras. Confirma su directorio de datos y
endpoint antes de crear la base de prueba. En este modo crea las tablas de fixture
directamente con `schema_editor`, sin aplicar la unicidad, e instala el trigger
de búsqueda para comprobar preservación de supervivientes. No ejecuta migraciones.
Conserva la ruta anterior de pruebas de migración, pero no se ejecuta en esta tarea.
El ensayo reproduce el desfase al cambiar metadatos de director y prueba rechazo
de cambios indebidos, rollback, reanudación, preservación de claves ajenas y cero
duplicados. Detiene el clúster al terminar. Solo admite el endpoint recién creado;
no permite conexiones a socialapp ni Render.
