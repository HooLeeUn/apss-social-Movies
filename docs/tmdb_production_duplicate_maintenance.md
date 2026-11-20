# Fase 5H: mantenimiento de duplicados en producción

Preparación local en staging. No se conectó a Render ni socialapp, no se hicieron
copias/restauraciones reales ni se ejecutaron migraciones o apply reales.

## Plan existente: verificación offline

Origen, preservado sin modificar:

`C:\dev\docs\TMDB production audit\plan-20261119\remote-20261119T055440Z-452a05b3eb0d`

- Contrato: `reccool.remote-duplicate-audit.v1`; modo plan; estado complete.
- Plan SHA-256: `8458a93fbf6347747f6968f52aaacf324a7a068c0c7c75f809840209e9d308e4`.
- Manifiesto SHA-256: `fa020fa19c6a162f84d19dba89d0c9f964ebbe7a5f2817553ff2abc07d8e5cf4`.
- Dependencias SHA-256: `6acf0cb454c30ebcf2f96e131e5e414cfdb7bf9138ae6110e579bf5c57c80cec`.
- 434.223 producciones registradas en ese snapshot; 1.295 grupos; 1.298 descartes;
  218 grupos con dependencias. Cero nulos, inválidos, grupos no planificables o bloqueos.
- Impacto registrado: 2 MovieRating, 19.518 UserDailyFeedCandidate y 1 WeeklyRecommendationItem.

El cargador valida hashes, contrato, modo, destino, huellas de todas las reglas de
selección/auditoría/modelo, tipos JSON estrictos, claves duplicadas, identificadores,
miembros repetidos, candidatos, conteos y grafo. No convierte el contrato ni crea
un plan local equivalente. No acepta un plan local. apply_enabled=false permanece
intacto: este dato no autoriza escribir; el comando de mantenimiento solicita otra
aprobación explícita, ligada al plan y al nuevo dry-run.

La compatibilidad de archivos pasó offline. **La vigencia en PostgreSQL no fue
comprobada en esta tarea.** El backend puede haber modificado feed/dependencias.
Si el snapshot vivo no reproduce exactamente los grupos/metadatos/dependencias,
el dry-run falla. No se permite ignorar esas diferencias o sobrescribir el plan.

## Validación complementaria necesaria

El plan contiene huellas de los campos usados para seleccionar supervivientes y
conteos de dependencias. No contiene las huellas de todas las columnas de Movie,
cada fila dependiente, preferencias ni padres de recomendaciones semanales.
No permite demostrar retrospectivamente cambios en campos no capturados entonces.

El nuevo dry-run obtiene esas huellas completas y una revisión de preferencias
específica de los usuarios de las calificaciones de Render. Su review_snapshot.json
se verifica mediante SHA-256 y se liga al destino, plan, manifiesto, usuario confirmado
(hash, sin almacenarlo en claro) y versión exacta del mantenimiento. No se reutiliza
ninguna revisión local de socialapp. Cambios posteriores bloquean apply/reanudación.

Se debe revisar esa evidencia y cualquier diferencia histórica antes de autorizar
el primer lote. No se reparan preferencias históricas arbitrariamente.

## Un único motor, alcance de las dependencias

Se reutiliza execute_cleanup con sus transacciones, validaciones, checkpoints y
recuperación. El motor local conserva su política de conexión loopback/default y
su conjunto original de dependencias admitidas. Un scope temporal de mantenimiento
usa un alias separado y añade únicamente WeeklyRecommendationItem.

La señal de eliminación de MovieRating utiliza el servicio existente de preferencias.
Se corrigió su bloque atomic para usar la base del usuario/rating, en lugar de abrir
default. El router temporal dirige los managers de esa operación al alias remoto;
un guard rechaza otras conexiones Django y reconexiones. default y DATABASE_URL
no se sustituyen. Estos scopes son para un proceso CLI dedicado, no un proceso web.

Los algoritmos de preferencias y selección no cambian. La compatibilidad histórica
solo admite las huellas anteriores exactas y la variante exacta de services.py que
corrige el alias; componentes desconocidos siguen bloqueados. No se reescriben
revisiones antiguas. Las revisiones remotas nuevas usan las huellas actuales.

- MovieRating: se eliminan únicamente las calificaciones de descartes; las señales
  aplican sus deltas exactos a preferencias y UserTasteProfile. La validación conserva
  discrepancias históricas de director no relacionadas y bloquea drift relacionado
  u otras inconsistencias. No se borra User ni Profile ni se reconstruye el perfil entero.
- UserDailyFeedCandidate: cascada limitada a los descartes, sin fusionar ni tocar
  candidatos ajenos. Se conserva el mecanismo existente de retirada del pool activo.
- WeeklyRecommendationItem: se borra el ítem del descarte y se decrementa el
  items_count de su snapshot en la misma transacción. No se renumeran posiciones,
  no se borra el snapshot y no se recalculan scores o recomendaciones.
  Se revisan el padre completo y todos sus ítems, proyectando solo los descartes
  ya confirmados. Un contador previamente inconsistente, cambios externos o triggers
  de padre no revisados bloquean. No se corrigen contadores históricos silenciosamente.
- Las demás relaciones, FK nuevas, SET_NULL, PROTECT, CASCADE no admitidos, triggers
  o reglas incompatibles bloquean. Collector debe coincidir exactamente con el alcance.

## Conexión y apply

El comando separado cleanup_render_movie_duplicates usa RECCOOL_RENDER_DATABASE_URL
y la configuración validada por la auditoría: host externo único confirmado, puerto,
base, usuario y TLS verify-full con CA de confianza. No imprime URL/contraseña.
La auditoría audit_render_movie_duplicates conserva exclusivamente lectura.

Dry-run abre sesión con default_transaction_read_only=on; el preflight completo
se realiza en REPEATABLE READ READ ONLY. Un guard adicional solo permite SELECT y
las sentencias fijas necesarias para iniciar el snapshot/timeout.

Solo un apply con todas las confirmaciones puede abrir la sesión de mantenimiento
en escritura. Confirma identidad real de Psycopg, usuario, base, puerto, TLS y modo
de cada transacción. Rechaza RLS, triggers/reglas de padres semanales no revisados.
El código no invoca migraciones. El ID de backup se conserva únicamente como hash;
el operador debe haber verificado que ese backup existe y es restaurable.

Se mantiene un advisory lock durante la sesión. Cada grupo usa transacción durable,
synchronous_commit=on, timeout de locks de 5 s y SHARE ROW EXCLUSIVE en Movie,
dependencias, preferencias y padre semanal. Se valida antes de borrar y después de
las cascadas. Los supervivientes deben permanecer idénticos en todas sus columnas.
La espera por locks puede hacer fallar un lote sin borrar parcialmente el grupo.

## Ventana de mantenimiento y backup: condiciones previas

1. Revisión y autorización expresa del procedimiento, plan y código.
2. Identificar TODOS los escritores: producción, preview, workers, cron, importadores,
   tareas de feed/recomendaciones y accesos manuales. Detener sus escrituras durante
   el dry-run final, limpieza y aplicación posterior de unicidad. La confirmación de
   mantenimiento es una declaración del operador; la herramienta no detiene servicios.
3. Preview comparte producción y su Start Command ejecuta migraciones: no arrancarlo,
   desplegarlo ni reiniciarlo antes de la fase de migración expresamente autorizada.
   Auto-Deploy Off de preview no detiene otros escritores ni migraciones de otros servicios.
   Esta tarea no cambia ningún servicio ni configuración de Render.
4. Obtener una copia anterior restaurable, verificar su restauración en un destino
   aislado y registrar el ID, alcance, versión, propietario y procedimiento de retorno.
   No se ejecutó copia ni restauración en esta fase. Una aprobación textual no prueba
   automáticamente que un backup sea válido.
5. Ejecutar el dry-run final y revisar huellas, dos calificaciones/preferencias,
   dependencias y contador semanal. Cualquier bloqueo requiere decisión separada;
   no ignorarlo ni reconciliar preferencias ajenas a las eliminaciones.
6. Autorizar un primer lote pequeño y verificar los comprobantes antes de ampliar.

Los table locks solo protegen cada transacción; no garantizan una ventana sin
escritores entre grupos o antes de 0079. El advisory lock solo coordina herramientas
que lo respeten. Sin pausa real de escritores, no autorizar apply. READ ONLY protege
el dry-run, no convierte las credenciales existentes en un rol sin escritura.

## Comandos futuros: NO EJECUTADOS

Desde C:\dev\apss-social-Movies. La URL debe estar disponible de forma segura en el
entorno del proceso; CA real y backup verificable siguen siendo requisitos externos.

```powershell
$python = 'C:\dev\apss-social-Movies\venv\Scripts\python.exe'
$plan = 'C:\dev\docs\TMDB production audit\plan-20261119\remote-20261119T055440Z-452a05b3eb0d'
$renderHost = 'dpg-dac5ji2jobas73ccg630-a.oregon-postgres.render.com'
$renderCa = '<ruta absoluta del CA de confianza>'
$reviewDir = 'C:\dev\docs\TMDB production maintenance\review-' + [guid]::NewGuid().ToString('N')
$applyDir = 'C:\dev\docs\TMDB production maintenance\apply-' + [guid]::NewGuid().ToString('N')
$common = @(
  '--plan-dir', $plan,
  '--host', $renderHost, '--port', '5432',
  '--name', 'reccool_postgres_production',
  '--confirm-host', $renderHost,
  '--confirm-database', 'reccool_postgres_production',
  '--confirm-user', 'reccool_postgres_production_user',
  '--tls-ca-file', $renderCa
)
```

Dry-run de producción, sujeto a autorización para conectar:

```powershell
& $python -B manage.py cleanup_render_movie_duplicates @common `
  --output-dir $reviewDir --dry-run
```

Después de revisar su estado validated y autorizar el primer lote de 10 grupos:

```powershell
$review = Get-Content -LiteralPath (Join-Path $reviewDir 'execution_summary.json') -Raw | ConvertFrom-Json
$reviewSha = (Get-FileHash -LiteralPath (Join-Path $reviewDir 'review_snapshot.json') -Algorithm SHA256).Hash.ToLowerInvariant()
if ($review.state -ne 'validated' -or $review.review_sha256 -ne $reviewSha) { throw 'Revisión no válida' }
$backupId = '<ID de copia restaurable verificada por el operador>'
$approved = @(
  '--review-dir', $reviewDir,
  '--confirm-plan-sha256', '8458a93fbf6347747f6968f52aaacf324a7a068c0c7c75f809840209e9d308e4',
  '--confirm-review-sha256', $reviewSha,
  '--confirm-backup-id', $backupId,
  '--confirm-maintenance', '--limit-groups', '10', '--apply'
)
& $python -B manage.py cleanup_render_movie_duplicates @common @approved --output-dir $applyDir
```

El programa también verifica SHA-256 del manifiesto, reglas de mantenimiento y
revisión. No basta con editar manualmente state o copiar un JSON de otra ejecución.

## Recuperación y reconciliación

- Conservar plan, revisión, execution_config, execution_summary, todos los
  attempt-*.json y group-*.json. No corregirlos manualmente ni cambiar failed a limited.
- El comprobante se escribe como pending_commit dentro de la transacción y se marca
  committed después. Una interrupción en ese intervalo exige comprobar PostgreSQL,
  no inferir el commit por la existencia del archivo o el resumen.
- Con autorización para continuar la misma ejecución, repetir el comando exacto con
  el MISMO $applyDir, $reviewDir, backup ID, plan y confirmaciones, añadiendo --resume:

```powershell
& $python -B manage.py cleanup_render_movie_duplicates @common @approved `
  --output-dir $applyDir --resume
```

- Antes de borrar el siguiente grupo, revalida todos los comprobantes contra los
  supervivientes completos, ausencia de descartes/dependencias, preferencias y
  padres semanales; reconstruye estrictamente todos los grupos pendientes. Funciona
  desde limited/failed y después de un commit sin publicación final. Un comprobante
  incompatible bloquea. Un resumen sin progreso verificado no demuestra eliminaciones.
- --limit-groups cuenta grupos ADICIONALES en esa invocación. Repetir sobre complete
  con --resume no repite borrados. Revisar deleted_by_model, removed_movies,
  committed_groups, pending_groups y remaining_duplicate_groups.
- No utilizar verify_local_movie_duplicate_resume con Render. --resume reconcilia y
  después puede continuar escribiendo: requiere autorización; no es un diagnóstico
  de lectura. Para una inspección sin permiso de escribir, conservar todo y ejecutar
  únicamente una nueva auditoría remota READ ONLY; no autorizar avance automático.
- Ante error de dependencias, parar manteniendo la ventana y analizar; no fusionar,
  desactivar constraints ni borrar archivos. Restaurar supone recuperar toda la copia
  coherente en un destino controlado, con aprobación separada; no reinsertar Movies
  sin sus interacciones/preferencias ni ejecutar una restauración automática.

## Finalización y migración 0079

Con todos los grupos confirmados, el motor comprueba cero duplicados no nulos. Antes
de migrar, realizar además una auditoría independiente READ ONLY y revisar los
conteos: se esperan 432.925 Movies si no hubo otros cambios desde el plan.

```powershell
& $python -B manage.py audit_render_movie_duplicates `
  --host $renderHost --port 5432 --name reccool_postgres_production `
  --confirm-host $renderHost --confirm-database reccool_postgres_production `
  --confirm-user reccool_postgres_production_user --tls-ca-file $renderCa `
  --output-dir 'C:\dev\docs\TMDB production maintenance\post-cleanup-audit'
```

Mantener TODOS los escritores pausados hasta establecer y comprobar la unicidad.
La 0079 existente adquiere ACCESS EXCLUSIVE con timeout, verifica duplicados y añade
movie_type_tmdb_id_uniq(type, tmdb_id). No se encontró un defecto que requiera cambiarla.
En pruebas se creó la misma constraint directamente; no se ejecutó la migración.

La migración real requiere una fase/aprobación separada y un contexto inequívoco de
producción. No ejecutar migrate con el default local, ni inventar --database con un
alias efímero que solo existe dentro de estos comandos. Revisar el plan de migración,
backups y writers; aplicar solo la 0079 prevista en ese contexto autorizado, y verificar
después su registro y la constraint física única, válida, inmediata y de ambas columnas
mediante introspección PostgreSQL. Una entrada en django_migrations no basta por sí sola.
Reactivar escritores únicamente tras esas comprobaciones. No arrancar preview con
migraciones pendientes o antes de autorizar sus efectos.

## Pruebas

```powershell
& .\venv\Scripts\python.exe -B scripts/test_production_movie_duplicates.py
& .\venv\Scripts\python.exe -B scripts/test_postgres_production_movie_cleanup.py
& .\venv\Scripts\python.exe -B scripts/test_postgres_movie_duplicate_cleanup.py `
  --postgres-bin 'C:\Program Files\PostgreSQL\18\bin' --no-migrations
& .\venv\Scripts\python.exe -B scripts/test_tmdb_importer.py
```

Los runners offline bloquean red y PostgreSQL. Los de integración crean clústeres
aislados con puerto/credenciales nuevos, comprueban data_directory antes de crear la
base, bloquean otros destinos y no cargan .env. Solo allí crean/modifican datos y
permisos de fixture. La observación TLS se sustituye únicamente para el loopback
desechable sin TLS; el handshake real de Render sigue sin comprobarse en esta fase.

Cobertura: JSON/contratos/hashes/destino/reglas, tres miembros por grupo, superviviente,
dos calificaciones y señales, feed, semanal/contador, preferencias históricas, FK nueva,
rollback después de las cascadas, ejecución limitada, estados failed/limited, commit gap,
checkpoint falso, idempotencia, concurrencia, cambios externos, cero duplicados,
rechazo de apply incompleto, usuarios/supervivientes intactos y constraint exacta 0079.

Resultados: 73/73 pruebas offline del mantenimiento/auditoría/limpiador y 169/169
del importador aprobadas. La integración PostgreSQL del nuevo comando y la suite
PostgreSQL previa del limpiador local pasaron en clústeres desechables, con cero
comandos de migración. Sintaxis Python, ayuda del comando y git diff --check aprobados.

Pendientes reales: dry-run complementario autorizado, vigencia del plan/dependencias,
preferencias de Render, contador semanal, CA/TLS real, backup restaurable, pausa de
todos los escritores y revisión/autorización del primer lote. No se autoriza ninguna
escritura por el hecho de que las pruebas hayan pasado.
