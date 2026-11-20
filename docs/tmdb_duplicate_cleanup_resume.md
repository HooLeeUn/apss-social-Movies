# Fase 5E — fallo de comparación y recuperación de la ejecución existente

## Resultado

La ejecución `apply-local-20261118` se verificó como **recoverable**, sin escrituras
en socialapp y sin modificar ninguno de los 89 archivos de las tres carpetas de
entrada. Se conserva `execution_summary.json` con su estado original `failed`.
La reanudación real **no se ejecutó** y requiere revisión/autorización del usuario.

Fuentes inspeccionadas:

```text
C:\dev\docs\TMDB duplicate cleanup\plan-local-20261118\
  cleanup_manifest.json
  cleanup_plan.jsonl
  cleanup_dependencies.json
C:\dev\docs\TMDB duplicate cleanup\review-local-20261118\
  execution_config.json
  execution_summary.json
  review_snapshot.json
  preference_diagnosis.json
  run.lock
C:\dev\docs\TMDB duplicate cleanup\apply-local-20261118\
  execution_config.json
  execution_summary.json
  group-000000.json ... group-000073.json
  preference_diagnosis.json
  run.lock
```

También se preservaron las subcarpetas de validación existentes. Los informes
nuevos se guardaron exclusivamente fuera de esas tres carpetas.

## Causa técnica exacta

En la versión inspeccionada, `delete_group`, en `core/movie_duplicate_cleanup.py`,
líneas 194–196, hacía esta comparación antes de eliminar:

```python
json_text(choose_survivor(rows)) != json_text(expected_plan_fields)
```

`choose_survivor` devuelve `scores` con claves enteras (PK de Movie). El JSON del
plan, leído del disco, tiene las mismas claves como cadenas. `json_text` usa
`sort_keys=True`: ordena los enteros numéricamente antes de convertirlos a JSON,
pero ordena las cadenas lexicográficamente. Comparar esos textos puede dar falso
aunque los valores y todas las huellas sean iguales.

Caso real que detuvo esta ejecución:

| Campo | Valor |
|---|---|
| Índice del plan, base cero | **74** — grupo 75 |
| Identidad | `movie / 78916` |
| Superviviente | Movie **77116** |
| Descarte | Movie **340370** |
| Orden numérico | `77116`, `340370` |
| Orden lexicográfico de las cadenas | `"340370"`, `"77116"` |

PostgreSQL confirmó membresía, metadata_sha256 y fingerprint completo de revisión
idénticos al plan. La comparación JSON semántica coincidió; solo fallaba el texto
con distintos órdenes de claves. Se encontraron 65 grupos pendientes susceptibles
a ese comparador antiguo. El defecto no fue causado por la primera limpieza ni
por un cambio de metadatos: también habría fallado en una ejecución continua al
alcanzar ese grupo.

El comparador nuevo está en `validate_group_metadata`: convierte primero la
salida calculada al mismo dominio JSON que el plan (claves de objeto como cadenas)
y compara representaciones canónicas después de esa normalización. Conserva los
tipos y valores JSON y verifica exactamente todos los campos, incluyendo IDs,
scores, razón y metadata_sha256. No cambia `choose_survivor`, los pesos ni el plan.

## Hechos confirmados en PostgreSQL y comprobantes

La carpeta real contiene **74**, no diez, comprobantes contiguos en estado
`committed`: índices 0–73. Cada uno fue contrastado con el plan y PostgreSQL,
incluyendo sus conteos declarados, superviviente y digest, ausencia de descartes
y dependencias, scope de preferencias y último estado confirmado.

| Medida | Valor |
|---|---:|
| Movie en manifiesto anterior | 434.223 |
| Movie actuales en PostgreSQL | **434.149** |
| Grupos confirmados y verificados | **74** |
| Movie descartados ausentes, exactamente los de esos recibos | **74** |
| Grupos pendientes | **1.221** |
| Filas duplicadas adicionales pendientes | **1.224** |
| Supervivientes del plan ausentes | **0** |
| Descartes ausentes sin comprobante | **0** |

Primeros diez checkpoints, todos verificados y preservados:

| Índice | Superviviente presente | Descarte ausente |
|---:|---:|---:|
| 0 | 915027 | 503614 |
| 1 | 914964 | 485364 |
| 2 | 915055 | 590009 |
| 3 | 612258 | 915015 |
| 4 | 497892 | 914954 |
| 5 | 914884 | 495427 |
| 6 | 577475 | 915016 |
| 7 | 485979 | 933211 |
| 8 | 62003 | 915095 |
| 9 | 560883 | 915071 |

Los diez primeros recibos corresponden a diez Movie eliminados. Siguiendo la
secuencia de dos invocaciones aportada por el usuario, la reanudación fallida
confirmó **64 Movie/grupos adicionales**, índices 10–73, antes de detenerse en el
74. Cada grupo tenía su propia transacción: un fallo posterior no revierte los
grupos ya confirmados. El grupo 74 conserva ambos Movie y no se eliminó.

Los recibos registran, acumulados, 74 Movie y 378 UserDailyFeedCandidate eliminados.
Los conteos declarados coinciden con el alcance aprobado, y PostgreSQL confirma
que no quedan esas dependencias. No se eliminó todavía el MovieRating previsto
en el plan; la huella de preferencias sigue siendo
`5cebf772511f4bdf7b16f08eec3ae0a1f2e55885e044e02ae8d15318942693b4`.

No hay indicios de modificaciones externas incompatibles en el alcance auditado:
coinciden todos los fingerprints de grupos pendientes y los de supervivientes
confirmados, esquema, preferencias y relaciones comprobadas. Estos artefactos no
constituyen una bitácora global de todo el catálogo fuera del alcance del plan;
no se atribuyen operaciones externas que no estén registradas.

## Por qué se perdió el progreso en el resumen

El manejador original capturaba la excepción y publicaba atómicamente otro
`execution_summary.json` con solo configuración, `state=failed`, tipo y motivo.
No fusionaba los contadores anteriores ni guardaba una copia histórica. Por eso
desaparecieron `committed_groups`, `groups` y `remove_movies`, sin que se perdieran
los recibos o se revirtieran las 74 transacciones confirmadas.

El resumen nunca fue la autoridad para reanudar. `--resume` usa la configuración,
checkpoints y estado real de PostgreSQL; ya admitía un resumen `failed`, pero
volvía a encontrar el falso error de comparación en el primer grupo pendiente.

Los informes nuevos conservan cada intento en `attempt-<uuid>.json`, con el
resumen anterior. Los errores futuros incluyen progreso verificado, grupos
pendientes, último grupo confirmado, grupo activo/fallido, Movie eliminados,
conteos por modelo, detalles de integridad y orientación para recuperación.
No se etiqueta como confirmado un checkpoint que todavía no pasó las consultas
de validación. Si una verificación no pudo completarse, los contadores desconocidos
se marcan `null`, en lugar de inventar cero.

El progreso se actualiza inmediatamente después de un commit durable, antes de
publicar el estado `committed`. Así, un fallo de publicación conserva el conteo
correcto y el `pending_commit` puede comprobarse contra PostgreSQL al reanudar.
El resumen `limited` perdido de esta ejecución no se reconstruyó manualmente:
los hechos se reconstruyeron desde recibos, el plan y PostgreSQL.

## Compatibilidad y validaciones conservadas

La revisión original incluye `movie_duplicate_cleanup.py` en su digest de reglas.
Cambiar el comparador hace variar ese digest, aunque no cambien las preferencias.
Para evitar reconstruir o editar la revisión se reconoce únicamente el digest
histórico exacto
`61815825840a7ffc31b3532a07d00a39ccb5b6bbad0aaae15ac870acd2ae8d69`.

Además, se exigen los SHA-256 exactos de los archivos originales de preferencias,
modelos, servicios y señales, y se vuelve a validar íntegramente el baseline,
política, alcance y diagnóstico. La adaptación es solo en una copia de memoria
usada para comprobar compatibilidad. No modifica review_snapshot, su hash ni el
scope original que registran los checkpoints. Un digest desconocido o un cambio
en la lógica de preferencias sigue bloqueando.

Se mantienen las guardas de destino, esquema, todas las columnas de Movie,
dependencias, colector, preferencias, transacciones, advisory lock, conservación
de supervivientes, controles por grupo y recuento final de cero duplicados.
Los checkpoints también exigen secuencia contigua, nombre canónico, IDs, conteos
por modelo y digests. Su mera presencia nunca prueba un commit.

## Resultado de pruebas

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe -B scripts\test_local_movie_duplicates.py
.\venv\Scripts\python.exe -B scripts\test_tmdb_importer.py
.\venv\Scripts\python.exe -B scripts\test_postgres_movie_duplicate_cleanup.py --postgres-bin 'C:\Program Files\PostgreSQL\18\bin' --no-migrations
```

* Limpiador: **50/50** pruebas, incluidas las 44 anteriores.
* Suite del importador: **169/169**, con SQLite sintético y red bloqueada; no es
  una ejecución del importador real.
* PostgreSQL desechable: aprobado, sin migraciones ni conexiones a socialapp/Render.

El ensayo conserva los escenarios anteriores y agrega: primera ejecución de diez,
reanudación desde limited y failed de formato antiguo, reproducción del comparador
defectuoso con IDs de distinto tamaño, checkpoints corruptos/falsificados, cambios
externos en supervivientes y pendientes, fallos previos al delete, interrupción
post-commit, contadores, conservación de informes, verificación no destructiva,
compatibilidad de revisión anterior y limpieza completa hasta cero duplicados.

## Validación real no destructiva

El comando nuevo `verify_local_movie_duplicate_resume` no tiene modo apply. Fuerza
READ ONLY y una guarda SQL, verifica las fuentes y el estado real, y escribe solo
en un directorio nuevo de informes fuera de plan/review/ejecución/repositorios.
El recálculo de contraste queda en un temporal que se descarta: no crea un plan
sustitutivo ni modifica el plan original.

Se ejecutó con las tres carpetas proporcionadas. Resultado: **recoverable**,
74 grupos comprobados, 1.221 pendientes, checkpoint siguiente índice 74. Guardó:

```text
C:\Users\USUARIO\AppData\Local\Temp\reccool-resume-final-verification-e35c745109a44563996c6bd356a59e83\
  recovery_summary.json
  readonly_facts.json
```

Registró 1.650 SELECT, dos sentencias de control y cero mutaciones. La sesión
confirmó socialapp / 127.0.0.1:5432 / `transaction_read_only=on`. Una comprobación
posterior confirmó los mismos conteos y la huella de preferencias. Los 89 archivos
originales mantienen exactamente sus hashes, incluso `execution_summary.json`
con `state=failed`. No se añadieron archivos en esas carpetas.

Huellas originales conservadas:

```text
plan:     cab809623bb71b3e9c248dc8a33914d64f7e2ac45f17eca94c311b47e273925a
review:   2efc8c79d36c030255f48650512a4783a96cb6aad60341769ec59d693455901a
manifest: dc70ec693dd71b635721513a3c87f3401a34eb7b269241081fee3e7f103584d7
```

Para repetir únicamente esa comprobación, siempre con una carpeta nueva:

```powershell
$cleanupRoot = 'C:\dev\docs\TMDB duplicate cleanup'
$cleanupCheck = Join-Path ([System.IO.Path]::GetTempPath()) ('reccool-resume-check-' + [guid]::NewGuid().ToString('N'))
.\venv\Scripts\python.exe -B manage.py verify_local_movie_duplicate_resume `
  --plan-dir (Join-Path $cleanupRoot 'plan-local-20261118') `
  --review-dir (Join-Path $cleanupRoot 'review-local-20261118') `
  --execution-dir (Join-Path $cleanupRoot 'apply-local-20261118') `
  --confirm-database socialapp `
  --confirm-plan-sha256 cab809623bb71b3e9c248dc8a33914d64f7e2ac45f17eca94c311b47e273925a `
  --report-dir $cleanupCheck
```

## Reanudación propuesta — NO ejecutada

`--limit-groups` es **adicional por invocación**, no acumulado. Tras 74 grupos,
`--limit-groups 1221` procesa como máximo los 1.221 pendientes. Un límite 1.285
también quedaría acotado al final del plan y no repetiría grupos anteriores.

Solo después de la revisión y autorización expresa, manteniendo la ventana de
mantenimiento, respaldo restaurable y ausencia de escritores concurrentes:

```powershell
Set-Location 'C:\dev\apss-social-Movies'
$cleanupRoot = 'C:\dev\docs\TMDB duplicate cleanup'
& '.\venv\Scripts\python.exe' -B manage.py cleanup_local_movie_duplicates `
  --plan-dir (Join-Path $cleanupRoot 'plan-local-20261118') `
  --review-dir (Join-Path $cleanupRoot 'review-local-20261118') `
  --output-dir (Join-Path $cleanupRoot 'apply-local-20261118') `
  --confirm-database socialapp `
  --confirm-maintenance `
  --confirm-plan-sha256 cab809623bb71b3e9c248dc8a33914d64f7e2ac45f17eca94c311b47e273925a `
  --limit-groups 1221 `
  --resume `
  --apply
```

Esto reutiliza el mismo plan, revisión y directorio de ejecución. No renombrar ni
editar checkpoints y no cambiar failed por limited manualmente. La migración 0079
y el importador siguen fuera de esta operación y no se ejecutaron. Cambios
posteriores en datos/esquema/código pueden bloquear otra vez la validación; no
hay una opción para ignorarlos.

En esta tarea: cero escrituras en socialapp, cero migraciones, cero consultas
TMDb/Render, ningún cambio a metadatos o algoritmo de supervivientes, ninguna
rama, commit, push o PR. Las eliminaciones de prueba fueron solo en el clúster
desechable, detenido al terminar.
