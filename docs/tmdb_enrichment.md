# TMDb — Fase 4: enriquecimiento offline-first

Actualización de contrato: ver [clasificación, elegibilidad y títulos](tmdb_eligibility.md).
Los nuevos envoltorios versión 2 añaden una evaluación de mínimos independiente
del estado histórico del normalizador. Los registros históricos y sus archivos
no se recalifican; ningún resultado queda autorizado para importar/publicar.

## Estado y límites de esta entrega

Implementación preparada para pruebas reales; la fase **no está terminada** hasta
aprobar y ejecutar un piloto live y revisar su calidad. No se ejecuta ningún
piloto live automáticamente. No hay importador, ORM, migraciones ni conexiones a
PostgreSQL. SQLite se usa exclusivamente como checkpoint **en disco local**.

El CSV procede de una comparación histórica con PostgreSQL local. No demuestra
que una identidad siga ausente en otra fecha o en Render. Una futura importación
debe verificar nuevamente `(type, tmdb_id)` en la base de destino. Este CLI nunca
consulta esa base ni intenta eliminar duplicados existentes.

## Inspección y decisiones

Se inspeccionaron `core/tmdb_normalizer.py`, `docs/tmdb_normalizer.md`, el comando
`compare_tmdb_exports`, su contrato `Reports.FIELDS["new_candidates"]`, y la cabecera/muestras de
`C:\dev\docs\TMDB comparison\completa-2026-11-06\new_candidates.csv` (58.232.165
bytes). No se volvió a ejecutar el comparador ni se sobrescribieron sus informes.

También se revisaron `tmdb.py`, `tmdb_catalog.py`, `tmdb_credits.py`, el comando
`enrich_movies_tmdb`, configuración, dependencias y convenciones de `Movie`.

* El comando anterior trabaja con el ORM y puede guardar cambios: no se reutiliza.
* `tmdb_credits.py` importa el modelo/cache y puede consultar temporadas; se evita.
* `CatalogEnricher` resuelve identidades por búsqueda y no entrega el contrato
  completo del normalizador; no se usa para candidatos con identidad ya conocida.
* `get_tmdb_json` depende de settings, no restringe el host configurado, admite
  redirecciones y puede incorporar cuerpo/excepción en errores. Por eso esta fase
  usa un transporte pequeño separado, sin cambiar el cliente compartido.
* Sí se reutilizan `requests` y `python-dotenv` ya instalados, las variables
  existentes y **directamente** `normalize_tmdb_metadata`, sin otra implementación
  de las reglas de géneros, títulos, sinopsis, créditos o ratings.

Se eligieron créditos estándar anexados al detalle inglés, tanto para películas
como series. Su `crew[].job` coincide con el contrato actual; los créditos
agregados de series tienen otra estructura y no se transforman en un segundo
normalizador. Se conserva el respaldo `created_by` que ya implementa el existente.
No se consultan temporadas, episodios, personas, pósteres ni videos.

Referencias oficiales consultadas, sin llamadas autenticadas:
[append_to_response](https://developer.themoviedb.org/docs/append-to-response),
[autenticación de aplicación](https://developer.themoviedb.org/docs/authentication-application),
[rate limiting](https://developer.themoviedb.org/docs/rate-limiting) y
[errores](https://developer.themoviedb.org/docs/errors). No se utiliza un supuesto
máximo permanente del servicio; el valor inicial es conservador y configurable.

## Arquitectura y flujo

1. `core.tmdb_enrich` valida cabecera y calcula SHA-256 del CSV por bloques.
2. Recorre `csv.DictReader` incrementalmente; no carga el lote en memoria.
3. `CheckpointStore` conserva identidades/posición en SQLite para deduplicar sin
   un conjunto de un millón de identidades en RAM.
4. En modo live, `TMDbMetadataClient` consulta EN con `append_to_response=credits`
   y ES sin repetir créditos. `/movie/{id}` o `/tv/{id}` según el tipo validado.
5. Cada respuesta correcta se cachea en el checkpoint antes de pedir la siguiente
   traducción. Un fallo de ES no obliga a volver a consultar EN al reanudar.
6. Se llama al normalizador con `details=english`, `english`, `spanish`,
   `language="en-US"` y los créditos del detalle EN.
7. Se escribe el resultado por estado y se confirma su posición/checksum.
8. Se publica un manifiesto y un snapshot actualizado de pendientes.

Un registro necesita normalmente **dos solicitudes HTTP**, no una. Los reintentos
también consumen el presupuesto. La solicitud ES no se sustituye por la EN.
`sources` del normalizador conserva respaldos de títulos y procedencia de sinopsis.
La procedencia de una solicitud localizada no garantiza que TMDb disponga de una
traducción real; esto debe revisarse en el piloto.

## Entrada

Cabeceras obligatorias, únicas, con nombres exactos (orden libre; extras ignoradas):

```csv
type,tmdb_id,original_title,popularity,adult,video
movie,25449,New World Disorder 9: Never Enough,0.7748,False,False
```

CSV UTF-8, con o sin BOM, comillas/newlines según CSV. `type` solo admite `movie`
o `series`; `tv` **no** es el tipo interno. `tmdb_id` debe ser entero decimal
positivo hasta 2.147.483.647. Se admiten espacios exteriores y ceros iniciales,
normalizando la identidad numérica. Filas con columnas faltantes/sobrantes son
`invalid`; cabecera incorrecta o sintaxis CSV rota detiene la ejecución.

`source.position` es la posición del registro empezando en 1, sin contar cabecera;
`line_end` es la línea física final, para registros con newlines entre comillas.
Se conservan las seis columnas originales para trazabilidad. No se interpretan
URLs del CSV, ni se usan popularidad, adult o video como filtros definitivos.
Las repeticiones de la misma identidad generan un evento `duplicate_input` y
no vuelven a consultar ni a producir otro resultado normalizado.

No modificar CSV, fixtures ni código durante una ejecución. La reanudación exige
la misma ruta de entrada, tamaño, SHA-256, cabeceras y configuración semántica.

## Salidas

Cada ejecución crea una subcarpeta nueva, fuera del repositorio, que contiene:

| Archivo | Contenido |
|---|---|
| `accepted.jsonl.gz` | Normalizador `accepted`; no equivale a aprobación de publicación |
| `excluded.jsonl.gz` | Exclusiones explícitas del normalizador |
| `incomplete.jsonl.gz` | Resultado consultado, con faltantes/advertencias; pendiente de calidad |
| `invalid.jsonl.gz` | Datos inválidos o identidad no encontrada (404), diferenciados por código |
| `pending.jsonl.gz` | Snapshot de consultas no resueltas, incluso auth/presupuesto/interrupción |
| `duplicates.jsonl.gz` | Eventos de duplicación del CSV con posición original y repetida |
| `checkpoint.sqlite3` | Estado autoritativo, deduplicación, cache parcial y offsets/checksums |
| `manifest.json` | Identificación del input/reglas, conteos, estado y checksums de outputs |
| `run.lock` | Bloqueo del sistema operativo, liberado incluso si muere el proceso |

Las cuatro salidas clasificadas contienen un objeto JSON estricto por línea,
UTF-8 y newline final. No se aceptan NaN/Infinity. Ratings conservan la forma que
produce el normalizador (`external_rating`, por ejemplo `"8.3"`, sin dividirlo).

```json
{"schema_version":1,"type":"movie","tmdb_id":101,"source":{"position":1,"line_end":2},"status":"accepted","normalization":{"status":"accepted","valid":true,"data":{"genre":"Action, Animation, Comedy, Documentary, Drama, Horror, Musical, Sci-Fi","external_rating":"8.3","external_votes":123}},"publication":{"decision":"pending_policy_approval","auto_importable":false,"normalizer_accepted":true,"requires_destination_identity_check":true}}
```

Ejemplo abreviado: los objetos reales incluyen toda la salida del normalizador
(`data`, `errors`, `warnings`, `missing_fields`, `field_states`, `sources`,
`update_data`, exclusiones, etc.), el SHA-256 del normalizador, contexto de
consulta y flags observados. Un 404 tiene `normalization=null` y
`lookup_error.code="tmdb_not_found"`; no se confunde con error temporal.

### Preparación para importación futura

Ningún registro se marca autoimportable. `accepted` significa solamente que pasó
las reglas actuales del normalizador. Aún falta aprobar política de publicación
y revisar el piloto, incluyendo flags adult/video, traducciones, campos mínimos
y calidad de créditos. No se inventan umbrales de popularidad, votos o rating.

`incomplete` no es rechazo definitivo: conserva sus campos aprovechables,
advertencias y faltantes para revisión/re-enriquecimiento posterior. Los cuatro
estados del normalizador son terminales **para esta consulta en esta ejecución**;
no se vuelven a pedir al reanudar. Una revisión posterior requiere otro lote.
Los pendientes de consulta sí se vuelven a intentar una vez por invocación de
`--resume` (más los reintentos HTTP configurados).

## Confirmación y recuperación

Cada línea se comprime como un miembro GZIP con `mtime=0`. Los archivos contienen
miembros concatenados, soportados por `gzip` de Python y gzip estándar. Incluso
una salida vacía contiene un miembro GZIP válido. El orden de claves es estable.

Por resultado: serializar estrictamente, anexar miembro, `flush`/`fsync`, y después
confirmar estado, offset, checksum y conteo en una transacción SQLite con
`synchronous=FULL`. Si el proceso se interrumpe después de escribir y antes de
confirmar, la reanudación trunca exclusivamente el sufijo no confirmado.
Si falta o está corrupto un prefijo ya confirmado, se rechaza la reanudación;
no se borra ni se considera correcto ese resultado.

El checkpoint se publica por renombrado atómico tras inicializarlo. Una interrupción
durante esa inicialización, antes de cualquier candidato, no deja un checkpoint
válido: se debe conservar la carpeta para diagnóstico y crear una ejecución nueva.

`pending.jsonl.gz` y `manifest.json` se reemplazan atómicamente. Se actualizan al
arrancar, cada 100 intentos de candidatos nuevos y al terminar/interrumpir. Tras
SIGKILL, pueden reflejar un estado anterior; SQLite manda y los reconstruye al
reanudar. No se promete recuperación de un disco dañado o que ignora `fsync`.

La versión semántica incluye SHA-256 del normalizador y de los tres módulos del
enriquecedor, modo, idiomas, política de créditos y fixtures offline. Cambiar esas
reglas exige otro lote. Se permite ajustar límite, velocidad, timeouts, presupuesto,
reintentos o credencial al reanudar; `last_invocation` deja sus valores operativos
(sin secretos). El archivo de entrada se relee hasta el cursor, sin almacenarlo.

Los archivos `.jsonl.gz` también existen en ejecuciones parciales: su presencia
**no** significa finalización. Solo `manifest.complete=true`, `state=completed`,
EOF alcanzado y cero pendientes indican finalización del CSV completo. Un piloto
con `--limit` normalmente queda `limited`, con `complete=false`, por diseño.

Conteos acumulados: `examined = terminal_results + pending + duplicates`.
Los reintentos de pendientes aumentan `candidate_attempts`, no `examined`.
`http_requests` cuenta intentos reservados persistentemente justo antes de llamar
al transporte (incluye reintentos); una muerte en esa pequeña ventana puede contar
un intento que no llegó a salir. `transient_errors` cuenta fallos HTTP/transporte,
no producciones únicas. `retries` cuenta intentos HTTP adicionales efectivamente
reservados, no simples esperas planificadas.

## Errores, seguridad y velocidad

* Solo `https://api.themoviedb.org/3` (barra final opcional), IDs y rutas construidas
  internamente. Sin redirects, proxies heredados ni desactivación TLS.
* `TMDB_READ_ACCESS_TOKEN` y `TMDB_BASE_URL`, mismas variables del backend. Solo
  `--live` carga `.env`; variables ya presentes prevalecen. No importar settings.
* No se imprimen headers, cuerpos HTTP ni mensajes de excepción del transporte.
  El token se elimina de datos persistidos si el proveedor lo devolviera.
* Timeout de conexión/lectura configurable: 3,05/15 segundos por defecto.
* Secuencial; 2 solicitudes/s por defecto. Sin concurrencia ni solicitudes anexas
  de temporadas/episodios. No se fija una velocidad cercana al máximo del servicio.
* Hasta 3 reintentos por defecto (configurable 0–10), backoff exponencial limitado
  a 30 segundos más jitter de hasta 25%; 429/500/502/503/504, conexión y timeout.
* `Retry-After` admite segundos o fecha HTTP y prevalece si es mayor al backoff.
  El cooldown también se respeta para el siguiente candidato y se guarda para
  reanudar. Esperas de más de 60 s pausan el lote (`waiting`) en lugar de bloquearlo
  durante horas. Reanudar antes de la fecha no emite consultas.
* 401/403 detienen el lote (`blocked_auth`); corregir credencial y reanudar
  explícitamente. Redirect/TLS inseguro detiene como `blocked_security`.
* 404 es `invalid`, sin reintento automático. Otros HTTP/protocolo quedan pendientes
  con código y `retryable`, sin inventar una exclusión de catálogo.
* `--max-requests` limita todos los intentos HTTP de la invocación. Por defecto 10.
  Presupuesto agotado conserva EN/ES ya obtenidos y deja el candidato pendiente.

## Pruebas y demo offline

Desde `C:\dev\apss-social-Movies`, en PowerShell:

```powershell
$tmdbPython = 'C:\dev\apss-social-Movies\venv\Scripts\python.exe'
& $tmdbPython -B -m unittest core.test_tmdb_enrichment core.test_tmdb_normalizer -v

# Usar una carpeta nueva: la demo no sobrescribe carpetas existentes.
& $tmdbPython -B -m scripts.create_tmdb_enrichment_demo --directory 'C:\dev\docs\TMDB enriched\demo-nueva'
& $tmdbPython -B -m core.tmdb_enrich --offline --input 'C:\dev\docs\TMDB enriched\demo-nueva\candidates.csv' --fixtures 'C:\dev\docs\TMDB enriched\demo-nueva\fixtures' --output-dir 'C:\dev\docs\TMDB enriched\offline-runs' --limit 0
```

La demo tiene ocho filas: dos accepted, una excluded, una incomplete, tres invalid
y un duplicado. Incluye Unicode, todas las categorías y creador de serie como
respaldo. No es un extracto de TMDb ni de PostgreSQL.

Verificar el directorio exacto que imprime el CLI:

```powershell
& $tmdbPython -B -m core.tmdb_enrich_verify 'C:\dev\docs\TMDB enriched\offline-runs\run-REEMPLAZAR' --input 'C:\dev\docs\TMDB enriched\demo-nueva\candidates.csv'
```

El verificador no modifica el lote: revisa SHA-256/tamaños, GZIP/UTF-8/JSON estricto,
identidades/posiciones sin duplicados, clasificación y conteos. Utiliza un SQLite
temporal para no mantener todas las identidades en RAM. Código 0: verificado y
completo; 2: verificado pero incompleto; 3: inválido. `--allow-incomplete` permite
código 0 para un piloto/pausa verificado sin afirmar que el CSV está completo.
No ejecutarlo simultáneamente con un escritor activo.

## Pilotos live preparados — NO ejecutados

Requieren revisión y aprobación previa del usuario. Diez candidatos completos
necesitan normalmente 20 consultas; por eso **no** se puede prometer diez
producciones completas con un presupuesto de diez solicitudes. Con el presupuesto
predeterminado se obtienen como máximo cinco pares completos, sin reintentos.

Los comandos siguientes usan cero reintentos para hacer explícito el máximo de
solicitudes. Los 20/200/2.000 intentos requieren autorización específica; no se
ejecutan como parte de la entrega. No crean ni actualizan registros PostgreSQL.

```powershell
$tmdbPython = 'C:\dev\apss-social-Movies\venv\Scripts\python.exe'
$tmdbCandidates = 'C:\dev\docs\TMDB comparison\completa-2026-11-06\new_candidates.csv'
$tmdbOutput = 'C:\dev\docs\TMDB enriched\live-pilots'

# Diez candidatos; máximo 20 intentos HTTP. Pendiente de autorización.
& $tmdbPython -B -m core.tmdb_enrich --live --input $tmdbCandidates --output-dir $tmdbOutput --limit 10 --max-requests 20 --max-retries 0 --requests-per-second 1
# Cien candidatos; máximo 200 intentos HTTP. NO ejecutar aún.
& $tmdbPython -B -m core.tmdb_enrich --live --input $tmdbCandidates --output-dir $tmdbOutput --limit 100 --max-requests 200 --max-retries 0 --requests-per-second 1
# Mil candidatos; máximo 2.000 intentos HTTP. NO ejecutar aún.
& $tmdbPython -B -m core.tmdb_enrich --live --input $tmdbCandidates --output-dir $tmdbOutput --limit 1000 --max-requests 2000 --max-retries 0 --requests-per-second 1
```

Cada comando anterior crea una ejecución independiente y empieza al inicio del
CSV. Para continuar la misma ejecución, usar su carpeta exacta y **no** output-dir:

```powershell
& $tmdbPython -B -m core.tmdb_enrich --live --input $tmdbCandidates --resume 'C:\dev\docs\TMDB enriched\live-pilots\run-REEMPLAZAR' --limit 10 --max-requests 20 --max-retries 0 --requests-per-second 1
& $tmdbPython -B -m core.tmdb_enrich_verify 'C:\dev\docs\TMDB enriched\live-pilots\run-REEMPLAZAR' --input $tmdbCandidates --allow-incomplete
```

`--limit` cuenta intentos de candidatos únicos/filas inválidas **por invocación**,
incluidos pendientes reintentados. Los duplicados no consumen ese límite; sí
aumentan examinados. `--limit 0` significa continuar hasta EOF, solo por elección
explícita. Estado `limited` sale con código 0; pendientes/bloqueos/presupuesto/
waiting con 2; configuración o almacenamiento con 3; interrupción con 130.

## Condiciones antes de enriquecimiento masivo

Revisar primero el piloto de 10: manifiesto/checksums, identidad EN/ES, traducciones,
créditos de películas y series, géneros completos, ratings/votos sin transformación
y causas de incomplete/excluded/invalid. Aprobar entonces 100 y después 1.000,
revisando tasa de errores, espacio y velocidad. No mezclar estos artefactos con los
informes del comparador ni modificar sus CSV.

Limitaciones pendientes: calidad real de las traducciones y créditos estándar de
series; política de publicación; coste de fsync por registro y cache/ledger en
disco a escala; compatibilidad de consumidores que lean solamente el primer
miembro GZIP. Se privilegia recuperación segura frente a máximo rendimiento.
El ledger crece proporcionalmente a las identidades y la cache parcial; las
respuestas confirmadas no se retienen completas en memoria ni en la cache.
La recuperación/verificación recorre los archivos para comprobar sus checksums.
