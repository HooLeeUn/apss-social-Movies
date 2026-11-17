# Fase 5B — preparación de import_ready.jsonl.gz

Esta etapa no consulta PostgreSQL ni TMDb. No utiliza decisiones del dry-run de
fase 5 como autorización y no compara de nuevo exportaciones masivas.
`--apply` del importador continúa bloqueado antes de abrir conexión.

## Diseño e inspección

Se revisaron los contratos v1/v2 del enriquecedor, `tmdb_eligibility`, los
manifiestos/verificador, los flags persistidos y la proyección de Movie del
planificador. Se eligió una etapa pequeña independiente de destino, reutilizando
`snapshot_input`, `stage_records`, `evaluate_record` y la validación de campos de
`prepare_movie`. No se duplican equivalencias, fallback de títulos ni política de
admisión. La cola y checkpoint SQLite son solo archivos locales de esta etapa.

`accepted.jsonl.gz` e `incomplete.jsonl.gz` son las únicas fuentes de candidatos
incluibles. Los resultados excluded/invalid se recorren únicamente para informar
omisiones y nunca se convierten en importables. Los eventos de duplicación y
pendientes upstream también quedan auditados por separado.

## Admisión

- API `tmdb_flags.adult` y exportación `source.adult` conservan sus fuentes.
  Cualquier true prevalece y excluye, incluso si la otra fuente dice false.
  Si ambas siguen desconocidas, el candidato queda pendiente. Nunca se inventa
  false. Una fuente false comprobable permite resolver el desconocido de la otra;
  una contradicción true/false siempre se excluye y se informa.
- `video` true/false/null/ausente no bloquea ni crea revisión manual. Se conserva
  con sus valores API/exportación; no se interpreta como tráiler.
- Se mantienen exclusiones Reality y series exclusivamente News/Talk, reglas
  históricas TV Movie y todas las categorías válidas de géneros, sin modificarlas.
- Los opcionales faltantes no bloquean: sinopsis, género, año, póster, créditos,
  rating/votos. Se conservan vacíos, parciales, warnings y missing_fields en el
  resultado normalizado original. No se fabrican metadatos ni equivalencias.
- Se exige identidad compatible y título principal utilizable de hasta 255
  caracteres. Se reutiliza el fallback ya existente a `title_english`, incluso si
  el título disponible está en otro idioma.

La proyección `movie_data` solo conserva `title_spanish` cuando su procedencia
normalizada es `es`; no copia un respaldo original/inglés como si fuera una
traducción española. La procedencia de una respuesta ES no permite demostrar
que TMDb tuviera una traducción real: no se intenta detectar idioma por texto.
El candidato original permanece íntegro para auditar los respaldos históricos.

## Entrada histórica: conversión explícita

Los sobres originales v1 no se renumeran como v2. La preparación exige
`--convert-legacy` si la ejecución no declaraba la política nueva de elegibilidad.
El contenedor preparado usa un contrato distinto, conserva el candidato v1 y
añade la evaluación vigente separadamente. La procedencia y versión antigua se
guardan en `trace`. Sin datos suficientes de adult, queda en omitted y no se incluye.

Se reutiliza el verificador del enriquecedor para SHA-256, tamaños, GZIP, UTF-8,
JSON estricto, versiones, clasificación, posiciones e identidades y conteos. Se
rechazan normalizadores desconocidos, estados activos/fallidos, entradas sintéticas
sin opt-in y lotes limitados sin `--allow-partial`. La entrada se copia primero a
un snapshot nuevo y se procesa la copia verificada; no se abre su checkpoint.

## Contrato de salida

Cada línea tiene `contract="reccool.import_ready"`, `schema_version=1` y:

| Campo | Contenido |
| --- | --- |
| `type`, `tmdb_id` | Identidad lógica; no hay Movie.id asignado. |
| `movie_data` | Valores normalizados utilizables, sin defaults ni campos derivados. No es kwargs directo del ORM. |
| `candidate` | Sobre de enriquecimiento original con normalización, procedencia, estados, fuentes y diagnósticos, conservando su versión. |
| `eligibility` | Evaluación vigente, independiente de la clasificación accepted/incomplete. |
| `admission.adult` | Valor resuelto, confirmaciones API/export y conflicto. |
| `admission.video` | Valores API/export, sin veto. |
| `title_sources` | Procedencia de los títulos. |
| `warnings` | Advertencias originales no eliminadas. |
| `trace` | Directorio/hash de manifiesto fuente, archivo original, posición y versión del sobre. |
| `import_authorized` | Siempre false. |

El candidato original conserva también campos parciales/no utilizables para que
no se pierda información diagnóstica. `movie_data` solo incluye valores no nulos
con estado valid: por ejemplo, no inventa votos cero ni sinopsis vacía. El futuro
importador aplica NULL/defaults y derivados al crear su propia propuesta.
No se incluyen credenciales de configuración, tokens de cliente ni datos de
usuarios; la preparación no carga clientes HTTP ni consulta relaciones sociales.
Los artefactos deben proceder de una fuente confiable, ya saneada por el enriquecedor.

Ejemplo sintético, **extracto** del contenedor generado para un título japonés
sin metadatos opcionales (se omiten candidate/eligibility/trace completos):

```json
{
  "contract": "reccool.import_ready",
  "schema_version": 1,
  "type": "movie",
  "tmdb_id": 1,
  "movie_data": {"type": "movie", "tmdb_id": 1, "title_english": "日本語"},
  "title_sources": {"title_english": "details.original", "title_spanish": "details.original"},
  "admission": {
    "adult": {"resolved": false, "api": false, "export": false, "conflict": false},
    "video": {"api": false, "export": "True"}
  },
  "import_authorized": false
}
```

La ausencia de title_spanish en movie_data es deliberada; el respaldo original
que había guardado el normalizador sigue visible dentro de candidate.

## Streaming, unicidad, publicación e integridad

La cola en disco ordena por posición fuente, incluso cuando accepted e incomplete
contengan resultados reintentados fuera de orden. Se evalúan lotes de 1–500 y
se confirman decisiones por lote en SQLite con synchronous=FULL. No se cargan los
archivos enteros ni todas las identidades en memoria.

Se escribe `import_ready.jsonl.gz.part` con cabecera GZIP determinista (mtime=0,
sin nombre de archivo) y JSON estricto UTF-8/newline. Se escribe también
`omitted.jsonl.part`. Antes de publicar se cierran, vacían y fsync ambos archivos;
se relee el GZIP, se recalcula la evaluación/proyección por registro, se comprueba
orden, unicidad por `(type,tmdb_id)` mediante SQLite temporal y se reconcilian los
conteos. Solo después se renombran los archivos y se confirma el manifiesto.

`verify_ready_run()` comprueba además tamaños/SHA-256, contrato y hashes de reglas
actuales, coherencia de la fuente y contenido. Un GZIP cuyo contenido fue alterado
no se acepta solo por haber recalculado su hash si contradice el candidato y la
política. Los hashes no autentican a un autor: un actor que reescriba todos los
datos originales y manifiestos puede falsificar una fuente. No hay firmas.

El marcador de una preparación consumible es `preparation_manifest.json` con
state=completed y complete=true **más** sus verificaciones; el nombre del GZIP
aislado no basta. Si falla/interrumpe el proceso, no se conserva un ready final
como resultado aprobado; quedan .part o una ejecución fallida/interrumpida.
Una terminación forzada justo entre renombres puede dejar archivos finales sin
manifiesto completo, que el importador rechazará. No se promete recuperación de
fallos de hardware o persistencia de renombres tras pérdida de alimentación.

Los duplicados terminales ilegales bloquean la verificación de toda la fuente;
no se elige arbitrariamente entre dos metadatos de la misma identidad. Los eventos
duplicate_input legítimos se informan, sin generar otra fila ready. Una película
y una serie con igual ID numérico siguen siendo identidades distintas.

## Archivos, omitidos y conteos

Cada invocación crea una carpeta única `prepare-<UTC>-<UUID>` fuera del repositorio:
la carpeta de salida tampoco puede estar dentro del enriquecimiento original ni
dentro de una preparación anterior utilizada para reanudar.

- `import_ready.jsonl.gz`: único archivo de candidatos aptos.
- `preparation_manifest.json`: fuente, reglas/versiones/SHA-256, opciones explícitas,
  estado, cantidades, hashes/tamaños de las dos salidas y datos de reanudación.
- `omitted.jsonl`: clasificaciones `excluded_adult`, `pending_adult`,
  `excluded_genre`, `invalid`, `duplicates`, `pending_enrichment`, con motivos,
  identidad y diagnósticos cuando corresponden al candidato terminal.
- `checkpoint.sqlite3`: cola y decisiones de preparación locales.
- `source_snapshot/`: copia de los informes fuente, sin modificar los originales.

Ejemplo de omisión por ausencia de adult comprobable, extracto:

```json
{"classification":"pending_adult","type":"movie","tmdb_id":3,"position":3,"source_status":"accepted","reason":"adult_not_confirmed_by_available_data","adult":{"resolved":null,"api":null,"export":null,"conflict":false}}
```

`examined` cuenta candidatos terminales; `included` y `eligible` cuentan los aptos
efectivamente escritos. Excluidos, pendientes adult e inválidos son terminales
omitidos. `duplicates` y `pending_enrichment` son eventos upstream adicionales.
Reconciliación: included + líneas omitted = examined + duplicates + pending_enrichment.
La suma de incluidos y omitidos terminales debe coincidir con los cuatro archivos
terminales fuente. El diagnóstico original se conserva, no se cambia incomplete a accepted.

## Reanudación controlada

`--resume` toma una preparación anterior interrumpida/fallida/completa, verifica
el hash del checkpoint y compara fuente, reglas, conversión y opciones. Hace un
backup desde conexión read-only a una **carpeta nueva**, verifica un prefijo
ordenado de decisiones y continúa lo pendiente. Exporta de nuevo a temporales y
verifica toda la salida antes de publicarla. No modifica el checkpoint anterior.

No se reparan automáticamente fuentes con state=running tras SIGKILL: se puede
reiniciar una preparación nueva desde el enriquecimiento intacto. Si cambian
código/modelo/reglas, se rechaza la reanudación incompatible; no se mezclan criterios.

## Consumo por el importador en dry-run

`import_tmdb_productions --run-dir <carpeta preparada>` detecta el manifiesto nuevo,
copia y verifica sus tres archivos públicos (manifiesto/ready/omitted), sin depender
del snapshot de enriquecimiento ni consultar TMDb. Una adaptación interna conserva
las versiones originales y entrega los candidatos al planificador existente.
La proyección de Movie respeta la misma política de title_spanish real y vuelve
a aplicar defaults/derivados, en lugar de pasar movie_data directamente al ORM.

Se mantiene la consulta ligera por lotes de identidad contra PostgreSQL local
READ ONLY. Existentes y duplicados existentes se omiten y se informan; no se vuelve
a comparar el catálogo completo. Apply sigue inhabilitado; concurrencia de
escrituras, respaldos y unicidad de destino continúan pendientes de otra fase.

Los checkpoints previos de fase 5 usan un hash de implementación distinto y no
deben reanudarse con reglas nuevas; se inicia un dry-run nuevo. No se modifica el
dry-run histórico `run-20261117T015949Z-33228bd6c974`.

## Comandos exactos

Pruebas offline sintéticas, sin PostgreSQL/HTTP:

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe -B scripts\test_tmdb_importer.py
```

Preparar el piloto histórico: **comando entregado, NO ejecutado en esta tarea**:

```powershell
.\venv\Scripts\python.exe -B manage.py prepare_tmdb_import `
  --run-dir 'C:\dev\docs\TMDB enriched\live-pilot-10\run-20261113T010627Z-019d61868a' `
  --output-dir 'C:\dev\docs\TMDB import ready' `
  --allow-partial `
  --convert-legacy `
  --batch-size 10
```

Después de generarlo localmente, verificar la carpeta exacta impresa:

```powershell
.\venv\Scripts\python.exe -B manage.py prepare_tmdb_import `
  --verify 'C:\dev\docs\TMDB import ready\prepare-REEMPLAZAR'
```

Dry-run posterior sobre ese preparado, pendiente de revisión/autorización local:

```powershell
.\venv\Scripts\python.exe -B manage.py import_tmdb_productions `
  --run-dir 'C:\dev\docs\TMDB import ready\prepare-REEMPLAZAR' `
  --output-dir 'C:\dev\docs\TMDB import' `
  --database default --batch-size 10 --limit 10
```

La preparación de fixtures sintéticos requiere `--allow-offline-input`; el
importador también exige su opt-in para consumir preparados de procedencia offline.
Reanudar una preparación usa el mismo --run-dir original y opciones, añadiendo
`--resume <carpeta anterior>`; siempre se crea otra carpeta nueva.

## Pruebas y límites

Las pruebas cubren accepted/incomplete, flags y conflictos, opcionales/títulos
Unicode, exclusiones, contratos v1/v2, conversión explícita, identidades repetidas
y por tipo, corrupción, manifiestos, interrupciones de evaluación/escritura,
renombrado fallido, verificación previa a publicar, hashes, conteos, determinismo,
reanudación y dry-run con existentes/duplicados. La preparación se ejecuta con
`connection.cursor` bloqueado; el consumo usa únicamente una tabla SQLite sintética.
El ejecutor bloquea sockets y PostgreSQL. No se ejecutan migraciones.

Resultado de validación: **156 pruebas aprobaron** (139 previas y 17 nuevas de
fase 5B). Los ejemplos se verificaron con artefactos sintéticos temporales; no
se generó todavía un import_ready del piloto real ni se consultó su destino.

Reservar disco para snapshots comprimidos, cola/resultados JSON, temporales y
archivo final. La memoria depende de un lote y de la línea/grupo de texto más
grande; no se midió una carga masiva. La ausencia de traducción no se interpreta
como defecto de identidad. No se inventan idioma ni metadatos.

No se ejecutó la preparación del piloto real, consultas TMDb, conexiones reales
a PostgreSQL/Render ni importaciones durante esta tarea. No se modificaron modelos,
géneros, frontend o ratings. No se crearon ramas, commits, push ni PR.
