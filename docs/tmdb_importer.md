# Fase 5 — importador controlado: primera entrega de planificación

Fase 5B: también se admiten carpetas con `preparation_manifest.json` e
`import_ready.jsonl.gz` verificados. Ver [preparación import_ready](tmdb_import_ready.md).
La preparación no consulta destino; el dry-run conserva su consulta agrupada local.

`import_tmdb_productions` prepara un **dry-run exclusivamente de lectura** sobre
PostgreSQL local. `--apply` está bloqueado incondicionalmente antes de abrir una
conexión o leer entradas. No existe código de INSERT/UPDATE/DELETE contra Django.
No se han implementado importación real, actualizaciones ni depuración.

## Inspección y compatibilidad

Se revisaron Movie, `Movie.save()`, helpers de búsqueda/genre_key, señales,
serializadores, el importador previo de ratings/votos y los componentes de fases
1–4, además de settings y migraciones existentes. No se modificó ninguno.

- Identidad: `(type, tmdb_id)`, valores `movie`/`series`; PK `id` interna.
  No hay unicidad sobre la pareja; el índice individual TMDb no evita duplicados.
- `title_english` es obligatorio, máximo 255; `title_spanish` es opcional.
- Género/director/póster tienen límites de 100/255/200 caracteres. Año y votos
  deben caber en los enteros del modelo; rating es decimal de una cifra decimal.
- `synopsis` no admite NULL, pero sí `""`; `external_votes` tiene default 0.
  Los opcionales restantes pueden quedar NULL. Son defaults del modelo, no
  afirmaciones de que TMDb haya proporcionado esos valores.
- `Movie.save()` deriva `genre_key` y cuatro campos de búsqueda. El planificador
  reutiliza `build_genre_key()` y `populate_search_fields()` en objetos sin guardar.
- `search_vector` lo genera el trigger PostgreSQL definido en la migración 0040.
  `full_clean()` lo rechaza vacío porque no tiene `blank=True`: la validación de
  propuestas excluye **solo** ese campo gestionado por el servidor. No se inventa
  un vector ni se cambia Movie. Antes de habilitar apply habrá que comprobar el
  trigger real en la base desechable/destino autorizado.
- No hay señales de guardado de Movie en `core.signals`; las señales sociales
  identificadas no se ejecutan porque no se llama a `save()` ni se crean relaciones.
- Los serializadores actuales usan `title_english` como título principal.
  Se conserva el respaldo multilingüe aprobado, sin modificar el frontend.
- El comando previo de ratings actualiza registros existentes y no sirve para
  crear producciones; no se reutiliza su ruta de escritura.
- `bulk_create()` omite `save()` y no equivale a él. No se utiliza ni se habilita
  en esta entrega. Tampoco se usa `ignore_conflicts=True`.

## Comandos preparados, no ejecutados contra una base real

Confirmar previamente que `default` corresponde a PostgreSQL local conocido.
La herramienta comprueba la configuración efectiva, no el nombre del alias;
no modifica `.env`, DATABASE_URL ni otras variables.

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe -B manage.py import_tmdb_productions `
  --run-dir 'C:\dev\docs\TMDB enriched\live-pilot-10\run-20261113T010627Z-019d61868a' `
  --output-dir 'C:\dev\docs\TMDB import' `
  --database default `
  --allow-partial `
  --batch-size 10 `
  --limit 10
```

Omitir modo equivale a `--dry-run`. `--allow-partial` permite explícitamente el
piloto verificado `limited`; no lo reanuda ni afirma que terminó su CSV completo.
`--limit` cuenta resultados terminales de enriquecimiento examinados por esta
invocación, no inserciones. Se incluye tanto accepted como incomplete, excluded
e invalid. Los pendientes upstream no se importan y se cuentan separadamente.

Para continuar **el plan de importación**, no el enriquecimiento:

```powershell
.\venv\Scripts\python.exe -B manage.py import_tmdb_productions `
  --run-dir 'C:\dev\docs\TMDB enriched\live-pilot-10\run-20261113T010627Z-019d61868a' `
  --output-dir 'C:\dev\docs\TMDB import' `
  --database default --allow-partial --batch-size 10 --limit 10 `
  --resume 'C:\dev\docs\TMDB import\run-REEMPLAZAR'
```

Cada invocación crea una carpeta distinta `run-<UTC>-<UUID>`. No sobrescribe la
anterior. Se permite lote 1–500 y límite positivo. Los resultados deben estar
fuera de los repositorios. Para fixtures sintéticos hay una excepción explícita
`--allow-offline-input`, exclusivamente en dry-run; nunca se aceptan por defecto.

`--apply`, incluso combinado con `--confirm-database` y `--limit`, genera
`CommandError: Apply is disabled...` sin abrir conexión. No hay flag oculto,
variable ni configuración que habilite escrituras.

## Verificación de entrada

1. Se copia el manifiesto y sus archivos de resultados a `source_snapshot/`
   dentro de la carpeta nueva. Las rutas deben ser canónicas y permanecer en el
   directorio origen; se rechazan escapes mediante enlaces/rutas manipuladas.
2. Se reutiliza `tmdb_enrich_verify.verify_run()` sobre esa copia: SHA-256,
   tamaños, GZIP concatenado, UTF-8, JSON estricto, identidades/posiciones únicas,
   clasificación, elegibilidad v2 y reconciliación de conteos.
3. Se exige normalizador conocido por su SHA-256. Se admiten envoltorios v1 y
   v2 compatibles; v1 se evalúa mediante la política vigente sin modificarlo.
4. Se rechaza modo sintético salvo opt-in, estados fallidos/activos y ejecuciones
   parciales sin autorización explícita. La procedencia de cada registro debe
   concordar con el modo del manifiesto. No se confía en el nombre accepted.
5. Los registros se colocan incrementalmente en una cola SQLite local indexada
   por posición original. Así se conserva el orden incluso cuando los archivos
   por estado contienen resultados reintentados fuera de orden.
6. Todas estas comprobaciones terminan **antes de abrir PostgreSQL**.

La copia evita depender de archivos que pudieran cambiar después de verificar.
No se modifica el enriquecimiento original ni su checkpoint. Los checksums
detectan corrupción/inconsistencias, no autentican al autor: alguien con permiso
para falsificar datos y todos sus hashes también puede falsificar `mode=live`.
Los artefactos deben proceder de un directorio confiable. No se conservan ni se
usan credenciales para autenticar estos archivos.

## Admisión adult/video

Los JSONL actuales conservan `tmdb_flags.adult/video` de la respuesta EN y
`source.adult/video` de la exportación. Eso permite resolver los casos conocidos
sin llamadas a TMDb ni cambios silenciosos al enriquecedor.

- La API solo se interpreta como confirmación si aporta un booleano JSON real.
- Los flags CSV aceptan booleanos o strings `true`/`false`, ignorando espacios y
  mayúsculas. Números, null, ausencia y otros valores son desconocidos.
- Si **cualquiera** de las fuentes confirma adult=true, se excluye. API true
  siempre prevalece, incluso si la exportación decía false. Una exportación true
  con API false también se excluye por prudencia; se informa del conflicto.
- Si no hay true y alguna fuente confirma false, se considera adult resuelto.
- Si ambas siguen desconocidas, se registra `pending_adult`: nunca equivale a false.
- `video` true/false/null/ausente no cambia admisión ni genera revisión manual.
  Se informa como dato independiente, sin relación con tráilers.
- Adult prevalece incluso sobre otros errores/elegibilidad. Se mantienen las
  exclusiones explícitas Reality y News/Talk del normalizador, y todas las
  equivalencias históricas de géneros sin límite de tres.

Los géneros desconocidos conservan diagnóstico; no se inventan categorías. Un
campo parcial/inválido no se transfiere al objeto propuesto como si fuera válido.
Los géneros transferidos deben pertenecer al vocabulario interno existente.

## Propuesta de Movie y títulos

Se recalcula elegibilidad desde `normalization`; la bandera v2 también se verifica
por el verificador. No se ejecuta `Movie.objects.create(**data)` ni se usa
`update_data`, cuyo propósito es proteger actualizaciones de títulos existentes.

Se construye una lista explícita de campos permitidos y solo se copian valores
con estado `valid`. El título principal sale de `data.title_english`, incluyendo
respaldos originales o españoles; se informa `title_sources`. No se duplican las
reglas de fallback ni se fabrican traducciones. Se preserva Unicode.

Se validan identidad del sobre frente al contenido, tipos JSON, límites, géneros
internos y compatibilidad del objeto mediante `full_clean(validate_unique=False,
validate_constraints=False, exclude=['search_vector'])`. No se consultan FK ni
unicidad durante esa validación. Se aplican NULL/defaults reales del modelo
cuando faltan datos opcionales y se separan en `model_defaults` para trazabilidad.
No se asignan author, Movie.id, timestamps, relaciones ni datos sociales.

## Destino local y consultas

Se reutilizan `require_local()` y `local_readonly_snapshot()` de la fase 2A:
motor PostgreSQL, host explícito loopback/localhost, puerto y nombre simples,
sin servicios/pools/hostaddr que sobrescriban el destino ni sesión preexistente.
Después se establece `REPEATABLE READ READ ONLY` y se confirma mediante
`host(inet_server_addr())` y `transaction_read_only=on`. Un wrapper adicional
rechaza escrituras y SQL ajeno al SELECT fijo de Movie. No es un parser SQL general.
No se consultan usuarios, ratings, comentarios, videos ni otras tablas sociales.

Por lote hay como máximo un SELECT de Movie agrupado por `(type,tmdb_id)` con
`COUNT(id)` y `MIN(id)`. Se seleccionan solo las identidades del lote: cero
coincidencias significa candidato nuevo; una, existente; varias, duplicado
existente. No se hacen consultas individuales ni se cargan modelos de destino.
Dry-run usa una instantánea consistente durante la invocación; se libera y se
cierra la conexión al terminar o fallar. Esto puede retener MVCC en ejecuciones
largas: limitar pilotos y vigilar el tiempo.

Loopback identifica la ubicación de red, no descarta túneles o servidores locales
maliciosos. Usar el PostgreSQL local conocido, sin redirecciones hacia Render.
La inicialización ordinaria de Django ocurre antes de establecer la transacción;
no se ejecutan checks/migraciones ni se garantiza código ajeno a este comando.

## Duplicados, concurrencia y apply bloqueado

Las repeticiones ilegales de identidad dentro de archivos terminales bloquean la
verificación antes de conectar. Los eventos `duplicate_input` legítimos del
enriquecedor se cuentan, sin producir otra propuesta. Movie.id no se reasigna y
los títulos nunca se usan como identidad.

Un SELECT previo **no garantiza** ausencia de duplicados concurrentes. Sin
restricción única, un advisory lock solo protege a escritores que respeten el
mismo protocolo. Un lock de tabla serializa durante una transacción, pero no
impide que un escritor ajeno inserte después usando una decisión antigua.
Por ese motivo apply sigue deshabilitado, incluso con confirmación del destino.

Diseño pendiente de aprobación antes de habilitarlo:

1. Validar dry-run y respaldo/restauración local.
2. Elegir una garantía de identidad demostrable: unicidad efectiva tras resolver
   duplicados en otra fase, o exclusividad operacional y protocolo obligatorio
   para **todos** los escritores durante el piloto.
3. Exigir destino local confirmado por nombre, límite explícito, entrada live
   verificada y nueva comprobación de identidad inmediatamente antes de insertar.
4. Transacciones por lote, bloqueo acordado, solo inserciones nuevas, sin updates,
   reasignaciones ni ignore_conflicts. Comprobar trigger de búsqueda y efectos de
   save/señales sobre una base PostgreSQL desechable.
5. Registrar únicamente inserciones confirmadas tras COMMIT; recuperar el caso
   COMMIT sin informe mediante nueva consulta de identidad, nunca reinserción ciega.

No se han escrito ni probado inserciones PostgreSQL ni locks de inserción. Las
pruebas de concurrencia actuales solo demuestran aislamiento de dos planificadores
dry-run y la imposibilidad de activar apply; no se presentan como garantía de
unicidad de escrituras reales.

## Journal, reanudación e informes

El SQLite de la carpeta nueva es un journal **local de informes**, no Django.
Guarda cola, configuración y decisiones confirmadas por lote con synchronous=FULL.
Una interrupción registra estado y exporta las decisiones ya confirmadas. Al
reanudar se verifica el SHA-256 del checkpoint anterior y la configuración
(fuente, destino, flags y hashes de implementación/modelo/normalizador/verificador).
Se copia mediante backup SQLite de conexión read-only a un journal nuevo; el
origen permanece intacto. Las decisiones deben formar un prefijo ordenado.

Se rechazan fuentes activas o incompatibles. Tras terminación abrupta que deje
estado `running` no se intenta reparación automática: en esta entrega sin
escrituras se puede iniciar otro dry-run desde la fuente verificada. No se usan
decisiones de un dry-run como órdenes de apply. Las decisiones previas de una
reanudación no se revalidan: el manifiesto lo advierte; el catálogo pudo cambiar
entre invocaciones. Un apply futuro siempre debe verificar de nuevo identidades.

La memoria contiene como máximo un lote, un resultado JSON por línea y caché
SQLite de unos 8 MiB; no hay conjunto Python con todo el catálogo. Reservar disco
para los GZIP copiados, cola JSON descomprimida, decisiones y journal: puede ser
varias veces el tamaño comprimido. No se midió rendimiento a escala completa.

Archivos por ejecución:

- `import_manifest.json`: modo/estado, fuente y hashes, destino sin credenciales,
  configuración, límites, conteos, pendientes upstream y datos de reanudación.
- `decisions.jsonl`: una fila JSON UTF-8 por identidad terminal examinada, posición,
  decisión, motivos, flags, elegibilidad, diagnóstico, título de procedencia,
  valores propuestos/defaults cuando corresponde. `inserted=false` siempre.
- `checkpoint.sqlite3`: cola, decisiones y configuración local.
- `source_snapshot/`: copia de los informes verificados, sin el checkpoint del enriquecedor.

Decisiones: `proposed_new`, `existing`, `existing_duplicate`, `excluded_adult`,
`excluded_genre`, `pending_adult`, `validation_error`. Hay conteos independientes
para incompletos aceptables, eventos de entrada duplicada y pendientes upstream.
`inserted=0` y `apply_enabled=false` siempre. `complete` se refiere al recorrido
de resultados terminales de **este plan**, no al CSV original ni a una importación.
Un plan puede terminar aunque el enriquecimiento fuente esté limitado.

Ejemplo generado por la prueba aislada con cuatro resultados terminales, una
identidad existente, una identidad con dos filas y dos candidatos nuevos:

```json
{
  "mode": "dry-run",
  "state": "completed",
  "complete": true,
  "examined": 4,
  "counts": {
    "proposed_new": 2,
    "existing": 1,
    "existing_duplicate": 1,
    "excluded_adult": 0,
    "excluded_genre": 0,
    "pending_adult": 0,
    "validation_error": 0
  },
  "incomplete_acceptable": 1,
  "inserted": 0,
  "apply_enabled": false
}
```

El ejemplo es sintético; no predice identidades actualmente existentes en la base
local. No se almacenan usuario, password, tokens ni excepciones de conexión.

## Pruebas y límites de la entrega

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe -B scripts\test_tmdb_importer.py
```

El ejecutor no carga settings de aplicación ni `.env`; solo permite una base
SQLite `:memory:` sintética y bloquea sockets/PostgreSQL. No hay migraciones.
Los tests usan SQL ORM real para identidades y PRAGMA query_only durante el plan,
comparan filas y tabla social antes/después y comprueban que UPDATE falla. Incluyen
los contratos v1/v2, corrupción, flags adult/video, opcionales, Unicode, derivados,
duplicados, repetición, concurrencia dry-run, protección apply, interrupción y
reanudación, y la suite previa completa del enriquecedor/normalizador/elegibilidad.
Las pruebas heredadas de frontera local validan READ ONLY y el rechazo de destinos
remotos mediante dobles. No sustituyen una integración PostgreSQL desechable.

Resultado: **139 pruebas pasaron**, incluidas las 112 de enriquecimiento,
normalización y elegibilidad, las nuevas de planificación y las de frontera local.
El ejemplo de conteos anterior se comprobó contra los informes realmente generados
por la prueba sintética. Las 139 pruebas no incluyen inserciones PostgreSQL ni
demuestran locks de escritura: apply permanece bloqueado.

No se ejecutó dry-run ni apply sobre PostgreSQL real durante esta tarea, ni se
consultó TMDb/Render. No se modificaron Movie, migraciones, frontend o datos reales.
No se crearon ramas, commits, PR ni despliegues. La primera entrega queda pendiente
de revisión; las escrituras reales siguen bloqueadas.
