# Importaciones de TMDb en Render

El comando `import_tmdb_productions` usa el alias `reccool_render_import` cuando
se selecciona explícitamente con `--database`. Lee
`RECCOOL_RENDER_DATABASE_URL` del entorno del proceso y compara sus datos con
las confirmaciones de host externo, puerto, base y rol de PostgreSQL. Obtén la
URL por el canal de secretos habitual. El archivo de CA debe provenir de una
fuente de confianza y permitir validar el certificado y el hostname con
`verify-full`. El alias se cierra al terminar. El flujo local sigue usando
`default` y, en la base `socialapp`, el autor `Admin`.

La sesión se abre con transacciones de solo lectura por defecto. El dry-run
verifica una transacción `REPEATABLE READ READ ONLY` y comprueba el índice
único, el trigger de búsqueda y la FK de autor. El usuario de la aplicación
`catalog_import` debe existir antes del apply. El login de PostgreSQL es
independiente de ese autor. Para apply, usa el rol autorizado que pueda leer
las tablas, tomar los bloqueos de tabla y autor, insertar y usar la secuencia
de Movie si corresponde. PostgreSQL aplica los permisos del rol. Los informes
guardan una huella del nombre del rol para vincular la reanudación al destino.

## Piloto de diez producciones

Ejecutar desde la raíz del repositorio tras la revisión y autorización del
equipo. Confirmar los parámetros con el destino aprobado y sustituir la ruta
de la CA. La preparación corresponde a las diez identidades verificadas en la
[fase 5F](tmdb_import_phase5f_validation.md).

```powershell
$python = '.\venv\Scripts\python.exe'
$ready = 'C:\dev\docs\TMDB import validation\phase5f-20261118-02\prepared\prepare-20261119T023243Z-ee93704e051f'
$reports = 'C:\dev\docs\TMDB Render import\pilot-10'
$renderCa = '<ruta absoluta al CA de confianza>'
$common = @(
  '--database', 'reccool_render_import',
  '--confirm-host', 'dpg-dac5ji2jobas73ccg630-a.oregon-postgres.render.com',
  '--confirm-port', '5432',
  '--confirm-database', 'reccool_postgres_production',
  '--confirm-user', 'reccool_postgres_production_user',
  '--tls-ca-file', $renderCa,
  '--author-username', 'catalog_import',
  '--run-dir', $ready,
  '--batch-size', '10',
  '--limit', '10'
)
& $python -B manage.py import_tmdb_productions @common --dry-run --output-dir (Join-Path $reports 'dry-run')
```

Revisar `import_manifest.json` y `decisions.jsonl`. Esperar diez registros
examinados, `state=completed`, `inserted=0` y autor `resolved`. El conteo
`proposed_new` identifica las producciones ausentes y elegibles. Confirmar
manualmente las decisiones y la huella del manifiesto preparado
`e280e39efd21eddeb7f5744f643847659b180112fcffe943c8a11ee4dfd2fc6f`.
Si el equipo utiliza otro rol PostgreSQL para el dry-run, cambiar
`--confirm-user` y la URL juntos al pasar al rol autorizado para apply.

Tras aprobar la escritura, ejecutar el primer apply con el mismo preparado.
El límite de diez cuenta inserciones por invocación. Cada lote vuelve a
comprobar las identidades bajo bloqueo, por lo que el conteo puede cambiar si
otro escritor se adelantó.

```powershell
& $python -B manage.py import_tmdb_productions @common --apply --confirm-write --output-dir (Join-Path $reports 'apply')
```

## Verificación e idempotencia

1. Registrar el total antes y después. En otra sesión autorizada de solo
   lectura, abrir `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;` y
   comprobar `SHOW transaction_read_only;`. Ejecutar las consultas siguientes
   y cerrar con `ROLLBACK;`.

   ```sql
   SELECT count(*) AS total FROM public.core_movie;

   SELECT m.id, m.tmdb_id, m.title_english, m.title_spanish, m.genre,
          u.username AS author, m.search_vector
   FROM public.core_movie m
   LEFT JOIN public.auth_user u ON u.id = m.author_id
   WHERE m.type = 'movie' AND m.tmdb_id IN (25449, 31975, 9, 91, 94, 125, 126, 127, 160, 190)
   ORDER BY m.tmdb_id;

   SELECT count(*) AS duplicate_groups
   FROM (
       SELECT type, tmdb_id
       FROM public.core_movie
       WHERE type IS NOT NULL AND tmdb_id IS NOT NULL
       GROUP BY type, tmdb_id HAVING count(*) > 1
   ) duplicates;
   ```

2. Comparar los IDs insertados y sus campos con `decisions.jsonl`. Revisar
   los metadatos adicionales en las filas indicadas por `movie_id`.
   Las nuevas filas deben tener género normalizado, autor `catalog_import`
   y vector de búsqueda poblado. Esperar cero grupos duplicados y conciliar
   cualquier diferencia de totales con escrituras concurrentes.
3. Ejecutar un dry-run nuevo con los mismos parámetros. Esperar diez
   `existing`, cero `proposed_new` y cero inserciones. Para verificar la
   idempotencia del apply, repetir el comando apply con la misma autorización
   y esperar cero inserciones y diez `existing`.
4. Si se interrumpe el apply, revisar primero las identidades presentes.
   Reanudar con `--resume <carpeta-del-informe-apply>`, los mismos datos
   de origen, destino y autor, y un límite explícito. La reanudación genera otro
   informe y consulta las identidades desde el principio, incluso si se perdió
   el checkpoint después de un commit. Ajustar el nuevo límite al cupo restante.

Queda por confirmar en la primera ejecución autorizada la CA, el acceso al
host, los permisos efectivos, la disponibilidad de `catalog_import` y cuántas
identidades están ausentes. El bloqueo de Movie del apply puede competir con
escrituras de la aplicación. Cada lote tiene un máximo de cinco segundos de
espera por bloqueo y puede haber lotes previos confirmados si otro falla.
