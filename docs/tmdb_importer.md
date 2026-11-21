# Fase 5C — importador local de TMDb

`import_tmdb_productions` usa dry-run de forma predeterminada. Verifica la
fuente y consulta PostgreSQL local en una transacción `REPEATABLE READ READ ONLY`.
El modo apply exige confirmación de escritura y un esquema de destino verificado.

Para la conexión dedicada a Render y el procedimiento del piloto, consultar
[Importaciones de TMDb en Render](tmdb_import_render.md).

## Piloto verificado el 17 de noviembre de 2026

En `socialapp` local, el dry-run verificó que las diez identidades preparadas eran
películas nuevas y elegibles. Los IDs TMDb fueron 25449, 31975, 9, 91, 94, 125,
126, 127, 160 y 190. Hubo cero exclusiones, pendientes, coincidencias e inserciones.
El resultado describe el destino en el momento de la consulta.

La auditoría confirmó 1.295 grupos duplicados de identidades positivas de
películas y series. El único índice único de la tabla era el de la PK `id`.
El índice de `tmdb_id` permite duplicados. El trigger de búsqueda instalado está
habilitado para INSERT y las 434.223 filas observadas tenían `search_vector`.
La comprobación de esquema de apply rechazó `socialapp` en una transacción
READ ONLY por falta del índice único compuesto. Durante esta fase se usaron
exclusivamente consultas de lectura en esa base.

## Ejecutar dry-run

```powershell
Set-Location 'C:\dev\apss-social-Movies'
$ready = 'C:\dev\docs\TMDB import ready\prepare-20261117T030953Z-04ae9a747d42'
$reports = Join-Path ([System.IO.Path]::GetTempPath()) 'reccool-tmdb-phase5c-author'
.\venv\Scripts\python.exe -B manage.py import_tmdb_productions --dry-run --author-username Admin --run-dir $ready --output-dir $reports --database default --batch-size 10 --limit 10
```

El límite cuenta candidatos examinados. El lote acepta valores de 1 a 500.
Cada ejecución crea una carpeta nueva fuera del repositorio con
`source_snapshot/`, `import_manifest.json`, `decisions.jsonl` y
`checkpoint.sqlite3`. El manifiesto identifica destino, huella de la fuente,
conteos por decisión y estado. El checkpoint de dry-run permite continuar un
prefijo con `--resume <carpeta de un dry-run anterior>`; las decisiones previas
conservan la información de su consulta original. Para obtener una vista fresca
de todas las identidades, iniciar un dry-run nuevo.

El manifiesto registra el nombre, ID y estado del autor, junto con
`author_blocks_apply`. La consulta usa el mismo alias y snapshot READ ONLY
que Movie. Sin `--author-username` se conserva el plan y se informa
`not_provided`. Si falta el usuario se informa `not_found`. Para `socialapp`,
un nombre distinto de `Admin` se informa como `expected_local_admin`.
Estos estados bloquean apply y dejan intacta la elegibilidad del contenido.
Al reanudar un dry-run se vuelve a consultar el autor solicitado.

Dry-run también admite enriquecimiento verificado. Una fuente de enriquecimiento
limitada exige `--allow-partial`, y los fixtures de procedencia offline exigen
`--allow-offline-input`. Para el archivo preparado se verifican el manifiesto,
tamaños, SHA-256, GZIP, UTF-8, identidad, conteos y elegibilidad. El manifiesto
del piloto se creó con LF en los dos módulos de preparación e importación.
`.gitattributes` fija esos finales de línea para que Git en Windows preserve
las huellas de esos dos archivos de reglas. Ante una discrepancia de reglas,
verificar la versión y los bytes de la copia de trabajo antes de reintentar.

## Apply, condicionado al esquema y a la autorización

Comando propuesto para el primer piloto, pendiente de autorización y de cumplir
las condiciones de integridad. Usa las variables PowerShell definidas arriba.

```powershell
.\venv\Scripts\python.exe -B manage.py import_tmdb_productions --apply --confirm-write --confirm-database socialapp --author-username Admin --run-dir $ready --output-dir $reports --database default --batch-size 10 --limit 10
```

- Se exige un `import_ready` verificado de procedencia live, una confirmación
  explícita, el nombre exacto de la base, un autor explícito y un límite positivo de inserciones por
  invocación. Se omiten filas existentes y se siguen revisando candidatos hasta
  alcanzar el límite o terminar la fuente.
- La configuración efectiva debe usar PostgreSQL y una IP de loopback literal,
  con puerto explícito y conexión nueva. Se rechazan los desvíos de libpq por
  hostaddr, servicio y otras opciones de enrutamiento. PostgreSQL confirma
  dirección, puerto, base, modo de transacción y que el servidor es primario.
- El preflight READ ONLY exige que el ORM resuelva `public.core_movie`, que exista
  un índice único válido, inmediato y sin predicado sobre las dos columnas, y
  que el trigger de búsqueda esté activo para INSERT. También exige una FK
  validada hacia la PK del modelo de usuario en el destino.
- Cada lote usa READ COMMITTED, confirma el destino y toma un bloqueo
  `SHARE ROW EXCLUSIVE` sobre Movie con espera máxima de cinco segundos. Se
  repiten las comprobaciones de esquema y se consulta la existencia de las
  identidades en una sola consulta bajo el bloqueo. El bloqueo serializa lotes
  y bloquea otras escrituras sobre Movie durante ese lote. El índice único
  protege la identidad también fuera del importador.
- Se crean únicamente identidades nuevas con `bulk_create`. Se releen los IDs,
  las identidades, el autor y los derivados en el mismo lote, y se exige un
  vector de búsqueda no vacío antes de confirmar. Un error revierte el lote
  completo. Los lotes anteriores pueden haber quedado confirmados.

`Movie.author` es una FK nullable a `auth.User`, el `AUTH_USER_MODEL`
configurado, con `SET_NULL` en el ORM. El apply anterior dejaba el autor en NULL.
El comando de ratings y votos sólo actualiza esos campos en películas existentes.
Apply resuelve el nombre de usuario exacto y bloquea su fila antes de cada lote
para comprobar que conserva la misma identidad. El usuario debe existir en la
base. `socialapp` exige `Admin`. El nombre previsto para una importación futura
en producción es `catalog_import`. El comando actual sólo admite PostgreSQL local.

## Proyección del catálogo

La proyección aplica los mismos helpers que `Movie.save()` para `genre_key` y
los cuatro campos de búsqueda normalizados. `bulk_create` omite `save()` y las
señales de Django. Las señales de calificación y recomendaciones pertenecen
a sus modelos sociales. El trigger PostgreSQL incorpora títulos, año, director,
género, reparto y tipo al vector ponderado. En el piloto, por ejemplo, 31975
proyectó `Comedy|Drama`, 160 proyectó `Documentary|Drama` y 25449 dejó la clave
en NULL al carecer de género. El orden y la cantidad de géneros provienen del
normalizador y del helper de Movie.

Se conserva el título Unicode disponible en `title_english` y se guarda
`title_spanish` cuando tiene procedencia ES. Se exige evidencia de `adult=false`
y se excluyen contradicciones, mientras que `video` se conserva como dato de
auditoría. Se mantienen las exclusiones de Reality, News y Talk. La ausencia
de opcionales usa el valor del modelo correspondiente, por ejemplo, sinopsis
vacía y votos en cero. Los ratings externos conservan la precisión
del modelo y las calificaciones sociales mantienen su lógica existente.

El feed y el detalle leen el catálogo Movie mediante el ID interno asignado.
El buscador usa los campos derivados y el vector GIN. Los filtros por género y
las afinidades de recomendaciones consultan `genre_key`. Tu Cartelera y las
listas de recomendaciones muestran la película cuando el usuario establece
la relación social correspondiente. La revisión verificó estos puntos en el
código, y la prueba sintética verificó que se conserva una relación existente.
La verificación funcional en PostgreSQL queda pendiente.

## Informes, reanudación y comprobaciones posteriores

Apply genera un manifiesto por ejecución con destino, huella de la fuente,
conteos, el autor resuelto e inserciones confirmadas observadas. `decisions.jsonl`
registra `inserted`, `movie_id` y `author_id` por cada alta. El manifiesto es el
checkpoint de informes y se publica después de cada lote. Para asociar una
reanudación al informe previo, añadir `--resume <carpeta de un apply anterior>`
al mismo comando y confirmar nuevamente el destino, el autor y el límite.
Apply sólo reanuda informes con el mismo nombre de usuario y PK resuelta.
Los informes de apply previos a esta validación se rechazan para reanudación.

Al reejecutar se verifica otra vez la fuente y se recorren sus identidades
desde el inicio. PostgreSQL determina qué falta, incluso si el proceso murió
después del commit y antes de publicar los informes. En ese intervalo el
checkpoint puede subcontar inserciones. El siguiente informe las clasifica
como existentes y no puede atribuirlas retroactivamente a la ejecución anterior.
Los informes previos se conservan. Revisar la identidad y los IDs en la base si
el manifiesto quedó en `running`, `failed` o `interrupted`. Ante error de lock,
reintentar tras resolver la contención. Ante un error de fuente, verificar sus
archivos y huellas. Ante un fallo de integridad o búsqueda, investigar el esquema
y el lote afectado antes de reintentar.

Antes de autorizar el piloto real, resolver los duplicados históricos con una
estrategia aprobada que preserve las relaciones y los IDs, provisionar y revisar
el índice único y el trigger, hacer un respaldo comprobado y repetir el dry-run.
Después de un apply autorizado, comparar IDs, campos, `genre_key` y vector con
los informes, comprobar que hay una sola fila por identidad y repetir el
dry-run para verificar que los nuevos se clasifican como existentes. Probar
búsqueda, feed, detalle, Cartelera y recomendaciones con el piloto autorizado.

## Pruebas aisladas

```powershell
.\venv\Scripts\python.exe -B scripts\test_tmdb_importer.py
```

El ejecutor usa SQLite sintético en memoria y bloquea PostgreSQL y la red.
Las pruebas de apply hacen inserciones y rollback reales en SQLite, con
sustitutos para el catálogo, los bloqueos y el trigger de búsqueda de
PostgreSQL. La auditoría de solo lectura aportó evidencia sobre el esquema
real. La integridad de las escrituras y la contención de PostgreSQL tendrán
que demostrarse en una instancia desechable.

Para continuar el ensayo, confirmar primero el nombre de la base sintética,
directorio de datos, usuario y puerto de loopback de una instancia separada
de `socialapp`. Con esa configuración aprobada, aprovisionar una base vacía
con el DDL de Movie, el trigger equivalente al de 0040, `unaccent` y el índice
único de identidad. Usar una preparación sintética de contrato live generada
sin red. Ejecutar dry-run, apply limitado y reanudación, lanzar dos apply
simultáneos sobre la misma fuente y verificar unicidad e IDs. Repetir con un
error inducido en el segundo lote para comprobar rollback y conservación del
primero. Verificar la búsqueda de un título acentuado, `genre_key`, los
campos de búsqueda y relaciones de prueba. Eliminar la instancia desechable
tras conservar los resultados. Esta fase dejó el ensayo pendiente de
confirmación del destino aislado.
