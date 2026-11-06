# Comparador TMDb — fase 1

Comando `compare_tmdb_exports`: compara sin importar, enriquecer, descargar ni
actualizar producciones. No utiliza la API TMDb.

## Revisión del modelo

`core.models.Movie`: `MOVIE="movie"`, `SERIES="series"`; `type` es un
`CharField` nullable con esas opciones. `tmdb_id` es `PositiveIntegerField`,
nullable, con índice B-tree y sin unicidad individual ni por `(type, tmdb_id)`.
También existen índices de búsqueda, votos, año, género y tipo/año/id; ninguno
impide duplicados TMDb. La clave primaria `id` tiene su índice habitual.
Se revisaron el modelo y la migración de incorporación del campo, sin consultar
el esquema desplegado. El modelo permite nulos: su presencia real se contará en
`database.null_tmdb_id`; no se consultó ninguna base real durante esta tarea.
El comando sigue el patrón `BaseCommand`, `CommandError`, CSV UTF-8 BOM e
`iterator(chunk_size=2000)` de los comandos de exportación existentes.

## Ejecución local desde PowerShell

Revisar previamente que el alias `default` apunte a PostgreSQL **local**, pues
`config/settings.py` también admite `DATABASE_URL` desde el entorno o `.env`.
El comando no distingue automáticamente una base de producción de una local.
No ejecutarlo con credenciales de Render.

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe manage.py compare_tmdb_exports `
  --movies-file 'C:\dev\docs\TMDB files\movie_ids_11_05_2026.json.gz' `
  --series-file 'C:\dev\docs\TMDB files\tv_series_ids_11_05_2026.json.gz' `
  --output-dir 'C:\dev\docs\TMDB comparison\fase1-2026-11-06' `
  --sample-size 20 `
  --database default
```

Este comando se entrega para una ejecución posterior autorizada; no se ejecutó
contra los archivos completos ni una base real. Para una exploración parcial,
agregar `--limit 10000`: limita **líneas físicas por archivo**, incluidas las
inválidas. `--sample-size 0` desactiva ejemplos del JSON; no limita los CSV.
La carpeta de resultados debe estar fuera del repositorio y ser nueva o vacía.
Para repetir, elegir otra carpeta: no se sobrescriben informes anteriores.

## Algoritmo y recursos

1. Una consulta PostgreSQL selecciona exclusivamente `id`, `tmdb_id`, `type`,
   ordenados por clave primaria, en bloques de 2000 filas. No carga instancias
   Movie ni consulta tablas sociales. Los nulos, ceros y tipos inesperados se
   informan como inválidos.
2. Un SQLite **temporal local independiente** guarda las claves y los IDs internos
   en disco, incluidas las filas duplicadas. No es el backend Django. Sus
   escrituras no afectan PostgreSQL.
3. Cada GZIP se lee línea por línea. Se exige un objeto JSON con `id` entero
   entre 1 y 2147483647 (rango PostgreSQL del campo actual). No se aceptan
   booleanos, cadenas numéricas, decimales, NaN ni infinitos. Una línea inválida
   se cuenta y se omite. Otros campos son metadatos opcionales, no se exige título.
4. El índice temporal identifica duplicados y coincidencias mediante
   `(type, tmdb_id)`. Las búsquedas por línea son locales, **no consultas N+1 a
   PostgreSQL**. El primer registro válido de cada clave determina los metadatos
   del candidato. Las repeticiones posteriores se informan por separado.
5. Un anti-join local encuentra las filas de base no observadas en la exportación.
   Los informes se escriben incrementalmente y el SQLite temporal se elimina.

Complejidad aproximada O((D+E) log(D+E)) por índices B-tree. Memoria adicional:
caché SQLite de aproximadamente 8 MiB, bloque de 2000 filas, una línea JSON y
hasta `5 × sample-size` ejemplos. Estimación orientativa: decenas de MiB por
encima del proceso Django, sin conjuntos Python de millones de claves. No es
un máximo estricto: depende del tamaño de las líneas, títulos, muestras y del
driver. Espacio temporal: reservar varios cientos de MB para unos 2 millones
de claves, además de los CSV; no se midió a escala completa. El journal de
SQLite puede aumentar el pico de disco. Las cifras de tiempo y memoria real
deben medirse en la ejecución local autorizada.

## Garantías de solo lectura

Antes del SELECT, dentro de una transacción nueva del alias elegido, se ejecuta
`SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`. PostgreSQL impide
escrituras a tablas persistentes durante esa transacción. `atomic()` solo
delimita la transacción; la prohibición viene de `READ ONLY`. El comando rechaza
otros motores, transacciones preexistentes y autocommit desactivado; si no puede
establecer la protección, falla. Un `execute_wrapper` adicional admite únicamente
el SELECT generado para Movie durante la lectura. No es un analizador SQL general.

La instantánea es consistente durante el volcado; se libera antes de procesar los
GZIP. No cubre cambios posteriores, otras conexiones, comandos ajenos, ni efectos
de inicialización externos al comando. `READ ONLY` de PostgreSQL permite ciertas
operaciones sobre tablas temporales; el comando no las utiliza en PostgreSQL.
Un usuario PostgreSQL con permisos exclusivamente SELECT aporta defensa adicional.
No se ejecutan migraciones ni comprobaciones que consulten tablas adicionales.

## Informes y significado

- `summary.json`: estado, alias, contadores, ejemplos acotados y tiempo total.
- `comparison_summary.csv`: tipo, métrica y contador.
- `new_candidates.csv`: claves únicas nuevas, título original, popularidad,
  adult y video cuando existen. Las series usan `original_name` como título.
- `database_duplicates.csv`: cada fila adicional y el primer ID interno de su clave.
- `export_duplicates.csv`: cada repetición y el número de la primera línea.
- `invalid_records.csv`: origen, tipo, línea o ID interno y motivo; no copia
  líneas originales ni datos personales.
- `database_not_in_export.csv`: cada fila válida no observada y su estado.

`existing` y `new_candidates` cuentan claves únicas. `valid` cuenta líneas válidas
incluidas repeticiones. `database_duplicate_extra_rows` y
`export_duplicate_extra_rows` cuentan filas adicionales, no grupos duplicados.
`database_valid_rows` y `database_not_in_export_rows` cuentan filas internas.
Los inválidos de base con tipo desconocido se cuentan globalmente.

Si hay truncamiento o líneas inválidas, el estado de ausencia advierte que la
exportación es parcial o defectuosa. Incluso con archivo completo, una ausencia
**no confirma eliminación en TMDb**, y nunca provoca un borrado. Un fallo GZIP,
de disco o de base aborta; puede dejar CSV parciales. Solo una ejecución que
finaliza genera `summary.json` con `status="complete"`. El proceso finaliza con
estadísticas por tipo y tiempo. Los títulos que podrían ejecutar fórmulas al
abrirse como hoja de cálculo reciben un apóstrofo inicial.

Ejemplo sintético: base con `(movie,7)` y `(series,9)`; archivo de películas con
7 y 8; archivo de series con 7. Resultado: una película existente; candidatos
`(movie,8)` y `(series,7)`; `(series,9)` ausente sin eliminación confirmada.

## Pruebas aisladas

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe scripts\test_tmdb_comparison.py
```

El ejecutor configura Django sin importar `config.settings` ni `.env`, bloquea
la apertura de conexiones y usa exclusivamente archivos GZIP sintéticos pequeños
en una carpeta temporal. No crea base de pruebas, tablas ni migraciones. Cubre
coincidencias, candidatos, colisión entre tipos, duplicados en ambos orígenes,
JSON/UTF-8/IDs inválidos, nulos, archivos vacíos, límites, muestras, metadatos,
rutas con espacios, salida existente o dentro del repo, GZIP corrupto, proyección
del SELECT y rechazo de SQL de escritura.

La frontera PostgreSQL se verifica mediante dobles de prueba: orden de
`SET TRANSACTION` y SELECT, alias, campos, rechazo de transacciones anidadas y
otros motores. **No se validó contra un servidor PostgreSQL** la denegación de
una escritura ni el rendimiento completo. Esa integración debe realizarse en
una base local desechable antes de la primera ejecución real.
