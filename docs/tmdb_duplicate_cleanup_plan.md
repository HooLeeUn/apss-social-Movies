# Fase 5D — plan de depuración de duplicados de Movie

La fase 5E añadió un [ejecutor con validación y reanudación](tmdb_duplicate_cleanup_execution.md).
Esta guía conserva el diagnóstico y la política de selección del plan.

## Diagnóstico local

La consulta a `socialapp` encontró 434.223 filas en Movie, con 384.630 películas
y 49.593 series. Encontró 0 `tmdb_id` nulos, 0 IDs no positivos o fuera del rango
entero del modelo y 0 filas de otros tipos. Hay 1.295 grupos válidos duplicados
y 1.298 filas adicionales. Tampoco hay pares no nulos duplicados excluidos de
la política de claves válidas. Las cifras corresponden a la instantánea consultada.

El catálogo local tiene un índice B-tree individual no único en `tmdb_id`, una
PK única en `id`, un CHECK de TMDb no negativo y los índices de catálogo y
de búsqueda enumerados en el informe. El check permite 0. No hay unicidad de la
pareja. El trigger de `search_vector` se ejecuta en INSERT o UPDATE de sus campos
de entrada. `Movie.save()` calcula `genre_key` y los cuatro campos de texto de
búsqueda. El plan conserva una fila íntegra por identidad.

## Generar y comprobar un plan

```powershell
Set-Location 'C:\dev\apss-social-Movies'
$plan = Join-Path ([System.IO.Path]::GetTempPath()) ('reccool-duplicate-cleanup-' + [guid]::NewGuid().ToString('N'))
.\venv\Scripts\python.exe -B manage.py plan_local_movie_duplicate_cleanup --database default --confirm-database socialapp --output-dir $plan
```

El directorio debe ser nuevo y estar fuera de los repositorios. La conexión
debe ser PostgreSQL local. La consulta se ejecuta en `REPEATABLE READ READ ONLY`
y confirma el nombre, puerto y resolución de la tabla Movie en el servidor.
El comando tiene una sola modalidad, planificación de lectura.

- `cleanup_plan.jsonl` contiene en orden de tipo e ID TMDb el sobreviviente,
  IDs candidatos a quitar, puntajes y campos puntuados, motivo, huella del
  conjunto de registros, impactos por película y estado del grupo.
- `cleanup_dependencies.json` contiene relaciones Django, FKs reales,
  restricciones, índices y triggers. Si el grafo vivo no concuerda, se omite
  la estimación por grupo y se registra un bloqueo de esquema.
- `cleanup_manifest.json` se publica al completar la lectura y escritura
  de los informes. Contiene el destino, versión de política, totales, impacto
  y SHA-256 de ambos archivos. Un directorio sin este marcador es incompleto.

Para reanudar la revisión de un plan completo, repetir con otro directorio
y añadir `--resume <directorio anterior>`. Se verifican las dos huellas y el
destino del plan anterior. Se recalcula todo desde un snapshot nuevo y se
indica si coinciden las huellas y conteos. Un plan parcial se reinicia en otro
directorio. Las huellas permiten detectar cambios respecto al manifiesto
conservado. Comparar el plan con la base antes de autorizar una operación real.

## Selección automática

Sólo se agrupan `movie` y `series` con un ID TMDb positivo que cabe en un entero de
32 bits. Película y serie con el mismo número son identidades distintas.

| Peso | Campos |
| --- | --- |
| 3 | Título principal y año |
| 2 | Título español, director, géneros, sinopsis EN, sinopsis ES y póster |
| 1 | Reparto, rating externo, votos externos, tráiler ES, tráiler EN e IMDb si el modelo dispone del campo |

Cada campo utilizable suma su peso una sola vez. Nulos, cadenas vacías o en blanco
y valores predeterminados del modelo aportan cero. El año debe estar entre
1800 y 2200, el póster debe ser una URL HTTP(S) con host, el rating debe ser
finito y estar en (0, 10], y los votos deben ser enteros positivos. Género
aporta su peso si contiene al menos un término no vacío separado por comas.
Se conservan todos los géneros de la fila seleccionada. El mayor puntaje gana
y se desempata por el menor `Movie.id`. La magnitud del rating, número de
votos, longitud de cadenas, antigüedad e interacciones no alteran el orden.

## Dependencias de eliminación

El ORM declara CASCADE para las 11 FKs directas a Movie. Incluyen comentarios,
videos, calificaciones, listas, recomendaciones e historial, favoritos,
notificaciones, candidatos de feed, enlaces de streaming y recomendaciones
semanales. El grafo incluye reacciones, destinatarios de comentarios
dirigidos y notificaciones transitivas. No se detectaron relaciones M2M
declaradas hacia Movie ni FKs entrantes PROTECT, RESTRICT o SET_NULL. La FK
saliente `Movie.author` tiene SET_NULL en Django cuando se elimina el usuario.
Eliminar una película no elimina al autor, perfiles ni usuarios.

Las FKs entrantes observadas son diferibles, inicialmente diferidas, con NO
ACTION en PostgreSQL. Un DELETE SQL directo no reproduce CASCADE del ORM.
El plan estima filas dependientes por ruta FK. Una misma notificación podría
contar en dos rutas. En esta ejecución hubo 174 grupos con dependencias,
1 calificación y 4.953 candidatos de feed, con cero grupos bloqueados por
PROTECT o RESTRICT y sin triggers de usuario de DELETE detectados. Movie y
sus dependencias se inspeccionaron en el catálogo PostgreSQL y en Django.

Antes de ejecutar una depuración, probar los efectos de `MovieRating.post_delete`
sobre gustos y caché, la señal de recomendaciones sensible al origen y la
reconciliación de snapshots de feed. Si el plan futuro incluye comentarios o
videos, revisar informes de contenido con referencias lógicas sin FK y archivos
de video, porque el colector de Django puede omitir la sobrescritura de
`VideoComment.delete()`. Si aparecen enlaces de streaming, revisar su vínculo
con TMDb/IMDb. El alcance aprobado para esta depuración futura consiste en
descartar dependencias de las filas duplicadas sin fusionarlas.

## Propuesta de unicidad

```python
models.UniqueConstraint(fields=["type", "tmdb_id"], name="movie_type_tmdb_id_uniq")
```

La restricción sería inmediata, sin predicado. PostgreSQL permite varias filas
con NULL en cualquiera de las columnas por defecto. La política propuesta
mantiene esa semántica para registros sin identidad completa. Cero y los tipos
inválidos siguen siendo controles de validez independientes. Los pares no
nulos duplicados, incluso fuera de la política de depuración, deben estar en
cero antes de crear el índice. El índice individual por TMDb puede seguir
sirviendo consultas por TMDb sin tipo. [Django UniqueConstraint](https://docs.djangoproject.com/en/6.0/ref/models/constraints/)

Una migración convencional puede bloquear escrituras durante la construcción
del índice. Para una tabla activa, evaluar una migración no atómica que cree
el índice único B-tree con `CONCURRENTLY` y lo adjunte después con
`ALTER TABLE ... ADD CONSTRAINT ... USING INDEX`, manteniendo al día el estado
de Django. La construcción concurrente tiene más trabajo y esperas, puede dejar
un índice inválido si falla y la adjunción todavía requiere un bloqueo
de tabla breve. Ensayar duración, espacio, permisos, timeout y recuperación en
una instancia aislada. Render requiere su propio diagnóstico, plan, respaldo,
permisos de DDL, ventana de mantenimiento y autorización independiente.
[PostgreSQL CREATE INDEX](https://www.postgresql.org/docs/18/sql-createindex.html) · [PostgreSQL ALTER TABLE](https://www.postgresql.org/docs/18/sql-altertable.html)

## Procedimiento para una ejecución futura

1. Ensayar colector, efectos secundarios, rollback e interrupción en una
   instancia desechable. Revisar el plan, sus huellas y un respaldo restaurable.
2. Exigir host local literal, puerto, base confirmada, huella aprobada y
   confirmación explícita. Coordinar una ventana para detener escritores de
   Movie y de tablas dependientes. Un bloqueo de Movie por sí solo no estabiliza
   todas las relaciones o efectos secundarios. Serializar operadores y fijar
   límites de espera. Revisar y bloquear cambios en el grafo durante la operación.
3. En cada transacción, releer todas las filas de la identidad, verificar
   sobreviviente y huella, y volver a calcular el impacto de dependencias.
   Ante datos nuevos o un bloqueo, abortar el lote y solicitar revisión. La
   eliminación futura usaría un queryset Movie limitado a los IDs aprobados y
   comprobaría los conteos del colector, el sobreviviente y la integridad
   referencial antes de confirmar.
4. Registrar por grupo IDs, resultados, conteos y transacción confirmada.
   En una reanudación, contrastar grupos pendientes con el estado real. Un
   grupo totalmente aplicado con el sobreviviente intacto puede reconciliarse.
   Una eliminación parcial o un sobreviviente distinto requiere investigación.
   El registro durable y la invalidación de caché necesitan diseño y ensayo.
5. Volver a contar todas las claves duplicadas, revisar usuarios, perfiles,
   efectos de feed y referencias lógicas. Crear la unicidad sólo en una fase
   autorizada y verificar el índice válido antes de reconsiderar apply TMDb.

## Revisión de las protecciones existentes

El snapshot compartido acepta tablas adicionales declaradas por código. El
guard de prefijo SELECT y nombre citado es una barrera contra SQL accidental,
con la transacción READ ONLY verificada por el servidor como protección de
integridad. El guard de texto no valida el árbol SQL ni sirve para SQL arbitrario.
El dry-run de importación amplía el ámbito al modelo de usuario y consulta
sólo username e ID. Este plan agrega tablas de dependencias y catálogos
PostgreSQL con consultas fijas o parametrizadas. Se conserva el comportamiento
existente en `diagnose_local_movie_duplicates.py`.

`.gitattributes` fija LF para dos módulos cuyas huellas de bytes forman parte
del manifiesto de preparación. Ayuda a reproducir el piloto en Windows. Los
otros módulos de reglas siguen sujetos a sus propios bytes de checkout, por
lo que esto no garantiza portabilidad de manifiestos entre plataformas.
Se conserva la configuración existente.

```powershell
.\venv\Scripts\python.exe -B scripts\test_local_movie_duplicates.py
```
