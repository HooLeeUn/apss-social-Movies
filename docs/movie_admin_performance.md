# MovieAdmin: diagnóstico y validación

Cambios limitados al administrador en la rama `staging`. No requieren migraciones.
No se modifican modelos, fórmulas, endpoints públicos ni configuración de Gunicorn.

## Causa probable

Antes, `MovieAdmin.get_queryset()` aplicaba `with_display_rating()` a cualquier
consulta del administrador, incluido el listado y el autocompletado. En
`MovieQuerySet.with_rating_stats()` esto introduce `COUNT(DISTINCT movie_ratings.id)`
y `AVG(movie_ratings.score)`, con LEFT JOIN y GROUP BY de películas. El listado no
presenta estas métricas. Django cuenta un queryset agrupado mediante una subconsulta
agrupada, antes de obtener la página: limitar a 100 resultados no limita ese trabajo.
Además, por defecto hace un segundo conteo del queryset raíz.

Es coherente con los timeouts del listado y el éxito de una ficha filtrada por PK,
pero no constituye una medición del plan de ejecución de producción.

Otros costes identificados:

- Los filtros automáticos de `genre` y `release_year` consultan valores DISTINCT
  del catálogo para construir sus opciones; el de autor enumera usuarios.
- `created_at` usa opciones temporales fijas: no enumera las fechas existentes.
  Aplicarlo todavía puede requerir un escaneo, pues no tiene índice específico.
- El orden heredado es `-created_at`, sin índice específico; Django añade PK para
  desempatar. Puede requerir ordenar gran parte del catálogo.

## Corrección

- Queryset base sin anotaciones de calificación. `get_object()` valida y busca el
  objeto con Django y después calcula las métricas mediante el mismo método central,
  filtrando por su PK. Esto hace dos consultas acotadas para la ficha.
- Entradas exactas de género, año y autor por ID, sin consultas para enumerar opciones.
  Conservan los parámetros anteriores (`genre__exact`, `release_year__exact`,
  `author__id__exact`), las otras condiciones y la búsqueda al aplicar un filtro.
  Se conserva el filtro temporal. La interfaz cambia de listas a entradas manuales.
- Orden `-pk` exclusivamente en el listado, aprovechando el índice primario existente.
  El orden solicitado por el usuario sigue funcionando. Autocompletado conserva su
  orden anterior por fecha, búsqueda, permisos y paginación de Django.
- `list_select_related = ("author",)` evita consultas por cada autor mostrado.
- `show_full_result_count = False` evita el segundo conteo. **Permanece un COUNT
  exacto del paginador**, ahora sin JOIN de ratings ni GROUP BY. Se preservan totales,
  navegación y selección del administrador, sin aproximaciones ni límites ficticios.
- Se desactivan los conteos opcionales de facetas para evitar agregaciones adicionales.

## Validación reproducible

En PowerShell, con el entorno virtual y PostgreSQL local disponibles:

```powershell
$env:DEBUG = 'True'
venv/Scripts/python.exe manage.py test core.test_movie_admin core.test_rating_display --noinput --keepdb
```

`DEBUG` se establece solo en ese proceso para evitar la redirección HTTPS que
activaba la configuración local al importar settings. No se edita configuración.
Las pruebas usan la base de pruebas de Django, sin modificar el catálogo existente.

Resultado local: **13 pruebas aprobadas**, comprobaciones de sistema sin incidencias
y `git diff --check` sin errores.

Se comprueban respuesta HTTP del listado y ficha, edición mediante POST, búsquedas
por todos los campos anteriores, filtros combinados, parámetros conservados,
entrada inválida, paginación entre dos páginas y autocompletado de Comment y
MovieRating. Se captura SQL del listado y se exige ausencia de consultas a ratings,
GROUP BY y enumeraciones DISTINCT, además de un solo COUNT sin JOIN.
Las pruebas de calificaciones conservan los límites 0, 1, 50, 99, 100 y 101 votos
y las comparaciones con búsquedas públicas y ficha pública.

## Limitaciones y riesgos pendientes

Los fixtures son pequeños (hasta 107 películas para paginación), no comparables a
900.000 producciones. No se midieron tiempos ni EXPLAIN ANALYZE de producción y no
se afirma que el timeout esté resuelto en producción solo con estas pruebas.

COUNT exacto, OFFSET en páginas profundas, búsqueda parcial en múltiples campos,
ordenamientos manuales y filtros sin índices adecuados aún pueden ser costosos.
El autocompletado conserva su COUNT y orden por fecha a propósito. Antes de dar por
cerrado el incidente, medir en staging con un catálogo representativo: listado sin
filtros, búsqueda, filtros y páginas profundas, revisando tiempos y planes SQL.
Si COUNT resulta dominante, evaluar por separado una paginación alternativa con
su cambio explícito de comportamiento; no se introduce aquí para preservar totales
y navegación. Los índices adicionales exigirían evidencia y una migración aparte.
