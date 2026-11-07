# Fase 2A: diagnóstico local de duplicados de Movie

Implementación exclusivamente diagnóstica sobre `staging`. No elimina, fusiona,
actualiza ni migra registros. No utiliza TMDb, archivos de exportación ni tablas
sociales para puntuar candidatos. No cambia `compare_tmdb_exports`; reutiliza su
validador `positive` y el patrón de transacción explícitamente `READ ONLY`.

## Revisión inicial del modelo

`Movie.id` es la clave primaria interna (`BigAutoField` por configuración de la
aplicación). `tmdb_id` es `PositiveIntegerField(null=True, blank=True,
db_index=True)`; su rango PostgreSQL permite cero, pero el diagnóstico solo
considera identificadores positivos. `type` es `CharField(max_length=10)`, admite
nulos y vacíos, con valores reales `movie` y `series`.

No hay `UniqueConstraint` ni `unique_together` para `(type, tmdb_id)`. El índice
individual TMDb no impide duplicados. Existen índices de votos/año/id,
tipo/año/id, género/tipo/año/id, título/año/id, búsqueda trigram y vectorial;
ninguno establece esa unicidad. No se cambió el modelo ni las migraciones.

Los metadatos reales incluyen títulos inglés/español, año, director, género,
reparto, sinopsis/española, imagen, `external_rating` decimal, votos enteros,
trailers, fechas y estado de consulta TMDb. **Movie no tiene campo IMDb**;
`StreamingProviderLink` sí tiene uno, pero pertenece a otra tabla y no se
consulta. El selector comprueba los campos disponibles antes de proyectarlos.

`technical_dependencies.json` describe todos los campos, tipos, valores
predeterminados estáticos, índices, restricciones y relaciones del registro de
modelos Django. No es una inspección del esquema físico desplegado: restricciones
manuales, disparadores o divergencias respecto al modelo requieren revisión
separada en una futura fase autorizada.

## Ejecución preparada para autorización posterior

Desde una consola PowerShell nueva, revisar que el alias elegido apunte al
PostgreSQL local autorizado. No se deduce el destino por el nombre `default`.
`config/settings.py` admite `DATABASE_URL` y `.env`; la herramienta no modifica
ninguna configuración para forzar un destino.

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe manage.py diagnose_local_movie_duplicates `
  --database default `
  --output-dir 'C:\dev\docs\RecCool diagnostics\fase2A-local' `
  --sample-size 20
```

Para analizar un máximo de diez grupos, elegir otra carpeta y agregar
`--limit-groups 10`. El conteo global de grupos y filas adicionales sigue siendo
completo; solo se limita la recuperación y evaluación de metadatos. El orden
es estable: `type`, `tmdb_id`, `Movie.id`.

`--sample-size 0` omite ejemplos del resumen; no limita los CSV. La carpeta
debe ser nueva o vacía y estar fuera de los repositorios. No se sobrescriben
informes anteriores. Un fallo puede dejar CSV parciales: solo
`duplicate_summary.json` con `status="complete"` señala una ejecución terminada.

Durante la implementación inicial no se consultó PostgreSQL. En la ejecución
posterior expresamente autorizada se diagnosticó exclusivamente el PostgreSQL
local; los resultados verificados se detallan al final de este documento.

## Seguridad de destino y lectura

Antes de abrir una conexión se exige:

- Motor PostgreSQL, `HOST` literal loopback o `localhost`, nombre de base simple
  y puerto explícito entre 1 y 65535. Se rechazan destinos vacíos, sockets Unix,
  redes privadas, nombres remotos —incluido Render— y cadenas de conexión.
- `localhost` debe resolver exclusivamente a direcciones loopback.
- Sin sobrescrituras de destino en `OPTIONS`, pools, `PGHOSTADDR`, `PGSERVICE`
  ni `PGSERVICEFILE`. Host, puerto y base se pasan explícitos a libpq.
- Una conexión todavía sin abrir y sin transacción existente. Se requiere un
  proceso administrativo nuevo; no se reutiliza una sesión previamente abierta.

Se inicia una transacción con:

```sql
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
```

Antes de consultar Movie, se verifica `host(inet_server_addr())` loopback y
`current_setting('transaction_read_only') = 'on'`. Cualquier fallo aborta el
diagnóstico. Durante las consultas del catálogo, un `execute_wrapper` acepta
solo los SELECT generados para Movie y rechaza escrituras y consultas ajenas.
Este filtro es una defensa adicional para las consultas fijas del comando,
no un analizador SQL general. La conexión se cierra al salir incluso si falla
la lectura. `atomic()` delimita la transacción; **no es la protección de lectura
por sí mismo**. No hay llamadas a `save`, `delete`, `update` ni importación.

La comprobación loopback acredita ubicación de red, no identidad criptográfica
ni autorización empresarial del servidor. No puede garantizar que un proxy,
túnel, redirección de puertos o servidor local malicioso no intermedie una base
remota. No configurar túneles hacia Render; utilizar el puerto del PostgreSQL
local conocido. Un servidor remoto normal tras túnel será rechazado por la
verificación de su dirección, pero esa verificación ocurre después de abrir la
sesión. La herramienta no promete detectar infraestructura oculta antes de
conectar. Un usuario local con permisos solo SELECT es una defensa adicional.

La inicialización habitual de una conexión Django ocurre antes de establecer la
transacción de diagnóstico. La protección no cubre código de terceros durante
arranque, otras conexiones ni comandos externos. PostgreSQL READ ONLY tiene
excepciones para ciertas operaciones temporales; el diagnóstico no las utiliza.

## Estrategia y consultas

En una misma instantánea consistente:

1. Una agregación sobre Movie cuenta filas totales, películas, series y claves
   válidas. Nulos, ceros y tipos ajenos quedan excluidos de los grupos y se
   contabilizan en `invalid_key_rows_excluded`.
2. Una consulta `GROUP BY type, tmdb_id HAVING COUNT(id) > 1` identifica primero
   los grupos. Se recorre con `iterator`, sin cargar todos los modelos.
3. Por cada lote de hasta 100 grupos seleccionados, un SELECT con condiciones
   exactas por pareja recupera exclusivamente campos de esos grupos mediante
   `values(...).iterator()`. No se confunden películas y series con igual ID.
4. Se analiza un grupo cada vez; se escriben sus resultados incrementalmente.
   La inspección técnica de modelos no ejecuta SQL.

Si se analizan G grupos, hay **3 + ceil(G/100) SELECT lógicos**: uno de verificación
de sesión, uno de totales, uno de agrupación y los lotes de metadatos. Con cero
grupos hay tres SELECT. Se agregan `SET TRANSACTION` y el control transaccional;
el driver puede emitir instrucciones de inicialización/cursor adicionales.
No hay consultas por producción ni consultas a relaciones sociales.

Memoria proporcional al mayor grupo, un lote de 100 claves, los bloques del
cursor y las muestras. Los textos completos de un grupo muy grande pueden
aumentarla. La instantánea permanece abierta mientras se analizan/escriben los
grupos: una ejecución prolongada puede retener versiones MVCC; usar solo local,
vigilar tiempo/disco y evitar ejecuciones simultáneas contra la misma carpeta.

## Recomendación reproducible

Cada campo conserva su valor original y un estado: `null`, `empty` (incluye
espacios), `default` o `present`. `default_matches` indica además coincidencias
con el predeterminado aunque este sea vacío, como `synopsis=""`. Esto describe
el valor actual: no permite saber si fue asignado explícitamente o por defecto.

Se validan año entre 1800 y 2200 (heurística fija), póster HTTP/HTTPS con host,
rating finito mayor que cero y hasta diez y votos positivos. Rating/votos con
solo uno disponible forman una pareja incompleta. No se valida la veracidad
externa del contenido ni la disponibilidad de las URLs.

Orden de preferencia:

1. Menor número de incoherencias detectadas.
2. Mayor completitud ponderada de valores utilizables.
3. Menor `Movie.id` para desempatar de forma estable.

Pesos: título inglés y año, 3 cada uno; título español, director, género,
sinopsis, sinopsis española y póster, 2 cada uno; reparto, rating, votos,
IMDb si existiera y cada trailer, 1 cada uno. Los nulos, vacíos, valores por
defecto y valores no utilizables no puntúan. **No puntúan la magnitud del rating,
los votos ni la fecha más reciente**: solo su disponibilidad coherente.

Se comparan todos los metadatos, normalizando mayúsculas y espacios para texto
y orden de géneros separados por comas. Las diferencias de fechas se conservan
en los registros pero no determinan calidad o conflictos. Se exige revisión
manual si hay valores presentes contradictorios, incoherencias en cualquier
miembro o datos complementarios que el candidato no tiene. El ID propuesto en
estos grupos es provisional, nunca una autorización de borrado.

## Relaciones técnicas y futura depuración

Dependencias directas actuales de Movie, todas con `on_delete=CASCADE` en Django:

- `StreamingProviderLink`
- `MovieRating`
- `MovieListItem`
- `MovieRecommendationItem`
- `MovieRecommendationHistory`
- `ProfileFavoriteMovie`
- `WeeklyRecommendationItem`
- `UserDailyFeedCandidate`
- `Comment`
- `VideoComment`
- `UserNotification`

Las cascadas de comentarios/videos alcanzan `DirectedCommentRecipient`,
`CommentReaction`, `VideoCommentReaction` y notificaciones. Se incluyen sus
restricciones, por ejemplo unicidad usuario/película, snapshot/película y
pool/película. Una reasignación futura podría violarlas. La relación saliente
`Movie.author → User` tiene `SET_NULL` al eliminar al usuario: eliminar una
película no elimina a su autor.

Riesgos adicionales identificados:

- Django aplica CASCADE mediante su recolector; SQL directo puede ser rechazado
  por las claves foráneas y no reproduce necesariamente la cascada ORM.
- `MovieRating.post_delete` modifica preferencias y cachés. Las señales de
  recomendaciones tienen lógica dependiente del origen del borrado.
- `VideoComment.delete()` borra archivos, pero una cascada de queryset/recolector
  no invoca automáticamente ese método en cada hijo: pueden quedar archivos.
- `ContentReport.content_type/object_id` referencia comentarios/videos sin FK:
  sus referencias lógicas pueden quedar colgantes tras las cascadas.
- Cachés, perfiles de gusto y contadores de snapshots pueden quedar desfasados
  aun si todas las claves foráneas permanecen válidas.
- Los proveedores pueden combinar FK con identificadores independientes TMDb/IMDb.

Estrategia propuesta para una fase futura: validar primero los grupos manuales,
aprobar un mapa explícito de IDs conservados/descartados, revisar los vínculos
técnicos y sus restricciones, respaldar la base local y diseñar una operación
transaccional que trate cascadas, señales, archivos y resúmenes. Las interacciones
locales pueden descartarse conforme al alcance autorizado, pero no deben
reutilizarse esos supuestos en Render. Probar después integridad/referencias y
recalcular los derivados que procedan. Una restricción única futura podría
prevenir recurrencia tras depurar y decidir el tratamiento de claves inválidas.
**Ninguna de esas operaciones de limpieza está implementada en esta fase.**

## Informes UTF-8

| Archivo | Contenido |
| --- | --- |
| `duplicate_summary.json` | Totales globales, grupos, filas adicionales, distribución por tamaño, desglose por tipo, grupos analizados, revisiones manuales y muestras acotadas. |
| `duplicate_groups.csv` | Una fila por grupo analizado, IDs internos, recomendación, motivo, revisión manual, diferencias y campos complementarios. |
| `duplicate_records.csv` | Metadatos originales por producción, estados/defaults, incoherencias, puntuación y señal de candidato. |
| `duplicate_conflicts.csv` | Grupos de revisión manual y valores contradictorios/incoherencias/datos complementarios. |
| `technical_dependencies.json` | Modelo, índices, restricciones, grafo de dependencias y riesgos técnicos. |

Los CSV usan UTF-8 BOM. Listas y mapas se serializan como JSON dentro de celdas.
Los CSV protegen textos iniciales que podrían ejecutar fórmulas en una hoja de
cálculo mediante un apóstrofo. Los estados distinguen nulos y vacíos aunque sus
celdas de valor aparezcan vacías. No se exportan autores, usuarios, correos,
contraseñas ni conexiones. Las sinopsis/títulos proceden del catálogo.

`manual_review_required_groups` cuenta únicamente grupos analizados; se indica
con `manual_review_scope`. Con límite, `partial_analysis=true` advierte que no es
una cifra de revisiones global. La distribución por tamaño y los contadores de
duplicados sí son globales. Filas adicionales significa suma de `(tamaño-1)`.

## Pruebas reproducibles

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe scripts\test_local_movie_duplicates.py
```

El ejecutor no carga `.env` ni `config.settings`. Permite únicamente SQLite en
memoria con filas sintéticas; no ejecuta migraciones ni contacta PostgreSQL.
Las consultas ORM reales se verifican sobre esa tabla aislada, con
`PRAGMA query_only=ON` durante el diagnóstico, comparando filas antes/después y
comprobando que UPDATE falla. Se prueban pares con distintos tipos, grupos de
dos/tres miembros, completitud, conflictos, nulos/defaults, empates, límites,
ausencia de duplicados, informes, lotes de más de cien grupos y rechazo seguro.

La frontera PostgreSQL se verifica mediante dobles: validación de destino antes
de conectar, configuración READ ONLY antes de las consultas, confirmación del
servidor, bloqueo de SQL inesperado y cierre ante fallo. **Esto no sustituye una
prueba de integración con PostgreSQL real** que intente una escritura sobre una
base local desechable. La ejecución autorizada posterior confirmó el estado
READ ONLY del servidor y las huellas antes/después sobre el catálogo real local,
sin intentar ninguna escritura real.

## Corrección mínima y ejecución local autorizada

La consulta original `inet_server_addr()::text` devolvió exactamente la cadena
Python `str` `127.0.0.1/32`. `transaction_read_only` devolvió `str` `on`; tanto
servidor como cliente usaban UTF8. `is_loopback()` rechazaba la máscara porque
usa `ipaddress.ip_address()`, que espera una dirección sin prefijo. El orden de
validación era correcto y READ ONLY estaba activo: el problema era la
representación textual de `inet`, no la transacción ni la codificación.

Se cambió únicamente la expresión SQL por `host(inet_server_addr())`, que en
ese mismo servidor devolvió `str` `127.0.0.1`. No se flexibilizó `is_loopback()`,
la validación de configuración ni el filtro SQL. Se añadió la prueba de
regresión `test_server_inet_text_netmask_is_removed_by_host_function`: modela
la respuesta con máscara de la consulta antigua y comprueba la versión corregida.
La suite completa pasó: **23 pruebas**.

Destino confirmado: `127.0.0.1:5432`, base `socialapp`, usuario `django_user`.
El proceso Windows `postgres`, PID 6040 durante la comprobación, escuchaba en
el puerto 5432. Todas las sesiones verificaron loopback y READ ONLY. No se
contactó Render. No se modificó el comparador TMDb ni los modelos.

Primero se ejecutaron y verificaron diez grupos en
`C:\dev\docs\RecCool diagnostics\fase2A-local-limitada-10` (4,420 s).
Después se ejecutó el diagnóstico completo en
`C:\dev\docs\RecCool diagnostics\fase2A-local-completa` (4,956 s;
16 SELECT lógicos, incluidos 13 lotes de metadatos).

| Tipo | Filas de catálogo | Grupos duplicados | Filas adicionales |
| --- | ---: | ---: | ---: |
| Películas | 384630 | 542 | 545 |
| Series | 49593 | 753 | 753 |
| Total | 434223 | 1295 | 1298 |

Hay 1292 grupos de dos registros y tres grupos de tres registros; 2593
producciones pertenecen a esos grupos. No se excluyeron claves inválidas.
Se propusieron 1295 IDs a conservar: las recomendaciones completas y sus motivos
están en `duplicate_groups.csv`.

Clasificación del análisis de resultados, sin usar fechas para comparar calidad:

- **0 grupos equivalentes** en todos los campos de metadatos analizados, tras
  normalizar espacios, mayúsculas y orden de géneros; se distinguen nulos,
  vacíos y valores predeterminados.
- **4 grupos complementarios sin contradicciones**: películas TMDb 267300,
  592625, 594231 y 1374835. IDs recomendados: 965488, 684048, 950620 y 47449,
  respectivamente. Dos siguen requiriendo revisión por coherencia.
- **1291 grupos con valores presentes contradictorios**: 538 de películas y
  753 de series. Son discrepancias detectadas por el algoritmo; no se ha
  determinado qué fuente o valor es correcto.
- **177 grupos tienen alguna variación de disponibilidad entre miembros**;
  esta cifra se solapa con las contradicciones. El candidato propuesto contiene
  los campos disponibles de su grupo en todos los casos: ninguno deja un campo
  utilizable exclusivamente en un descartable. Esto no resuelve contradicciones.
- **1293 grupos requieren revisión manual**. Los dos sin incidencias detectadas
  son películas TMDb 592625 → Movie 684048 y 594231 → Movie 950620; ambos
  candidatos aportan director frente a un miembro sin él. Son recomendaciones,
  no una autorización para borrar.

Las discrepancias más frecuentes son reparto (1076 grupos), director (885),
título español (417), año (285), título inglés (180) y género (163). Una
diferencia textual de reparto/director puede corresponder a variantes de
escritura u orden; este diagnóstico conservador no las declara errores
confirmados. La política actual también marca `external_rating=0.0` como no
utilizable: aparecen 782 filas así. Esos ceros pueden ser marcadores de ausencia
de rating, no pruebas de identidad incorrecta, y no se cambió esa política en
esta corrección. Hay ocho filas con rating/votos incompletos.

Se verificaron los cinco informes, sus conteos de grupos, registros y conflictos,
los IDs recomendados y el análisis completo sin truncamiento. Se añadieron dos
archivos de evidencia fuera del repositorio:

- `execution_verification.json`: destino y huellas del catálogo antes/después.
- `result_analysis.json`: clasificación, campos en conflicto y recomendaciones
  para todos los grupos.

La huella se calculó mediante SELECT con cursor incremental sobre
`row_to_json(Movie)` de **todas las columnas**, ordenado por `Movie.id`,
codificado UTF-8 y separado por saltos de línea. Antes y después hubo 434223
filas, con el mismo SHA-256:

```text
bcc6e0ac68d4860043a4280693621e2f8e457688ce0178534306544b7a726c31
```

Esto verifica que no cambió ningún valor persistido de Movie entre ambos
puntos de comprobación; no es una auditoría de modificaciones transitorias por
otros procesos. La herramienta solo hizo SELECTs bajo READ ONLY. No se
eliminaron, fusionaron ni actualizaron registros ni se implementó limpieza.
La siguiente fase debe revisar los grupos manuales, especialmente diferencias
de identidad/título/año, y definir el tratamiento técnico de cascadas, señales,
archivos y referencias lógicas antes de aprobar cualquier eliminación local.
