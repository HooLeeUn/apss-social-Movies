# Normalizador TMDb — fase 3

## Alcance e inspección

`core/tmdb_normalizer.py` es Python puro: no importa Django, no consulta servicios,
no escribe archivos y no muta entradas. Procesa un registro por llamada. No incluye
enriquecedor, importador, migraciones ni integración automática con procesos actuales.

Se revisaron `Movie`, serializadores de detalle, `tmdb.py`, `tmdb_catalog.py`,
`tmdb_credits.py`, comandos `enrich_movies_tmdb` y `enrich_catalog_csv`, y usos de
imágenes/sinopsis en el frontend (solo lectura). El enriquecimiento anterior pide
detalles EN/ES; los créditos usan `/movie/{id}/credits` o `/tv/{id}/credits` y
pueden venir anexados a detalles. El frontend usa directamente `movie.image` y
recibe ambas sinopsis. No se ha cambiado ninguno de esos componentes.

Campos reales de Movie:

| Campo | Convención |
| --- | --- |
| type | `movie` / `series`; TMDb llama `tv` al segundo |
| tmdb_id | PositiveIntegerField nullable, índice no único |
| title_english | CharField(255), obligatorio |
| title_spanish | CharField(255), nullable |
| genre / genre_key | CharField(100); texto con comas / clave derivada al guardar |
| release_year | PositiveIntegerField nullable |
| director | CharField(255), nullable, nombres con comas |
| cast_members | TextField nullable, sin max_length, nombres con comas |
| image | URLField nullable, límite implícito 200 |
| synopsis / synopsis_es | TextField EN / ES, segundo nullable |
| external_rating | DecimalField(3,1), nullable, escala 0–10 |
| external_votes | PositiveIntegerField, default 0 |

Movie también tiene `author`, campos derivados de búsqueda, seguimiento de consultas,
tráilers y timestamps. Todos se excluyen del normalizador, junto con el ID interno
y las relaciones/calificaciones sociales. `genre_key` lo deriva el modelo; no se
prepara aquí. No se modifica el algoritmo híbrido ni se calculan promedios.

## Géneros: reglas históricas aprobadas

La primera inspección no encontró la equivalencia en el repositorio porque se
aplicaba mediante SQL en otro computador. El usuario recuperó las reglas históricas
y las aprobó expresamente en la solicitud de continuación de fase 3. Esas reglas
reemplazan la propuesta inicial y están activadas por defecto. La política final
conserva todas las categorías válidas, sin límite de cantidad ni prioridades.

Se leyó, sin alterar, el informe existente
`C:/dev/docs/RecCool diagnostics/fase2A-local-completa/duplicate_records.csv`.
Sus categorías son `Action` (233 apariciones), `Animation` (34), `Comedy` (227),
`Documentary` (283), `Drama` (752), `Horror` (222), `Musical` (41), `Sci-Fi` (81).
Es evidencia del formato del catálogo exportado en fase 2A, no una consulta actual
ni un inventario completo de PostgreSQL. Se valida exclusivamente ese vocabulario.

Películas (`MOVIE_GENRES`):

| ID TMDb | Género TMDb | Equivalencia implementada |
| --- | --- | --- |
| 28 | Action | Action |
| 12 | Adventure | Action |
| 16 | Animation | Animation |
| 35 | Comedy | Comedy |
| 80 | Crime | Drama, Action |
| 99 | Documentary | Documentary |
| 18 | Drama | Drama |
| 10751 | Family | Drama, Comedy |
| 14 | Fantasy | Animation, Sci-Fi |
| 36 | History | Drama, Documentary |
| 27 | Horror | Horror |
| 10402 | Music | Musical |
| 9648 | Mystery | Drama |
| 10749 | Romance | Drama |
| 878 | Science Fiction | Sci-Fi |
| 53 | Thriller | Drama, Horror |
| 10770 | TV Movie | Ignorar; Drama solo si es el único ID distinto |
| 10752 | War | Action |
| 37 | Western | Action |

Series (`SERIES_GENRES`):

| ID TMDb | Género TMDb | Equivalencia implementada |
| --- | --- | --- |
| 10759 | Action & Adventure | Action |
| 16 | Animation | Animation |
| 35 | Comedy | Comedy |
| 80 | Crime | Drama, Action |
| 99 | Documentary | Documentary |
| 18 | Drama | Drama |
| 10751 | Family | Drama, Comedy |
| 10762 | Kids | Action, Animation, Comedy |
| 9648 | Mystery | Drama |
| 10765 | Sci-Fi & Fantasy | Animation, Sci-Fi |
| 10766 | Soap | Drama |
| 10768 | War & Politics | Drama, Action |
| 37 | Western | Action |
| 10763 | News | Ignorar; excluir si solo hay News/Talk |
| 10767 | Talk | Ignorar; excluir si solo hay News/Talk |
| 10764 | Reality | Excluir siempre, incluso con otros géneros |

Referencias de los catálogos separados en la documentación oficial TMDb:
[Movie List](https://developer.themoviedb.org/reference/genre-movie-list) y
[TV List](https://developer.themoviedb.org/reference/genre-tv-list).
Se consultó documentación web, sin ejecutar endpoints de la API ni usar credenciales.
Las equivalencias a RecCool proceden de la aprobación del usuario, no de TMDb.

La configuración opcional `genre_mapping` reemplaza las equivalencias predeterminadas
(no se fusiona) y conserva las exclusiones obligatorias. Tiene claves `(type, genre_id)`
y valores string o lista/tupla de categorías internas. `None` activa las históricas;
`{}` no asigna equivalencias. Es una capacidad avanzada para reglas explícitamente
aprobadas; no es necesaria para el flujo normal. TV Movie único mantiene el fallback
histórico incluso con configuración personalizada.

Se aceptan `genres: [{id, name}]` de detalles y
`genre_ids: [id]` de resultados de búsqueda; si existen ambos se prefiere `genres`.
Se ignoran nombres traducidos. La salida elimina categorías repetidas y se serializa
en orden lexicográfico estable, separadas por `, `; el orden de presentación no
representa prioridad de selección. Cada ID no asignado produce `unmapped_genre`
con `genre_id`. Desconocido no significa excluido ni error de identidad. News/Talk
con ID desconocido no se considera una combinación exclusivamente News/Talk.
TV Movie con desconocido no genera automáticamente Drama. Datos malformados se
mantienen incompletos; no se infieren exclusiones por el nombre traducido.

Se conservan **todas las categorías internas válidas** generadas por las equivalencias,
sin máximo de tres, prioridades ni protección especial de Sci-Fi. Se deduplican,
se ordenan alfabéticamente y se unen con `, `. Fantasy y Sci-Fi & Fantasy siguen
aportando Animation y Sci-Fi; no se descarta ninguna categoría.

`genre_candidates` conserva todas las categorías. Por compatibilidad,
`genre_selection` permanece null y `requires_review` permanece False; se retira
`scifi_protected`. No hay selección ni categorías descartadas. El registro puede
quedar `accepted` y el género completo entrar en `update_data` si no hay otros
problemas. Desconocidos conservan estado `partial` y excluyen `genre` de actualizaciones.

### Compatibilidad con claves, filtros y búsqueda

Las ocho categorías ocupan como máximo **70 caracteres** en `genre`:
`Action, Animation, Comedy, Documentary, Drama, Horror, Musical, Sci-Fi`.
Cabe en `Movie.genre`, CharField(100), sin una restricción de cantidad de categorías.
`build_genre_key` separa por comas, deduplica, ordena y une con `|`, sin límite de
cantidad; la clave completa ocupa **63 caracteres**, dentro de CharField(100) de
`genre_key` y del campo de preferencias de géneros. `Movie.save` deriva esa clave
del texto completo y `core/services.py` usa la combinación completa para preferencias.

Se ejecutaron offline las definiciones puras existentes `build_genre_key` y
`_genre_key_matches`, extraídas mediante AST sin cargar Django: clave completa
correcta y coincidencia de las ocho categorías. No se conectó a PostgreSQL.

Los filtros `apply_feed_genre_filters` y `_genre_lookup_query` comprueban tokens
al inicio, final o interior de la clave; el primero también acepta delimitadores
de comas como respaldo. `_genre_key_matches` comprueba tokens entre `|`.
Las rutas de búsqueda revisadas usan el campo `genre` completo (incluidos
`icontains`, búsqueda ponderada e índice trigram), sin recorte de categorías.
La revisión de consultas fue estática; no se probaron consultas contra PostgreSQL.

`parse_feed_genre_filters` mantiene `normalized[:3]`: limita las opciones elegidas
por el usuario, no las categorías almacenadas. Ese límite independiente y el
frontend permanecen intactos. No se encontraron incompatibilidades demostradas;
no se modificaron modelos, filtros, búsqueda ni las funciones de clave existentes.

## Contrato

```python
normalize_tmdb_metadata(
    production_type, tmdb_id, details, *, language='en-US',
    english=None, spanish=None, credits=None, genre_mapping=None,
)
```

`details`, `english` y `spanish` son diccionarios de una producción, no envoltorios
`results`. Las respuestas localizadas son opcionales; el llamador garantiza su
idioma. La respuesta base pertenece al idioma `language`. Cada respuesta debe tener
el mismo `id` entero positivo; se exige al menos un título utilizable. Se valida
`media_type` cuando existe y se detectan los campos exclusivos del tipo contrario.
Los detalles TMDb no siempre declaran tipo: sin señal contradictoria se confía en
el tipo de endpoint que proporciona el llamador. No es posible demostrarlo solo
con el ID numérico. Identidad lógica: `(type, tmdb_id)`.

Salida JSON-compatible:

| Clave | Contenido |
| --- | --- |
| data | Solo metadatos permitidos, valores normalizados o null |
| valid | False si existe algún error bloqueante |
| status | `accepted`, `excluded`, `incomplete` o `invalid` |
| exclusion_reasons | `reality` o `news_talk_only`, sin errores de identidad |
| requires_review | False; se conserva por compatibilidad |
| genre_candidates | Todas las categorías internas deduplicadas y ordenadas |
| genre_selection | null; se conserva por compatibilidad, sin selección de categorías |
| errors | Objetos `{field, code}` |
| warnings | Objetos `{field, code}`, más contexto cuando corresponde |
| missing_fields | Campos de salida sin entrada disponible |
| field_states | `valid`, `missing`, `empty`, `invalid` o `partial` |
| sources | Origen de cada valor; documenta fallback de títulos |
| creators | Nombres de creadores, auxiliar, no campo de Movie |
| director_entries | Nombres y roles `Director` o `Creator` usados para Movie.director |
| update_data | Propuesta conservadora de cambios; nunca una escritura |

Con identidad inválida o entrada principal no estructurada se retorna temprano;
no se prometen todas las claves de `data`. Los consumidores deben comprobar `status`
y `valid`. `valid` mide validación, no elegibilidad para importar.
Un registro inválido puede tener datos parciales para diagnóstico, pero siempre
carece de `update_data` utilizable.

Precedencia de estados: errores → `invalid`; exclusión funcional → `excluded`;
advertencias o campos ausentes/vacíos/parciales/inválidos → `incomplete`; resto →
`accepted`. Un excluido puede ser válido; `update_data` queda vacío. Se conservan
motivos de exclusión aunque otro error dé precedencia a `invalid`. Un incompleto
puede tener cambios seguros en `update_data`; nunca el género parcial. Campos
opcionales vacíos también producen `incomplete`: no equivalen a identidad inválida.

## Políticas de campos

Títulos: se prefiere título localizado del idioma solicitado. Si falta, se usa
título original (base, EN, ES, en ese orden); después, cualquier título localizado
disponible, en ese mismo orden. No se supone que el original sea inglés. El origen
queda registrado como `en`, `es`, `details.original`, etc., con `title_fallback`.
Unicode se preserva; solo se quitan espacios exteriores. No se truncan títulos:
superar 255 caracteres bloquea aceptación. No se inventan traducciones.

Sinopsis: `synopsis` es EN y `synopsis_es` es ES. No se copia automáticamente un
idioma en el otro. Un vacío localizado produce null con estado `empty`; si la
respuesta/campo no existe se usa `missing`. Si TMDb entrega el mismo texto en
ambos idiomas se conserva: el normalizador no puede determinar si es traducción.

Créditos: `credits` explícito prevalece sobre `details.credits`. Se comprueba su ID
si está presente. Se prioriza `crew.job == 'Director'`, con nombres únicos en orden
de entrada. Solo para series, si no hay directores utilizables se emplea `created_by`.
Se prepara `Movie.director` como texto con comas y `director_entries` conserva el
rol `Creator` del respaldo o `Director` de los créditos. `sources.director` distingue
`details.created_by` de `credits`; `creators` conserva los creadores incluso cuando
se priorizan directores. Sin ambos se devuelve null y lista vacía. Se comprobó
la lógica equivalente en `tmdb_credits.build_director_entries`, sin modificarlo.
No se realizan consultas a temporadas ni se reproduce el reparto por
temporadas del endpoint dinámico del frontend.

Reparto: orden TMDb ascendente; orden ausente/inválido al final, empates mantienen
entrada. Se deduplica por ID y por nombre casefold sin quitar acentos. Se ignoran
nombres vacíos. Se unen nombres con comas, como hace el contrato local; nunca se
almacena una representación Python. Se conserva todo el reparto suministrado:
TextField no tiene límite. Director superior a 255 caracteres genera error.

Póster: solo ruta relativa `/archivo.jpg` (también jpeg/png/webp), convertida a
`https://image.tmdb.org/t/p/w500/archivo.jpg`, consistente con el enriquecedor actual.
Rutas incompatibles producen advertencia y null; no se descargan imágenes.

Fecha: película `release_date`, serie `first_air_date`; solo fecha ISO completa
y válida. Ausencia/vacío produce null; fecha incompleta o inválida genera advertencia,
sin inventar año. Año compatible con PositiveIntegerField.

Rating: int/float/Decimal finito en 0–10, sin aceptar bool ni cadenas numéricas.
Se redondea HALF_UP a una decimal y serializa como cadena decimal, compatible con
DecimalField y JSON. No se divide por votos. Votos: entero 0–2147483647, nunca bool.
Ausencia se mantiene null aunque el modelo tenga default 0: un futuro importador
deberá aplicar defaults explícitamente. Rating/votos inválidos son bloqueantes.
Par incompleto o rating positivo con cero votos produce advertencia y excluye ambos
de `update_data`. Cero votos con rating cero sí es válido.

## Futuras altas y actualizaciones

`data` representa una candidatura para alta, no kwargs listos para crear Movie.
El importador futuro deberá revisar aceptación, defaults, campos nullables y reglas
aprobadas. `update_data` contiene solo campos válidos, no vacíos, sin identidad,
sin géneros parciales ni títulos obtenidos por fallback. Vacío explícito nunca
significa borrar. Campos ausentes, inválidos y parciales tampoco reemplazan datos
existentes. El llamador puede ofrecer un modo de borrado autorizado por separado,
pero no existe en esta fase. Ratings/votos solo se renuevan como par consistente.

Estados describen el valor final preparado; `sources` permite distinguir un título
con fallback de una traducción disponible. No se compara con registros existentes.
No hay resolución de duplicados ni cambios a calificaciones propias.

## Ejemplos sintéticos ejecutables

```python
from core.tmdb_normalizer import normalize_tmdb_metadata

movie = normalize_tmdb_metadata('movie', 12, {
    'id': 12, 'title': 'A film', 'original_title': '原作',
    'release_date': '2020-02-29', 'vote_average': 8.26, 'vote_count': 123,
    'poster_path': '/poster.jpg', 'genres': [{'id': 28}],
}, spanish={'id': 12, 'title': 'Una película', 'overview': 'Sinopsis española'})
# valid=True; type=movie, release_year=2020, external_rating='8.3',
# title_english='A film', title_spanish='Una película'; género 'Action',
# status='incomplete' por campos ausentes; synopsis EN ausente.

series = normalize_tmdb_metadata('series', 34, {
    'id': 34, 'name': 'A series', 'first_air_date': '2021-01-02',
    'created_by': [{'id': 1, 'name': 'Ana'}],
})
# valid=True; type=series, release_year=2021, creators=['Ana']; director='Ana'.
# title_spanish='A series' por fallback, external_rating/votes null.
# director_entries=[{'name': 'Ana', 'job': 'Creator'}]; warning title_fallback.

multi_movie = normalize_tmdb_metadata('movie', 50, {
    'id': 50, 'title': 'Crime and family', 'genre_ids': [80, 10751],
})
# genre='Action, Comedy, Drama', requires_review=False.

multi_series = normalize_tmdb_metadata('series', 60, {
    'id': 60, 'name': 'Kids fantasy', 'genre_ids': [10762, 10765],
})
# genre='Action, Animation, Comedy, Sci-Fi'; requires_review=False.
# genre_candidates=['Action', 'Animation', 'Comedy', 'Sci-Fi']; genre_selection=None.
# status='incomplete' por otros campos ausentes; 'genre' completo entra en update_data.

fantasy_movie = normalize_tmdb_metadata('movie', 61, {
    'id': 61, 'title': 'Fantasy drama comedy', 'genre_ids': [14, 18, 35],
})
# genre='Animation, Comedy, Drama, Sci-Fi'; ninguna categoría descartada.

crime_series = normalize_tmdb_metadata('series', 62, {
    'id': 62, 'name': 'Crime family', 'genre_ids': [80, 10751],
})
# genre='Action, Comedy, Drama'.

direct_scifi = normalize_tmdb_metadata('movie', 63, {
    'id': 63, 'title': 'Direct science fiction', 'genre_ids': [878, 28, 18, 35],
})
# genre='Action, Comedy, Drama, Sci-Fi'; ninguna categoría descartada.

excluded = normalize_tmdb_metadata('series', 70, {
    'id': 70, 'name': 'Reality drama', 'genre_ids': [10764, 18],
})
# valid=True, status='excluded', exclusion_reasons=['reality'], update_data={}.
```

Un enriquecedor futuro obtendrá respuestas fuera del normalizador, llamará a esta
función por registro, comprobará `status`/`valid` y guardará el resultado incrementalmente
con `json.dumps(result, ensure_ascii=False, allow_nan=False) + '\n'` en un stream
GZIP. No necesita cargar el catálogo en memoria. No se implementa ese flujo aún.

## Verificación

`python -m unittest core.test_tmdb_normalizer -v`: 59 pruebas offline.
Se conservan las pruebas existentes, adaptando las expectativas al conjunto completo,
y se añaden casos de uno, tres, cuatro, cinco y ocho géneros, accepted, actualizaciones,
permutaciones y ausencia de efectos secundarios. Cada equivalencia tiene un subcaso
con expectativa independiente.
Se comprueban todas las combinaciones de tres IDs de cada catálogo (1.529 en total)
para verificar que todas las categorías se conservan y la longitud es compatible.
También se verifican los 256 subconjuntos de las ocho categorías internas:
longitud máxima 70, invariancia al invertir entradas y serialización JSON estricta.
Cubren película/serie, idiomas/fallback, todas las categorías configurables,
identidad, tipo, límites, créditos, duplicados, sinopsis, póster, fechas, ratings,
votos, JSON estricto, Unicode, determinismo e inmutabilidad. Un bloqueo de socket
en la prueba de efectos secundarios impide red; no hay configuración ni imports
de Django y las pruebas no necesitan PostgreSQL o Render.

La política definitiva elimina el límite de tres y las prioridades anteriores.
Kids + Sci-Fi & Fantasy produce `Action, Animation, Comedy, Sci-Fi`.
No hay decisiones funcionales pendientes en esta fase.
Las equivalencias, exclusiones y el respaldo de creadores permanecen aprobados.
Mantener detenido el enriquecedor/importador
hasta revisión local y autorización; no se ha creado PR ni integración automática.
