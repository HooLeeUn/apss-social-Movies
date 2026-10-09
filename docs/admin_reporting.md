# Reportería del Django Admin de ReCCool

## Implementación

La app `reporting` aporta una única entrada REPORTES → Generar reportes en
`/admin/reporting/reportaccess/`. `ReportAccess` es un proxy sin tabla nueva ni
acciones CRUD. El Admin existente conserva sus registros y comportamiento.

`forms.py` valida filtros y periodos; `periods.py` construye límites en
America/Bogota; `demographics.py` calcula edad histórica en PostgreSQL;
`queries.py` define las fuentes reales; `services.py` agrega y suprime resultados;
`privacy.py` aplica el umbral; `exports.py` produce CSV/XLSX del mismo resultado.
`views.py` comprueba permisos en cada URL y pagina a 50 filas. Los recursos JS/CSS
pertenecen exclusivamente al Django Admin; no se modificó el frontend.

El catálogo incluye usuarios, rankings de producciones/géneros/combinaciones,
producciones más recomendadas, cinco métricas directas y cinco sociales, con
consolidados y comparaciones mensuales. Máximo 24 meses por solicitud. No hay PDF
ni endpoints públicos. La tabla muestra totales por métrica/mes y diferencias
entre meses seleccionados en orden cronológico, incluso si no son consecutivos.

## Selección temporal y presentación

Todos los reportes mensuales usan dos controles de selección múltiple: Meses
(Enero–Diciembre) y Años (desde la primera alta real hasta el año actual).
Se genera el producto cartesiano y se ordena cronológicamente: Enero y Febrero
con 2025 y 2026 generan Enero 2025, Febrero 2025, Enero 2026 y Febrero 2026.
La selección debe contener ambas dimensiones. Más de 24 combinaciones produce
un error sin truncamiento. Una combinación que incluya meses futuros se rechaza
completa con un error; el mes actual sí se permite y contiene datos parciales.

Los cuatro reportes de usuarios ofrecen tres modos mutuamente excluyentes:

- Acumulado total: semántica histórica existente, con edad a fecha actual;
  deshabilita meses, años y Desde/Hasta.
- Comparación mensual: ambos checkboxes apagados; una columna por mes/año,
  usuarios cuyo `date_joined` pertenece a ese mes, sin acumular meses previos.
- Personalizado: requiere Desde/Hasta, ambos inclusive en America/Bogota;
  un periodo por cada día con etiqueta `DD/MM/YYYY`, sin acumulado progresivo;
  deshabilita Acumulado total, meses y años.

Todos los reportes de Contenido, Actividad directa y Actividad indirecta/social
ofrecen comparación mensual y Personalizado. Acumulado total sigue siendo
exclusivo de usuarios. En Personalizado, Desde/Hasta están habilitados y Meses/Años
deshabilitados, por lo que no se envían como filtros activos. Al desactivarlo se
invierten estos controles. El servidor rechaza modos o filtros temporales mezclados.

Cada día usa el intervalo semiabierto `[inicio, día siguiente)`, con ambos límites
a las 00:00 en America/Bogota. Desde y Hasta son inclusivos en la interfaz:
01/09/2026 a 09/10/2026 produce 39 periodos diarios, incluyendo ambos extremos.
Se conserva cada día aunque tenga cero eventos. No hay máximo nuevo ni
consolidación automática de rangos largos. Pantalla, metadata, CSV y XLSX consumen
el mismo resultado y las mismas etiquetas diarias; XLSX conserva Reporte y Metodología.
Las consultas conservan sus fuentes, filtros y semánticas históricas existentes.

El servidor rechaza combinaciones ambiguas incluso sin JavaScript. País e
identidad siguen siendo actuales, edad mensual/personalizada corresponde al
alta, inactivos cuentan y pendientes se excluyen. Se conserva el umbral de 10
usuarios en segmentos protegidos y cada celda, también en CSV/XLSX. Si alguna
celda comparada se suprime, diferencia y porcentaje también se suprimen.

Las columnas identifican la métrica: «Dif. Usuarios», «Var. Usuarios %»,
«Dif. Nº calificaciones», «Var. Nº calificaciones %», comentarios, follows,
etc. En consolidados se indican eventos de la métrica de la fila. La diferencia
es actual menos anterior; porcentaje = `(actual - anterior) / anterior * 100`,
frente al periodo seleccionado anterior. Si anterior es cero, la diferencia
se conserva y el porcentaje muestra «No aplica». 1→1 produce 0 y 0 %;
2→0 produce -2 y -100 %. En rankings de ratings la comparación usa el número
de calificaciones; cada mes o día mantiene su promedio aparte. En Personalizado,
las diferencias y variaciones comparan días consecutivos del rango seleccionado.

La población analizada se omite de pantalla y exportaciones cuando coincide
con la única cifra de usuarios registrados. Se conserva cuando aporta contexto
en comparaciones y otros reportes, reservándola cuando permitiría inferir celdas.
La metodología en pantalla aparece exclusivamente dentro del desplegable
«Filtros y semántica del reporte», cerrado por defecto.

## Fuentes y semántica

- Registrados: `auth.User` con `Profile`, incluidos inactivos, por `date_joined`.
  `PendingUserRegistration` queda fuera; `terms_accepted_at` es dato legal.
- Ratings: fila vigente por `MovieRating.updated_at` y score actual. La corrección
  de idempotencia evita nuevos desplazamientos al reenviar el mismo score; no
  restaura timestamps artificialmente modificados antes del despliegue.
- Géneros individuales: expansión de `genre_key` con un lateral SQL y agregación
  en PostgreSQL. Un rating puede contribuir a varios géneros; nunca se suman
  preferencias. Combinaciones: `genre_key` exacto, sin cambiar su normalización.
- Comentarios: `visibility=public`, por `created_at`; dirigidos excluidos.
- Videos: por `created_at`. Ocultos por moderación siguen contando.
- Reacciones: actor `user`, estado vigente por `updated_at`; únicamente comentarios
  públicos. Las reacciones eliminadas o valores anteriores no se reconstruyen.
- Follows: actor `follower`, relaciones existentes por `created_at`. No hay log
  de unfollows y no se creó uno.
- País e identidad son los valores actuales del perfil. No existe historial de
  cambios demográficos. Nacimiento faltante se muestra como Sin dato; edades
  menores de 13 o negativas, como Fuera de rangos definidos. Con filtro de edad
  explícito se excluyen nacimientos faltantes y edades fuera de selección.
- Edad: años cumplidos en fecha local del evento; en acumulados de usuarios,
  fecha actual. Nacidos el 29 de febrero cumplen años el 1 de marzo en años no
  bisiestos, según la comparación de cumpleaños usada y verificada contra SQL.
- Un porcentaje con base cero muestra No aplica. Promedio sin ratings también.

## Histórico de Recomendadas

`core.MovieRecommendationHistory` conserva intervalos con `user`, `movie`,
`started_at` y `ended_at` nullable. Intervalos son una correspondencia directa
con pertenencia mensual y evitan reconstruir el estado leyendo un event log.

Restricciones: un solo intervalo abierto por usuario/producción y fin mayor o
igual al inicio. Índices nuevos únicamente en esta tabla: claves foráneas para
joins, índice parcial único de intervalo abierto, y B-tree de inicio/fin para
altas, retiros y candidatos a solapamiento. No se agregaron índices a tablas
existentes sin mediciones de staging.

Una producción cuenta para un usuario en un mes cuando:

```sql
started_at < inicio_mes_siguiente
AND (ended_at IS NULL OR ended_at >= inicio_mes)
```

Se usa `COUNT(DISTINCT user_id)` por producción y mes. Varias altas/retiros del
mismo usuario en un mes cuentan una sola vez en el ranking, pero todas las altas
y retiros sí cuentan en actividad directa. El fin exactamente en el inicio del
mes cuenta en ese mes, conforme a la comparación inclusiva solicitada. No se
exige que transcurran 24 horas. La edad de intervalos se referencia al primer
instante de solapamiento: `greatest(started_at, inicio_mes)`; cada intervalo puede
calificar al usuario para el segmento y el usuario se deduplica después.

En Personalizado se reutiliza exactamente este solapamiento, sustituyendo mes
por día, con usuarios únicos por producción y día. Una recomendación iniciada
01/09 a las 15:00 y retirada 03/09 a las 10:00 cuenta los días 01, 02 y 03,
pero no el 04. Se conserva también la regla histórica de fin inclusivo: un retiro
exactamente a medianoche cuenta en el día que comienza en ese instante.

`MovieRecommendationItem` sigue representando el estado actual. La API usa una
transacción y bloqueo de la fila del usuario, incluso si no existe item. Las
señales abren/cierra intervalos para creaciones/eliminaciones ORM normales. Una
segunda alta existente es idempotente. El retiro conserva el histórico. Si un
item antiguo se retira antes del backfill, se recupera su `created_at` real y se
cierra el intervalo en ese retiro. Borrar el usuario o la producción elimina su
histórico por CASCADE; no se crean filas nuevas durante esas cascadas.

Las escrituras mediante `bulk_create`, SQL directo o un proceso antiguo no
emiten estas señales: las integraciones futuras deben utilizar el servicio y el
procedimiento de backfill. No mezclar workers antiguos y nuevos en el corte.

### Backfill

`python manage.py backfill_recommendation_history` es idempotente: INSERT SELECT
por usuario, transacción corta por usuario y el mismo bloqueo que usa la API;
`ON CONFLICT DO NOTHING` y ausencia de intervalo abierto evitan duplicados. No
materializa todos los items ni hace una consulta por item. Preserva el
`MovieRecommendationItem.created_at`, no modifica el estado actual y no inventa
retiros. Se puede reejecutar tras una interrupción: usuarios ya procesados quedan
confirmados y la ejecución retoma mediante la misma comprobación idempotente.

Se revisaron modelos, feed social y snapshots disponibles: el feed social deriva
actividades de filas actuales; `WeeklyRecommendationSnapshot/Item` almacena
rankings agregados semanales sin usuario ni retiros. No constituyen un historial
fiable de Mis recomendadas. No se reconstruyen items eliminados antes del nuevo
histórico. Los items activos pueden recuperar su inicio real, pero si alguna vez
se retiraron y reañadieron antes del despliegue no se recupera el intervalo previo.
Desde el corte se registran altas/retiros realizados por el flujo integrado.

## Idempotencia de MovieRating

La API bloquea la fila de User dentro de una transacción para serializar primeras
creaciones, actualizaciones y eliminaciones del mismo usuario. `get_or_create`
crea la fila si falta. Si existe y el score coincide, no se llama a `save`, no
cambia `updated_at` y no se ejecutan señales ni cambios en preferencias.

Si el score cambia, se guarda `score` y `updated_at`; las señales existentes
capturan el score anterior, lo restan de las distribuciones y agregan el nuevo.
No se modificaron los modelos ni el algoritmo de preferencias. El contrato sigue
siendo `{movie, my_rating, created}`. La garantía se aplica al endpoint integrado;
otras futuras escrituras directas de MovieRating deberán respetar esta política.

## Privacidad y exportaciones

Un reporte con filtros demográficos, o agrupación demográfica, requiere 10
usuarios únicos globales y por celda. Para actividad son contribuyentes a esa
métrica/mes; para contenido, contribuyentes a producción/género/mes. Cero usuarios
también es muestra inferior al umbral en reportes protegidos.

En Personalizado el mismo umbral se aplica al segmento y a cada celda diaria:
usuarios únicos, no número de eventos. Si la celda actual o anterior está
suprimida, ambas comparaciones se suprimen, incluso en los días siguientes a una
celda oculta. No se publica población consolidada que permita deducir celdas
diarias protegidas. Estas reglas se comparten entre HTML, CSV y XLSX.

Si la población global es insuficiente, no hay filas ni exportación. En reportes
con población global suficiente se suprimen celdas individuales, sus promedios,
diferencias y porcentajes. Se reserva el total de población para reportes
protegidos con varias celdas, evitando restar un subtotal publicado. Una producción
sin ninguna celda publicable no revela título ni tipo. El orden en contenido
protegido utiliza solo conteos publicables. No hay totales marginales de celdas
suprimidas. Las exportaciones incluyen el mensaje, nunca su valor oculto.

CSV: UTF-8 con BOM. XLSX real: `openpyxl==3.1.5`, verificado con Python 3.13 y
Django 6.0.4; escritura optimizada y archivo temporal que pasa a disco a partir
de 8 MiB. Textos potencialmente interpretables como fórmulas reciben apóstrofo.
Los archivos contienen filtros, periodos, fecha/hora, zona y población cuando
aporta contexto y es publicable. XLSX tiene las hojas «Reporte» (metadatos y
resultados) y «Metodología» (fuentes, semántica y todas las advertencias históricas,
incluidos updated_at, follows, Recomendadas y contenido oculto). CSV contiene
metadatos y dataset sin filas metodológicas o «Limitación».
CSV usa streaming; SQL de contenido usa cursor por
bloques. Una nueva descarga ejecuta el mismo servicio con los mismos filtros;
si los datos cambian entre pantalla y descarga, el resultado puede actualizarse.

## Archivos

Nuevos: app `reporting/` completa (catálogo, formularios, consultas, servicio,
privacidad, permisos, vistas, Admin, exportadores, plantillas, recursos, tests,
comando de grupo y migración); `core/recommendation_history.py`,
`core/management/commands/backfill_recommendation_history.py`,
`core/migrations/0072_movierecommendationhistory.py`, y este documento.
Modificados: `config/settings.py`, `core/models.py`, `core/signals.py`,
`core/views.py`, `requirements.txt`.

## Staging: comandos y orden

Usar Python >=3.12, la configuración habitual de staging y una base PostgreSQL
separada de producción. No copiar credenciales a comandos ni logs. Partiendo de
`apss-social-Movies`, dentro del entorno virtual del proyecto:

```bash
python -m pip install -r requirements.txt
python manage.py check
python manage.py makemigrations --check --dry-run
python manage.py migrate --plan
```

1. Respaldar staging y ensayar con un volumen representativo. Congelar altas y
   retiros de Recomendadas durante el corte y detener workers con código antiguo.
2. Aplicar `core.0072` (tabla de intervalos) y después `reporting.0001` (proxy y
   permisos), usando el orden de dependencias de Django:

```bash
python manage.py migrate --noinput
python manage.py backfill_recommendation_history
python manage.py backfill_recommendation_history
python manage.py setup_report_analysts
python manage.py collectstatic --noinput
python manage.py test reporting core --noinput
```

3. La segunda ejecución del backfill debe informar cero intervalos nuevos, salvo
   nuevas escrituras legítimas. Verificar por SQL que cada item activo tiene un
   intervalo abierto y que `started_at` conserva el `created_at` de los items
   migrados. Confirmar que no se inventaron fechas de cierre.
4. Iniciar todos los workers con código nuevo y reabrir escrituras.
5. Con superusuario, abrir `/admin/` y comprobar REPORTES → Generar reportes.
   Probar acumulado, mes y personalizado de usuarios; comparativas de contenido y
   actividad; casos con 9 y 10 usuarios; grupos de 10 y 1 en la misma tabla;
   CSV y XLSX; ratings 8→8→9; recomendación alta→retiro→alta.
6. Con analista, probar URLs directas y confirmar ausencia de edición de fuentes.
   Descargar XLSX y abrirlo en Excel/LibreOffice. Probar recursos con DEBUG=False.
7. Medir latencias y planes antes de producción. No se ha desplegado este cambio
   ni se ha consultado PostgreSQL de producción desde este trabajo.

### Crear el analista

Después de `migrate`, ejecutar `python manage.py setup_report_analysts`.
El comando configura el grupo Django estándar «Analistas de reportes», visible
en Authentication and Authorization → Groups y asignable en Groups del User.
No agrega campos a User ni modelos administrativos adicionales.
En `/admin/auth/user/add/`, crear usuario
con contraseña segura. Activar Active y Staff status; dejar Superuser status
apagado. Asignar únicamente el grupo Analistas de reportes; no dar permisos CRUD
sobre fuentes. El grupo creado recibe can_view_reports y can_export_reports.
Para un analista solo de lectura, asignar únicamente can_view_reports, mediante
permisos individuales o un grupo distinto, sin pertenecer al grupo exportador.
Probar inicio de sesión desde otro equipo por la URL HTTPS habitual del Admin.

Comprobación alternativa para una cuenta existente, desde `python manage.py shell`:

```python
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
user = get_user_model().objects.get(username="analista")
user.is_active = True
user.is_staff = True
user.is_superuser = False
user.save(update_fields=["is_active", "is_staff", "is_superuser"])
user.groups.add(Group.objects.get(name="Analistas de reportes"))
```

Revisar los otros grupos/permisos de una cuenta reutilizada: asignar el grupo de
reportes no revoca permisos existentes, por diseño.

## Mediciones pendientes y atención antes del merge

- Personalizado amplía el número de consultas/agregaciones y columnas en función
  de los días seleccionados. Los consolidados recorren cada métrica por día y
  contenido genera un fragmento SQL por día. El rango de 39 días se cubre en tests;
  medir latencia y ancho de exportaciones con datos representativos en preview.
  La tabla existente permite desplazamiento horizontal. Rangos muy largos pueden
  exceder la capacidad del worker o las columnas de Excel; no se impuso un límite
  arbitrario ni se consolidan días para evitarlos.

- Medir scans temporales de MovieRating.updated_at, Comment.created_at, fechas de
  reacciones y Follow.created_at. Prioridad a MovieRating.updated_at; no se creó
  ese índice sin medición. Usar EXPLAIN inicialmente y EXPLAIN ANALYZE en staging
  con statement_timeout apropiado. No ejecutarlo sobre producción por defecto.
- Medir COUNT DISTINCT, expansión de géneros, ORDER/GROUP BY, el OR de solapamiento
  y cardinalidad de intervalos. Solo después evaluar índices compuestos o GiST
  sobre rangos si las métricas lo justifican. Para índices grandes existentes,
  usar AddIndexConcurrently y migración atomic=False según patrón del proyecto.
- El bloqueo por usuario serializa escrituras sensibles del mismo usuario, sin
  bloquear todo el catálogo. Medir contención y duración del backfill.
- El cursor SQL necesita la configuración apropiada si se usa pool en modo
  transacción: probar streaming con el pool de staging antes de producción.
- XLSX consume tiempo de worker aunque la memoria sea acotada; exportaciones muy
  grandes pueden necesitar una fase futura de trabajos asíncronos.
- Respaldar el histórico antes de cualquier rollback. No revertir core.0072 en
  producción sin conservar sus datos: revertir esa migración elimina su tabla.
- Revisar los dos flujos modificados y el orden de despliegue; no mezclar versiones
  antiguas/nuevas. No se alteraron feed, frontend, autenticación, moderación ni
  preferencias existentes.

## Validación realizada en este entorno

### Personalizado diario (9 de octubre de 2026)

Cambios locales en `codex/reporting-custom-daily-periods`, sin commit, push,
operaciones remotas, modelos ni migraciones. Personalizado genera un periodo
por día para los 20 reportes. Se añadieron 11 casos Django y una prueba de
JavaScript que verifica exclusión de modos y controles para los 20 reportes.

Validación final con PostgreSQL local, `DEBUG=True` y R2 deshabilitado:

```powershell
$env:DATABASE_URL=' '
$env:DB_HOST='127.0.0.1'
$env:DEBUG='True'
$env:R2_ENDPOINT_URL=' '
python manage.py test reporting core.tests.MovieRatingEndpointTests --noinput --keepdb
python manage.py check
python manage.py makemigrations --check --dry-run
node --check reporting/static/reporting/reports.js
node reporting/tests/test_reports_js.cjs
git diff --check
```

Los espacios son intencionales: PowerShell elimina variables asignadas a `''`,
permitiendo que `.env` las repueble. La configuración aplica `.strip()`, de modo
que `' '` deshabilita DATABASE_URL y R2 sin cambiar archivos de configuración.
Se comprobó `R2_ENABLED=False` y `DB_HOST=127.0.0.1`.

80 tests focalizados aprobados con el hasher habitual; checks de Django,
migraciones, sintaxis JavaScript, controles y diff aprobados. Los casos nuevos
cubren días inclusivos, Bogotá/UTC, medianoche, años bisiestos, 39 días y rangos
mayores, altas diarias, filtros/edad histórica, privacidad, ratings/promedios,
géneros/combinaciones, solapamientos, actividad directa/social, validación
servidor, diferencias/porcentajes y paridad HTML/CSV/XLSX.

La suite completa descubrió 729 tests y ejecutó 725 en 115,595 segundos:
55 fallos y 43 errores, todos en core, ninguno en reporting. Cuatro casos no
llegaron a ejecutarse por un error en `ProfileActivityPhaseG3Tests.setUpClass`. Para esta ejecución
se usó MD5 solo como hasher de pruebas y se bloquearon conexiones/resoluciones
externas en el runner; no se modificó la configuración del proyecto. Se repitió
fuera del sandbox para descartar errores de permisos de temporales de Windows:
en el resultado final no hay PermissionError ni errores del bloqueo de red.

Ejemplos fuera de alcance: fixtures de GuestMode/ProfileActivity usan
`MovieRating(rating=...)`, argumento inexistente; otras expectativas difieren
en feeds, autenticación, reacciones, proveedores y ventanas semanales UTC frente
a Bogotá. El documento ya registra fallos previos de core, pero no se hizo una
nueva comparación contra otra rama o commit en esta tarea. No se afirma que
cada fallo actual haya sido contrastado individualmente con el baseline.
El log completo de esta ejecución queda en
`C:\Users\USUARIO\AppData\Local\Temp\reccool-daily-full-tests.log`.

Pendiente en preview: exclusión de controles al cambiar entre familias,
desplazamiento de tablas de 39 días en móvil/zoom, latencia con un volumen real
y apertura de CSV/XLSX en Excel/LibreOffice. No se impuso un máximo de días.

### Iteración de periodos y UX (8 de octubre de 2026)

Validación local con PostgreSQL, `DATABASE_URL` vacío, `DB_HOST=127.0.0.1`,
`DEBUG=True` y almacenamiento R2 deshabilitado para las pruebas:

```bash
python manage.py test reporting core.tests.MovieRatingEndpointTests --noinput --keepdb
python manage.py check
python manage.py makemigrations --check --dry-run
python manage.py collectstatic --noinput
node --check reporting/static/reporting/reports.js
git diff --check
```

69 tests aprobados, incluidos 9 casos nuevos de periodos, presentación y
exportación. Las comprobaciones anteriores pasan; no se detectan migraciones.
Los primeros intentos encontraron redirecciones HTTPS por configuración local
y después un manifiesto estático sin recursos de reporting. Se resolvieron
con configuración de desarrollo y collectstatic. No quedan fallos en la suite
focalizada; no se repitió la suite completa de core en esta iteración.

Pendiente verificar visualmente en preview: selectores largos con sidebar,
desktop/móvil y zoom, exclusión de modos en JavaScript, selección múltiple con
teclado y apertura de ambas hojas XLSX en Excel/LibreOffice. Los enlaces antiguos
con `month`/`year` o meses `YYYY-MM` deben regenerarse con Meses/Años.

### Validación de la implementación inicial

Python 3.13.5, Django 6.0.4, PostgreSQL 17 y openpyxl 3.1.5. Se aplicaron
todas las migraciones desde cero en una base local aislada. Pasaron `check`,
`makemigrations --check --dry-run`, `collectstatic`, `git diff --check` y la
validación de sintaxis de JavaScript con `node --check`.

- 51 tests de reportería aprobados en la ejecución inicial final, incluidos
  concurrencia, CSV/XLSX, privacidad y permisos; 2 tests adicionales de
  restricciones/backfill también aprobados: 53 casos de reportería verificados.
- Ejecución conjunta de reportería (51), `core.tests.MovieRatingEndpointTests` y
  `core.test_account_management`: 69 tests aprobados. Con los 2 adicionales son
  71 casos distintos verificados en el alcance focalizado.
- Suite completa de ese momento: 689 tests ejecutados, con 53 fallos y 43 errores
  en tests existentes de core. Ningún fallo de reportería. Se comparó contra una
  copia aislada del commit original para distinguir problemas preexistentes.
  Los resultados definitivos de esa comparación se registran a continuación.

Ejecutar la suite completa en staging antes de merge; no considerar su estado
verde solo porque los tests del módulo nuevo están aprobados.

Comparación completada: el commit original ejecutó 652 tests de core, con
54 fallos y 46 errores. Los 93 casos distintos problemáticos de la ejecución
con el cambio también aparecen en el original: no hay casos fallidos nuevos.
El original presentó además tres errores de Admin por no haber recolectado sus
estáticos en la copia aislada y un fallo adicional de orden del feed. El código
con el cambio tenía collectstatic ejecutado. Estos resultados no convierten la
suite completa en verde ni sustituyen la comprobación en staging.
