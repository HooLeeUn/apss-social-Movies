# Reportería del Django Admin de ReCCool

## Implementación

La app `reporting` aporta una única entrada REPORTES → Generar reportes en
`/admin/reporting/reportaccess/`. `ReportAccess` es un proxy sin tabla nueva ni
acciones CRUD. El Admin existente conserva sus registros y comportamiento.

`forms.py` valida filtros y periodos; `periods.py` construye límites en
America/Bogota; `demographics.py` calcula edad histórica en PostgreSQL;
`queries.py` define las fuentes reales; `services.py` agrega y suprime resultados;
`privacy.py` aplica el umbral; `exports.py` produce CSV/XLSX del mismo resultado.
`views.py` comprueba permisos en cada URL y pagina a 50 entidades/columnas. Los recursos JS/CSS
pertenecen exclusivamente al Django Admin; no se modificó el frontend.

El catálogo incluye usuarios, rankings de producciones/géneros/combinaciones,
producciones más recomendadas, cinco métricas directas y cinco sociales, con
consolidados y comparaciones mensuales. Máximo 24 meses por solicitud. No hay PDF
ni endpoints públicos. Los periodos diarios o mensuales aparecen en filas;
entidades, segmentos y métricas en columnas. Se conservan todos los periodos,
incluidos los vacíos, en orden cronológico, incluso meses no consecutivos.

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
- Comparación mensual: ambos checkboxes apagados; una fila por mes/año,
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

## Tablas pivotadas y totales

La primera columna es «Fecha» en Personalizado y «Periodo» en mensual/acumulado.
Los nombres de los periodos no llevan sufijos «· Total» ni «· Calificaciones».
Un mes usa la misma orientación que varios meses; no hay alternativa horizontal.

- Usuarios registrados: Fecha/Periodo, Registros, Diferencia y Variación %.
- Usuarios por país, edad o identidad: una columna por segmento, TOTAL del
  periodo, Diferencia y Variación %. Los segmentos son excluyentes; se conserva
  el orden lógico del catálogo, incluyendo Sin dato/Fuera de rangos cuando aplica.
- Ratings por producción, género o combinación: cada entidad tiene dos
  subcolumnas «No calif.» y «Promedio Calif.», seguidas de TOTAL,
  «Dif. Nº calificaciones» y «Var. Nº calificaciones %».
- Recomendadas: una columna por producción. Sin diferencia, variación ni TOTAL
  general. La suma final por producción cuenta contribuciones usuario-periodo,
  no usuarios distintos de todo el rango: 8, 4 y 11 suman 23.
- Actividad directa/social: las métricas son columnas; los individuales tienen
  una sola columna de valor. Sin diferencia, variación ni TOTAL general.

Cada tabla temporal termina en una fila TOTAL que suma las columnas de conteos
a través de todos los periodos. En ratings, los Promedio Calif. de esta fila
quedan vacíos: no se suman ni se calculan promedios consolidados. El modo
Acumulado total conserva una única fila, sin repetir una sumatoria redundante.

La columna TOTAL, exclusiva de usuarios segmentados y ratings, suma las celdas
de conteos del periodo. En géneros representa contribuciones a géneros: un rating
puede aparecer en más de un género, conforme a la agregación existente. No se
presenta ese total como cantidad de ratings distintos.

Solo usuarios y ratings muestran comparaciones. En filas normales se compara el
total del periodo (Registros en usuarios registrados) con el periodo anterior:
`actual - anterior` y `(actual - anterior) / anterior * 100`. La primera fila
muestra «-». Base cero mantiene la diferencia y muestra «No aplica» en porcentaje.
En Personalizado se comparan días consecutivos; en mensual, meses seleccionados.
En la fila TOTAL se compara **último periodo contra primero**, sin usar la suma
del rango. Registros 6, 5, 1, 2, 8 generan TOTAL 22, diferencia +2 y variación
33,33 %. Un único periodo compara contra sí mismo en la fila TOTAL (0 y 0 %,
o No aplica si su conteo es cero). Las comparaciones de ratings usan conteos,
nunca promedios. HTML muestra signos/porcentajes; CSV conserva números y XLSX
conserva valores numéricos con formato de porcentaje, sin cambiar su escala.

La privacidad se aplica antes de pivotar y `privacy.protected_sum` propaga la
supresión: cualquier fila/columna/gran total que contenga una celda protegida
también se suprime. No se publica una suma parcial de celdas publicables que
pueda confundirse con un total completo. Diferencias/porcentajes dependientes
se suprimen. La comparación último/primero de TOTAL puede publicarse si ambos
extremos son publicables, aunque un periodo intermedio suprima la sumatoria.

HTML usa dos filas de encabezados para ratings, con colspan/rowspan y etiquetas
de entidad escapadas; se mantiene el diseño del Admin y scroll horizontal manual,
sin comprimir columnas ni desplazarlas automáticamente. Producciones incluyen
tipo en su nombre y, solo ante títulos duplicados, un identificador estable.
Títulos/tipos de entidades completamente protegidas se sustituyen por el mensaje
de privacidad, sin publicar identificadores. Géneros tienen orden alfabético;
producciones, combinaciones y Recomendadas ordenan actividad del rango descendente
con desempate por clave. En reportes protegidos solo el conteo publicable participa
en ese orden. HTML, CSV y XLSX comparten el mismo orden.

CSV usa nombres planos («Acción - No calif.», «Acción - Promedio Calif.»).
XLSX usa dos filas equivalentes, sin celdas combinadas para conservar la escritura
optimizada; congela encabezados/periodos y conserva Reporte y Metodología.
Todos los formatos consumen las mismas filas, totales y valores protegidos del
Result, sin recalcular rangos en exportaciones.

La paginación conserva 50 entidades, ahora como columnas, y muestra **todos los
periodos y TOTAL en cada página**. El contexto indica el rango de entidades.
TOTAL y sus comparaciones incluyen todas las entidades del reporte y permanecen
idénticos al cambiar de página; se indica explícitamente en pantalla cuando hay
más de 50 columnas. Una celda protegida en otra página suprime también ese TOTAL.
Exportar incluye todas las columnas, como antes, aunque se pulse desde la página
2. Los periodos no se paginan ni se truncan. Páginas fuera de rango se ajustan
a la última página de entidades existente.

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
bloques. Para pivotar, servicios materializa exclusivamente las celdas agregadas
entidad-periodo, nunca eventos individuales. Una nueva descarga ejecuta el mismo servicio con los mismos filtros;
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

- Personalizado amplía el número de consultas/agregaciones en función
  de los días seleccionados; la tabla ahora amplía filas por día.
  Los consolidados recorren cada métrica por día y
  contenido genera un fragmento SQL por día. El rango de 39 días se cubre en tests;
  medir latencia y ancho de exportaciones con datos representativos en preview.
  La tabla existente permite desplazamiento horizontal. Rangos muy largos pueden
  exceder la capacidad del worker o las filas de Excel; no se impuso un límite
  arbitrario ni se consolidan días para evitarlos.
- Pivotar necesita conocer todas las entidades y materializar sus celdas
  agregadas: memoria proporcional a entidades × periodos, incluso para una
  página HTML, porque los totales globales deben considerar también otras páginas.
  Medir con un catálogo/rango representativo; el cursor sigue acotando lecturas
  SQL y XLSX conserva escritura optimizada. Muchos títulos producen columnas
  anchas; validar scroll en móvil/zoom y el límite de columnas de Excel para
  exportaciones extraordinariamente grandes. No se introdujo un límite nuevo.

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

### Pivot de periodos y entidades (9 de octubre de 2026)

Cambios únicamente en el working tree de `codex/reporting-pivot-tables`, sin
commit, push, PR, merge, despliegue, operaciones remotas, modelos ni migraciones.
Las consultas/filtros temporales no cambian. Result comparte datos protegidos,
encabezados agrupados/planos y sumatorias para todos los formatos.

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

96 tests focalizados aprobados: los 80 anteriores adaptados a la nueva
orientación y 16 casos de pivot. Se cubren el ejemplo 6/5/1/2/8, bases cero,
mes único y múltiples meses, segmentos, conteos/promedios, contribuciones de
géneros, TOTAL último/primero, sumatorias protegidas, celda oculta en otra página,
Recomendadas 8/4/11, los 12 reportes de actividad, agrupación de encabezados,
paridad de valores HTML/CSV/XLSX, títulos duplicados, paginación y exports completos.
Siguen pasando idempotencia de ratings, histórico/backfill de Recomendadas,
concurrencia, privacidad, permisos y periodos diarios/mensuales. Checks aprobados;
makemigrations no detecta cambios. JavaScript no necesita modificaciones.

Suite completa: 745 casos descubiertos, 741 ejecutados en 66,504 segundos,
55 fallos y 43 errores, todos en core; ningún fallo de reporting. Cuatro casos
no se ejecutan por el error de preparación de ProfileActivityPhaseG3Tests.
La lista normalizada de los 98 encabezados ERROR/FAIL coincide exactamente con
el log de la validación diaria anterior; no aparecen casos problemáticos nuevos.
Esto compara las ejecuciones locales registradas, sin cambiar de rama ni
ejecutar otro commit. Ejemplos: fixtures MovieRating(rating=...), expectativas
de feeds/autenticación/proveedores y ventanas semanales UTC frente a Bogotá.

La suite completa usó MD5 únicamente como hasher del runner, PostgreSQL local,
R2 deshabilitado y conexiones/resoluciones externas bloqueadas. Se ejecutó fuera
del sandbox con la autorización existente para evitar problemas conocidos de
temporales Windows. No hay PermissionError ni errores del bloqueo de red en el
resultado. Log: `C:\Users\USUARIO\AppData\Local\Temp\reccool-pivot-full-tests.log`.
No se modificaron módulos ajenos para resolver fallos históricos.

Preview pendiente: encabezados agrupados y TOTAL en móvil/zoom, scroll manual,
columnas largas, navegación entre páginas, apertura XLSX en Excel/LibreOffice,
y memoria/latencia con entidades × periodos representativos del uso real.

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

## Reporte interno: Elegibilidad de creadores

`creator_eligibility` aparece en «Uso interno» exclusivamente para superusuarios
activos con acceso al admin. Los analistas staff, incluso con ambos permisos
existentes de consulta/exportación, reciben 403 al solicitar su URL, CSV o XLSX.
El servicio también exige explícitamente el usuario autorizado antes de consultar
datos. No se crean permisos, modelos ni migraciones.

Solo admite periodos y «Mínimo de seguidores»: entero positivo, por defecto
10000. Reutiliza Meses × Años, máximo 24 periodos ordenados cronológicamente,
y Personalizado diario con Desde/Hasta inclusivos en America/Bogota. No admite
Acumulado total ni filtros demográficos o de tipo de contenido; el servidor
rechaza combinaciones incompatibles aunque se envíen manualmente.

Se evalúan cuentas no superusuarias con cualquier VideoComment existente,
incluso fuera del rango seleccionado, o que alcanzan el umbral actual. Esto
incluye a todos los creadores con reacciones recibidas en el rango, porque esas
reacciones pertenecen a un video existente. Se incluyen cuentas inactivas y
staff. No existe una marca confiable de cuenta técnica: no se excluyen usernames
arbitrariamente. Un usuario sin videos y por debajo del umbral queda fuera.

- Seguidores actuales: Follow vigentes cuyo `following_id` es el creador;
  captura única al generar, repetida en todos los periodos. No se reconstruyen
  seguidores históricos ni unfollows.
- Video reacciones: publicaciones VideoComment del creador (`user_id`),
  contadas por `created_at`. Los videos ocultados por moderación siguen contando
  mientras existan.
- Likes/dislikes recibidos: VideoCommentReaction atribuidas por
  `video_comment.user`, nunca por el usuario que reacciona. Se cuenta el estado
  vigente `reaction_type` por `updated_at`; no se reconstruyen cambios anteriores
  ni registros eliminados. No incluye reacciones de comentarios públicos.

La tabla muestra una fila por periodo y creador: Periodo/Fecha, Usuario
(username), User ID, Seguidores actuales, Video reacciones, Likes recibidos,
Dislikes recibidos, Interacciones recibidas y Cumple umbral (Sí/No). Cada periodo
ordena primero quienes cumplen el umbral, después interacciones descendentes,
seguidores descendentes y username, con ID como desempate. No añade TOTAL,
diferencias ni variaciones. El resumen cuenta usuarios únicos evaluados y que
cumplen, e informa umbral y periodos seleccionados.

HTML pagina 50 filas; CSV y XLSX exportan todas las filas con los mismos valores.
XLSX conserva las hojas Reporte y Metodología. La excepción a la supresión de
celdas menores de 10 se limita a este reporte autorizado: no modifica
`privacy.py` ni la protección de otros reportes. Solo se exponen username e ID,
sin email, nombre legal ni nacimiento. Cumplir el umbral es una clasificación
analítica interna; no concede monetización, aceptación de programa ni derecho
a compensación.

La selección y captura de seguidores usa una consulta con subconsulta y Exists;
las métricas usan dos agregaciones por periodo. No hay consultas por creador
ni por video. Las páginas HTML consultan solamente los periodos intersectados.
La exportación ordena en memoria los creadores de cada periodo; su coste crece
con el número de creadores y periodos. Las métricas se leen al producir las filas,
sin una transacción que garantice una instantánea conjunta frente a escrituras
concurrentes. No se añaden índices ni migraciones.

Verificación visual pendiente: controles en desktop/móvil, navegación entre
páginas y apertura de ambas hojas XLSX en Excel/LibreOffice.

Validación local de esta entrega: 17 pruebas nuevas y las 89 existentes de
reportería pasan. La ejecución conjunta con MovieRatingEndpointTests,
VideoCommentReactionAPITests y MeFollowingEndpointTests ejecutó 129 casos:
128 aprobados y un fallo existente de follows por la clave adicional
`display_name`, también presente en el registro de referencia anterior.
Pasan `check`, `makemigrations --check --dry-run` (sin cambios), la sintaxis
y pruebas de controles JavaScript y `git diff --check`.

Suite completa local: 762 casos descubiertos, 758 ejecutados en 75,157 s;
55 fallos y 43 errores, todos en core. Cuatro casos no se ejecutan por un error
en setUpClass existente. Se usó MD5 solo para acelerar contraseñas de prueba,
con conexiones externas bloqueadas. Frente a `reccool-pivot-full-tests.log`,
97 casos problemáticos coinciden y cambia un caso de orden del feed:
falla `test_feed_uses_release_year_as_reasonable_tiebreaker` y deja de fallar
`test_feed_orders_null_release_years_last`. No se afirma equivalencia exacta
con el baseline ni una suite completa verde; no se modifica core.
Registro: `%TEMP%/reccool-creator-full-tests.log`.
Al repetir aisladamente los dos casos de feed, pasa el desempate por año y
falla el orden de años nulos, inverso a la suite completa: queda registrada
la variación de estas pruebas de core sin atribuirle una causa no comprobada.
