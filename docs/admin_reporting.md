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
Los archivos contienen filtros, periodos, fecha/hora, zona, población publicable
y advertencias semánticas. CSV usa streaming; SQL de contenido usa cursor por
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

Ejecutar primero `setup_report_analysts`. En `/admin/auth/user/add/`, crear usuario
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
