# Fase 5E — preferencias históricas y delta de eliminación

## Alcance

Trabajo local en staging. No se rediseñó la selección de supervivientes, ni se
modificaron `Movie`, `MovieRating`, servicios, señales, algoritmos de preferencias
o feed, ni la migración 0079. No se ejecutan correcciones de preferencias,
eliminaciones o migraciones sobre socialapp. El nuevo comportamiento pertenece
exclusivamente a la validación del ejecutor de duplicados.

Se conservó el informe fallido:
`C:\Users\USUARIO\AppData\Local\Temp\reccool-cleanup-validation-6fe79b6c1ad6434485ef9d2a06b27bb2\execution_summary.json`.
Su motivo era `Preference distribution disagrees with remaining ratings.`
No fue necesario regenerarlo ni sobrescribirlo.

## Diagnóstico exacto (solo lectura)

Destino confirmado: socialapp / 127.0.0.1:5432, transacción
`REPEATABLE READ READ ONLY` y `default_transaction_read_only=on`.

* Usuario técnico: **2** (sin exponer nombre, correo u otros datos de cuenta).
* Calificación: **160**, score **8**.
* Movie candidato a eliminación: **915090**, tipo movie, TMDb **637920**,
  `Miracle in Cell No. 7`, director `Mehmet Ada Öztekin`, género `Drama`.
* Superviviente del plan existente: **476801**, sin cambios.
* Perfil de gustos: **51** calificaciones actuales, coincide con MovieRating.

Todos los buckets no indicados son cero:

| Clave director | Estado actual | Esperado según las calificaciones y metadatos actuales |
|---|---|---|
| Byron Howard | Fila 65: count_7=1, ratings_count=1, score=7.00 | Sin fila; distribución vacía, count=0 |
| Byron Howard, Chris Williams | No existe fila | count_7=1, ratings_count=1, score=7.00 |
| Eric Darnell | Fila 12: count_5=1, ratings_count=1, score=5.00 | Sin fila; distribución vacía, count=0 |
| Eric Darnell, Tom McGrath | No existe fila | count_5=1, ratings_count=1, score=5.00 |

La calificación 97 del Movie 525094 explica el aporte 7 al campo actual
`Byron Howard, Chris Williams`; la 32 del Movie 519824 explica el aporte 5 al
campo `Eric Darnell, Tom McGrath`. No son la calificación 160 ni su aporte.
Las filas existentes tienen métricas internas consistentes; el problema es la
distribución entre claves, no una suma o un redondeo incorrecto.

## Causa demostrada y límites de la evidencia

`get_movie_preference_targets` utiliza el campo `Movie.director` completo,
normalizado con strip, como **una sola clave**, incluso si incluye codirectores.
No divide directores individuales. El feed personalizado consulta la misma
cadena; sus puntuaciones/confianzas utilizan score y ratings_count. Los perfiles
de inspección serializan las filas almacenadas.

Las señales de `MovieRating` agregan/restan su score al guardar/eliminar la
calificación. Una actualización del metadato `Movie.director` no reconstruye las
preferencias de las calificaciones que ya existían. El ensayo PostgreSQL reproduce
exactamente la firma observada: calificar con una clave individual, ampliar después
el campo a codirectores, y conservar la preferencia antigua sin crear la nueva.

Esto demuestra un desfase histórico de claves compatible con cambios de metadatos;
no demuestra quién cambió esos campos ni en qué momento. No hay un historial en
estos datos que permita atribuir la operación original. No se encontró un error
actual en la suma/resta de una calificación que justifique cambiar el algoritmo.

`rebuild_user_taste_profile` eliminaría y reconstruiría todas las preferencias
del usuario. No es una corrección acotada autorizada para esta limpieza y no se usa.

## Decisión: estrategia B, sin reconciliación real

La alternativa A corregiría dos filas obsoletas y crearía dos claves combinadas.
Aunque pequeña, cambiaría preferencias históricas y potencialmente afinidad del
feed ajenas a la interacción de prueba cuya eliminación fue aprobada. No hace
falta para verificar el efecto de esa eliminación y requiere autorización aparte.

Se implementa B:

1. El dry-run captura una línea base de todos los histogramas/filas/IDs del usuario
   afectado, perfil de gustos y entradas técnicas de sus calificaciones.
2. Solo admite discrepancias previas de director en claves ajenas a **todas** las
   calificaciones del plan que se eliminarían. Métricas internas incorrectas,
   discrepancias de géneros/tipos o de claves afectadas siguen bloqueando.
3. Fija política, código de validación/modelos/servicios/señales y alcance del plan.
   El snapshot íntegro queda ligado al SHA-256 revisado por el comando existente.
4. Bajo los bloqueos y transacciones ya existentes, compara estado anterior con
   el último estado revisado/confirmado. Después de borrar, resta solamente los
   aportes previstos y exige igualdad exacta del resultado.
5. Conserva íntegramente preferencias ajenas, incluidas filas faltantes. No crea,
   elimina ni corrige las cuatro claves históricas. El delta de errores previo
   permanece igual porque esas claves no reciben ningún aporte de la eliminación.
6. Cualquier diferencia nueva de filas, IDs, buckets, métricas o entradas de otras
   calificaciones provoca rollback. No se permite compensar un error nuevo con otro.
7. La reanudación proyecta los grupos ya confirmados desde el mismo baseline y
   verifica la huella del último estado confirmado. Conserva la recuperación del
   intervalo entre commit y publicación del recibo.

Se conserva la comprobación estricta anterior `assert_preferences(users)` cuando
no se proporciona un review diferencial. Los reviews antiguos no se aceptan como
si tuvieran baseline: se exige un dry-run nuevo. No hay una opción para omitir
validaciones ni una lista de usuarios/directores exentos del control de cambios.

## Efecto previsto de eliminar la calificación 160

Se eliminaría ese MovieRating por CASCADE, sin trasladarlo ni fusionarlo.
Se restaría una unidad de `count_8` en género `Drama`, tipo `movie` y director
`Mehmet Ada Öztekin`, recalculando únicamente las métricas correspondientes con
las señales vigentes. Si el histograma de una de esas filas llega a cero, la señal
elimina esa fila. El perfil de gustos pasa de 51 a 50 calificaciones y actualiza
su timestamp. Las otras 50 calificaciones, las cuatro discrepancias históricas,
usuarios, perfiles de cuenta y Movie supervivientes se preservan.

Proyección exacta a partir del snapshot del nuevo dry-run (no aplicada):

| Preferencia | Antes | Después previsto |
|---|---|---|
| Drama, fila 17 | count_8=14, ratings_count=44, score=7.39 | count_8=13, ratings_count=43, score=7.37 |
| movie, fila 3 | count_8=16, ratings_count=51, score=7.29 | count_8=15, ratings_count=50, score=7.28 |
| Mehmet Ada Öztekin, fila 121 | count_8=1, ratings_count=1, score=8.00 | Fila eliminada por quedar sin aportes |

Los demás buckets de esas filas no cambian.

## Pruebas y comandos de lectura

```powershell
Set-Location 'C:\dev\apss-social-Movies'
.\venv\Scripts\python.exe -B scripts\test_local_movie_duplicates.py
.\venv\Scripts\python.exe -B scripts\test_postgres_movie_duplicate_cleanup.py --postgres-bin 'C:\Program Files\PostgreSQL\18\bin' --no-migrations
```

La suite sintética conserva las 30 pruebas anteriores y agrega 14 pruebas de
baseline/delta: **44/44 aprobadas**. Solo usa SQLite en memoria.
El runner PostgreSQL crea y verifica un clúster temporal independiente, bloquea
conexiones Django a cualquier otro endpoint, no ejecuta migraciones en el modo
indicado y detiene el servidor al terminar. Conserva los casos anteriores de
limpieza/rollback/commit-gap/idempotencia y agrega los del bloqueo histórico.
El modo anterior que prueba migraciones sigue disponible pero no se ejecutó.

El ensayo aislado verifica:

* Preferencias inicialmente consistentes y cuatro discrepancias preexistentes.
* Eliminación de dos calificaciones relacionadas, en grupos separados.
* Preservación de preferencias ajenas y de otros usuarios/supervivientes.
* Rechazo de cambios posteriores al review y de discrepancias en claves afectadas.
* Rollback ante señales sintéticas que alteran buckets afectados o ajenos.
* Interrupción tras commit, reanudación, repetición idempotente y cero duplicados.

Para repetir solamente el dry-run local, con salida nueva:

```powershell
$cleanupPlan = 'C:\Users\USUARIO\AppData\Local\Temp\reccool-duplicate-cleanup-e1d1bcc556344035ba34334119e13493'
$cleanupReview = Join-Path ([System.IO.Path]::GetTempPath()) ('reccool-cleanup-preferences-review-' + [guid]::NewGuid().ToString('N'))
.\venv\Scripts\python.exe -B manage.py cleanup_local_movie_duplicates --dry-run --confirm-database socialapp --plan-dir $cleanupPlan --output-dir $cleanupReview
```

No se propone un comando de corrección real de preferencias: con B no es
necesario modificar las cuatro claves para validar la depuración. No ejecutar
`rebuild_taste_profiles` ni un UPDATE/DELETE de preferencias como parte de esta
fase. Si se elige A después, se debe preparar y aprobar por separado un cambio
acotado con valores previos/esperados y guardas de concurrencia.

Un dry-run válido **no autoriza apply**. Antes de cualquier escritura siguen
pendientes revisión del plan y baseline, autorización expresa, respaldo restaurable,
mantenimiento/congelación de escritores, mismos datos/esquema/código y las
confirmaciones del ejecutor. No se ejecutan el importador ni la migración 0079.

## Resultado del dry-run real de lectura — 2026-11-18

Se ejecutó después de las pruebas aisladas, forzando además
`default_transaction_read_only=on` para la conexión y una guarda SQL adicional
contra escrituras. Resultado: **validated**, sin bloqueos de preferencias.

* 1.295 grupos; 1.298 Movie candidatos a eliminación; 0 grupos confirmados.
* Dependencias previstas: 1 MovieRating y 4.953 UserDailyFeedCandidate.
* 4 discrepancias previas de director explícitamente preservadas; 0 filas
  históricas reconciliadas; sin dependencias no admitidas.
* Catálogo antes/después: 434.223 Movie y los mismos grupos/candidatos.
* Huella de preferencias antes/después idéntica:
  `5cebf772511f4bdf7b16f08eec3ae0a1f2e55885e044e02ae8d15318942693b4`.
* Guarda SQL: 261 SELECT, 4 sentencias de control, **0 mutaciones**.
* Cero apply sobre socialapp; cero migraciones en todos los destinos.

Informes nuevos, sin sobrescribir los anteriores:

```text
C:\Users\USUARIO\AppData\Local\Temp\reccool-cleanup-preferences-review-84ced1c2275546eebf228743aef0d298\
  execution_summary.json
  preference_diagnosis.json
  review_snapshot.json
  readonly_audit.json
```

El plan conserva SHA-256
`cab809623bb71b3e9c248dc8a33914d64f7e2ac45f17eca94c311b47e273925a`.
El nuevo review SHA-256 es
`2efc8c79d36c030255f48650512a4783a96cb6aad60341769ec59d693455901a`.
Estos informes deben revisarse antes de autorizar cualquier escritura.
El apply de prueba se ejecutó únicamente con registros sintéticos en el clúster
temporal independiente y detenido al concluir.
