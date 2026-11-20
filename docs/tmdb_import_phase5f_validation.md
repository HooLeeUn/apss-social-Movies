# Fase 5F: piloto local preparado, apply pendiente

Validación del 18 de noviembre de 2026 (los nombres de ejecución usan UTC del 19).
Rama local `staging`. No se ejecutaron migraciones, importaciones reales, consultas a
TMDb ni conexiones a Render. Los únicos cambios de esta fase son este documento y
los dos scripts de validación/pruebas; el importador no necesitó cambios.

## Compatibilidad y artefactos

La preparación histórica de `C:\dev\docs\TMDB import ready\prepare-20261117T030953Z-04ae9a747d42`
queda correctamente rechazada por `verify_ready_run`: cambió exclusivamente la
huella de `models.py`, al añadir `Movie.Meta.constraints`:

- Histórica: `3d3cf9336330df741e989a4a3c5fc87b10fc79a5f509128bf2e33fc7c309d6af`.
- Actual: `ee3f60b91a7b12217d3ab6fdb13242b7825ccf40f261a0e2a1ad96e06385d1bb`.

La migración 0079 no está directamente en `RULE_FILES`; el cambio del modelo sí.
Se regeneró solamente la preparación, usando el enriquecimiento original
`C:\dev\docs\TMDB enriched\live-pilot-10\run-20261113T010627Z-019d61868a`.
Se mantuvieron las opciones originales `convert_legacy=True`, `allow_partial=True`:
el enriquecimiento es un piloto limitado v1, con diez resultados y cero pendientes.
La preparación nueva es completa. El apply acepta esa preparación verificada sin
opciones de excepción. No se relajaron las verificaciones.

Artefactos definitivos:

- Preparación: `C:\dev\docs\TMDB import validation\phase5f-20261118-02\prepared\prepare-20261119T023243Z-ee93704e051f`.
- Manifiesto SHA-256: `e280e39efd21eddeb7f5744f643847659b180112fcffe943c8a11ee4dfd2fc6f`.
- `import_ready.jsonl.gz` SHA-256: `b7611cc5c77bf984b30c838d6156e61f42b177a31de33f2b79951dc94e4a48a4`.
- Informe de regeneración y huellas originales: `C:\dev\docs\TMDB import validation\phase5f-20261118-02\pilot_validation.json`.
- Preflight final: `C:\dev\docs\TMDB import validation\phase5f-20261118-final-02\pilot_validation.json`.
- Dry-run final: `C:\dev\docs\TMDB import validation\phase5f-20261118-final-02\dry-run\run-20261119T023642Z-6cc25eef3a5d`.

Los registros JSON históricos y regenerados son idénticos, incluyendo metadatos,
géneros, admisión y trazas. Las huellas de todos los archivos originales permanecieron
iguales. El primer intento de preparación en `phase5f-20261118` quedó fallido porque
no se había trasladado la opción histórica `allow_partial`; se conservó su informe.
Los directorios intermedios se conservaron. Utilizar exclusivamente la preparación
definitiva indicada arriba.

## PostgreSQL real: exclusivamente lectura

El script verifica la configuración exacta `default / 127.0.0.1:5432 / socialapp`
antes de conectar, establece `default_transaction_read_only=on` al abrir la sesión
y realiza las consultas en transacciones `REPEATABLE READ READ ONLY`.

- 432.925 producciones; cero grupos duplicados, filas duplicadas adicionales o TMDb nulos.
- Migración 0079 registrada como aplicada, sin ejecutar migraciones durante esta fase.
- `movie_type_tmdb_id_uniq`: restricción física única sobre `type, tmdb_id`.
- Índice único inmediato, válido, activo y sin predicado, comprobado por `check_schema`.
- FK de autor validada; `Admin`, ID 6, resuelto.
- Trigger de INSERT activo y función con la expresión esperada de búsqueda ponderada
  de la migración 0040; no se ejecutó dicha migración.
- Diez examinadas, diez nuevas, cero existentes, duplicadas o rechazadas; cero insertadas.

## Pruebas

`scripts/test_tmdb_importer.py`: 169/169 aprobadas. Incluye importador, preparación,
apply, enriquecimiento, normalización, elegibilidad y límites de seguridad local.
Red y conexiones PostgreSQL bloqueadas; SQLite solamente en memoria.

`scripts/test_postgres_tmdb_import.py`: 9/9 aprobadas en un clúster PostgreSQL 18
nuevo y desechable, puerto aleatorio, credenciales nuevas, sin cargar `.env` y sin
ejecutar migraciones. Se crean las tablas de fixture y la restricción directamente.
La comprobación de directorio del servidor se realiza antes de crear la base; un
guard impide conexiones Django a destinos distintos del clúster desechable.

Casos cubiertos: restricción única real y separación película/serie; esquema actual;
autor; ocho géneros; títulos multilingües; opcionales y defaults; campos derivados y
trigger real; ejecución limitada/reanudación/idempotencia; preservación exacta de
filas existentes; rollback de lote; reanudación desde informe fallido; interrupción
después del commit y antes de publicar el informe; dos importadores concurrentes;
restricción ausente; archivo manipulado; rechazo del manifiesto histórico y aceptación
del regenerado; igualdad de sus diez registros; inserción del piloto real en un solo
lote de diez, y repetición sin duplicados. Estas inserciones fueron exclusivamente
en el clúster desechable, que se detuvo y eliminó al finalizar.

Comandos ejecutados:

```powershell
& .\venv\Scripts\python.exe -B scripts/test_tmdb_importer.py
& .\venv\Scripts\python.exe -B scripts/test_postgres_tmdb_import.py `
  --historical 'C:\dev\docs\TMDB import ready\prepare-20261117T030953Z-04ae9a747d42' `
  --prepared 'C:\dev\docs\TMDB import validation\phase5f-20261118-02\prepared\prepare-20261119T023243Z-ee93704e051f'
```

## Comandos operativos

Desde `C:\dev\apss-social-Movies`:

```powershell
$python = 'C:\dev\apss-social-Movies\venv\Scripts\python.exe'
$ready = 'C:\dev\docs\TMDB import validation\phase5f-20261118-02\prepared\prepare-20261119T023243Z-ee93704e051f'
```

Este fue el comando final ejecutado. Invoca el dry-run del comando Django mediante
`call_command`, con `--database default --dry-run --author-username Admin --limit 10
--batch-size 10`, después de verificar el esquema y el destino en modo de solo lectura:

```powershell
& $python -B scripts/validate_local_tmdb_pilot.py `
  --prepared $ready `
  --output-dir 'C:\dev\docs\TMDB import validation\phase5f-20261118-final-02'
```

Para repetir el dry-run directamente; cada invocación genera una subcarpeta única:

```powershell
& $python -B manage.py import_tmdb_productions `
  --run-dir $ready `
  --output-dir 'C:\dev\docs\TMDB import validation\phase5f-repeat-dry-run' `
  --database default --dry-run --author-username Admin --limit 10 --batch-size 10
```

**Futuro apply: no ejecutado; requiere autorización expresa.**

```powershell
& $python -B manage.py import_tmdb_productions `
  --run-dir $ready `
  --output-dir 'C:\dev\docs\TMDB import apply\phase5f-pilot-10' `
  --database default --author-username Admin `
  --confirm-database socialapp --confirm-write `
  --limit 10 --batch-size 10 --apply
```

El apply comprueba de nuevo el destino, autor e integridad de la fuente. Cada lote
usa transacción durable, `synchronous_commit=on`, timeout de lock de cinco segundos,
`SHARE ROW EXCLUSIVE` sobre Movie y bloqueo del autor. Dentro de esa transacción
revalida esquema, consulta identidades existentes, inserta únicamente las nuevas y
comprueba autor, identidad, derivados y vector generado. No actualiza Movie existentes.
La restricción protege también frente a otros escritores.

Verificación posterior, exclusivamente lectura y en carpeta nueva:

```powershell
& $python -B scripts/validate_local_tmdb_pilot.py `
  --prepared $ready --expect-present `
  --output-dir ('C:\dev\docs\TMDB import validation\phase5f-post-' + [guid]::NewGuid().ToString('N'))
```

Exige las diez identidades presentes, igualdad de todos los campos proyectados,
autor Admin y vectores no vacíos; vuelve a comprobar esquema y ausencia de duplicados.
El dry-run posterior debe registrar diez `existing`, cero nuevas y cero insertadas.
Se esperan 432.935 producciones si ningún otro escritor cambió el catálogo.
El informe contiene IDs locales y diferencias por campo si se consulta sin
`--expect-present`; con esa opción falla si falta algún registro o hay discrepancias.

## Recuperación

1. Detener el proceso anterior antes de reintentar; conservar fuente, manifiestos,
   `decisions.jsonl` e informes, incluso si sus contadores parecen incompletos.
2. Ejecutar un preflight nuevo sin `--expect-present` para conocer cuántas de las diez
   identidades existen y comprobar sus campos. No reconstruir ni editar la fuente.
3. Con autorización para continuar escribiendo, repetir el apply anterior añadiendo
   `--resume 'RUTA EXACTA DE LA SUBCARPETA run-... DEL APPLY ANTERIOR'`.
   Requiere el mismo origen, destino y autor/ID; escribe un informe nuevo.
4. El límite de apply cuenta inserciones nuevas de esta invocación, no el total histórico.
   Al reutilizar esta misma fuente solo hay diez identidades posibles. Recorre la fuente
   desde el principio y reconsulta PostgreSQL; omite las presentes, sin volver a insertarlas.
   Esto permite recuperar un commit aunque la escritura posterior del informe se interrumpa.
5. Repetir la verificación posterior con `--expect-present` en otro directorio nuevo.

Los informes de apply son por invocación: tras un commit sin publicación, el contador
del informe puede ser inferior a las filas realmente confirmadas. No deducir de él
por sí solo cuántas faltan. No se detectó un bloqueo técnico pendiente. Si cambia
el código incluido en las huellas, el autor, el esquema o los artefactos, repetir
la validación antes de autorizar escrituras. La autorización del primer apply sigue pendiente.
