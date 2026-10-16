# Transferencia local → copia de destino → Render

Los comandos no consultan TMDB. Usan la base `default` del entorno activo.
Ejecutar primero en una copia de la base destino; después repetir en Render.
No usar la base local ya depurada para comprobar el manifiesto de borrado:
los IDs eliminados ya no existirán allí.

En local, con la sincronización finalizada y sin escrituras concurrentes:

```powershell
python manage.py export_tmdb_rating_votes --output tmdb_rating_votes_final.csv
python manage.py build_movies_delete_manifest --missing-tmdb-csv movies_without_tmdb_id_backup.csv --tmdb-404-csv movies_tmdb_404_to_delete.csv --output movies_to_delete_render.csv
```

Esperado: 438526 filas exportadas, cero sin TMDB; manifiesto con 558822
missing_tmdb_id, 3757 tmdb_404, cero duplicados/ inválidos y 562579 IDs únicos.
Los conteos no están codificados: reflejan las entradas reales.
El exportador exige TMDB positivo en todos los Movie; rating nulo se exporta vacío.
Los archivos de salida y reportes nuevos no sobrescriben archivos existentes.

Transferir ambos CSV a la copia de destino y posteriormente a Render. En cada
entorno, comprobar conexión y ejecutar:

```powershell
python manage.py import_tmdb_rating_votes_csv --input tmdb_rating_votes_final.csv --batch-size 5000 --report-csv tmdb_rating_votes_render_dry_run.csv
python manage.py import_tmdb_rating_votes_csv --input tmdb_rating_votes_final.csv --batch-size 5000 --report-csv tmdb_rating_votes_render_apply.csv --apply
python manage.py delete_movies_from_csv --affected-csv movies_to_delete_render.csv --batch-size 1000 --report-csv movies_delete_render_dry_run.csv
python manage.py delete_movies_from_csv --affected-csv movies_to_delete_render.csv --batch-size 1000 --report-csv movies_delete_render_apply.csv --apply
```

Importador: exigir 438526 válidas/encontradas, cero missing, identity_mismatch,
invalid_row y errores; would_update + unchanged = 438526. Apply debe devolver
updated + unchanged = 438526; una repetición debe dar unchanged = 438526.
El modo predeterminado es dry-run y acepta también `--dry-run` explícito.
Cada lote se confirma por separado. Un error de bulk_update revierte ese lote
y reporta error para sus cambios; hay que revisar el resumen y reporte incluso
si el proceso termina normalmente. Los errores de lectura/conexión detienen
la ejecución; lotes anteriores pueden estar confirmados.

La identidad es database_id + tmdb_id + type (movie/series); un ID repetido
es invalid_row a partir de su segunda aparición válida. Rating vacío equivale
a NULL; se aceptan valores finitos 0..10 sin pérdida de precisión decimal y
votos enteros 0..2147483647. Apply bloquea las filas del lote hasta confirmar.
Solo se actualizan external_rating y external_votes, sin save() ni señales
de guardado. Los reportes se escriben incrementalmente; no son una transacción
atómica con la base. Mantener los archivos fuente para poder repetir el proceso.

Manifiesto: deduplicación global, orden por ID; missing_tmdb_id tiene prioridad
si un ID aparece en ambos archivos. Los conteos por razón son IDs únicos
incluidos. Las filas inválidas se omiten y cuentan; revisar que sean cero.

Borrado: exigir 562579 existentes en dry-run y 562579 eliminados en apply,
sin no encontrados/protegidos/errores. El comando existente solo comprueba
database_id: ignora reason y no revalida TMDB. Dry-run no simula cascadas ni
protecciones; apply elimina también dependencias CASCADE y ejecuta sus señales.
El contador refleja Movie, no todas las filas de tablas relacionadas.
No hay comprobación automática contra los 438526 registros conservados: las
identidades y conteos de la copia destino deben validarse antes de aplicar.
