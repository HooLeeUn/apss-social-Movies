# Fase 4: clasificación, elegibilidad y títulos

Esta ampliación no modifica Movie, el frontend, géneros, ratings, el normalizador
ni el piloto real. No implementa importación ni controles de publicación.

## Títulos: política aprobada y comportamiento existente

El normalizador ya implementa el contrato: prefiere el título de la respuesta
EN en `data.title_english`, y el de la respuesta ES en `data.title_spanish`.
Si no dispone del título preferido, busca títulos originales y después otros
títulos localizados entre las respuestas proporcionadas. El título principal
puede estar en español, japonés, francés, alemán o cualquier otro idioma.
No se exige una traducción EN/ES: se exige al menos un título utilizable y
compatible con los límites del modelo. No hay una segunda lógica de fallback.

`sources` identifica el origen: `en`, `es`, `details.original`,
`english.original`, `spanish.original`, `spanish.localized`, etc. Un título
original no se etiqueta automáticamente como inglés. La procedencia de una
respuesta EN/ES tampoco demuestra que TMDb tuviera una traducción real.
El fallback conserva su advertencia `title_fallback`, sin bloquear elegibilidad.
Las restricciones existentes de identidad, estructura y longitudes permanecen.

Un único título en otro idioma se presenta a través del campo principal
existente `data.title_english`. No es necesario cambiar los serializadores ni
el frontend para renombrar ese campo. No se probó publicación ni interfaz real.
`title_spanish` puede conservar también un fallback según el contrato previo;
su procedencia permite distinguirlo de una traducción ES.

Atención para el importador futuro: `update_data` protege títulos existentes y
omite respaldos no localizados. No es un contrato de creación. La creación debe
tomar el título principal de `data.title_english`, no exigir que esté presente
en `update_data`. No se ha implementado esa creación en esta fase.

## Cuatro ejes independientes

1. **Validez técnica:** `normalization.valid` y sus errores bloqueantes.
2. **Completitud:** se conservan `field_states`, `missing_fields`, `warnings`,
   `sources`, valores vacíos/nulos y estados parciales/inválidos.
3. **Importación:** evaluación de mínimos de datos, nunca autorización de acción.
4. **Publicación:** evaluación separada de los mínimos aprobados y de indicadores
   recomendados de calidad, nunca publicación automática.

No se cambian los estados históricos `accepted`, `incomplete`, `excluded`,
`invalid` y `pending`, ni el significado de sus archivos. `accepted` sigue siendo
la clasificación estricta del normalizador; **no es el filtro de importación**.
`incomplete` puede ser elegible y conserva todo su diagnóstico. `excluded` sigue
obedeciendo las reglas explícitas de Reality y News/Talk; `invalid` conserva sus
errores. `pending` indica consulta no terminada y no recibe elegibilidad final.

## Contrato nuevo: envoltorio de registro versión 2

Los nuevos resultados terminales usan `schema_version=2` y añaden `eligibility`:

| Campo | Significado |
| --- | --- |
| `policy_version` | Versión 1 de esta evaluación. |
| `technical_valid` | Resultado válido del normalizador y sin errores bloqueantes. |
| `metadata_complete` | Hay estados y todos son `valid`; no elimina advertencias ni sustituye `status`. |
| `non_valid_fields` | Campos con estado distinto de `valid`, con su estado original. |
| `import.eligible` | Identidad/tipo válidos, título principal utilizable, sin errores bloqueantes ni exclusiones. |
| `import.blocking_reasons` | Códigos `technical_validation_failed`, `invalid_identity`, `no_model_compatible_primary_title`, `explicit_exclusion` que correspondan. |
| `import.authorized` | Siempre `false`. |
| `import.requires_destination_identity_check` | Siempre `true`; no se consulta la base de destino. |
| `publication.eligible` | Mínimos de datos aprobados; un único título válido de cualquier idioma basta. |
| `publication.blocking_reasons` | Bloqueos de mínimos; los recomendados no son bloqueos. |
| `publication.authorized` | Siempre `false`. |
| `publication.quality_indicators` | `has_genre`, `has_poster`, `has_synopsis`; sinopsis EN o ES utilizable. |
| `publication.editorial_review_flags` | Carencias recomendadas: `missing_genre`, `missing_poster`, `missing_synopsis`. No impiden elegibilidad. |
| `publication.scope` | `approved_minimum_metadata_only`; no afirma aprobación editorial general. |

No se exige sinopsis, créditos, año, género, póster, rating o votos cuando faltan.
Los datos malformados mantienen las validaciones del normalizador. Un valor
opcional que el normalizador descarta como inválido/parcial no se inventa: se
mantiene su diagnóstico y solo se consideran los datos normalizados utilizables.
No se eliminan errores bloqueantes de longitudes o valores numéricos.

Con las reglas aprobadas, las dos banderas `eligible` pueden coincidir, incluso
en un registro con solo título. No se inventa un veto editorial para hacerlas
diferentes. La evaluación de publicación añade indicadores de presentación que
la importación no exige. El campo exterior histórico `publication` sigue
presente: `decision="not_authorized"`, `auto_importable=false`,
`normalizer_accepted` y `requires_destination_identity_check` conservan su sentido.
Los envoltorios antiguos mantienen sus campos originales al verificarlos.

## Manifiestos, checkpoints y verificador

El manifiesto y la estructura SQLite siguen en versión 1, sin migración. La
configuración añade `eligibility_policy_version=1` y el SHA-256 de
`tmdb_eligibility.py` a `implementation_sha256`. Los conteos siguen agrupados por
el estado histórico: no se trasladan resultados de incomplete.jsonl.gz a
accepted.jsonl.gz por ser elegibles.

`tmdb_enrich_verify` admite envoltorios históricos versión 1 sin evaluación y
versión 2 con la política declarada. Para versión 2 recalcula la evaluación
desde el resultado normalizado y rechaza cambios, autorizaciones alteradas,
versiones desconocidas o contratos mezclados. No recalifica ni reescribe archivos
históricos y no exige que sus hashes de implementación coincidan con el código
actual para poder verificarlos.

Reanudar una ejecución nueva con idéntica configuración conserva su journal,
deduplicación, conteos y evaluación. Cambiar semántica/política/código rechaza la
reanudación mediante la comparación existente de configuración. Un checkpoint
antiguo no se convierte ni se reanuda con esta implementación nueva: para
continuarlo habría que usar exactamente la implementación original. La nueva
política requiere una ejecución distinta, previamente autorizada. El piloto
anterior no se modifica ni se reanuda.

`limited`, `complete` e `input_exhausted` conservan su significado. El piloto
de diez terminó sus diez consultas de candidatos dentro del alcance, con diez
resultados terminales, pero no terminó el CSV completo: `limited`, `complete=false`.

## Relectura del piloto sin consultas nuevas

Aplicando la evaluación pura a los resultados guardados:

| TMDb ID | Estado diagnóstico conservado | Importación mínima | Publicación mínima | Indicadores recomendados ausentes |
| --- | --- | --- | --- | --- |
| 25449 | incomplete | Elegible | Elegible | Género |
| 31975 | incomplete | Elegible | Elegible | Ninguno; créditos y traducción siguen incompletos |
| 9 | incomplete | Elegible | Elegible | Póster |
| 91 | accepted | Elegible | Elegible | Ninguno |
| 94 | incomplete | Elegible | Elegible | Ninguno; falta sinopsis ES |
| 125 | incomplete | Elegible | Elegible | Ninguno; falta reparto y sinopsis ES |
| 126 | accepted | Elegible | Elegible | Ninguno |
| 127 | accepted | Elegible | Elegible | Ninguno |
| 160 | accepted | Elegible | Elegible | Ninguno |
| 190 | incomplete | Elegible | Elegible | Póster |

Los diez son técnicamente válidos; ninguno queda autorizado. Los archivos del
piloto conservan su clasificación y formato originales. El indicador `video=true`
de 31975 se conserva como dato observado: no se ha aprobado como exclusión.

## Decisiones editoriales pendientes

Quedan por definir la presentación sin póster/sinopsis/género, cualquier política
adicional sobre adult/video y el proceso concreto de autorización y publicación.
No bloquean silenciosamente los mínimos aprobados y no se activan controles en
PostgreSQL. No se imponen mínimos de votos, popularidad o rating.

## Pruebas offline

```powershell
.\venv\Scripts\python.exe -B -m unittest core.test_tmdb_enrichment core.test_tmdb_normalizer core.test_tmdb_eligibility -v
```

Las pruebas nuevas cubren títulos EN/ES y exclusivos de otros idiomas, límites,
procedencia/fallback, campos opcionales, calidad separada, exclusiones, Unicode,
inmutabilidad, determinismo, resultados sintéticos equivalentes al piloto,
JSONL.GZ, verificación histórica/nueva, alteraciones de elegibilidad, ejecución
limitada, checkpoints y reanudación. No usan PostgreSQL ni consultas TMDb reales.

Resultado de esta entrega: **112 pruebas offline pasaron**. El verificador
actualizado también verificó el piloto histórico sin reanudarlo: 4 accepted,
6 incomplete, cero pendientes, `limited`, `complete=false`. La evaluación pura
de sus diez resultados confirmó ambas elegibilidades mínimas en los diez.
Las huellas SHA-256 de todos los archivos de esa carpeta coincidieron antes y
después de la verificación; el SHA-256 del normalizador sigue coincidiendo con
el registrado en el piloto. No hubo solicitudes reales, conexiones PostgreSQL,
importaciones, modificaciones del piloto, commits ni integración al índice Git.
