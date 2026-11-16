# Validación offline — Fase 4 — 2026-11-12

## Resultado

**101/101 pruebas offline aprobadas** mediante:

```powershell
.\venv\Scripts\python.exe -B -m unittest core.test_tmdb_enrichment core.test_tmdb_normalizer -q
```

Incluye 42 pruebas nuevas del cliente/pipeline/verificador y 59 del normalizador
existente. Sin solicitudes reales ni PostgreSQL. Las pruebas del pipeline bloquean
`requests.Session.get` y `socket.socket.connect`; un caso bloquea también imports
de Django, modelos y psycopg. Se probaron fallos HTTP/transporte mediante mocks,
no contra el servicio real.

Cobertura: CSV incremental/cabeceras/BOM, IDs/tipos/duplicados, EN/ES/créditos,
integración directa con el normalizador, ocho categorías, los cuatro estados,
404/429/500/502/503/504, timeouts/conexión, autenticación/TLS/redirect,
backoff/jitter/Retry-After/cooldown/presupuesto, JSON estricto/Unicode,
interrupciones a ambos lados de la confirmación, GZIP parcial/corrupción,
reanudación sin duplicados, recuperación de cache y pendientes, conteos,
redacción de secretos y verificación semántica de artefactos.

La primera ejecución del runner quedó bloqueada por permisos del sandbox en
carpetas temporales de Windows. Se repitió fuera del sandbox; no se habilitó red ni
PostgreSQL para las pruebas. La ejecución final fue satisfactoria.

## Ejemplo generado y verificado

Carpeta sintética, creada sin sobrescribir archivos anteriores:

```text
C:\dev\docs\TMDB enriched\offline-fase4-20261112T235550Z-a4aeeea8\
  demo\candidates.csv
  demo\fixtures\
  runs\run-20261112T235550Z-ff4a8ae5a7\
```

El CLI offline procesó ocho filas: **2 accepted, 1 excluded, 1 incomplete,
3 invalid, 1 duplicado y 0 pendientes**. Hubo siete resultados terminales y
siete intentos de candidatos, sin reintentos. `http_requests=0`.
El manifiesto tiene `state=completed`, `complete=true`, porque la demo alcanzó EOF.

El verificador confirmó GZIP/JSON/Unicode, tamaños/SHA-256 de los seis archivos,
identidades/posiciones, estados y reconciliación de conteos con el CSV sintético.
Puede repetirse sin consultar ningún servicio:

```powershell
.\venv\Scripts\python.exe -B -m core.tmdb_enrich_verify 'C:\dev\docs\TMDB enriched\offline-fase4-20261112T235550Z-a4aeeea8\runs\run-20261112T235550Z-ff4a8ae5a7' --input 'C:\dev\docs\TMDB enriched\offline-fase4-20261112T235550Z-a4aeeea8\demo\candidates.csv'
```

El accepted sintético `movie/101` conserva:

```json
{"genre":"Action, Animation, Comedy, Documentary, Drama, Horror, Musical, Sci-Fi","external_rating":"8.3","external_votes":123,"title_spanish":"Órbita española 日本","cast_members":"Zoë 日本"}
```

El archivo real contiene el resultado completo del normalizador, no solamente
estos campos. Todas las producciones tienen `publication.auto_importable=false`.

## Pendiente

No se ejecutaron los lotes live de 10, 100 o 1.000 candidatos, ni ningún otro
endpoint autenticado. Los comandos exactos y los presupuestos están en
`docs/tmdb_enrichment.md`. Diez pares EN/ES normalmente requieren 20 solicitudes;
ese presupuesto requiere aprobación expresa y no está autorizado por esta demo.

La implementación queda lista para revisión y autorización del piloto; **la fase
no se declara terminada**. Falta comprobar calidad real de metadatos/traducciones,
créditos de series, manifiesto y archivos del primer piloto, y aprobar la política
de publicación antes de plantear enriquecimiento masivo o importación.

No se modificaron normalizador, modelo, migraciones, comparador, informes
anteriores, frontend, recomendaciones ni calificaciones. No hubo conexiones ni
escrituras PostgreSQL, ramas nuevas, commits, push, PRs o despliegues.
