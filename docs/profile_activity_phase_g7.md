# Phase G7: diagnóstico wall/CPU, conexión y PostgreSQL

G7 es instrumentación backend exclusiva del Candidate shadow de
`GET /api/profile-feed/activity/?scope=me`. Reutiliza
`PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED`; con el flag apagado no crea el
profiler, no consulta relojes/GC y conserva exactamente Legacy, payload,
orden, privacidad, localization, timestamps, reactions, logical count,
frontier, certificación, fallback, K y batches. Shadow sigue sirviendo Legacy.
Por ello no requiere cambios en iOS/iPadOS, Android, tablet o desktop.

## Métricas

Cada family publica `queryset_definition` y `fetch`, con `wall_ms` medido con
`perf_counter`, `cpu_ms` con `process_time`, `wall_minus_cpu_ms=max(0,wall-cpu)`
y collections GC observadas mediante diferencias de `gc.get_stats()`. Fetch de
hidratación ofrece el mismo bloque. Candidate e hydration publican wall, CPU,
gap y hydration collections. El gap es una señal de espera/descheduling, no
una atribución automática a Render. Los contadores GC son agregados del proceso
y, con threads, no prueban que la collection fuese causada por ese request.

El wrapper SQL conserva por family/fase sólo SHA-256 truncado (16 hex) del SQL
normalizado, jamás SQL ni parámetros. Literales numéricos y strings se eliminan
defensivamente; se agregan count, spikes >=50 ms, máximo SQL y máximo gap
wall/CPU. Esto permite correlacionar los spikes staging de 80–90 ms sin PII.

## Conexión y cold/warm

La señal Django `connection_created` marca en el wrapper la identidad y hora
monotónica local de apertura, sin DSN ni credenciales. En la primera ejecución
profiled observada sobre esa conexión se registra `cold`; las posteriores sobre
la misma conexión son `reused`. Si Candidate comienza desconectado también es
`cold`; una conexión preexistente que no pudo observar la señal es `unknown`.
`connection_age_ms` sólo existe si este proceso observó su creación y es una
edad aproximada del socket en el proceso, no la edad de sesión que PostgreSQL o
un pooler pudieran reportar. Reinicios, workers diferentes y poolers impiden a
Django determinar por sí solo spin-down, red, throttling o causa del gap.

Prueba staging, sin tráfico artificial: habilitar ambos flags de shadow/profile
y sampling 1.0; tras inactividad efectuar una request real autenticada y
etiquetar su log COLD/UNKNOWN; efectuar inmediatamente tres requests idénticas
(WARM 1–3). Comparar `candidate_*`, `hydration_*`, suma `hydration_sql_ms`,
definition/fetch wall/CPU/GC, connection state/age y fingerprints. No reiniciar
servicios desde código. Un cold lento/warm rápido sugiere conexión/cold start;
gaps altos con CPU baja sugieren espera; wall≈CPU alto indica Python; SQL
consistentemente dominante requiere plan; spikes warm aleatorios son
compatibles (no prueba) con contention/network/free-tier scheduling.

## EXPLAIN manual seguro

Ratings fue elegida como family inicial frecuente y representativa. Ejecutar
explícitamente, nunca desde el endpoint:

```
python manage.py diagnose_profile_activity_plan --family ratings --user-id <ID> --limit 11
```

El comando acepta sólo `ratings`, limita 1..101, abre una transacción read-only,
ejecuta exclusivamente SELECT/`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` y
redacta el viewer id. El JSON incluye Planning/Execution Time, actual rows,
loops, hits/reads, node/scan/sort/join/index data. ANALYZE sí ejecuta el SELECT,
por eso debe usarse manualmente contra un usuario diagnóstico autorizado. No
crea writes, índices ni migrations.

## Decisión posterior

G7 no implementa índices, pooling/CONN_MAX_AGE, workers, cache, Redis, ORM
rewrites, `only/defer/values`, ni configuración Render. En G8: reunir primero
la tabla cold + warm1..3 y planes de los fingerprints con spikes; sólo si el
plan demuestra scans/sorts/reads dominantes proponer el índice mínimo. Si CPU
Python domina, perfilar el builder concreto; si sólo domina el gap, investigar
métricas Render/PostgreSQL/red fuera de Django.
