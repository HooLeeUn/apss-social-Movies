# Fase 5G: credenciales existentes con transacción READ ONLY

El diagnóstico admite el usuario PostgreSQL existente, incluido CREATEROLE y
permisos de escritura. No se crean roles ni se modifican permisos reales. No hay
opción --apply ni conexión al ejecutor de limpieza o al importador.

## Bloqueo corregido

verify_session rechazaba las banderas de privilegios (incluido rolcreaterole) y los
permisos efectivos de escritura/CREATE. Esos vetos fueron retirados. Ahora exige
identidad confirmada y protección READ ONLY establecida y verificada por PostgreSQL.

## Conexión y protección

- Fuente exclusiva: RECCOOL_RENDER_DATABASE_URL del entorno del proceso. Se analiza
  con Psycopg 3. DATABASE_URL y el alias default permanecen intactos.
- Esquema postgres/postgresql, contraseña obligatoria, un único endpoint externo
  dpg-….render.com y puerto válido. Host, puerto, base y usuario deben coincidir
  exactamente con los parámetros confirmados por el operador.
- Solo se aceptan host, port, dbname, user, password, sslmode y sslrootcert. Se rechazan
  parámetros ambiguos, routing alternativo, service, hostaddr, options y overrides
  libpq ambientales comprobados por la herramienta. El puerto omitido en URL es 5432.
- --tls-ca-file exige un CA de confianza legible y sslmode=verify-full. Si la URL
  contiene otro sslmode o CA distinto, se rechaza. No hay fallback a require/prefer/disable.
  Ante fallo TLS revisar CA, cadena del certificado, host y conectividad; no rebajar TLS.
- Alias efímero reccool_production_audit; nunca sustituye default. Se compara la
  identidad real de Psycopg (host, puerto, base, usuario). TLS verify-full autentica
  el hostname; no se presupone que la IP interna coincida con el endpoint público.
- PostgreSQL confirma current_database, inet_server_port, current_user/session_user,
  TLS, search_path y los indicadores transaction_read_only/default_transaction_read_only.
- La sesión abre con default_transaction_read_only=on. Antes de diagnosticar se
  establece REPEATABLE READ READ ONLY y se verifica. Se comprueba de nuevo al salir.
  Si falla la activación o verificación no se ejecuta el diagnóstico.
- Una sola conexión y snapshot. Se bloquean otras conexiones Django, reconexiones
  y las factorías síncronas psycopg.connect/psycopg.Connection.connect durante el
  comando. Los guards son temporales; no están destinados a un proceso web compartido.
- Guard SQL: solo SELECT individuales. Rechaza DML, DDL, SET, ejecución múltiple y
  funciones conocidas que cambian estado. PostgreSQL READ ONLY impide además las
  escrituras permanentes dentro de la transacción incluso saltándose ese guard.
- Finalización mediante ROLLBACK tanto en éxito como ante errores, seguido de cierre
  y retirada del alias. No se hace COMMIT del diagnóstico.
- Se rechaza RLS en las tablas auditadas para no presentar conteos parciales como completos.

Una transacción READ ONLY no convierte al usuario en un rol lector: las mismas
credenciales pueden escribir desde otras conexiones ajenas al comando. No es un
sandbox contra código hostil que eluda deliberadamente las APIs protegidas, utilice
otras librerías o invoque funciones con efectos externos. La herramienta no acepta
SQL arbitrario y solo ejecuta las consultas revisadas de catálogo y dependencias.
No presentar esta protección como equivalente a credenciales sin escritura.

Timeouts: conexión 10 s, sentencia 60 s, espera de lock 1 s e inactividad 60 s.
Se utilizan locks AccessShare normales, sin bloqueos exclusivos. Un snapshot largo
puede retener versiones MVCC: ejecutar con baja carga. No se amplían timeouts solos.

## Planificador e informes preservados

Se conservan algoritmo weighted-usable-metadata-v1, desempate por Movie.id, análisis
de dependencias, contrato reccool.remote-duplicate-audit.v1 y hashes existentes.
No se priorizan interacciones ni se fusionan metadatos o preferencias. Los IDs
proceden del snapshot remoto, nunca de PostgreSQL local.

Cada invocación crea una carpeta única fuera del repositorio con remote_manifest.json,
remote_candidates.jsonl, remote_dependencies.json y remote_manifest.sha256. No se
sobrescriben informes. verify_report(path) verifica contrato y huellas sin conexión.
SHA-256 detecta cambios, pero no constituye una firma autenticada.

Ambos modos incluyen candidatos y dependencias; --plan marca planificación. Se
conservan bloqueos de esquema, dependencias incompletas y duplicados no planificables.
La migración 0079 se consulta si puede leerse y se informa separada del esquema físico.
Errores e interrupciones generan estado failed/interrupted sin mensajes crudos del
driver. Solo un informe completo con hashes válidos es revisable como resultado.
Repetir genera otro snapshot y directorio; no se reanuda un snapshot anterior.

No se guardan URL, contraseña ni usuario conectado en los informes. El usuario solo
se utiliza en memoria para verificar la identidad. No imprimir variables de entorno
ni compartir tracebacks o memoria de proceso que puedan contener secretos.

## Comandos propuestos, NO EJECUTADOS

Requieren revisión y autorización expresa antes de conectar. La URL externa debe
estar disponible en RECCOOL_RENDER_DATABASE_URL en la sesión que ejecutará el comando,
mediante un canal seguro. No copiarla al repositorio, argumentos, historial ni informes.
Las antiguas variables RECCOOL_PRODUCTION_READONLY_USER/PASSWORD ya no se utilizan.

Desde C:\dev\apss-social-Movies:

```powershell
$python = 'C:\dev\apss-social-Movies\venv\Scripts\python.exe'
$renderHost = '<endpoint externo exacto de Render>'
$renderPort = 5432 # sustituir por el puerto real confirmado en la URL
$renderCa = '<ruta absoluta del CA de confianza>'
$common = @(
  '--host', $renderHost,
  '--port', [string]$renderPort,
  '--name', 'reccool_postgres_production',
  '--confirm-host', $renderHost,
  '--confirm-database', 'reccool_postgres_production',
  '--confirm-user', 'reccool_postgres_production_user',
  '--tls-ca-file', $renderCa
)
```

Diagnóstico:

```powershell
& $python -B manage.py audit_render_movie_duplicates @common `
  --output-dir 'C:\dev\docs\TMDB production duplicate audit'
```

Planificación, después de revisar el diagnóstico:

```powershell
& $python -B manage.py audit_render_movie_duplicates @common --plan `
  --output-dir 'C:\dev\docs\TMDB production duplicate plans'
```

Los valores se contrastan con la URL sin mostrarla. La variable no estaba disponible
en el proceso del agente durante esta tarea; no se solicitaron ni buscaron secretos.
Quedan pendientes el endpoint/puerto, CA y URL en la sesión del operador, además de
la autorización para conectar. No se obtuvo ningún conteo real de producción.

## Pruebas y evidencia

```powershell
& .\venv\Scripts\python.exe -B scripts/test_remote_movie_duplicates.py
& .\venv\Scripts\python.exe -B scripts/test_postgres_remote_movie_audit.py
```

66/66 pruebas sintéticas aprobadas (16 remotas y 50 anteriores del limpiador),
integración PostgreSQL desechable aprobada, sintaxis y git diff --check aprobados.
Los parámetros se comprobaron mediante --help.

El runner sintético bloquea sockets y PostgreSQL y solo permite SQLite en memoria.
Cubre URL ausente/inválida, contraseña, TLS/CA, identidad, política de privilegios,
rechazo DML/DDL/migraciones, compatibilidad, manifiestos, secretos, errores e interrupciones.

El runner PostgreSQL utiliza exclusivamente un clúster desechable identificado por
directorio y endpoint, sin .env ni migraciones. Prueba lector, escritura y CREATEROLE,
planificación idéntica, rechazo real de INSERT/UPDATE/DELETE/DDL, rollback en éxito/error,
conexiones secundarias prohibidas, fallos READ ONLY antes de diagnosticar, cierre y
conservación de filas. Solo allí se crean datos/roles y conceden permisos de fixture.

La observación TLS se sustituye únicamente en ese fixture loopback sin TLS. La
negociación del certificado real de Render sigue pendiente de autorización.
No hubo conexiones a Render ni a socialapp, migraciones, escrituras reales, cambios
en servicios, modelos, frontend, importadores ni algoritmos de selección.