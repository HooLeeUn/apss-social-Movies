# Recuperación de contraseña

Solo backend; no se implementa la pantalla frontend.

## Despliegue

1. Ejecutar `python manage.py migrate`.
2. Programar `python manage.py send_password_resets` cada minuto con las mismas variables y base de datos que el servidor. Cada ejecución procesa hasta 100 solicitudes; se pueden ejecutar trabajadores concurrentes en PostgreSQL.
3. Configurar `FRONTEND_BASE_URL`, `DEFAULT_FROM_EMAIL` y el backend SMTP `EMAIL_*` existente. El backend console de desarrollo imprime el correo completo, incluido el enlace: no usarlo en producción ni enviar cuerpos de correo a logs. El procesador no registra direcciones, tokens ni excepciones SMTP.

El endpoint siempre encola los correos válidos, sin consultar usuarios ni esperar SMTP. El procesador solo envía a cuentas activas con contraseña utilizable. Elimina las solicitudes procesadas (también ante error SMTP); el usuario puede volver a solicitar el enlace. Descarta solicitudes pendientes de más de una hora. Vigilar el comando y su frecuencia: sin ejecutarlo no se envían correos.

## API

`POST /api/auth/password-reset/request/` con `{"email":"usuario@example.com"}`.

HTTP 200 para direcciones registradas, inexistentes e inactivas, con exactamente:

```json
{"detail":"Si el correo está asociado a una cuenta activa de ReCCool, recibirás un enlace para restablecer tu contraseña. El enlace será válido durante 1 hora."}
```

Las direcciones malformadas devuelven 400 por validación, sin consultar usuarios. El límite por IP es 5 solicitudes por hora (429 al excederlo), independientemente de la dirección. DRF usa el caché configurado; el caché local predeterminado limita por proceso. Para un límite común entre procesos configurar un caché compartido. El throttling DRF no garantiza un contador estricto frente a solicitudes concurrentes. Configurar correctamente el proxy para que la IP identificada sea la del cliente.

Enlace enviado: `${FRONTEND_BASE_URL}/reset-password/<uid>/<token>`.

`POST /api/auth/password-reset/confirm/`:

```json
{"uid":"<uid>","token":"<token>","new_password":"<nueva>","confirm_password":"<nueva>"}
```

HTTP 200: `{"detail":"Contraseña restablecida correctamente. Inicia sesión de nuevo."}`.
Errores de enlace o contraseña: 400. Límite: 20 intentos por IP por hora.

Se usa `default_token_generator` de Django, sin almacenar tokens. `PASSWORD_RESET_TIMEOUT = 3600` fija la validez desde la generación en el procesador. Cambiar contraseña, email o estado relevante del usuario invalida el token. La confirmación exige usuario activo y valida contraseñas con los validadores configurados. `select_for_update()` serializa confirmaciones; `set_password()` invalida todos los enlaces anteriores incluso al elegir la misma contraseña. Se elimina DRF TokenAuth existente tras el cambio, siguiendo el patrón del cambio de contraseña autenticado. No se devuelven tokens en JSON.
