# Mensajes dirigidos con varios destinatarios

## Corrección: amistad completa y distribución individual

Un nuevo grupo requiere todas las amistades aceptadas entre sus participantes:
`n * (n - 1) / 2` parejas. La creación consulta las amistades en un solo lote y
comprueba también las restricciones por lotes.

Si el autor es amigo de cada destinatario pero falta una amistad entre estos,
se guarda **un solo Comment** con todas sus relaciones de destinatario y
`individual_dispatch=True`. Ese indicador conserva la decisión inicial; no se
recalcula al leer ni al cambiar amistades posteriormente.

`conversation_contexts(viewer)` genera representaciones en memoria de las
parejas autorizadas, sin insertar copias del comentario. Cada una conserva el
mismo ID y cuerpo, con autor y destinatario de esa pareja, su clave individual y
`is_group=False`. El autor ve un hilo por receptor; cada receptor solo ve su
pareja con el autor. `/me/messages` conserva una entrada por mensaje físico y
añade `conversation_contexts` para el autor; el parser del frontend expande esa
metadata en los hilos correspondientes. El detalle, historial y notificaciones
de un receptor incluyen exclusivamente su contexto. Las menciones del cuerpo no
determinan la agrupación.

Las respuestas consultan esos contextos: el receptor responde solo al autor y
el autor al destinatario de la clave seleccionada. Una respuesta ambigua del
autor se rechaza. Campanita utiliza el mismo contrato de ID y clave.

Una amistad nueva no convierte un multi-envío existente en grupo. Un grupo
existente conserva su historial y clave; si deja de cumplir la amistad completa,
una respuesta nueva se rechaza en lugar de redistribuir silenciosamente el grupo.
La migración `0078_comment_individual_dispatch` añade el indicador con valor
predeterminado `False`, preservando los registros históricos.

Archivos modificados **en esta corrección**, respecto a la implementación anterior:

- Backend: `core/models.py`, `core/views.py`, `core/serializers.py`, `core/social_feed.py`.
- Nuevos: `core/migrations/0078_comment_individual_dispatch.py` y `core/test_directed_dispatch.py`.
- Frontend: `lib/social.ts` y `tests/directed-groups.test.mjs`.
- Informe: `docs/directed_group_messages.md`.

Verificación actual: **52/52 backend**, **37/37 frontend dirigido**, build de
producción, Django check y consistencia de migraciones correctos. Se cubren
amistad incompleta y pendiente, tres y cuatro participantes, entrega física
única, historiales separados, replies de autor y receptores, campanita,
navegación, aislamiento de actividad/reacciones y consultas constantes. Las
suites históricas ampliadas citadas más abajo corresponden a la verificación
de la implementación anterior.

## Inspección y dependencias del destinatario único

No se encontró un modelo persistente de conversación reutilizable. Los hilos
existentes se calculaban a partir de `Comment.author`, `Comment.target_user` y
película. La dependencia de un único destinatario estaba en:

- `Comment.has_valid_target_mention()` y la validación de `target_user`.
- `MovieCommentsListCreateView`: resolución de una mención con `search()`,
  payloads `mentioned_username`/`recipient_username` y guardado dirigido.
- Listados recibidos/enviados, detalle, `/me/messages` y permisos de reacciones.
- Contadores privados y los cinco endpoints de marcado como leído.
- `CommentSerializer`, `DirectedConversationMessageSerializer`,
  `DirectedConversationSerializer`, `MeMessageSerializer` y actividad privada.
- Agrupación del historial por un counterpart y endpoint por username.
- Frontend: `DirectedConversation`, `groupDirectedConversations`,
  `resolveDirectedConversationUser`/`buildCounterpartData`, claves, respuestas,
  filtros, navegación desde campanita y `NotificationCommentViewer`.

La campanita deriva los mensajes privados de `Comment`, usando `pm-<id>`.
No existían filas `UserNotification` por cada nuevo mensaje privado. Se conserva
esa representación para no duplicar los elementos ni los contadores del cliente.

## 1. Modelos

`DirectedCommentRecipient` relaciona un comentario con cada destinatario y tiene
`is_read` individual. Incluye unicidad `(comment, user)` e índice `(user, is_read)`.
`Comment.individual_dispatch` persiste la decisión de distribución individual.
Se conserva `Comment.target_user`, apuntando al primer destinatario nuevo, y
`Comment.is_read` como compatibilidad con ese destinatario legacy.

`Comment` incorpora acceso a destinatarios y participantes, clave canónica y
lectura por usuario. Los permisos y las respuestas usan las relaciones
estructuradas. No se duplican comentarios.

## 2. Migraciones

- `0076_directedcommentrecipient`: tabla, restricción e índice.
- `0077_backfill_directed_recipients`: backfill incremental.
- `0078_comment_individual_dispatch`: distribución individual persistida.

No se modificaron migraciones anteriores. Se aplicaron mediante el runner de
pruebas sobre la base de pruebas; la base habitual conserva estas dos migraciones
pendientes, además de `0078`. Para activar el código allí, ejecutar `python manage.py migrate`.

## 3. Backfill histórico

La migración recorre comentarios `mentioned` con `target_user`, crea la relación
correspondiente y copia `Comment.is_read`. Usa lotes de 2.000 e inserciones que
ignoran conflictos. Es idempotente; no modifica cuerpos ni destinatarios legacy.
Los registros sin relación conservan fallback a `target_user`.

## 4. Endpoints

Se amplían sin cambiar rutas:

- `POST /api/movies/<id>/comments/` y los endpoints dirigidos de creación.
- `GET/POST /api/movies/<id>/comments/directed/`.
- `GET /api/movies/<id>/comments/directed/conversations/<username>/messages/`:
  acepta `conversation_key`; sin clave sigue devolviendo únicamente el hilo 1-a-1.
- `/api/comments/directed/`, `/received/`, `/sent/` y detalle de comentario.
- `/api/me/messages/`, `/summary/`, `/mark-as-read/`.
- `/api/me/notifications/`, `/mark-read/` y los endpoints de marcado batch,
  contexto y todos leídos.
- Reacciones de comentario y representación de actividad privada de Profile Feed.

Payload nuevo de creación:

```json
{"body":"Vayan a cine a verla.","mentioned_usernames":["DennisseJamaica","JulianHernandez"]}
```

Los campos singulares siguen aceptándose. Sin colección explícita, se recorren
todas las menciones del cuerpo con `finditer()`. Se eliminan duplicados y se
conserva resolución case-insensitive. Las menciones implícitas inválidas se
ignoran como antes; las explícitas inválidas se rechazan. Se comprueba amistad
aceptada entre autor y destinatarios y restricciones bilaterales entre todos los
pares de integrantes antes de persistir.

## 5. Serializers

`DirectedMembershipSerializerMixin` amplía los serializers de comentario,
historial dirigido y `/me/messages` con `participants`, `recipients`,
`other_participants`, `conversation_key`, `is_group` e `is_read` del solicitante.
Se conservan los campos `target_user`, `recipient`, `counterpart` existentes.
`DirectedConversationSerializer` añade identidad y participantes del hilo.
`SocialActivitySerializer` conserva esa información al representar actividad
privada. La configuración de privacidad incluye los destinatarios adicionales
en su comportamiento existente de eliminación de mensajes dirigidos.

## 6. Clave de conversación

`movie:<movie_id>:participants:<IDs ordenados separados por guiones>`.
Los participantes son autor más destinatarios estructurados, sin duplicados.
Para un multi-envío individual, cada contexto tiene únicamente autor y un destinatario.
No depende del texto, orden de menciones ni usuario que responde. Los hilos con
distinto conjunto exacto permanecen separados aunque compartan un counterpart.

## 7. Lectura individual

Los filtros de no leídos consultan `DirectedCommentRecipient.is_read` del usuario.
Los cinco endpoints de marcado actualizan únicamente su relación y su
notificación. Se aceptan tanto `pm-<id>` como IDs numéricos de notificación.
Leer como segundo destinatario no afecta al primero. El booleano legacy solo se
sincroniza cuando lee `target_user`; el nuevo flujo no lo usa como estado global.

## 8. Notificaciones

La creación de comentario, relaciones y notificaciones ocurre en una transacción.
Se crea una `UserNotification` por destinatario, todas con el mismo `comment_id`,
actor y película. Se reutiliza la restricción única existente. El autor queda
fuera de los destinatarios y no recibe notificación propia. La campanita conserva
sus IDs `pm-<id>` y añade la clave del hilo, sin contar dos veces las nuevas filas.

## 9. Respuestas desde la campanita

El modal envía `reply_to_comment_id` y, si está disponible, `conversation_key`.
Detail Movie usa el mismo contrato. El backend obtiene el comentario original
desde un queryset limitado al usuario y a la película, verifica coincidencia de
clave y reconstruye los destinatarios a partir de la membresía persistida,
excluyendo al autor de la respuesta. El cuerpo puede no contener ninguna mención.

La navegación nueva lleva la clave canónica. Para notificaciones antiguas, o
hilos ausentes de la primera página de conversaciones, Detail recupera el
comentario autorizado y carga su hilo. No elige un grupo solo por el actor.

## 10. Frontend

Archivos de implementación modificados en `C:/dev/apss-social-frontend`:

- `app/movies/[id]/page.tsx`: agrupación, etiquetas, filtrado por integrantes,
  respuesta, recuperación de hilo, paginación e identidad en navegación.
- `components/social/CommentComposer.tsx`: colección de menciones al crear.
- `components/NotificationCommentViewer.tsx`: respuesta estructurada al hilo.
- `lib/social.ts`: parsing de participantes, clave y tipo grupal.
- `lib/notification-navigation.ts`: clave en la ruta de notificación.
- `lib/profile-feed/types.ts` y `adapters.ts`: clave opcional de notificación.

Se mantienen avatar, colores y estructura del panel. Los grupos muestran los
otros usernames, truncamiento CSS, `Grupal`/`Group` al estar cerrados y `Ocultar`
al abrirse. Los hilos individuales mantienen su etiqueta y controles existentes.

## 11. Pruebas y resultados

- `manage.py check`: correcto.
- `manage.py makemigrations --check --dry-run`: sin cambios pendientes de generar.
- `manage.py migrate --plan`: `0076`, `0077` y `0078` pendientes en la base habitual.
- Suite enfocada de backend: **52/52**, incluyendo 36 pruebas nuevas de grupos y multi-envío,
  paginación, consulta constante de inbox y pruebas de actividad de Profile Feed.
- Suite histórica ampliada (verificación anterior): **83/95**, con **8 fallos y 4 errores preexistentes**.
  La comparación aislada con `git archive HEAD` reprodujo los mismos 12 fallos
  en el código original, además de un error de usuario anónimo que ahora se corrige.
  Los fallos históricos incluyen expectativas de paginación, menciones en body,
  403 frente a 404, orden de actividad y contratos de notificaciones antiguos.
- Frontend dirigido, modal y compositor: **37/37**.
- `npm test` (verificación anterior): **395/396**. El único fallo es
  `the existing guest session supports phones without classifying tablets as phones`;
  sus archivos `useDesktopGuest`, `GuestRouteGuard` y `app/page.tsx` coinciden con HEAD.
- `npm run build`: correcto, incluyendo TypeScript y generación de páginas.
- `git diff --check`: correcto en ambos repositorios.

Las pruebas nuevas cubren comentario único, destinatarios y notificaciones,
lectura independiente en todos los endpoints, respuestas de cada integrante,
campanita, claves, deduplicación, case-insensitivity, amistad, restricciones entre
pares, privacidad, reacciones, backfill y compatibilidad histórica. Frontend
incluye pruebas funcionales de agrupación y envío desde el modal, además de
contratos de estilos y controles. No se realizó una prueba visual manual en móvil.

## 12. Compatibilidad y límites

- Aplicar las migraciones antes de ejecutar el backend nuevo. El frontend puede
  seguir leyendo respuestas antiguas y el backend acepta payloads singulares.
- Un cliente antiguo solo conoce el primer destinatario de un grupo; no puede
  mostrar la membresía completa sin actualizarse.
- Las respuestas vuelven a validar amistad y restricciones. Un multi-envío
  individual responde exclusivamente dentro de su pareja. Un grupo debe
  conservar la amistad completa para aceptar una respuesta nueva; su historial
  no se reclasifica automáticamente.
- La clave se calcula a partir de las relaciones; no se añade un modelo de grupo
  ni un índice persistido por clave. El listado/historial conserva agrupación en
  Python, como la arquitectura anterior.
- Se preserva la representación legacy de actividad privada 1-a-1; el nuevo
  contenido grupal y sus reacciones se excluyen para espectadores ajenos.
- Los fallos históricos descritos no se ocultaron ni se modificaron pruebas
  ajenas para hacer pasar la suite.
