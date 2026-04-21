# Movies detail & comments API contract (backend real contract)

Base path: `/api`

## 1) Movie detail

- **Path:** `/movies/{id}/`
- **Method:** `GET`
- **Auth:** `AllowAny` (token optional)
- **Query params:** none
- **Body:** none
- **Response serializer:** `MovieListSerializer`

### Example
```bash
curl -X GET "http://localhost:8000/api/movies/42/"
```

## 2) Public comments by movie

- **Path:** `/movies/{id}/comments/`
- **Method:** `GET`
- **Auth:** `AllowAny`
- **Query params:** `page`, `page_size`
- **Body:** none
- **Filter applied in backend:**
  - `movie_id={id}`
  - `visibility=public`
- **Response serializer:** `CommentSerializer`
- **Ordering:** `created_at` desc, then `id` desc (newest first)

### Example
```bash
curl -X GET "http://localhost:8000/api/movies/42/comments/?page=1&page_size=10"
```

## 3) Directed conversations by movie

- **Path:** `/movies/{id}/comments/directed/`
- **Method:** `GET`
- **Auth:** `IsAuthenticated` (token required)
- **Query params:** `page`, `page_size`
- **Body:** none
- **Filter applied in backend (source messages):**
  - `movie_id={id}`
  - `visibility=mentioned`
  - requester participates in message:
    - `author=request.user` OR `target_user=request.user`
- **Response serializer:** `DirectedConversationSerializer`
- **Ordering:** conversations by `last_message_at` desc (most recent first)

### Response shape
```json
{
  "count": 2,
  "next": "http://localhost:8000/api/movies/42/comments/directed/?page=2",
  "previous": null,
  "results": [
    {
      "other_user": {
        "id": 7,
        "username": "peck",
        "display_name": "Peck Hernandez",
        "avatar": "http://localhost:8000/media/avatars/peck.jpg"
      },
      "last_message_at": "2026-04-20T20:10:00Z",
      "messages_preview": [
        {
          "id": 101,
          "content": "@julian ...",
          "created_at": "2026-04-20T20:10:00Z",
          "likes_count": 0,
          "dislikes_count": 0,
          "my_reaction": null,
          "author_username": "peck",
          "author_display_name": "Peck Hernandez",
          "author_avatar": null,
          "direction": "received"
        }
      ],
      "messages_endpoint": "http://localhost:8000/api/movies/42/comments/directed/conversations/peck/messages/"
    }
  ]
}
```

### Example
```bash
curl -X GET "http://localhost:8000/api/movies/42/comments/directed/" \
  -H "Authorization: Token <TOKEN>"
```

## 3.1) Directed conversation messages by movie + user

- **Path:** `/movies/{id}/comments/directed/conversations/{username}/messages/`
- **Method:** `GET`
- **Auth:** `IsAuthenticated` (token required)
- **Query params:** `page`, `page_size`
- **Body:** none
- **Filter applied in backend:**
  - `movie_id={id}`
  - `visibility=mentioned`
  - only messages exchanged between requester and `{username}`
- **Response serializer:** `DirectedConversationMessageSerializer`
- **Ordering:** `created_at` desc, then `id` desc (newest first)
- **Field `direction`:**
  - `sent` if `author == request.user`
  - `received` otherwise

## 4) Create public comment

- **Path:** `/movies/{id}/comments/`
- **Method:** `POST`
- **Auth:** `IsAuthenticated` (token required)
- **Query params:** none
- **Body expected:**
  - `body` (string, required)
- **Auto-assigned by backend:**
  - `movie` = movie from URL
  - `author` = authenticated user
  - `visibility` = `public` **when mention is absent or invalid**
  - `target_user` = `null` when public

### Valid example
```bash
curl -X POST "http://localhost:8000/api/movies/42/comments/" \
  -H "Authorization: Token <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"body":"Excelente película"}'
```

## 5) Create directed comment with valid mention

- **Path:** `/movies/{id}/comments/` (or `/movies/{id}/comments/directed/`)
- **Method:** `POST`
- **Auth:** `IsAuthenticated` (token required)
- **Query params:** none
- **Body expected:**
  - `body` (string, required)
- **Mention contract:**
  - Mention is parsed from `body` using `@username`
  - Alias fields also accepted: `mentioned_username`, `recipient_username`
  - Mention is valid only if:
    1. user exists,
    2. is different from author,
    3. has `Friendship.STATUS_ACCEPTED` with author.
- **Auto-assigned by backend on valid mention:**
  - `visibility` = `mentioned`
  - `target_user` = mentioned friend user id

### Valid example
```bash
curl -X POST "http://localhost:8000/api/movies/42/comments/" \
  -H "Authorization: Token <TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"body":"Te la recomiendo @comment_friend"}'
```
