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
- **Query params:** none
- **Body:** none
- **Filter applied in backend:**
  - `movie_id={id}`
  - `visibility=public`
- **Response serializer:** `CommentSerializer`

### Example
```bash
curl -X GET "http://localhost:8000/api/movies/42/comments/"
```

## 3) Directed recommendations/comments by movie

- **Path:** `/movies/{id}/comments/directed/`
- **Method:** `GET`
- **Auth:** `IsAuthenticated` (token required)
- **Query params:** none
- **Body:** none
- **Filter applied in backend:**
  - `movie_id={id}`
  - `visibility=mentioned`
  - requester participates in the comment:
    - `author=request.user` OR `target_user=request.user`
- **Response serializer:** `CommentSerializer`

### Example
```bash
curl -X GET "http://localhost:8000/api/movies/42/comments/directed/" \
  -H "Authorization: Token <TOKEN>"
```

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

- **Path:** `/movies/{id}/comments/`
- **Method:** `POST`
- **Auth:** `IsAuthenticated` (token required)
- **Query params:** none
- **Body expected:**
  - `body` (string, required)
- **Mention contract:**
  - Mention is parsed from `body` using `@username`
  - There is **no** accepted field like `mentioned_username` or `recipient_username`
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
