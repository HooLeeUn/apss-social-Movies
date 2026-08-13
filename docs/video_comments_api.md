# Video comments API

## Endpoints

- `GET /api/movies/<pk>/video-comments/`: lists paginated video comments for one movie/series with `next`, `previous`, `count`, and `results`.
- `POST /api/movies/<pk>/video-comments/`: authenticated multipart upload. The backend resolves the movie first, uses `request.user`, ignores any submitted `user`, and returns `201` on success.
- `GET /api/video-comments/<pk>/`: retrieves one video comment visible to the authenticated user.
- `DELETE /api/video-comments/<pk>/`: deletes only when the requester is the author; the stored media object is deleted with the model record.
- `PUT /api/video-comments/<pk>/reaction/`: toggles an authenticated user's reaction. Send `{"reaction": "like"}` or `{"reaction": "dislike"}`. Sending the current reaction removes it; sending the other reaction switches it.
- `DELETE /api/video-comments/<pk>/reaction/`: explicitly removes the authenticated user's reaction, if any.

Reaction responses use the following shape, and the same three reaction fields are included on every item returned by the list and detail endpoints:

```json
{
  "video_comment_id": 42,
  "likes_count": 4,
  "dislikes_count": 1,
  "my_reaction": "like"
}
```

`my_reaction` is `"like"`, `"dislike"`, or `null`. Counts and the current user's reaction are computed in the list query, so clients do not need a request per video.

## Upload limits and formats

Video comments accept only `.mp4`, `.mov`, and `.webm` files with compatible MIME types: `video/mp4`, `video/quicktime`, and `video/webm`. The default maximum size is 50 MB and can be configured with `VIDEO_COMMENT_MAX_SIZE_MB`.

The maximum duration is 20 seconds, with a 0.5 second metadata tolerance for small ffprobe/container rounding differences. The backend validates that the file is non-empty, readable, not corrupt, has a real video stream, has an allowed extension, has an allowed client MIME when provided, and has an ffprobe-confirmed compatible container.

## ffprobe / Render

The backend uses `ffprobe` to inspect real media metadata and does not trust the filename, client MIME header, or frontend-provided duration. Configure the executable with `VIDEO_COMMENT_FFPROBE_PATH`; the default is `ffprobe`. Render deployments must install ffmpeg/ffprobe (usually by adding ffmpeg to the build/runtime image) or set `VIDEO_COMMENT_FFPROBE_PATH` to the installed binary. If ffprobe is unavailable, the API returns a controlled `400` validation error instead of an unhandled server error.

## Storage and Cloudflare R2

`VideoComment.video` uses Django's default media storage. This project switches the default storage to Cloudflare R2 when the existing R2 settings are complete, so video comments are stored in the same R2 bucket/configuration as other media without creating a second storage backend. Filenames are generated under `video_comments/<movie_id>/<user_id>/<uuid>.<extension>` and do not trust the original upload name.

## Ordering and pagination

Public movie comments and video comments are ordered in SQL before pagination by annotated follower count (`Count(author/user followers, distinct=True)`) descending, then `created_at` descending, then `id` descending. The follower count uses the real `Follow.following` target relation, so it counts users who follow the author and does not produce N+1 queries.
