# Profile activity feed — Phase A legacy contract

Phase A deliberately does **not** optimize `GET /api/profile-feed/activity/?scope=me`.
The request path, serializer, PageNumber pagination, querysets, consolidation,
and output remain unchanged. Test-only helpers record the effective ordering
tuple: event timestamp, activity priority, entity id, then Python's stable
pre-sort family order for a total tie.

Creation time orders public comments, private messages, and created videos.
`activity_at` orders ratings and reaction events/summaries. Received public and
private comment reactions sharing a `comment_id` consolidate into one summary;
received video reactions consolidate per `video_comment_id`; given reactions
remain individual. A test with eleven database rows protects the many-rows to
one-logical-summary behavior from a future fixed per-family limit.

The comparison utility normalizes only the scheme/host of pagination links.
It retains count, page link paths and query parameters, result order, IDs,
types, every timestamp, actor, movie, payload, and all video fields.

## Phase B (not implemented)

Build a separately selectable lazy candidate, measure source rows and query
count, and compare every page against the frozen legacy implementation before
considering adaptive family batches. Do not use a fixed family limit because
many received rows can collapse to one summary.
