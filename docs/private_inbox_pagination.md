# Private inbox pagination contract

## Compatibility and client review

`GET /api/me/messages/` continues to return the existing flat JSON list. The
repository contains no web, iOS, or iPadOS client source, so client-side scroll,
search, loading, onboarding, tab restoration, and deduplication behavior cannot
be audited directly here. The backend contract shows that clients can identify
messages by their numeric `id`, reactions by the `private-reaction-<id>` alias,
and order both families by `created_at` descending. Because pagination is
strictly opt-in, existing mobile/tablet clients receive byte-for-byte the same
response shape and retain their current list and touch behavior.

The coordinated frontend PR may opt in with:

```http
GET /api/me/messages/?paginated=1
```

The response is cursor based:

```json
{
  "next": "https://example.test/api/me/messages/?paginated=1&cursor=...",
  "previous": null,
  "results": []
}
```

There is intentionally no exact `count`: each page therefore avoids two global
count queries. `results` items are unchanged from the flat-list contract,
including message users, movie, body, direction, read state and type, and the
reaction aliases, actor, comment author, movie, direction, flags and timestamp.

## Ordering and cursor semantics

The stable global key is `created_at DESC`, family priority (message before
reaction), then family-local `id DESC`. The signed opaque cursor contains the
last returned item's exact ISO-8601 timestamp, family (`message` or `reaction`),
and database ID. It means “return items strictly after this key.” Consequently,
newer inserts made between requests do not shift page two.

Each family is keyset-filtered in SQL and independently reads at most 11 rows
(page size 10 plus one look-ahead). Merging those two already ordered bounded
windows by the global key is exact: no global top 11 can require the twelfth row
of either family. Only the selected 10 objects are serialized. `next` is emitted
only if the merged look-ahead contains an eleventh item.

## Scale and operational notes

For inboxes of 10, 100, 1,000, or 10,000 items, paginated application work and
serialization remain bounded by the page size rather than growing with total
history. Keyset predicates also avoid `OFFSET`; database scan cost still depends
on the query planner and available indexes. No migration is included because no
PostgreSQL `EXPLAIN` evidence was collected. Candidate composite indexes should
only be evaluated later against production-like data and the complete privacy
filters.

Privacy/authentication query predicates are shared by legacy and paginated
paths. The mark-as-read route and all other feeds are untouched. Residual risk is
limited to frontend integration: it must append `results`, follow `next`, and
deduplicate using the existing item IDs without re-sorting pages.
