# Profile activity feed — Phase D adaptive certified top-K

Phase D is an internal service only. `ProfileFeedActivityView` still invokes
`build_feed()` and no flag, view, serializer, paginator, model, migration,
rating-only endpoint, URL, or frontend file changed. Consequently PageNumber,
`page`, `page_size`, `count`, `next`, `previous`, `results`, IDs, types,
timestamps, actors, movies, payloads, summary IDs, and ordering remain governed
by legacy production code. Infinite scroll, deduplication, React keys, cards,
onboarding, and scroll behavior are unchanged. iPhone/iOS, iPadOS, Android,
tablet, and desktop require no modification.

## Logical streams and keysets

`build_feed_candidate_adaptive(user, scope="me", k=...)` merges these logical
streams: ratings, public comments, public reactions given, private messages,
video created, video reactions given, comment-reaction summaries received, and
video-reaction summaries received. Given reactions remain individual. Received
reactions are one logical item per comment/video-comment, with `MAX(updated_at)`,
the latest tied reaction ID, object ID as entity ID, and the frozen Phase-A
family rank.

Ordinary ORM streams use descending `(candidate_activity_at, id)` keysets and
never growing OFFSET. SQL-grouped public-comment and video summaries use
`(latest_activity_at, object_id)`. Every read requests `batch_size + 1`; the
extra lightweight key is retained as the best-unread frontier. The default
batch is 10 and is configurable. Private messages validate
`has_valid_target_mention()` before becoming candidates. Private received
summaries retain Phase C's hybrid Python validation and lightweight over-read;
an unvalidated row is never a frontier. Privacy/block/self-reaction filters are
unchanged and run before public/video grouping.

## Adaptive proof and hydration

The merge orders only metadata by the frozen total key: effective timestamp,
legacy priority, entity ID, and stable family rank, all descending. Once K
known keys exist, a stream expands only when its frontier is absent or is
greater than or equal to the current Kth key. Equality deliberately expands,
so timestamp/priority/entity ties are resolved through family rank. Selection
stops only when every non-exhausted frontier is strictly below Kth (or every
stream is exhausted). Thus merely collecting K candidates never terminates the
algorithm.

Only after certification are selected IDs grouped by family and passed to the
Phase B/C batch hydrators and converters. Selected summaries hydrate every
currently authorized reaction and reuse `_consolidate_received_reactions()`.
The hydrated output is sorted by the legacy key plus frozen rank and its IDs
must exactly match candidate order; mismatch makes the run uncertified.

Configurable `max_batches` and `max_candidate_rows` prevent loops. Any budget,
privacy/frontier uncertainty, hydration-order mismatch, unsupported scope, or
internal error produces no approximate result: tests/shadow callers may use
the default legacy `[:k]` fallback. The service reports non-serialized,
PII-free diagnostics: certification, fallback reason, batches per family,
source rows inspected, logical keys inspected, and hydrated rows. Query counts
remain distinct from row counters; batching introduces no query per selected
item. No production shadow execution was added.

## Test coverage and structural benefit

Phase D tests compare strict activity dictionaries against `build_feed()[:k]`
for K 1/10/20/30/50, small batches, heavily rating-skewed and empty families,
updates/ties, and guard-rail fallback. A 12-source-row received group remains
one logical candidate while mixing enough other activity to exceed the initial
batch. Diagnostics demonstrate source rows inspected separately from at most K
hydrated activities; they do not claim synthetic timings. Phase A/B/C tests are
unchanged.

PostgreSQL-backed tests are the authority for SQL keyset behavior and exact
contract equivalence. Phase E may, after production-like shadow validation,
design logical count and PageNumber integration behind a default-off rollout.
It must not be inferred from or activated by Phase D.
