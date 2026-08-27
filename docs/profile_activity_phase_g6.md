# Profile activity Phase G6: what “query build” actually measures

G6 is a backend-only diagnostic for the adaptive Candidate used by
`GET /api/profile-feed/activity/?scope=me`. It changes neither the response nor
Legacy, logical count, frontier/certification, privacy rules, batch limits, or
the G1/G2 query optimizations. Consequently the same JSON contract remains for
iOS/iPadOS, Android, tablet, and desktop. G4 caches and the full temporary G5
profiler are not present.

## Result of the audit

The G5 label `query_build_ms` did **not** prove that Django's lazy QuerySet
construction was slow. It timed the call which prepares a hydration QuerySet;
depending on the measured boundary it could include Python expression setup,
auxiliary database work, connection/cursor acquisition, and/or waiting before
SQL execution. G6 now reports `queryset_definition_ms` separately from
`fetch_ms`, and only when the existing Candidate profiler is enabled. Definition
contains the hydrator and its annotation/Subquery construction; fetch contains
`list(queryset)` and the database round trip. With profiling disabled there are
no new clocks, wrappers, caches, or request work.

The lazy-definition test covers all nine underlying selector sources and shows
**0 queries during definition, 1 after explicit evaluation**. Adaptive sequence
setup has exactly **1 auxiliary query before/after G6**: the private received
comment-reaction hybrid must materialize candidate rows to run
`Comment.has_valid_target_mention()` safely in Python. It is used by one family,
is not duplicated across families, and was therefore not removed or cached.

## Builder map

| Candidate family | principal builder and helpers | preparation/evaluation |
|---|---|---|
| ratings | `rating_candidates_queryset`; hydration `hydrate_rating_ids` → `rating_activity_queryset`; movie display/viewer/following rating Subqueries | lazy annotations; no auxiliary query |
| public_comments | `public_comment_candidates_queryset`; hydration `hydrate_public_comment_ids` → `_public_comment_activity_queryset` → `_annotate_movie_feed` | lazy QuerySet/Subqueries |
| public_reactions_given | `public_reaction_candidates_queryset` plus `user_id`; hydration `_public_reaction_activity_queryset` | lazy filters, exclusions, joins and movie Subqueries |
| private_messages | `private_message_candidates_queryset`; G1 adds predicate fields and `target_user`; hydration `_private_message_activity_queryset` | Candidate evaluation runs the required per-row Python mention predicate, with no G1 N+1 |
| videos_created | `video_created_candidates_queryset`; hydration `video_reaction_created_queryset` and `with_reaction_stats` | lazy annotations/Subqueries |
| video_reactions_given | `video_reaction_candidates_queryset` plus `user_id`; hydration `_video_reaction_activity_queryset` | lazy filters, joins and annotations |
| comment summaries received | `_public_received_reaction_rows`, a latest-id correlated Subquery, `values/Max/Count`, `_adaptive_group_fetcher`; hydration public reaction builder | group QuerySet remains lazy until page fetch |
| private comment-summary hybrid | `private_comment_received_logical_candidates` → `_private_received_reaction_rows` | deliberately evaluates one `select_related/only` QuerySet, iterates it, validates mentions, and builds one request-local dictionary/list |
| video summaries received | `_video_received_reaction_rows`, latest-id Subquery, `values/Max/Count`, `_adaptive_group_fetcher`; hydration video reaction builder | group QuerySet remains lazy until page fetch |

Arguments computed before these builders are the authenticated user's ID, the
single `actor_ids` list for `scope=me`, selected IDs grouped by namespace, and
deduplicated summary ID lists. The correlated Subquery/OuterRef, Exists-like
visibility exclusions, annotations, and following-rating QuerySets are SQL
expressions and do not evaluate while being defined. No hidden `first`, `last`,
`exists`, `aggregate`, `count`, boolean conversion, tuple/set conversion, or
evaluated `values_list` occurs in the adaptive builders. Explicit `list()` calls
occur at batch fetch/hydration boundaries and in the private hybrid described
above. Logical-count evaluations are separate and unchanged.

## Code versus infrastructure

The audit found no repeated friendship, visibility, permission, allowed-ID, or
profile query in `scope=me`, so a shared request context would add complexity
without removing work. QuerySet construction performs no cursor acquisition or
transaction/connection initialization. Those happen on evaluation. Therefore
wall time greatly exceeding CPU time, and otherwise 3–5 ms SQL calls sometimes
spiking to 80–90 ms, are consistent with waiting outside Python: a cold/reused
connection, network/database scheduling, contention, or free-tier Render/Postgres
variability. The repository alone cannot distinguish those causes, and G6 does
not change global connection or transaction settings or claim staging timings.

## Decision and residual risk

No safe code optimization was applied because no duplicated auxiliary query or
expensive eager helper was found. Removing the one private-hybrid query would
weaken privacy; caching it globally would violate request isolation. High
selected-column counts remain a later concern, but G5 showed negligible row
fetch/conversion, so G6 intentionally adds no `only`, `defer`, `values`, or
`values_list` projection optimization.

For G7, collect several cold and warm PostgreSQL staging samples using the new
definition/fetch boundary plus connection age and database-side
`EXPLAIN (ANALYZE, BUFFERS)` for a spiking family. This can separate ORM setup
from connection/network/Render scheduling and query-plan variance before any
query or infrastructure change is proposed.
