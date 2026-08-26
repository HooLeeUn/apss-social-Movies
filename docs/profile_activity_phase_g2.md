# Profile activity Phase G2: Candidate hydration

Phase G2 is exclusively a backend/query-level optimization of the adaptive
Candidate hydration for `GET /api/profile-feed/activity/?scope=me`. It does not
change the HTTP contract, payload, localization, ordering, privacy predicates,
frontiers, certification, batch limits, logical count, Legacy, models, or any
mobile/desktop client.

## Diagnosis and flow before G2

After certification, `_hydrate_adaptive_candidates` groups the selected top-K
IDs by activity namespace. Each non-empty ordinary family evaluates one bulk
`pk__in` queryset; comment summaries evaluate public and private reaction
querysets, and video summaries evaluate one video-reaction queryset. Those
querysets already join most payload relations and calculate movie/reaction
metadata in SQL:

| family | hydration | expected base queries |
| --- | --- | ---: |
| ratings | rating + user/profile + movie and movie annotations | 1 |
| public comments | comment + author/profile + movie and annotations | 1 |
| public reactions given | reaction + user/profile + comment/author/profile/movie and annotations | 1 |
| private messages | comment + author/profile + target + movie and annotations | 1 |
| videos created | video + user/profile + movie, reaction stats and annotations | 1 |
| video reactions given | reaction + user/profile + video/owner/profile/movie and annotations | 1 |
| comment summaries received | public and private reaction hydration | up to 2 |
| private hybrid summaries | shares the private half of comment-summary hydration | included above |
| video summaries received | video-reaction hydration | 1 |

This audit did **not** find a general movie, comment, video, sender, recipient,
or reaction-count query per feed item: the existing `select_related` joins and
SQL annotations already bulk those paths. It did find one concrete N+1. The
private comment-reaction queryset joined `comment__author`, but not
`comment__author__profile`. `_serialize_comment_reaction` then calls
`_serialize_actor(reaction.comment.author)`, whose avatar lookup evaluates the
missing reverse one-to-one profile. Consequently private received-summary rows
could issue one extra profile query per hydrated reaction (including repeated
references to the same author because joined rows have separate model
instances). This explains why the observed hydration query total grew with K
despite family-level bulk hydration.

## G2 flow

The private reaction queryset now joins `comment__author__profile` in its one
bulk query. Serialization therefore reads the exact same user/profile/avatar
data without deferred database access. Existing joins for the reaction user,
comment, target and movie remain unchanged, as do all privacy filters and the
Python directed-comment predicate.

Selected identifiers remain in a request-local dictionary keyed by namespace.
It contains only the certified top-K IDs and is discarded after the request;
G2 adds no global cache and does not load an unbounded object set. No new model,
migration, index, Redis dependency, or frontend change is involved.

When Candidate profiling is enabled, hydration evaluation is additionally
measured in aggregate per family under `profile.hydration_families`. The
existing `hydration_ms` and `hydration_queries` remain totals (the latter sums
the new child phases), and profiling-disabled requests execute no extra timers
or queries. There are no per-row logs.

## Verification and expected impact

The regression fixtures cover 10, 50, and 100 private summary items. Hydration
is asserted at two queries for every size: one public-reaction query and one
private-reaction query. A duplicated-reference fixture uses 25 reactions that
share the same comment, movie and author and also remains at two queries while
preserving counts and metadata. A complete adaptive Candidate/Legacy equality
test covers summary payload and ordering and verifies the profiler total.

Before G2, staging reported roughly 7–18 hydration queries and 338–700 ms. G2
does not claim a post-change staging duration: that must be measured in real
PostgreSQL staging. The isolated test expectation after G2 is 2/2/2 queries for
10/50/100 private summary items rather than `2 + N` on that path.

## Boundaries and residual work

Legacy `build_feed`, logical-count queries/algorithm, G1 private-message
selection, frontier/certification and batch sizes are untouched. Result IDs,
timestamps, stable ordering, reaction counts, movie metadata, localization,
privacy and payload construction all continue through the same serializers.
Thus the change is reversible by removing one `select_related` path plus the
candidate-only aggregate instrumentation.

Residual hydration cost comes from the intentionally separate per-model family
queries and their correlated movie-rating annotations. PostgreSQL staging
should confirm wall-clock improvement and query plans. Logical count remains a
known roughly 39-query, 200–300 ms opportunity for G3, but was deliberately not
modified. Any future index or cross-family movie-stat materialization should be
profiled as a separate phase; no such work belongs to G2.
