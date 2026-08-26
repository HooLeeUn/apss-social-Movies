# Profile activity Phase G1: private-message candidate selection

Phase G1 is a backend-only query optimization for the adaptive Candidate's
`private_messages` family. It does not change the HTTP contract, payload,
ordering, timestamps, identifiers, pagination, global hydration, logical
count, certification, batch limits, Legacy builder, or client behavior.

## Query diagnosis and map

The initial queryset is `private_message_candidates_queryset`: mentioned
`Comment` rows authored by an actor and having a target. Its lightweight
projection includes ids and `created_at`, but not `body` or `visibility`.
The adaptive stream joined `target_user`, then evaluated
`Comment.has_valid_target_mention()` once per row.

Before G1 the query map was:

1. one page query for up to `batch_size + 1` comments, including
   `target_user`;
2. per comment, one deferred-field query for `visibility`;
3. per comment, one deferred-field query for `body`;
4. no per-row author, profile, or movie access during selection; those are
   loaded later by the unchanged global hydration stage.

Thus this was a demonstrated `1 + 2N` deferred-field N+1 pattern in candidate
selection, not a serializer N+1. The existing target relation join already
prevented a further target-user lookup.

G1 adds `body` and `visibility` to the adaptive private-message projection and
keeps `select_related("target_user")`. The page and validation now execute as
one query per batch while retaining the exact Python privacy predicate. No
large collection is prefetched and no request-external cache is introduced.

The regression fixture measures one selector query for 1, 10, and 50 valid
messages. PostgreSQL staging latency is intentionally not inferred from this
local query-count test.

## Residual work (not implemented)

Later phases may independently profile global hydration and logical count.
Any index recommendation should be based on PostgreSQL `EXPLAIN` evidence;
G1 adds neither models nor migrations.
