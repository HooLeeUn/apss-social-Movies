# Profile activity feed — Phase C logical received-reaction groups

Phase C remains internal to `SocialActivityFeedService`. The production
`ProfileFeedActivityView` still calls `build_feed()`, and rating-only behavior,
serializers, models, migrations, URL routing, and PageNumber pagination are not
changed. Therefore `count`, `next`, `previous`, `results`, IDs, summary IDs,
types, timestamps, actor/movie payloads, ordering, infinite scroll, and the
frontend contract remain byte-for-byte governed by legacy behavior. There is
no iOS/iPadOS impact and no card-remount or scroll change.

## Logical selection

`logical_group_candidates` changes only the internal representation of
received reactions:

* comment namespace: `comment_reactions_received_summary`, key `comment_id`;
* video namespace: `video_reactions_received_summary`, key
  `video_comment_id`;
* effective timestamp: `MAX(updated_at)` (the model field used as
  `activity_at`), with the greatest reaction ID among rows tied at that latest
  timestamp;
* entity ID: the comment/video-comment ID, never a reaction ID;
* family rank: the frozen Phase-A `LEGACY_FAMILY_RANK` value.

Public and video authorization filters are applied to source rows before SQL
`GROUP BY`. They retain self-reaction exclusion and both owner/reactor
visibility-block exclusions. Private received reactions deliberately use a
hybrid selector: one batched query loads minimal reaction and parent-comment
fields, then the exact `has_valid_target_mention()` method runs before grouping.
Privacy correctness takes priority over forcing an unproven SQL translation.

Public and private comment candidates are merged into one dictionary keyed
only by `comment_id`, so their logical count is the union rather than the sum of
two distinct counts. With the current schema, one comment has one current
visibility value, making simultaneous public/private source membership
structurally impossible; the union boundary nevertheless prevents duplicate
summaries if source families overlap in a future compatible representation.
Given public and video reactions remain individual row candidates.

## Hydration, observability, and queries

Selection carries only namespace, object ID, latest effective timestamp,
latest reaction ID, family rank, and source-row count. Actor lists, counters,
movie data, text, and video URLs are not computed there. Selected comment IDs
are hydrated with two batched queries (public and private), and selected video
IDs with one batched query. Existing converters build reaction activities and
`_consolidate_received_reactions()` remains the only summary builder.

The candidate exposes `source_rows` on internal logical candidates, enabling
tests to compare source rows, logical groups, hydrated rows, and final
summaries without PII logging. Selection costs a constant two queries for
comments (public grouped SQL plus private hybrid validation) and one for video;
hydration costs two and one respectively. It never performs a query per group.
Logical count helpers are not connected to a paginator.

Both internal modes remain available: `full_legacy_like` is Phase B's full-row
materialization and `logical_group_candidates` is Phase C's group-aware full
feed evaluation. Tests compare both with legacy, including many-rows/one-summary,
like/dislike switches, deletes, privacy, ties, actor/counter payloads, logical
counts, video fields, and batched hydration.

## Explicitly deferred to Phase D

Phase D may design a globally adaptive merge over the seven families, prove a
certified top-K frontier, and hydrate only groups that can enter a requested
page. It must re-run full contract and pagination equivalence before any
production activation. Phase C implements none of adaptive batching, global
limits, keyset/PageNumber changes, or production routing.
