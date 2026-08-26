# Profile activity feed — Phase B

Phase B introduces an internal candidate for `GET /api/profile-feed/activity/?scope=me`.
The view still calls `SocialActivityFeedService.build_feed`; it does not call the
candidate. Consequently the URL, response serializer, PageNumber pagination,
IDs, timestamps, ordering and payload contract remain unchanged for every client.

Each of the seven source families now has a lazy, lightweight selector and an
ID hydrator. Hydration restores the relationships and annotations needed by the
payload converters. Private messages and private reactions deliberately retain
`has_valid_target_mention()` as a post-hydration Python check: equivalence of its
token/username semantics in SQL has not been established.

`build_feed_candidate_full_materialization` evaluates every selector, hydrates
every selected ID, runs the existing legacy summary consolidation, and applies
the legacy global key. It has no limit, batches, keyset boundary, grouped SQL, or
adaptive merge. The extra selector query per family is fixed rather than per
item; Phase B is an equivalence architecture, not a query-count optimization.

Phase C should retain the same converters and consolidation contract while
designing group-aware selection for received reactions. It should first prove a
safe candidate frontier (including groups which collapse to summaries), measure
source rows versus logical items, and only then introduce adaptive hydration.
No part of that proposal is implemented here.

