# Profile activity Phase G4: request-local Python memoization

G4 is exclusively a backend optimization in candidate hydration. It leaves the
HTTP payload and the iOS, iPadOS, Android, tablet and desktop clients unchanged.
It does not change querysets, logical count, Legacy, G1/G2 batching, frontier,
certification, fallback, ordering or batch-size limits.

## G3 evidence and redundant work

The staging samples supplied for G3 rank hydration Python first (roughly
381–666 ms), SQL second (roughly 17–44 ms), with stable hydration query counts.
Within the available G3 component map, evaluated-family serialization is the
largest attributable Python section; summary aggregation follows, while fetch
SQL is small. G3 does not split individual dictionary literals from serializer
time, so no finer numeric ranking is claimed.

Code inspection then demonstrates the repeated work: every serialized row
called `_serialize_actor` and `_serialize_movie`, including reaction-summary
rows. Reactions also serialized the owner for every row, and private messages
built the same compact recipient twice. Thus repeated movies and users caused
the same field/property reads, avatar/media URL resolution and dictionary
construction even though their fragments were equal.

## Optimization

`_hydrate_adaptive_candidates` now creates one `_CandidateSerializationCache`.
It is passed through ordinary families and both received-summary hydrators and
becomes unreachable when that candidate hydration returns. There is no global,
Django, Redis or cross-request cache.

* Actor fragments use `user.id`.
* Compact-user fragments use `user.id`.
* Movie fragments use `(movie.id, display_rating, my_rating,
  following_avg_rating, following_ratings_count)`. The annotation context is
  part of the key, preventing a private/unannotated fragment from replacing an
  annotated one. Both English and Spanish titles remain in the unchanged
  fragment, so no request language can contaminate another.

Before G4, common-fragment construction is O(N) for N hydrated rows. After G4
it is O(U + M), where U is the number of distinct users and M the number of
distinct movie/annotation tuples; activity-specific dictionaries remain O(N).
Memory is O(U + M), bounded by the already certified top-K plus actors found in
its summary rows, and released after hydration.

No sorting or copying was removed: the final correctness sort remains required,
and the code contained no safe redundant deep copy. Reaction counts and summary
grouping are unchanged. A later phase may use staging G3 metrics to split the
remaining family serialization time more finely or investigate logical count;
neither is implemented here.
