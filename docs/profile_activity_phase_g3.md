# Profile activity Phase G3: hydration diagnostics

G3 is backend-only instrumentation for the existing candidate shadow path. It
does not change the API response, mobile/desktop clients, ordering, logical
count, frontier/certification, Legacy, or the G1/G2 query optimizations.

## Hydration map

The certified candidate list is grouped by namespace and follows this path:

1. selected candidate IDs;
2. one evaluated, related/annotated queryset for each ordinary family;
3. queryset serialization, including actor properties, movie metadata, rating
   annotations and final payload dictionaries;
4. public/private comment and video reaction summary querysets;
5. reaction serializers, mention validation where applicable, and summary
   consolidation (like/dislike counts and latest reaction);
6. final cross-family extension, stable sort, and hydrated-order check.

The profile reports SQL wall time from the local Django execute wrapper, Python
time as hydration total minus SQL, fetch/serialization/aggregation timings per
family, row/query counts, and accounted/unaccounted time. Component fields for
payload construction, auxiliary lookups, reaction counts, movie fetch/metadata,
localization and other Python work are numeric-only. Work inseparable from the
current serializer is conservatively included in `serialization_ms`; SQL
annotations are included in family fetch/SQL time. The residual exposes loop,
dispatch, list-extension and clock/bookkeeping time rather than hiding it.

No SQL text, bind parameters, object values, usernames, email addresses,
message/comment bodies, or video URLs are retained or logged.

G3 deliberately applies no performance optimization: local measurements are
required from real staging PostgreSQL before a bottleneck can be demonstrated.
If SQL dominates, inspect the slow family query plan; if serialization dominates,
profile that family's movie/reaction payload helpers. Logical-count work remains
reserved for G4.
