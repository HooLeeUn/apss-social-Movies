# Profile activity Phase F: candidate profiling

Phase F is backend-only observability. It does not change the legacy builder,
candidate ordering or result, logical count, pagination, certification,
fallback, API payload, or any frontend/mobile behavior. The endpoint continues
to return the legacy response while shadow mode runs.

## Staging activation

Enable a fully sampled diagnostic window, then restart the application:

```env
PROFILE_ACTIVITY_SHADOW_ENABLED=true
PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=1.0
PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=true
```

Disable it after collecting the window:

```env
PROFILE_ACTIVITY_SHADOW_ENABLED=false
PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=0
PROFILE_ACTIVITY_CANDIDATE_PROFILE_ENABLED=false
```

All three settings default to the safe/off state. Profiling never causes the
candidate to run when shadow mode is disabled. With candidate profiling off,
the adaptive builder does not create the profiler, install a query wrapper, or
execute detailed timers; only the existing shadow measurements remain.

## Consolidated log

The additional sampled log is a single `PROFILE_ACTIVITY_CANDIDATE_PROFILE`
entry. It contains `k`, legacy/candidate counts, total inspected/logical/
hydrated rows, total candidate SQL query count, frontier, certification,
hydration, and logical-count timings/query counts. `families` contains, for all
nine candidate families, `batches`, inspected `rows`, total `ms`, average batch
milliseconds, and local query count.

Example (values abbreviated):

```text
PROFILE_ACTIVITY_CANDIDATE_PROFILE k=20 total_ms=997.4 queries=31 legacy_count=84 candidate_count=20 rows_inspected=72 logical_candidates=63 hydrated_rows=20 frontier_ms=0.4 certification_ms=0.1 hydration_ms=81.2 hydration_queries=9 logical_count_ms=302.8 logical_count_queries=10 families={'ratings': {'batches': 2, 'rows': 22, 'ms': 44.1, 'avg_batch_ms': 22.05, 'queries': 2}, ...}
```

No SQL, parameters, usernames, email addresses, bodies, comment/message text,
video URLs, or serialized payload values are recorded.

## Phase G hypotheses (not implemented)

The measurements can distinguish expensive candidate selection families,
repeated batch growth, Python/private-hybrid setup, merge/frontier work,
hydration, and logical count. A future Phase G should use staging measurements
to decide whether to inspect query plans/indexes, count subqueries, hydration
relations, or adaptive budget/batch policy. Phase F intentionally changes none
of those queries, indexes, relationships, or thresholds.
