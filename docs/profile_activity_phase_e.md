# Profile activity Phase E: controlled shadow mode

Phase E does not change the API contract or response path. The URL, DRF
`PageNumberPagination`, `page`, bounded `page_size`, `count`, `next`,
`previous`, `results`, IDs, ordering, timestamps and payload all continue to
come exclusively from `build_feed()` (legacy). Candidate work is read-only and
cannot mark messages, create notifications, or update timestamps.

## Staging activation and rollback

The default is off, independently of `DEBUG`. Configure staging with:

```text
PROFILE_ACTIVITY_SHADOW_ENABLED=true
PROFILE_ACTIVITY_SHADOW_LOG_SAMPLE_RATE=1.0
```

Restart the application after changing environment variables. Immediately
disable it by setting `PROFILE_ACTIVITY_SHADOW_ENABLED=false` and restarting.
Production should retain `false` until rollout is explicitly authorized.

For each request, legacy is built and paginated normally. After DRF resolves
the page and bounded page size, shadow uses `K = page * page_size`, runs the
adaptive candidate with its existing batch/row guards, strictly compares the
complete candidate dictionaries with `legacy[:K]`, and validates the logical
count against `len(legacy)`. It never supplies candidate results or count to
DRF, including on mismatch, fallback, an uncertified result, or an exception.

Sampled logs contain only match/certification/fallback state, aggregate counts,
K, per-family batches, inspected/hydrated rows, durations, mismatch category,
and SHA-256 fingerprints made from internal IDs, types, timestamps and value
shapes. Bodies, usernames, email, titles, URLs, actor lists and payload values
are excluded.

The dedicated `core.profile_activity_shadow` logger writes `INFO` records to
stdout with `propagate=false`, independently of the root logger's `WARNING`
level. Search Render Application Logs for the stable marker
`PROFILE_ACTIVITY_SHADOW`. At a sample rate of `1.0`, every completed shadow
attempt emits one result record.

Observe certification and match rates, logical-count match rate, fallback
reasons, latency distribution, guard exhaustion, rows inspected/hydrated and
per-family batches on real PostgreSQL. A future candidate response rollout
should require sustained 100% strict and count equivalence, no unexplained
fallbacks, acceptable tail latency/database load, and a reviewed rollback plan.
Because the client response and pagination remain legacy, this phase has no
effect on infinite scroll, onboarding, iOS/iPadOS, Android, tablet or desktop.
