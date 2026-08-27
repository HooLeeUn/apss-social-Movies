# Profile activity Phase G5: fetch and materialization diagnostics

G5 is backend-only, request-local instrumentation for the Candidate hydration
used by `GET /api/profile-feed/activity/?scope=me`. It does not alter the API
payload, ordering, privacy predicates, logical count, frontier/certification,
fallback, Shadow's Legacy response, or any client platform. Legacy, G1's
private-message batching, and G2's private-summary batching are unchanged.

## Actual evaluation path

Each of the nine hydration families constructs its existing model QuerySet,
then Django compiles and executes it in `SQLCompiler.execute_sql()`. The
connection execute wrapper measures only cursor execution. Django's
`results_iter()` then fetches rows and applies backend/field converters. At
that real boundary, a request-local `ModelIterable` times each converted row.
It separately times the unchanged `Model.from_db()`, annotation assignment,
known-related caches, and every `select_related` populator. `list(queryset)`
finishes list materialization before the existing family serializer/helper is
called.

This path applies to ratings, public comments, public reactions given, private
messages, videos created, video reactions given, public and private-hybrid
comment-reaction summaries, and video-reaction summaries. Summary families
then retain their existing consolidation step.

## Metric definitions and technical boundary

* `query_build_ms`: wall time spent calling the existing QuerySet builder and
  adding its filter; it ends before evaluation.
* `sql_execute_ms`: time inside Django's request-local connection execute
  wrapper. This is the prior `sql_ms` under an explicit name.
* `row_fetch_and_conversion_ms`: calls to `next()` on `results_iter()`. This
  includes cursor fetch, driver decoding, and Django database/field converters.
  Django does not expose a portable seam between those operations, so G5 does
  **not** claim a separate `cursor_fetch_ms`.
* `model_materialization_ms`: `from_db()`, main-model field assignment,
  annotation assignment, `select_related` instance population, and known
  relation-cache assignment.
* `queryset_iteration_ms` / `materialization_wall_ms`: complete
  `list(queryset)`, including compile, execute, row conversion, model creation,
  iterator/list overhead, and scheduling.
* `materialization_cpu_ms`: process CPU consumed across that same block.
* `serialize_ms`: the existing payload/helper conversion after model objects
  are available.
* `selected_column_count`: `len(compiler.select)` after compilation.
* `select_related_count`: top-level related populators constructed by Django;
  it is structural metadata, not a claimed count of non-null related objects.
* `possible_overfetch`: `true` for these deliberately unchanged full-model
  hydration queries because their models/joins include large text, media/URL,
  metadata, or otherwise unused fields. It is a diagnostic flag only.

SQL compilation is not reported separately: Django's supported evaluation
path combines compiler setup with `execute_sql()`, and forcing an extra compile
would add work and could give a misleading number. Driver decoding cannot be
separated portably from row fetching, and nested `select_related` objects
cannot be counted without walking object graphs and adding material overhead.

The global residual is clamped at zero and is:

`hydration wall - (query build + execute + row fetch/conversion + model
materialization + serialization + summary aggregation)`.

Small disagreement is expected from clock calls, iterator bookkeeping,
scheduling, and the fact that the execute/fetch/model probes are nested inside
the wall-clock iteration probe. The additive accounting uses only the inner,
non-overlapping buckets.

G5 intentionally does not run model-vs-`values()` production benchmarks. Such
queries would add database load and cache-order bias to sampled Shadow requests.
A controlled PostgreSQL benchmark should be performed after staging metrics
identify a family, using identical IDs and alternating several bounded runs.
Likewise no single profiler-overhead number is claimed from SQLite unit tests;
production-like PostgreSQL and representative data are required.
