# Summary storage

> Status: active
>
> MVP relation: stores the state needed for summary-backed query execution.

## TL;DR

Summary storage is a plan-aware materialized-state store. It accepts only state
compatible with an active BackendPlan, indexes it by semantic materialization,
series/group, and logical window, and exposes complete state to planned
readouts. It is not a general raw time-series database and does not choose
summaries.

## Stored object

Each stored state belongs to:

- one tenant and source;
- one active plan version;
- one materialization identity;
- one canonical series or aggregation group;
- one logical window;
- one summary family, parameter set, and representation version; and
- one producer/checkpoint lineage when full or delta state is used.

The materialization identity includes the query-relevant semantics required for
safe reuse: source matchers, summarized value, grouping, family, parameters,
accuracy contract, and window definition. Storage location and delivery cadence
do not create a different logical materialization.

## Ingestion

Ingestion validates payload metadata against BackendPlan before changing
queryable state. Full state replaces a declared checkpoint. Delta state applies
only to its expected base and sequence. Duplicate delivery is idempotent;
missing or conflicting sequences create a visible gap rather than guessed
state.

A window becomes queryable only after its completeness and freshness conditions
are satisfied. Payload receipt alone is insufficient.

## Query lookup

The data plane resolves a BackendPlan route to one or more materializations and
logical windows. Lookup returns either:

- the complete compatible state required by the readout; or
- an explicit reason it is unavailable, such as missing coverage, stale state,
  plan mismatch, delta gap, or unsupported merge.

For example:

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
```

may read five compatible one-minute DDSketch panes. The store may compose them
only when they exactly cover the requested interval and share the same
materialization contract.

## Reconfiguration and lifecycle

New and old plan versions may coexist during warm-up and drain, but state is
never mixed across incompatible materializations. Activation makes one version
authoritative for its declared interval. Retirement waits until readers,
lateness, and rollback policy no longer require the old version.

State lifecycle includes staged, active, draining, expired, and rejected
conditions. “Present in storage” is not equivalent to “eligible for query.”

## Memory and persistence

The MVP may use bounded in-memory state, but it must report memory use and fail
visibly when limits prevent correct service. Evicting required state without
changing routing/readiness would violate correctness.

Disk tiers, background flush, compaction, backfill, and standalone storage
service deployment are future extensions described in
[future storage and compression](future-storage-and-compression.md).

## Guarantees

- Incompatible summary states never merge.
- Missing series, groups, or windows are not treated as zero.
- Query responses never combine stale-run and current-run state.
- Accuracy metadata follows the selected logical result; storage does not
  invent a new bound.
- Every accepted payload and served readout is traceable to plan and
  materialization identity.

## Non-goals

This document does not define summary algorithms, Planner candidate selection,
Rust storage types, database schemas, file layouts, cache implementation, or
benchmark results.
