# Future storage and compression

> Status: dormant
>
> MVP relation: not required for the OTel summary-pipeline MVP.

## TL;DR

This document records future backend storage and representation scopes without
turning unimplemented proposals, analytical estimates, or benchmark snapshots
into current architecture. Each scope requires its own acceptance criteria and
implementation proposal before becoming active.

## Durable summary storage

A future tier may flush immutable summary parts to disk or object storage and
recover them after restart. It must preserve the active storage contract:
materialization identity, logical windows, completeness, delta/checkpoint
lineage, and plan lifecycle.

Example query:

```promql
quantile_over_time(0.99, request_duration_seconds[24h])
```

Serving older persisted panes is valid only when they are compatible and fully
cover the requested interval.

## Semantic compaction

Compaction may merge adjacent panes to reduce objects or read work. It is legal
only for a mergeable summary and must not cross incompatible materialization,
accuracy, grouping, or representation boundaries.

Example: sixty compatible one-minute quantile-summary panes may compact into
one one-hour pane for:

```promql
quantile_over_time(0.95, request_duration_seconds[1h])
```

The actual PromQL mapping remains Planner-owned; this example describes only
the storage operation after a valid plan exists.

## Backfill and refresh

Backfill may build missing summary windows from an exact retained source. It
must be deterministic for the same source snapshot and materialization
contract, isolated from live ingestion, and atomically publish coverage.

## Pluggable service mode

Summary storage may eventually run as an in-process library or a separate
service. Both deployment shapes must expose equivalent semantic validation,
readiness, and error behavior. Network transport must not become a second
planning or identity authority.

## Compression and representation

Future work may include sparse summary state, delta checkpoints, compressed
raw fallback blocks, shared timestamp columns, or family-specific encodings.
Every representation must declare compatibility, recovery, and accuracy
effects. Lossy compression cannot be presented as exact.

Example workload:

```promql
sum by (service) (rate(http_requests_total[1h]))
```

Compression is evaluated on the state selected for this workload; it does not
change the logical query or choose a different summary.

## Sampling and learned summaries

Sampling, wavelets, anomaly models, or learned summaries require Planner-owned
logical semantics and guarantees before backend support. The backend may store
and execute an accepted family but must not define its query mapping locally.

## Profiling and cost inputs

A profiler may measure update cost, merge cost, readout latency, memory, and
encoded size for Planner's cost model. Measurements must identify the summary
implementation, parameters, workload, and hardware. Checked-in estimates or
one-off benchmark results are not substitutes for reproducible artifacts.

## Activation rule

A future scope becomes active only when it has:

- an owning component and stable semantic interface;
- predeclared correctness and performance criteria;
- failure and recovery behavior;
- compatibility with BackendPlan and CollectorPlan; and
- reproducible end-to-end validation.
