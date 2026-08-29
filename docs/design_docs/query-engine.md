# Plan-aware query execution

> Status: proposed
>
> MVP relation: required for every summary-backed query and exact fallback.

Developer guides:
[BackendPlan runtime](../developer_docs/query-engine-backend-plan.md),
[OTLP summary ingestion](../developer_docs/ingest-engine-otlp.md), and
[query routing/readout](../developer_docs/query-engine-routing-and-readout.md).

## TL;DR

The data plane accepts only state compatible with its active BackendPlan. At
query time it matches the PromQL request to a planned readout, checks coverage
and freshness, reads the matching materialization, and returns a
Prometheus-compatible result. A query that cannot be served safely is rejected
or sent to the exact fallback selected by the plan.

## Data flow

```text
ASAPCollector -- OTLP summary state --> ingest validation --> SummaryStore
                                                        |
PromQL request --> protocol adapter --> planned routing + readiness
                                                        |
                              summary readout <----------+
                                      |
                         Prometheus-compatible response

Unsupported/planned-exact query --> exact fallback backend
```

The data plane never infers a summary family from a metric name or query text.
It uses the materialization and readout declared by BackendPlan.

## Ingestion contract

Before accepting a payload, the data plane validates:

- plan and plan-version identity;
- materialization, producer, and tenant identity;
- summary family, parameters, grouping, window, and representation;
- full/delta sequence and checkpoint requirements; and
- lifecycle and schema compatibility.

Unknown, expired, reordered, or incompatible state is rejected and surfaced.
Receiving bytes is not evidence that the corresponding window is queryable.

## Query routing

Routing has three outcomes:

1. **Summary readout** when BackendPlan contains a compatible route and the
   required windows are fresh and complete.
2. **Exact fallback** when BackendPlan explicitly routes the query shape to an
   exact backend.
3. **Explicit failure** when neither route is valid.

A store miss, stale window, or incompatible payload must not be converted into
an empty or plausible approximate result.

## Supported aggregation shapes

The MVP exercises these shapes without defining Planner's query-to-summary
rules here:

- within one series over time:

  ```promql
  quantile_over_time(0.95, request_duration_seconds[5m])
  ```

- across label groups at an evaluation timestamp:

  ```promql
  sum by (region) (http_requests_total)
  ```

- across both a time range and label groups:

  ```promql
  sum by (region) (rate(http_requests_total[5m]))
  ```

Whether a particular expression is exact, summary-backed, or unsupported is
the selected plan's decision. The data plane only executes that decision.

## Readiness and freshness

A readout is ready only when all materializations required by its route:

- belong to the active plan version;
- cover the requested logical interval;
- satisfy watermark and allowed-lateness policy;
- have no unresolved delta gap; and
- meet any declared source-completeness requirement.

For example, a query at `12:05` over `[5m]` cannot reuse complete panes from an
earlier run merely because their labels match. The plan identity and logical
window must also match.

## Result semantics

The response preserves Prometheus labels, timestamps, result type, and error
behavior. Summary error guarantees come from the selected Planner result and
are carried by BackendPlan; the data plane neither tightens nor loosens them.

When several physical shards contribute to one result, they may be merged only
if their materialization contracts match and the chosen summary supports the
declared merge.

## Exact fallback

Fallback is a correctness path, not a silent catch-all. BackendPlan identifies
the backend and query scope eligible for fallback. Transport failures and exact
query errors remain visible to the caller.

Examples that may require exact fallback include an unsupported PromQL
operator, a request outside retained summary coverage, or a query whose exact
accuracy requirement has no compatible maintained state.

## Non-goals

This document does not define PromQL parsing, summary selection, Planner IR,
summary algorithms, state byte encoding, storage-engine implementation, or
protocol-specific server code.
