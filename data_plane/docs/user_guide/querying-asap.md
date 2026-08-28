# Querying ASAP

## TL;DR

Send PromQL through the Prometheus-compatible API as you would to an exact
backend. ASAPQuery answers a request from planned summary state when that state
is compatible, complete, and fresh. Otherwise it uses the configured exact
fallback when the active plan permits it, or returns an explicit error.

## Query behavior

A request has one of three outcomes:

1. **Summary-backed result:** the active plan contains a matching readout and
   all required state is ready.
2. **Exact result:** the active plan routes the query to the configured
   Prometheus/VictoriaMetrics-compatible fallback.
3. **Error:** neither route can produce a correct answer.

The data plane never turns missing or stale state into a zero-valued result.

## PromQL examples

ASAP's MVP workload includes aggregation within a series over time:

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
```

aggregation across label groups:

```promql
sum by (region) (http_requests_total)
```

and aggregation across both time and label groups:

```promql
sum by (region) (rate(http_requests_total[5m]))
```

These examples do not promise that every deployment accelerates each query.
The submitted workload, selected Planner result, available summaries, and
active BackendPlan determine the route.

## Accuracy

Some maintained summaries are exact; others have a declared approximation
guarantee. The query response must correspond to the guarantee selected for
that query. ASAPQuery does not silently substitute a less accurate summary.

For validation, compare results with the exact backend over identical samples,
labels, timestamps, and logical query ranges. Missing or additional series are
errors, not values to ignore.

## Freshness

A summary-backed result is served only when its state:

- belongs to the active plan and current run;
- covers the requested logical interval;
- has passed its watermark and allowed-lateness rules; and
- has no missing delta or producer state.

A completed window from an earlier plan is not a fresh answer for the current
plan merely because its metric labels match.

## Fallback and errors

Fallback is explicit plan behavior. Unsupported operators, unavailable summary
coverage, or exact accuracy requirements may be routed to the exact backend.
Fallback transport and query failures remain visible; they are not returned as
successful empty vectors.

Operational errors should identify the failed boundary where possible, such as
unsupported query shape, inactive plan, stale window, missing delta sequence,
incompatible materialization, or exact-backend failure.

## Related documentation

- [Data-plane design](../design_docs/query-execution.md)
- [Summary storage](../../../docs/design_docs/summary-storage.md)
- [ASAPCollector MVP demo runbook](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/user_guide/mvp-demo-runbook.md)
