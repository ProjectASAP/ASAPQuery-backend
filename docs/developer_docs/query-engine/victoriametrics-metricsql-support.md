# VictoriaMetrics and MetricsQL query support

This document is for backend developers extending the query surface.

## Architecture

VictoriaMetrics uses its Prometheus-compatible HTTP API, but its query language
is MetricsQL. The protocol boundary is therefore independent while successful
canonical bindings reuse the existing query runtime:

```text
VictoriaMetrics HTTP request
        |
MetricsQL binder
        |-- PromQL-compatible subset --> canonical QueryExpr
        |                                --> ASAPPlanner physical plan
        |                                --> shared DAG executor
        |                                --> SDS lookup --> SummaryStore
        |                                --> VictoriaMetrics result adapter
        |
        `-- unsupported MetricsQL syntax --> VictoriaMetrics exact fallback
```

The VictoriaMetrics adapter accepts instant and range GET/POST requests at
`/api/v1/query` and `/api/v1/query_range`. It preserves the Prometheus-compatible
JSON result and error envelope used by VictoriaMetrics clients and Grafana.

## Language boundary

ASAPPlanner's independent MetricsQL frontend parses a MetricsQL AST and lowers
it directly to the canonical query DAG. Selectors, matchers, range selectors,
aggregates, grouping, binary expressions, and supported calls share the existing
physical planner and DAG executor. `default_rollup(metric[range])` lowers to
`LastOverTime` over `TimeRange`. An implicit `default_rollup(metric)` needs the
runtime evaluation step, and `keep_metric_names` needs metric-name lineage that
the canonical IR does not represent; both fail closed to the configured
VictoriaMetrics backend with the original expression and parameters.

This boundary adds no variants to PromQL, SDS, QueryExpr, or the shared physical
DAG. Native acceleration for additional MetricsQL syntax must be added in a
MetricsQL-specific frontend and lowered to existing canonical operations only
when the equivalence is defined.

## Configuration

Run an independent listener with `--victoriametrics-http-port` and select the
exact backend with `--victoriametrics-url`. The ordinary Prometheus listener and
its fallback remain unchanged.

## Verification

Focused tests cover request parsing, response compatibility, canonical binding
of the common subset, fail-closed handling of MetricsQL-only syntax, and exact
fallback forwarding for instant and range queries.
