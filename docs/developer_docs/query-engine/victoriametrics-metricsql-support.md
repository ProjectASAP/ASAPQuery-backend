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

This boundary adds no variants to SDS, QueryExpr, or the shared physical DAG.
`QueryPlanEntry.language` records which frontend produced the canonical query
identity. Native acceleration for additional MetricsQL syntax must be added in a
MetricsQL-specific frontend and lowered to existing canonical operations only
when the equivalence is defined.

## Configuration

Run an independent listener with `--victoriametrics-http-port` and select the
exact backend with `--victoriametrics-url`. The ordinary Prometheus listener and
its fallback remain unchanged.

The listener also accepts VictoriaMetrics cluster paths
`/select/{tenant}/prometheus/api/v1/query` and
`/select/{tenant}/prometheus/api/v1/query_range`. The path tenant scopes the
installed routing snapshot and is preserved in the exact fallback URL.

The control plane lowers the MetricsQL AST to `QueryExpr`, invokes the shared
ASAP-aware physical compiler, and publishes a language-tagged entry in the
authoritative `QueryPlan`. `canonical_query` is the frontend's AST identity;
the language tag prevents a MetricsQL request from resolving a PromQL entry.

The backend stages and activates that `QueryPlan` atomically with the SDS
catalog, precompute plan, and transmission plan. MetricsQL bindings therefore
receive the same SummaryCatalog validation, including pane duration and pane
origin, as PromQL bindings. The executor reads the installed entry directly;
it does not clone the DAG into a compatibility view. A plan miss, incomplete
coverage, validation failure, or execution failure routes the original request
to VictoriaMetrics.

## MetricsQL operator coverage

| Construct | Canonical acceleration | Boundary behavior |
| --- | --- | --- |
| metric selectors and one matcher set | yes | AST lowers to a time-series scan |
| explicit positive range selectors | yes | lowers to `TimeRange` |
| `default_rollup` with explicit range; `last/first/avg/min/max/sum/count/stddev/stdvar_over_time`; `rate`, `irate`, `increase`, `changes`, `delta`, `idelta`, `deriv`, `resets`, `mad`, `present`, `absent`, and numeric `quantile_over_time` | yes | exact arity is required |
| `sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, numeric `quantile`, with `by`/`without` | yes | exact arity is required |
| scalar unary and binary arithmetic/comparison/set operators without vector modifiers | yes | lowers to canonical binary nodes |
| `keep_metric_names`, aggregate `limit`, implicit `default_rollup`, offsets, `@`, subquery/inherited steps, OR-delimited matcher groups, binary vector matching, `if`, `ifnot`, `default`, non-rollup functions, unsupported aggregates, malformed or extra arguments | no | typed frontend rejection, then exact VictoriaMetrics fallback |

The vendored upstream parser currently has 21 known upstream-baseline failures
and three parser-support compatibility failures in its broader internal suite.
They cover WITH expansion, OR matcher/tokenization, filter pushdown, and
simplifier behavior. Those constructs are outside the accelerated subset and
remain in the fail-closed fallback denominator; they are not reported as
accelerated queries.

## Verification

Focused tests cover request parsing, response compatibility, canonical binding,
strict aggregate arity, language-tagged QueryPlan serialization and validation,
atomic installation, tenant-prefixed fallback, upstream status/header
preservation, and instant/range fallback.
