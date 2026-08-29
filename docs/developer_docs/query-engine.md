# Developing the query engine

## Architecture

```text
PromQL HTTP adapter
  -> parse/classify query shape
  -> BackendStorageRouting + capability matching
  -> EngineRouter
       +-> ASAPQueryEngine -> SummaryStore readout
       +-> ThanosQueryEngine -> exact archive
       +-> configured Prometheus remote engine
  -> PromQL response + infos
```

ASAPPlanner owns logical query normalization and query-to-summary mapping. The
Backend control plane compiles the selected DAG into BackendPlan routing and
readout instructions. Query engine executes that installed physical contract;
it must not invent a second mapping rule.

## Interfaces and extension rules

`QueryEngine` exposes capabilities and instant/range execution. Routing first
checks requested accuracy and planned storage targets, then state compatibility.
A capability miss may try the configured exact fallback; an engine failure stays
distinguishable from a miss.

New readouts require Planner semantics, a BackendPlan representation,
capability matching, stored-state compatibility, result labels/timestamps,
accuracy/provenance, and exact-fallback tests. New protocols adapt to the same
query request/result contract.

Detailed guides:

- [Routing and readout](query-engine-routing-and-readout.md)
- [BackendPlan runtime](query-engine-backend-plan.md)
- [Extension points](query-engine-extension-points.md)

## Verification

Test planned warm success, incompatible-state miss, missing coverage, exact-only
routing, warm-to-archive fallback, range semantics, annotations, and plan
cutover. End-to-end tests compare against the declared exact baseline.
