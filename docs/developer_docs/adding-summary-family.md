# Adding a summary family to ASAPQuery-backend

## TL;DR

ASAPQuery-backend adds runtime support for a summary family only after
ASAPPlanner defines its logical query mapping and guarantee, and the producing
collector/library defines compatible state semantics. The backend must not
invent those contracts locally.

## Ownership prerequisites

Before changing this repository, confirm:

- [ASAPPlanner](https://github.com/ProjectASAP/ASAPPlanner) can represent and
  select the family for concrete PromQL examples;
- the summary library defines parameters, update, merge/readout, encoding, and
  accuracy behavior; and
- [ASAPCollector](https://github.com/ProjectASAP/ASAPCollector) can advertise,
  configure, construct, and transmit the same family/version.

## Backend work

Backend support covers four boundaries:

1. **Capability:** advertise the exact family, algorithm, parameter, readout,
   merge, representation, and full/delta support implemented.
2. **Physical compilation:** accept only selected Planner nodes that can be
   assigned to compatible collector and backend executors.
3. **BackendPlan and ingestion:** preserve the selected contract and reject
   incompatible payloads.
4. **Readout:** execute the declared operation and return aligned
   Prometheus-compatible labels, timestamps, values, and errors.

For example, support for a new quantile family is incomplete until this query
can be planned, produced, ingested, and read end to end:

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
```

## Validation

The cross-repository test must cover:

- supported and deliberately unsupported parameters;
- full-state transmission and delta transmission when claimed;
- duplicate, missing, reordered, stale, and incompatible payloads;
- merge across every claimed grouping/window shape;
- aligned comparison with an identical exact input stream;
- the declared accuracy and freshness SLA; and
- capability downgrade and exact fallback behavior.

Unit tests for serialization or a local readout alone do not establish pipeline
support.
