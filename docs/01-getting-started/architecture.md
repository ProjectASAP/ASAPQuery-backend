# ASAP system architecture

> Status: active

## TL;DR

ASAP separates logical planning, physical control, edge summary maintenance,
and backend query execution. ASAPQuery-backend contains the physical control
plane and data plane; it consumes ASAPPlanner decisions and coordinates with
ASAPCollector.

```text
Query workload
      |
      v
ASAPPlanner -- selected logical workload plan
      |
      v
ASAPQuery control plane
      | physical compile
      +---------------------------+
      |                           |
      v                           v
CollectorPlan                 BackendPlan
      |                           |
      v                           v
ASAPCollector -- summary state --> ASAPQuery data plane
                                      |
PromQL client ------------------------+
                                      |
                         summary readout or exact fallback
```

## Planning path

The workload is planned as a whole so common state can be shared. ASAPPlanner
owns logical parsing, summary alternatives, accuracy reasoning, and selection.
The ASAPQuery control plane owns deployment capabilities, physical placement,
windows, transmission, matching runtime plans, and activation.

See the [control-plane physical-planning design](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design_docs/control-plane/physical-planning.md).

## Ingestion path

ASAPCollector applies CollectorPlan, maintains summaries from observed OTLP
metrics, and transmits raw observations, full state, or deltas as directed. The
data plane accepts a payload only when it matches the active BackendPlan and
stores it under the declared materialization and logical window.

ASAPQuery-backend does not treat legacy Telegraf, OTAP, Kafka, Prometheus
remote-write, or in-backend raw precomputation paths as the MVP architecture.
Future adapters may exist behind explicit interfaces without changing the
primary OTel path.

## Query path

A PromQL request enters through a protocol server and adapter. The data plane
uses BackendPlan to locate a compatible readout and checks plan identity,
coverage, freshness, and state compatibility before execution. If the selected
logical plan requires exact execution, the request is sent to the configured
exact backend.

See [plan-aware query execution](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design_docs/asapquery-backend/query-query-engine.md).

## State and identity

The three levels are related but must not be collapsed:

- a **plan identity** versions one deployed physical decision;
- a **materialization identity** is the stable descriptor/fingerprint for the
  maintained summary semantics: bound source/filter, summarized value,
  reduction and retained grouping keys, aggregation family/algorithm and
  parameters, accuracy contract, window semantics, and compatible state schema;
  and
- a **summary series identity (`sid`)** identifies one canonical materialized
  metric series under that descriptor, including the canonical metric and the
  concrete retained label values. A raw materialization may likewise allocate
  a distinct SID for its raw sample series.

For example, “DDSketch over `request_duration_seconds`, grouped by `service`,
alpha 0.01, one-minute panes” is one materialization definition. Its
`service=checkout` and `service=payments` outputs are two materialized series
and therefore have different SIDs. Changing alpha or retained grouping keys
creates a different materialization definition and cannot reuse either SID.

Conflating them can cause incompatible state reuse. The storage and identity
contracts are described in [summary storage](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design_docs/asapquery-backend/query-summary-store-engine.md)
and [series identity](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design_docs/cross-cutting/summary-series-id.md).

## Component failure behavior

- Planner or physical-compilation failures prevent a new plan from staging.
- Partial plan application leaves the previous valid plan authoritative.
- Incompatible or gapped payloads are rejected by ingestion.
- Missing or stale state prevents summary readout.
- Exact fallback errors are returned as errors, not empty results.

The system fails closed at every boundary where a plausible but incorrect
answer could otherwise be produced.
