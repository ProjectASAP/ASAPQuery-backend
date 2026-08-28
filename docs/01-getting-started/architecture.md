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

See the [control-plane design](../../control_plane/docs/README.md).

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

See [plan-aware query execution](../../data_plane/docs/query-execution.md).

## State and identity

Three identities remain distinct:

- a **plan identity** versions one deployed physical decision;
- a **materialization identity** describes maintained summary semantics; and
- a **series identity (`sid`)** identifies one canonical metric series.

Conflating them can cause incompatible state reuse. The storage and identity
contracts are described in [summary storage](../design_docs/summary-storage.md)
and [series identity](../design_docs/series-identity.md).

## Component failure behavior

- Planner or physical-compilation failures prevent a new plan from staging.
- Partial plan application leaves the previous valid plan authoritative.
- Incompatible or gapped payloads are rejected by ingestion.
- Missing or stale state prevents summary readout.
- Exact fallback errors are returned as errors, not empty results.

The system fails closed at every boundary where a plausible but incorrect
answer could otherwise be produced.
