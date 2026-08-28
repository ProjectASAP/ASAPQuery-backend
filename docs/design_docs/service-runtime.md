# Backend service runtime

## TL;DR

ASAPQuery-backend separates control-plane decisions from data-plane execution.
The control plane turns workload intent into versioned collector and backend
plans. The data plane ingests summary OTLP, installs backend routing state, and
serves a PromQL-compatible API from the warm summary tier or a configured exact
backend.

**Status:** active MVP design

**Audience:** developers and architects integrating or operating the backend.

## Problem and scope

A query result crosses planning, plan publication, ingest, storage, routing, and
readout. Component-local health can therefore coexist with an unusable query
path. This document defines the service-level flow, ownership boundaries, and
observable acceptance behavior. Detailed plan schemas, storage structures, and
operators remain in their focused documents.

## End-to-end flow

```text
workload request
      |
      v
control plane ---- collector plan ----> ASAPCollector
      |                                      |
      +---- backend plan                     | summary/raw OTLP
                 |                           v
                 +--------------------> data plane
                                           |
PromQL request ----------------------> route + readout
                                           |
                         warm summary <----+----> exact backend
                                           |
                                     PromQL response
```

The response includes provenance and, for approximate results, an accuracy
annotation. A syntactically successful response without the expected source and
plan-compatible state does not establish correct execution.

## Runtime boundaries

| Boundary | Input | Output | Failure behavior |
| --- | --- | --- | --- |
| Workload analysis | Query workload and accuracy target | Logical summary requirements | Reject unsupported or invalid intent |
| Physical compilation | Logical requirements and topology | CollectorPlan and BackendPlan | Do not emit incompatible partial plans |
| Publication | Versioned physical plans | Runtime application reports | Preserve the last valid plan on rejection |
| Data-plane ingest | Modified OTLP summary envelopes | Validated windowed summary state | Reject incompatible identity, family, parameters, or sequence |
| Query routing | PromQL request and installed routing | Warm or exact engine selection | Fail explicitly or use only the configured fallback |
| Readout | Compatible stored state | PromQL result with annotations | Never substitute a plausible incompatible summary |

## Authoritative definitions

- ASAPPlanner owns logical query normalization and query-to-summary mapping.
- The ASAPQuery control plane owns physical placement, plan partitioning,
  versioning, and runtime publication.
- ASAPCollector owns collector-plan application and edge summary production.
- The ASAPQuery data plane owns backend-plan application, summary storage,
  routing, readout, and response annotations.
- Summary libraries own encoding and mathematical guarantees.

These boundaries avoid parallel definitions of query semantics and summary
compatibility.

## Design decisions and alternatives

- **Separate control and data planes:** runtime execution remains available while
  planning is temporarily unavailable. Embedding every planning concern in the
  query path would couple query latency and availability to replanning.
- **One PromQL-compatible query surface:** clients do not select an internal
  engine for ordinary operation. Separate public warm and exact APIs would push
  plan knowledge into every caller.
- **Explicit provenance:** routing choices are observable in responses. Inferring
  the tier from latency or result shape is ambiguous.
- **Fail closed on incompatibility:** exact fallback is allowed only when it is
  configured and identified. Using an unrelated summary is never a fallback.

## Acceptance behavior

An end-to-end check supplies a declared workload, observes compatible plan
versions at collector and backend, ingests deterministic metrics, and queries
both planned and unsupported shapes. Planned shapes must use compatible stored
state and meet their accuracy/freshness contract. Unsupported shapes must reach
the declared exact tier or return an explicit error. Restart and stale-plan
tests must show that the last valid state is preserved or safely reconstructed.

## Risks and exit criteria

Version skew, inconsistent series identity, stale routing, and incomplete delta
sequences can all yield believable wrong answers. Release evidence must exercise
these boundaries, not only parser or storage unit tests. The service is ready for
a declared workload when plan application, ingest, routing, result provenance,
accuracy, and freshness are observable for the same run.

## Related documents

- [Physical planning](../../control_plane/docs/physical-planning.md)
- [BackendPlan contract](../../control_plane/docs/backend-plan.md)
- [Plan-aware query execution](../../data_plane/docs/design_docs/query-execution.md)
- [Summary storage](summary-storage.md)
- [Series identity](series-identity.md)
