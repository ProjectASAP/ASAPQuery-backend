# ASAPQuery-backend control-plane design

> Status: active

## TL;DR

These documents describe only the design owned by ASAPQuery-backend: its
integration boundary with ASAPPlanner, physical compilation for the collector
and backend, and the plan consumed by the ASAPQuery data plane.

ASAPPlanner owns PromQL parsing, pre-ASAP and post-ASAP IR, query-to-summary
mapping, accuracy reasoning, workload sharing, candidate generation, and
logical plan selection. Refer to the
[ASAPPlanner repository](https://github.com/ProjectASAP/ASAPPlanner) for those
designs; they are intentionally not repeated here.

## Documents

| Document | ASAPQuery-backend-owned scope |
| --- | --- |
| [ASAPPlanner integration](asapplanner-integration.md) | Ownership boundary and integration contract for consuming a selected logical workload plan. |
| [Physical planning](physical-planning.md) | Placement, windows, representation, transmission, and compilation into matching collector and backend plans. |
| [BackendPlan](backend-plan.md) | Versioned contract installed and executed by the ASAPQuery data plane. |

## Developer documentation

- [Planner adapter and physical compiler](developer_docs/planner-and-physical-compiler.md)
- [Runtime plan publication](developer_docs/runtime-plan-publication.md)

The corresponding collector-facing plan is documented by
[ASAPCollector](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md).

## Scope rule

A design belongs here only when ASAPQuery-backend owns the decision or must
enforce the contract at runtime. If ASAPPlanner owns the decision, this
directory links to Planner instead of maintaining a second description.
