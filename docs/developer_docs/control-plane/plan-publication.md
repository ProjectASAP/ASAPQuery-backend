# MVP physical-plan publication

This page describes the implemented MVP contract. The control plane compiles
ASAPPlanner's selected post-ASAP IR once and projects that decision into one
authoritative `SummaryCatalog`, matching Precompute, Transmission, and Query
plans, plus one target-specific `CollectorPlan` per Collector.

## API

`POST /api/v1/physical-plan/compile-and-publish` accepts:

- `queries`: query ID, PromQL, metric, window seconds, grouping labels, a
  typed `AccuracyTarget`, and lifecycle evidence (evaluation interval,
  ingestion rate/freshness, optimization horizon, and primitive state costs),
  plus `window_cost_model` and `evaluation_phase_ms` with versioned,
  workload-scoped physical cost evidence;
- `collector_ids`: the required OpAMP agent IDs;
- `capability_snapshot_id` and the exact `planner_revision`;
- `plan_version`, activation/optional expiry timestamps, and the exact backend
  compatibility identity;
- optional per-query TopK evidence, with `max_evidence_age_ms`; and
- `apply_timeout_ms` (default 10000).

Unknown JSON fields, empty target/query sets, zero windows/timeouts, stale
evidence, and a Planner revision mismatch are rejected. The response is only
successful only after every target has applied the exact generated
`(plan_id, plan_version)` and the backend activates that generation.

The physical compiler passes normalized recurrence, data-arrival evidence,
runtime capabilities, and complete implementation costs to ASAPPlanner's
summary-maintenance lifecycle and abstract-window selection. It retains the
concrete implementation identity corresponding to Planner's selected
framework and copies both decisions into each Collector materialization.
Missing, stale, or incomplete implementation evidence fails closed. For the
current tumbling-window Collector runtime the executable commitment is
`continuously_maintained / incremental / per_update / summary_state`; missing
or stale lifecycle evidence, a non-tumbling selected framework, mismatched pane
width, and any unexecutable lifecycle fail closed.

## Installation order and failure semantics

```text
validate request and compile one bundle
              |
              v
preflight every Collector capability
              |
              v
POST one atomic SummaryCatalog + PrecomputePlan + TransmissionPlan + QueryPlan bundle (stage)
              |
              v
publish target-specific CollectorPlans over OpAMP
              |
              v
require exact (agent_id, plan_id, plan_version, APPLIED) from every target
              |
              v
wait until activation and atomically activate backend snapshot
```

Preflight happens before staging. Publication repeats capability validation to
close disconnect races. A missing backend endpoint, backend non-2xx, Collector
disconnect, timeout, malformed report, wrong identity/version, incompatible
schema, or `FAILED` status fails the request. A failed rollout leaves the
previous active generation untouched; a staged generation never serves before
explicit activation.

## OpAMP custom capability

The exact capability and message types shared with ASAPCollector are:

| Field | Value |
| --- | --- |
| capability | `io.projectasap.collector-plan.v1` |
| server-to-agent message type | `collector_plan` |
| agent-to-server message type | `plan_status` |
| payload encoding | UTF-8 JSON |

`collector_plan` is the serialized `physical::compiler::CollectorPlan`. A
status payload is strict JSON:

```json
{"plan_id": 42, "plan_version": 3, "status": "APPLIED", "error": null}
```

`status` is exactly `APPLIED` or `FAILED`. Transport/config acknowledgements
are not accepted as semantic plan evidence. Reports are scoped to the agent ID
of the WebSocket connection, preventing one Collector from acknowledging
another Collector's plan.

## Backend wire contract

The authoritative SummaryCatalog snapshot is embedded with the typed PrecomputePlan,
TransmissionPlan, CollectorPlans, and QueryPlan in `POST /api/v1/physical-plan`.
The backend validates all shared
identities, fingerprints, schemas, parameters, producers, and lifecycle fields
before returning `staged`; `POST /api/v1/physical-plan/activate` performs the
single immutable-snapshot swap.
