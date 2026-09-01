# MVP physical-plan publication

This page describes the implemented MVP contract. The control plane compiles
ASAPPlanner's selected post-ASAP IR once and projects that decision into one
typed `BackendPlan` plus one target-specific `CollectorPlan` per Collector.

## API

`POST /api/v1/physical-plan/compile-and-publish` accepts:

- `queries`: query ID, PromQL, metric, window seconds, grouping labels, and a
  typed `AccuracyTarget`;
- `collector_ids`: the required OpAMP agent IDs;
- `capability_snapshot_id` and the exact `planner_revision`;
- optional per-query TopK evidence, with `max_evidence_age_ms`; and
- `apply_timeout_ms` (default 10000).

Unknown JSON fields, empty target/query sets, zero windows/timeouts, stale
evidence, and a Planner revision mismatch are rejected. The response is only
successful after every target has applied the same generated `plan_id`.

## Installation order and failure semantics

```text
validate request and compile one bundle
              |
              v
preflight every Collector capability
              |
              v
POST typed BackendPlan protobuf; require 2xx
              |
              v
publish target-specific CollectorPlans over OpAMP
              |
              v
require exact (agent_id, plan_id, APPLIED) from every target
```

Preflight happens before backend mutation. Publication repeats capability
validation to close disconnect races. A missing backend endpoint, backend
non-2xx, Collector disconnect, timeout, malformed report, wrong plan ID, or
`FAILED` status fails the request. This path intentionally does not inherit the
legacy replanner's best-effort behavior.

The MVP endpoint installs the backend before enabling new producers. It does
not claim distributed atomic activation or rollback; those remain post-MVP
work. If a Collector fails after backend installation, the backend has a
superset accepting view but the request fails and no success is reported.

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
{"plan_id": 42, "status": "APPLIED", "error": null}
```

`status` is exactly `APPLIED` or `FAILED`. Transport/config acknowledgements
are not accepted as semantic plan evidence. Reports are scoped to the agent ID
of the WebSocket connection, preventing one Collector from acknowledging
another Collector's plan.

## Backend wire contract

The matching `BackendPlan` uses the protobuf contract documented in
`control_plane/docs/design-backend-plan-wire-format.md` and is sent to
`POST /api/v1/backend-plan` with `application/x-protobuf`. Both projections
carry the same numeric `plan_id`; the compiler, not either transport, owns the
materialization choice.
