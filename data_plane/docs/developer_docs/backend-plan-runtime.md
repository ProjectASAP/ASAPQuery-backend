# BackendPlan installation and runtime state

> Implementation status: partial; atomic plan storage exists, while complete
> staging, lifecycle, and cross-runtime activation checks remain target work.

## Purpose

This component validates, stages, and atomically installs the BackendPlan
produced by the control plane. Query and ingest operations take stable snapshots
of the active plan; they do not observe a partially written plan.

Design source: [BackendPlan](../../../control_plane/docs/backend-plan.md).

## Current code map

| Responsibility | Current entry point |
| --- | --- |
| Shared BackendPlan types/decoder | [`control_plane::backend_plan`](../../../control_plane/src/backend_plan/mod.rs) |
| Atomic runtime holder | [`HotReloadBackendPlan`](../../src/storage_engines/types/hot_reload_config.rs) |
| HTTP installation surface | [`drivers/query/servers/http.rs`](../../src/drivers/query/servers/http.rs) |
| Plan-aware readout | [`l4_readout.rs`](../../src/query_engines/asap_query_engine/l4_readout.rs) |

The current `HotReloadBackendPlan` is an `ArcSwap` holder. Atomic pointer swap
is necessary but not sufficient: target validation, version ordering, staged
activation, expiry, and matching collector evidence belong around that holder.

## Installation pipeline

```text
received BackendPlan
      |
decode and closed-schema validation
      |
capability + state-compatibility validation
      |
stage materializations/routes
      |
match CollectorPlan application evidence
      |
atomic activation
```

Validation must complete before replacing the active snapshot. Readers already
holding the previous snapshot may finish, but a single request must not mix plan
versions.

## Required validation

- plan/version/lifecycle ordering;
- immutable content for repeated `(plan_id, plan_version)`;
- unique materialization and route identity;
- known family, algorithm, parameters, grouping, windows, and readout;
- backend capability support for ingest, merge, representation, and readout;
- route references resolve to declared materializations or exact fallback;
- result guarantees meet the route's declared requirement; and
- collector compatibility/evidence matches before activation.

Reject the whole plan on failure. Do not drop one invalid route and activate the
rest unless a future schema explicitly defines partial activation semantics.

## Reader contract

Ingest and query entry points obtain one active-plan snapshot at operation
start. They pass it through validation/routing rather than consulting global
state repeatedly. State emitted under another version is not accepted merely
because its summary bytes decode.

## Current migration boundary

`StreamingConfig`, `BackendStorageRouting`, and BackendPlan currently coexist.
Treat the first two as legacy runtime inputs being absorbed into BackendPlan.
New semantic fields should be added to the versioned plan contract rather than
creating another independently hot-reloaded configuration source.

## Required tests

- malformed and unknown schema values fail without swapping;
- older/conflicting versions fail;
- concurrent readers see one complete version;
- routes cannot reference missing materializations;
- plan activation waits for matching collector evidence; and
- rollback restores a complete retained snapshot.
