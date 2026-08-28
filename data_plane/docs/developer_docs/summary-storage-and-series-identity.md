# Summary storage and series identity

> Implementation status: partial; the store/index and SID resolver exist, while
> plan/materialization lifecycle convergence remains target work.

## Purpose

The store maintains plan-compatible summary materializations by canonical
series/group and logical window. The SID registry provides stable metric-series
identity; it does not replace plan or materialization identity.

Design sources:

- [Summary storage](../../../docs/design_docs/summary-storage.md)
- [Series identity](../../../docs/design_docs/series-identity.md)

## Current code map

| Responsibility | Current entry point |
| --- | --- |
| Store/index and metadata | [`sketch_db/index/mod.rs`](../../src/storage_engines/sketch_db/index/mod.rs) |
| Epoch data layout | [`sketch_db/index/epoch_columnar.rs`](../../src/storage_engines/sketch_db/index/epoch_columnar.rs) |
| Summary payload/config types | [`sketch_db/data/mod.rs`](../../src/storage_engines/sketch_db/data/mod.rs) |
| Window/timeline query | [`sketch_db/query/`](../../src/storage_engines/sketch_db/query/) |
| Lifecycle/reconciliation | [`sketch_db/lifecycle/`](../../src/storage_engines/sketch_db/lifecycle/) |
| SID resolution | [`drivers/ingest/series_resolver.rs`](../../src/drivers/ingest/series_resolver.rs) |
| Persistence (future/non-MVP scope) | [`sketch_db/persistence/`](../../src/storage_engines/sketch_db/persistence/) |

## Identity hierarchy

```text
plan_id / plan_version
  materialization_id
    tenant + canonical series SID or reduction group
      logical window
        compatible summary state/checkpoint
```

`sid` identifies a raw metric series. Materialization identity adds source
binding, summary family/parameters, reduction/grouping, and window semantics.
Plan identity versions deployment. Never use one identity as a substitute for
another.

## Store operations

The component boundary should expose semantic operations rather than internal
maps:

- register/stage a materialization contract;
- append or replace compatible full state;
- apply compatible delta state;
- resolve exact window coverage for a readout;
- mark state active, draining, expired, gapped, or rejected; and
- retire state only after readers/lateness/rollback no longer require it.

Lookup returns complete state or a typed unavailability reason. It never
silently skips a missing group/window.

## Concurrency and lifecycle

- Registration and state append validate against one plan snapshot.
- Readers get stable metadata/state for the operation duration.
- Plan transition may keep old/new versions concurrently but never merges them.
- Eviction cannot remove required active state without first changing
  readiness/routing.
- Persistence recovery must restore identity/compatibility before making parts
  queryable.

## Adding a stored family

Follow [Adding a summary family](../../../docs/developer_docs/adding-summary-family.md).
Store work includes canonical parameter identity, payload validation, supported
merge/readout, accuracy metadata, full/delta encoding identity, and lifecycle
tests. A byte decoder alone is not store support.

## Required tests

- canonical labels yield stable SID independent of order;
- distinct tenants/label sets never collide;
- same SID across different materializations remains isolated;
- incompatible parameters/windows/versions never merge;
- exact coverage and missing-window failure;
- concurrent append/read/retire safety; and
- recovery never exposes state before compatible metadata.
