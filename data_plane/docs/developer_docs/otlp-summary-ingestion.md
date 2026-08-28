# OTLP summary ingestion

> Implementation status: partial; OTLP decoding, SID resolution, and summary
> handling exist, while full BackendPlan-gated validation remains target work.

## Purpose

The ingestion component receives OTel metric payloads from ASAPCollector,
resolves series identity, validates summary state against the active
BackendPlan, applies full/delta semantics, and appends queryable windows to
summary storage.

Design sources:

- [Plan-aware query execution](../design_docs/query-execution.md)
- [Summary storage](../../../docs/design_docs/summary-storage.md)
- [Series identity](../../../docs/design_docs/series-identity.md)

## Current code map

| Responsibility | Current entry point |
| --- | --- |
| OTLP receivers and decoding | [`drivers/ingest/otel.rs`](../../src/drivers/ingest/otel.rs) |
| Canonical SID resolution | [`drivers/ingest/series_resolver.rs`](../../src/drivers/ingest/series_resolver.rs) |
| Full/delta evaluation helpers | [`sketch_db/query/delta_apply.rs`](../../src/storage_engines/sketch_db/query/delta_apply.rs) |
| Summary decoders | [`sketch_db/query/decoders.rs`](../../src/storage_engines/sketch_db/query/decoders.rs) |
| Store/index | [`sketch_db/index/`](../../src/storage_engines/sketch_db/index/) |

## Processing stages

1. Decode OTLP without discarding resource, scope, metric, point labels, or
   source timestamps needed for identity and freshness.
2. Resolve tenant and canonical series key to `sid`.
3. Locate the materialization in the active BackendPlan.
4. Validate producer, family, parameters, grouping, window, encoding, and
   schema compatibility.
5. Apply full or delta sequencing to an isolated candidate state.
6. Commit the window/state atomically to the store.
7. Update readiness/freshness evidence and structured ingest telemetry.

Each batch/item must have an explicit failure policy. A decoding error must not
silently convert an incompatible summary into a raw metric or another family.

## Full and delta invariants

- Full state identifies its checkpoint and replaces only the compatible base.
- Delta identifies plan, materialization, producer, window, sequence, and base.
- Duplicate delivery is idempotent.
- A missing/reordered/conflicting delta creates a visible gap.
- State after a gap remains unqueryable until a compatible full checkpoint.
- State from different plan versions or materialization contracts never merges.

## SID behavior

Always canonicalize metric name and identifying labels deterministically.
Sender-provided numeric IDs are shortcuts, not authority. Unknown or conflicting
IDs must be resolved from canonical identity evidence or rejected.

## Freshness evidence

Preserve the source sample/window timestamp separately from receive and commit
timestamps. The MVP freshness measurement starts at the source timestamp and
ends at the first queryable committed state; substituting ingestion wall time
understates lag.

## Adding an encoding

An encoding is supported only when its decoder, compatibility identity,
full/delta behavior, checkpoint recovery, capability advertisement, collector
parity, and corrupt-input tests land together.

## Required tests

- full payload for every MVP family;
- delta duplicate, gap, reorder, wrong base, and recovery checkpoint;
- wrong plan/materialization/family/parameters/window rejection;
- SID cache hit, unknown ID, and conflicting identity recovery;
- timestamps retained for freshness; and
- rejected input never changes queryable state.
