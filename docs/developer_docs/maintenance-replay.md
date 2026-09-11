# Maintenance execution, replay and completion

Use the [E2E physical-DAG walkthrough](../evaluation/e2e-physical-dag.md) for
commands, installed artifacts and the current acceptance boundary. The
production adapter now executes an explicit validated maintenance subDAG; the
older description of a merge-only adapter is obsolete.

## Plans and runtime capability

Planner emits the actual maintenance-time operations. The control plane binds
source and derived `SummaryDefinitionId`s into the existing owned executable
DAG, then validates the complete PrecomputePlan against SummaryCatalog. Raw
inputs update raw materializations. Derived configurations must not receive raw
collector/backfill jobs or additive fragments.

The finite implementation supports a bounded exact-input/readout/arithmetic to
summary-update subset: explicit Finalize over supported exact Sum/Count states,
strictly aligned arithmetic rows, and the installed supported outer family/update
expression. Query-time rows are not silently reinterpreted as maintenance rows.
The catalog contains window width, slide, layout and origin. Current automatic
execution requires matching full stored nonoverlapping windows; it does not
invent a pane merge or enable sliding merely because the layout DTO can express
it.

The accepted finite multi-source path requires complete source definitions and
its admitted population scope. The wider canonical multi-group path is a
separate integration: all source and target identities must use the canonical
version, all expected groups/windows must be present, and a global reduction
must publish one transaction over the entire cohort. Per-group partial updates
are not equivalent. A real differing-group DDS quantile probe exposed a readout
interpolation mismatch; the new scope is not an accuracy success until its
selected-family process oracle passes.

## Queue admission is not immutable completion

`POST /api/v1/write` accepts Remote Write v1 scalar samples into bounded queues.
It returns `204` after validation, capacity reservation and in-memory dedup
bookkeeping. Accumulator mutation and summary persistence happen later. There
is no durable raw-sample WAL or recovered receiver dedup log. A stale marker is
excluded from numeric aggregation, not propagated as a lifecycle event.

`POST /api/v1/precompute/drain` is a finite-input operation. It closes the
receiver, waits for admitted work and forces the required summary payloads to
become durable before sealing completion. Failed closure is not a successful
proof; retry must finish the same closure. The durable generation checkpoint
rejects new raw input after a same-generation restart. A newly installed
generation has separate admission/visibility rules and cannot reuse the old
derived result while its source population is still open.

## Reserved durable publication

The immutable output writer reuses the existing SID sidecar, part format,
manifest and flusher. Under the source/lifetime and mutation guards it reserves
one output window for a physical SID, recording input lineage and payload
fingerprint. It writes the part, publishes it through the manifest, and only
then advances the immutable frontier and clears the pending reservation.

The latest completed receipt is retained per SID. An identical latest retry
requires matching input lineage, valid retained payload and live manifest
membership. A conflicting lineage cannot reuse equal output bytes. The consumer
looks up committed output or resumes a matching durable pending part **before**
recomputing a randomized sketch. This prevents restart from creating another
part or accepting a different random payload as the same publication.

A pending reservation whose payload was never made durable may still require
retained source data. If that data is gone, recovery fails closed; at-most-once
publication is not a promise of unlimited replay availability. Expired/retired
lifetimes must not be resurrected. The immutable end frontier forbids late
mutation; it is not itself proof of gap-free coverage. Readers and schedulers
must still check concrete completed window coordinates.

## Continuous producer barriers

The existing shared `SummaryWatermarkBarrier` carries catalog generation,
producer ID, partition ID, producer epoch, monotonic sequence and watermark.
`MultiSourceCoordinator` uses an explicit source-partition specification and
persisted staging/checkpoints. Superseded epochs cannot satisfy readiness for a
new epoch. All required inputs and partitions must be complete for the output
window before a batch is ready.

`OutputSink::advance_summary_watermark` is an internal Rust API. The ordinary
remote-write sample payload supplies neither this authority nor a closed
producer roster, and there is no public HTTP watermark endpoint. Worker maximum
sample time, an idle timeout, HTTP acceptance and a query coverage watermark are
not interchangeable with all-producer completion. A continuous integration must
bind the producer roster, order barriers after publication/durability, reject
late writes in a closed range, and preserve that proof across restart.

The finite drain path must not be advertised as continuous operation. The next
sliding layer should first consume explicitly selected FullWindow states at the
exact origin/slide coordinates; overlapping pane composition requires its own
explicit operator, cost and coverage validation.

## Earlier in-process receipt machinery

The generic maintenance sink also has bounded in-process replay receipts for
ordered batches. Those receipts expire by the retained-state retry horizon and
pin a partially accepted batch until retry completes. They do not replace the
SID sidecar/manifest protocol above and must not be used as durable immutable
publication evidence. Their existence alone does not authorize arbitrary
maintenance DAGs, groups or producer completion claims.
