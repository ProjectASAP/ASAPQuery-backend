# Precomputation publication and recovery

Ingestion accepts source data; precomputation executes the installed Physical
DAG and publishes stored results. Planner defines computation and permitted
update behavior. Backend binds inputs, enforces correction and retention limits,
and persists results under the installed SDS identities.

## Continuous local ingestion

With `--remote-write-revision-dir`, Backend checkpoints accepted Remote Write
input and evaluates the selected precomputation DAG against a fixed input
revision. Late input within the correction horizon produces a replacement
revision, not an additive replay of the complete result. Out-of-range requests
are rejected before admission.

Outputs can publish independently. Queries pin a common eligible snapshot across
all required outputs and range steps. If one branch has r2 and another is still
at r1, the query may read common r1 while it satisfies freshness. It must not mix
incompatible revisions. Same-version recovery restores input history and committed
outputs; a new plan version needs its own warm-up.

See [continuous input and recovery](../design_docs/precompute-dag-execution.md#continuous-local-input-revision-snapshots)
for configuration, publication rules and acceptance cases. This local-input path
does not require an external producer/partition watermark protocol.

## Finite input

Without continuous revision configuration, `POST /api/v1/write` acknowledges
queue admission; it does not prove durable publication or input completion.
`POST /api/v1/precompute/drain` closes the finite receiver, waits for admitted
work and durably publishes the required outputs before sealing completion.
A failed drain must be retried; it cannot authorize derived output publication.
The recovered same-generation checkpoint rejects further input after closure.
Drain cannot seal a continuously revisable input.

The finite output writer records input lineage and payload identity, writes a
part, commits its manifest entry, then advances completion. Recovery resumes a
matching durable pending publication before recomputing randomized state.
Conflicting lineage cannot reuse equal bytes. Missing retained inputs or durable
payload fail explicitly; in-memory retry receipts are not durable proof.

See the [E2E walkthrough](../evaluation/e2e-physical-dag.md) for commands and
installed artifacts. External producer watermark coordination is a separate
input contract, not an inference from flush timers or observed timestamps.
