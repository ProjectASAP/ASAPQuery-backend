# Catalog-backed physical-plan installation

`BackendPlan` and its standalone protobuf and installation endpoint have been
removed. The backend now stages and atomically activates one physical-plan
envelope containing the authoritative `SummaryCatalog` snapshot plus the
`PrecomputePlan`, optional `CollectorPlan`, `TransmissionPlan`, and `QueryPlan`
that reference installed stored outputs and their semantic definitions.

Catalog schema 4 separates `StoredOutputId` (deployment routing) from
`SummaryDefinitionId` (versioned semantic SHA-256). `StoredOutputReference`
binds both. Planner exports the persisted output's typed semantic dependency
closure; explicit raw summary configurations use a restricted typed description.
Derived outputs require the complete Planner closure at installation.

Writes must match the installed output's operator and format. Durable metadata
records both identities and the immutable catalog snapshot. Recovery validates
that snapshot before restoring authoritative state. Old payload decoders remain
available, but legacy metadata without semantic identity cannot authorize bound
reads. Reinstall/rebuild those outputs rather than guessing their meaning.

See [SDS architecture](../../design_docs/summary-catalog-sds-architecture.md)
for the contract and [migration gates](../../design_docs/asapplanner-migration-plan.md#5-bound-query-sds-implementation-across-the-pr-stack)
for the downstream acceptance requirements. Ad-hoc discovery is not implemented.
The HTTP lifecycle is exposed through `/api/v1/physical-plan`,
`/api/v1/physical-plan/activate`, `/api/v1/physical-plan/discard`, and
`/api/v1/physical-plan/status`.

The current V1 storage path reads only the installed plan version. Matching
definition identity does not authorize cross-version payload reuse; that would
require an explicit reader binding and separate compatibility support.

## Persisted physical computation and outputs

Installed SQL entries include a serialized native physical DAG. Installation
compiles it once; activation and requests validate its version, roots and input
schemas, then bind concrete batches. Missing physical programs require plan
recompilation. Serving does not lower relational expressions again.

Native summary snapshots use `native_batch_v1`, with an explicit storage tag
separate from legacy sketch frames. The payload preserves the physical schema,
Float64 values, typed heap identities and the summary codec. Legacy sketch
readers reject this tag. Existing installed schemas may keep a nonempty, unique
subset of supported encodings when new decoders are added.

`SketchStore::publish_native_summary_output` publishes one summary row for one
group/window through the existing immutable commit path. It requires the complete
durable raw input cohort and validates the installed family and group. Repeated
publication of the same lineage reuses the existing commit.

`read_native_summary_output` resolves the installed output/group through the
persistent series resolver without allocating an identity. It validates the
plan version, definition, exact window, completeness, encoding, physical schema,
group values and read budget before returning a batch to the physical executor.
The supported path reads a complete immutable window; pane composition remains
an explicitly planned physical operation.

The storage integration test covers durable per-series sums, native quantile
state construction, publication, bound readout and restart of both the store and
resolver. Automatic PromQL physical-candidate installation, Rate-to-heap storage
execution and spatial latest-value heap execution remain pending. This test does
not establish those paths.

Bound frozen reads resolve the stored-output/group prefix through the persistent
series resolver. Each cached part builds a sorted output/window index once on
open, including older parts written in flush order. Range reads inspect only the
matching prefix and start-time range, then validate end times and exact coverage.
Duplicate windows remain visible and are rejected as ambiguous. The index memory
counts toward the part-cache budget; the on-disk part format is unchanged.

For selected explicit-`by` current-series TopK, the population store now supplies
all eligible members through a snapshot binding. Planner compiles the ranking
above that boundary; the QueryPlan persists that physical program before
candidate pricing and activation. Serving recovers its operators and supplies
full-label native rows without parsing or lowering the query. Automatic cost
estimates include sorting CPU and temporary workspace; provider quote manifests
include the physical program itself. Other population readouts retain their
existing paths. This path does not imply that spatial heap candidates or buffered
Rate-to-heap persistence are deployed yet.
