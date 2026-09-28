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
resolver. That test does not establish the full PromQL physical-candidate handoff or
precomputed heap publication.

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
estimates include native operator CPU and temporary workspace; provider quote
manifests include the physical program itself. Other population readouts retain
their existing paths.

Planner also exposes a CountSketch-with-heap candidate over that same snapshot.
Backend requires scoped membership/score evidence, including the enforced
`topk_max_distinct_items` bound, before installation. Source cardinality estimates
do not supply that accuracy bound. Signed sample values cannot authorize CMS.
Backend compares the complete physical programs; it does not replace the selected
heap with Sort/Limit. The heap is rebuilt for each evaluation, so decreases,
staleness and expiry do not accumulate historical weights. Automated real-process
Remote Write/HTTP coverage verifies these changes with no exact-backend fallback.
This is a query-time heap candidate; buffered Rate-to-heap persistence is separate.

For `topk by (...) (..., rate(metric[1m]))`, Planner also compiles exact ranking,
CMS heap and CountSketch heap above the exact per-series Rate boundary. Backend
binds that input to the installed counter SDS. `QueryPlanNode::Physical` maps
source node IDs to deployment readouts and supplies the run budget; it contains
no second operator representation. Recovery requires the persisted physical
program, matching source IDs, complete identity and the same readout window.

The process E2E tests ingest raw counters, publish durable counter windows, run
native ranking, restart the process and repeat both window queries. Both heap
families preserve hidden series labels, account for counter resets and rebuild
ranking independently for each window. Missing windows cannot serve a warm
result. This path stores counter state and builds the heap at query time;
publishing the heap itself during precompute requires a separate placement.

Fixed-window Rate ranking has a second placement: Planner finalizes the complete
per-series counter window and builds the heap during maintenance. The deployment
binds the resulting heap to SDS; the query graph contains readout and ranking
projection only. Counter states are the bounded input buffer, including timestamps
and resets. Rates from different windows are never added as heap updates.

This placement runs after the finite input cohort is closed and durable. It does
not infer population completeness from a timer or a missing series. Continuous
maintenance needs an explicit population/window completion contract before it can
use this path. Binding requires matching complete source windows at the query's
evaluation cadence. Full windows may overlap: a 60-second lookback evaluated
every five seconds stores independent `(t-60s,t]` outputs every five seconds.
A bound read selects exact window endpoints and never adds neighboring snapshots.

Each native heap output is one atomic batch record per window. Logical groups
and series identities remain in the typed batch; its outer storage address uses
an empty group. Publishing groups independently would permit recovery to expose
an incomplete group set. The canonical SummaryDefinition still describes the
logical grouping and expressions. This storage granularity does not erase them.

Installed maintenance programs retain Planner operator definitions and typed
input/output node identities. Recovery validates those boundaries against the
installed semantic document and stored-output bindings. The query resolves its
exact deployed output through the store index, checks the definition, generation,
window, encoding and batch schema, then executes the retained query graph.

Grouped Rate supports the same placement choice. Planner can emit per-series
Rate readout → grouped Sum → Sum readout entirely at query time, or persist the
complete grouped Sum batch during maintenance. Both preserve grouping and drop
ungrouped series labels in the result. Backend prices both physical programs;
it does not move Sum across Rate or pool raw counters before calculating Rate.
