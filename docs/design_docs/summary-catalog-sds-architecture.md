# Summary Catalog and Self-Describing Summary Architecture

## Audience and relationship to physical plans

Audience: architects and developers. This document owns SDS identity, metadata,
state compatibility, and lifecycle semantics. The
[Planner and physical-plan architecture](asapplanner-integration.md) owns
compilation, the four runtime projections, transmission policy, and publication.
The target model below is distinct from the implementation notes that follow.
Those notes describe bounded paths and do not establish support for every target
lifecycle, distributed recovery mode, or completeness proof.

SDS describes what a summary represents and which concrete state is available.
QueryPlan describes how to answer a query using it. TransmissionPlan describes
how authorized producers deliver state updates. A sketch envelope is one payload
carrier; it is not the SDS catalog or a physical execution plan.

## Semantic model and authority

| Layer | Meaning | Changes when |
| --- | --- | --- |
| Summary Descriptor | Operator, parameters, fidelity and compatible state representation | Operator/configuration or guarantee contract changes |
| Data Descriptor | Source, population, grouping and observation semantics | Input meaning or population changes |
| Summary Definition | Stable logical materialization referencing descriptors | The semantic definition changes |
| Summary Instance | Concrete extent/group, provenance, status and state reference | State is materialized, updated or retired |

The control plane owns the desired `SummaryCatalog`. It is constructed from the
selected semantic definitions and common physical compilation decisions before
projecting CollectorPlan, PrecomputePlan, TransmissionPlan and QueryPlan. Plans
reference the same immutable catalog snapshot. They do not independently define
summary meaning, and PrecomputePlan is not the catalog's semantic authority.

During migration, installed DTOs may repeat parameters, population or window
fields required by existing consumers. These must agree with the catalog and be
mechanically derived from the common bindings. A target artifact may include a
self-contained catalog subset; it must be verifiable against its publication.
Resolve references at installation rather than through per-update remote lookups.

The observed `ObservedSummaryInventory` reports actual instances and their
readiness. It is not desired state and contains no encoded payloads. Planner may
use this scoped availability evidence without reading sketch bytes. Runtime
reconciliation creates, recovers, retires and expires state according to the
installed contracts; metadata declarations alone do not execute those actions.

## Identity and state references

Keep these identities distinct:

| Identity | Scope and purpose |
| --- | --- |
| Semantic node ID | Node within the selected Planner DAG; physical bindings retain provenance |
| SummaryDescriptorId / DataDescriptorId | Immutable semantic descriptor content |
| SummaryDefinitionId | Logical materialization; currently backed by a typed policy fingerprint |
| SummaryInstanceId | Concrete materialized instance identity |
| Producer / partition / epoch | Source contribution and restart lifetime |
| SeriesId | Backend physical storage lifetime, not a descriptor or plan identity |
| CatalogGeneration | Catalog publication reference, including digest and plan version |
| Publication identity | Exact installed plan content, including execution and transmission choices |
| Sequence / checkpoint | Update history and applicable delta base within a declared stream scope |

Current descriptor IDs use versioned canonical semantic strings. Changing their
encoding must preserve semantic identity and explicitly address collisions.
A new interval/group creates an instance without redefining its descriptors.
Moving a producer or changing transmission cadence need not change its semantic
definition, but requires an authorized publication transition. Changed sampling
or observation semantics require guarantee and state-compatibility validation.
A catalog digest alone cannot identify every change to the four physical plans.

The target instance metadata contract is:

```rust
struct SummaryInstance {
    instance_id: SummaryInstanceId,
    summary_definition_id: SummaryDefinitionId,
    summary_descriptor_id: SummaryDescriptorId,
    data_descriptor_id: DataDescriptorId,
    time_range: HalfOpenTimeRange,
    group_values: GroupValues,
    catalog_generation: CatalogGeneration,
    placement: SummaryPlacement,
    state_reference: SummaryStateReference,
    status: SummaryInstanceStatus,
    completeness: InstanceCompleteness,
    lifecycle: InstanceLifecycle,
}
```

This is a conceptual shape, not a new wire DTO. `SummaryStateReference` is an
opaque storage locator with schema version, generation, sequence and optional
checksum. SummaryStore owns the referenced payload. Concrete frame identity
additionally records the producer stream and checkpoint context required by its
TransmissionPlan; an instance reference alone does not authorize delta application.

Sketch libraries own payload schemas, decoding/reconstruction and supported state
operations. Runtime contracts own catalog, plan and frame metadata. Transport
adapters map these contracts into OTLP or another supported carrier without
redefining sketch payload schemas. Full-state replacement, replay, and delta-base
rules are specified in the [integration design](asapplanner-integration.md#identity-and-update-application).
Matching bytes or descriptor IDs alone never proves safe merging or complete data.

## Desired state and observed lifecycle

Persistent desired materializations come from control-plane planning. A runtime
fast path may create only an authorized ephemeral instance with a finite lease,
report it, and await promotion or expiry. It cannot silently make that instance
persistent desired state.

Reconciliation compares desired definitions with observed placement, extent,
state references, status and completeness. Catalog and plan activation authorize
execution; they do not establish source completeness, durability, or query
readiness. State reuse across generations requires explicit compatibility, and
retired physical lifetimes remain fenced from late updates.

## Implemented backend representation

The in-memory descriptor representation is normalized. `SummaryDescriptorRegistry`
content-interns Summary and Data Descriptors. A SID owns an `SdsBinding` with
shared `Arc` references to both descriptors. Pane rows store the SID foreign
key, `[start, end)`, interned group values and state; together these fields form
the Summary Instance. This avoids repeating descriptors in every pane and lets
catalog snapshots and query lookups clone pointers rather than descriptor data.
The registry holds weak references, so retiring the final SID also releases its
descriptors. `SketchInstanceMetadata` remains the registration and persistence
compatibility DTO while older sidecars are read.

The implemented `SummaryDescriptor` currently contains one `SummaryOperator`,
one derived `FidelityGuarantee`, and a numeric state-schema version. The
implemented `DataDescriptor` contains typed source and value projections, a
canonical population filter, typed grouping columns and versioned observation
semantics. The shared contract now also
defines `SummaryInstance`, `ObservedSummaryInventory`, placement, completeness,
state references, catalog generation and ephemeral leases. The control-plane
reconciler emits create, update, recover, retire, garbage-collect, promote and
expire actions. Summary payloads and the application of those actions remain in
the SummaryStore runtime.

The same `GroupingProjection` supplies source columns to precompute configuration,
`DataDescriptor` and the state-schema contract. Each column retains the Planner's
name, type and nullability; routing derives names without storing a second list.
Legacy label lists decode as non-null UTF-8 columns and keep their existing
identities. A changed type or nullability changes catalog and policy identity.
A SQL map column is one grouping value, not a set of PromQL labels. Typed
ClickHouse group transport remains a separate execution capability: the current
reader rejects non-label projections until that transport is implemented.

`DataDescriptor`, precompute configuration and state-schema validation share
`ValueProjectionIdentity`: sample value, named column, or a finite numeric
constant using the Planner's `ScalarValue`. A constant input such as `1` does
not masquerade as a table column. Projection identity participates in catalog
and policy identity; existing column identities remain unchanged. Older
`value_column` config and state-schema fields are accepted only by wire adapters
and become the same typed projection in memory. ClickHouse backfill binds a
constant as a typed query parameter and applies the installed table population
and timestamp projection. Its Float64 ingest boundary rejects integer constants
outside the exactly representable range. This contract enables literal inputs;
query lowering must still establish each aggregate's null and row semantics.

The durable `sid_metadata.json` format is versioned independently of the wire
contracts. Descriptor tables and bindings avoid repeating semantic definitions;
later metadata revisions also preserve definition identity and catalog provenance.
Legacy records are interpreted by versioned recovery code and must not acquire
authoritative catalog bindings without validation. See
[completeness and recovery](continuous-summary-completeness.md).

An ingest record is never an SDS instance. Raw samples can be transient inputs to
the precompute engine, but the backend does not retain them as a second exact
query store. Exact residual subtrees run in Prometheus.

The SDS metadata and inventory types represent the following invariants. The
current runtime enforces descriptor binding and non-overlapping pane selection.
Full runtime conformance still requires applying and durably persisting every
reconciliation action, including recovery, promotion, lease expiry, retirement,
and garbage collection:

1. An instance references exactly one immutable Summary Descriptor and one
   immutable Data Descriptor.
2. `[start, end)` plus concrete group values identifies the summarized extent;
   different panes are different instances.
3. State may be merged only when the Summary Descriptor permits the operation,
   Data Descriptors are compatible, and interval coverage does not double-count.
4. Completeness and approximation fidelity are independent. An exact operator
   over a partial interval is still incomplete.
5. State bytes always carry a state schema version. A codec match alone does not
   imply semantic compatibility.
6. Rollups never become authoritative state. `RollupCategory::ExactMax` and
   future categories live below one `rollups` collection and can be discarded
   and rebuilt from instances.

## 1. Summary Descriptor

A Summary Descriptor defines **how the data is summarized** and **which fidelity
claims the summary supports**. It does not identify a source population or a
particular time interval.

| Field | Type | Definition |
| --- | --- | --- |
| `summary_descriptor_id` | `QualifiedId` | Immutable descriptor identity |
| `operator` | `SummaryOperator` | Algorithm, semantic version, parameters and supported operations |
| `fidelity` | `FidelityGuarantee[]` | Exactness or error guarantees, with their scope and conditions |
| `state_representation` | `StateRepresentation` | State type, codec and codec version |

`SummaryOperator` contains an algorithm identifier, versioned semantics,
type-specific parameters, and supported build/update/merge/readout signatures.
Parameters and operation arguments depend on the summary type; `item` and
`weight` are not mandatory common fields.

For example, a KLL operator may specify `k: 200`. The value of `k` is an
algorithm parameter, **not itself a numerical error guarantee**. Its fidelity
contract separately identifies the supported rank-error bound or versioned
bound derivation, probability of failure, readout scope and required conditions.
If that guarantee is unavailable, fidelity is explicitly `Unknown`.

For a shared UnivMon state, `heap_size`, `sketch_rows`, `sketch_cols`, and
`layers` describe one configuration. They do not establish one error bound for
all readouts. The backend's `UnivMonFrequency` contract records these parameters
and the unit-frequency update domain: each sample value contributes one
occurrence. Total count is exact in that domain; distinct count, frequency L2,
and frequency entropy require their own accuracy evidence. Frequency L2 means
`sqrt(sum(frequency(key)^2))`; entropy is measured in bits.

ERP evidence must state the readout's units: relative error for distinct and L2,
and absolute bits error for entropy. A measured error is not a certified failure
probability. Readouts may share state only when their configuration and data
population match and each readout's accuracy requirements are satisfied. A
small configuration suitable for L2 may therefore be unsuitable for entropy.
Completeness of the input window remains a separate requirement for every
readout, including exact count.

A `FidelityGuarantee` contains:

- The applicable operation and error quantity, such as quantile rank error.
- A category: `Exact`, `DeterministicBound`, `ProbabilisticBound` or `Unknown`.
- A bound or versioned bound derivation, and a failure probability when applicable.
- The population/readout/evaluation scope and required assumptions.

A `StateRepresentation` identifies the logical state type and versioned encoding.
Compatible bytes alone do not establish that two operators have compatible
semantics or guarantees.

## 2. Data Descriptor

A Data Descriptor defines **which data is summarized**. It is independent of the
summary algorithm and of a particular materialized interval.

| Field | Type | Definition |
| --- | --- | --- |
| `data_descriptor_id` | `QualifiedId` | Immutable data-scope identity |
| `source` | `SourceBinding` | Metric/series or dataset, including its versioned field definitions |
| `population` | `PopulationDefinition` | Selection predicate and grouping/entity scope |
| `observation_semantics` | `SemanticContract` | Value projection, units and handling of missing, duplicate or invalid observations |

For example, the source can be the metric `cpu_usage`, and the summarized
population can be the series satisfying `container_type="login"`.

`PopulationDefinition` records both selection and partitioning. It distinguishes
one summary over all selected observations, independent summaries per series,
and summaries grouped by specified label keys. Concrete group values belong in
the instance metadata when one descriptor describes a reusable grouping rule.

A population predicate is a typed, resolved data-selection definition. It is not
an arbitrary executable program attached to a summary.

## 3. Summary Instance

A Summary Instance describes a concrete materialization and references its
stored state, one Summary Descriptor and one Data Descriptor. The metadata DTO
and inventory never embed the encoded payload.

| Field | Type | Definition |
| --- | --- | --- |
| `instance_id` | `QualifiedId` | Materialized instance identity |
| `summary_descriptor_id` | `QualifiedId` | Referenced operator/fidelity descriptor |
| `data_descriptor_id` | `QualifiedId` | Referenced source/population descriptor |
| `metadata` | `InstanceMetadata` | Concrete extent, population binding, completeness and provenance |
| `state_reference` | `SummaryStateReference` | Opaque locator for separately stored state and its schema/provenance |

`InstanceMetadata` contains the concrete time range or dataset extent, any group
values needed by the population rule, completeness (`Complete`, `Partial` or
`Unknown`), producer/generation/sequence provenance and instance-specific fidelity
evidence. Time ranges specify their clock, units and interval boundaries.
Completeness is separate from mathematical approximation error.

The referenced payload is maintained state, not a quantile readout or other
query result. A transported delta identifies its authorized producer stream and
base checkpoint as well as the supported apply operation. A descriptor or instance
ID alone is insufficient to interpret it as a full state.

## Shared-descriptor example

The following example summarizes `cpu_usage` observations from login containers
using KLL with `k=200`. All three instances reuse the same Summary Descriptor and
Data Descriptor; only the instance time range and state change.

```yaml
summary_descriptor:
  summary_descriptor_id: example:kll-200-v1
  operator:
    algorithm: KLL
    parameters: {k: 200}
    semantics: example:kll-semantics-v1
  fidelity:
    - operation: quantile
      error_quantity: rank_error
      category: Unknown  # No numerical guarantee is inferred from k alone.
  state_representation: example:kll-state-codec-v1

data_descriptor:
  data_descriptor_id: example:login-cpu-v1
  source: {metric: cpu_usage}
  population:
    predicate: {container_type: {equals: login}}
    grouping: global
  observation_semantics: example:cpu-observations-v1

instances:
  - instance_id: example:login-cpu-0
    summary_descriptor_id: example:kll-200-v1
    data_descriptor_id: example:login-cpu-v1
    metadata: {time_range: "[0,10)", clock: example:seconds}
    state_reference: {store: example-store, key: S0, state_schema_version: 1}
  - instance_id: example:login-cpu-1
    summary_descriptor_id: example:kll-200-v1
    data_descriptor_id: example:login-cpu-v1
    metadata: {time_range: "[10,20)", clock: example:seconds}
    state_reference: {store: example-store, key: S1, state_schema_version: 1}
  - instance_id: example:login-cpu-2
    summary_descriptor_id: example:kll-200-v1
    data_descriptor_id: example:login-cpu-v1
    metadata: {time_range: "[20,30)", clock: example:seconds}
    state_reference: {store: example-store, key: S2, state_schema_version: 1}
```

`S0`, `S1` and `S2` are opaque keys for separately stored KLL states. This
conceptual example omits full state-reference provenance and producer evidence;
it is not an installable DTO and makes no completeness or numerical error claim. Descriptor references must resolve within the supplied context or a
durably retained descriptor registry.

Changing `k` creates a new Summary Descriptor. Changing the source or population
creates a new Data Descriptor. Advancing the time range creates a new Summary
Instance. Merge compatibility additionally requires the operator's merge rules,
compatible data scopes and valid instance coverage; sharing descriptors alone
does not authorize merging overlapping observations.

### Catalog-scoped runtime ERP evidence

A runtime observation describes the input of one allocated summary, not an
entire deployment. `ErpPopulationObservations` identifies its catalog generation,
summary definition, observation time, input window and separate summary-instance
populations. The control plane resolves the `DataDescriptor` from its successfully
activated catalog; a telemetry payload cannot provide replacement descriptors.
Alternative sketch parameters may use this evidence only when the compiler
verifies the same data and update semantics.

The typed physical-plan HTTP endpoints accept `target: backend_local_remote_write`
with an empty `collector_ids` list. Omitting `target` preserves the distributed
collector deployment. Both paths use catalog publication and activation. Typed
activations are serialized, and the accepted catalog is retained only after the
backend acknowledges activation, including ClickHouse publications.

An ERP `observed_shape_source.population_scope` supplies the expected catalog
and definition, input semantics, and explicit `max_age_ms` /
`max_future_skew_ms` bounds. Each compilation reads the latest runtime record
again. Missing, stale, malformed, foreign or incomplete observations invalidate
all population fits. This is an ERP miss handled by theoretical sizing or exact
execution; it must not restore an older fit or match the artifact's legacy
distribution descriptor. Offline single-shape inputs remain a separate path.

The initial eligibility is deliberately limited to verified raw per-series
frequency/cardinality readouts over a complete matching window. A 30-second pane
observation does not certify a one-hour input distribution. These checks do not
implement an autonomous drift-triggered replan scheduler, continuous source
completion, or durable restoration of the control plane's active catalog. After
a control-plane restart, live evidence remains ineligible until an authoritative
catalog has been activated again.

### Retired physical series and catalog reactivation

A persisted removal tombstone prevents late fragments and stale metadata flushes
from reopening the same physical `SeriesId`. A later installed catalog generation
may authorize a fresh physical series for the same logical definition/group.
The resolver writes that rotation and its catalog provenance before changing its
cache; ordinary writes from the original generation cannot authorize rotation.
The original physical ID remains tombstoned so old disk parts cannot enter the
replacement's readout.

Queued precompute inputs carry their captured catalog generation and physical
series ID separately from an optional admission receipt. Workers preserve both
on publication. A delayed output writes its original physical series, never a
newly resolved replacement. Derived materializations resolve their own target
series while retaining the source generation proof. Backfill processors capture
the catalog generation when attached to the store; old jobs cannot authorize a
new catalog's rotation. An older queued input that has not yet published its
first storage instance is conservatively rejected after a catalog change. Already
registered retained series can drain their birth generation or accept the current
generation. Seamless re-planning of unpublished old inputs requires additional
first-mint provenance; it is not guaranteed by this transition.

This is an explicit lifetime transition, not cross-generation recovery of arbitrary
summary state. Legacy records without trustworthy catalog provenance remain
unbound. Tombstone reclamation still requires coordinated removal of old physical
parts and is not implemented by this transition.

### Derived summary input identity

A summary computed from another summary has a different data source from the
original raw table or metric. `PrecomputeMaterialization.derived_input` and
`DataSourceIdentity::Derived` use the same `DerivedInputIdentity`: the referenced
`SummaryDefinitionId`s and a SHA-256 of the maintenance program. The executable
program remains in `OwnedPostAsapDag`; the catalog does not retain another copy.

The signature replaces materialized input frontiers with stable summary IDs and
hashes the remaining node payloads, schemas, guarantees, and edge semantics. It
excludes query names, plan-local node numbering, and catalog generations. Literal
leaves are hashed directly; raw input leaves still require catalog frontiers. A changed
input definition or transformation creates a new identity. Existing raw-source
identities retain their previous byte representation. Catalog validation rejects
missing input definitions and dependency cycles.

Typed installation accepts the bounded immutable maintenance contract below
only when the complete installed DAG matches the catalog input identity. Legacy
raw YAML still rejects derived inputs; raw routing excludes them. Neither raw-table
substitution nor treating late correction fragments as new observations is valid.

### Immutable completed windows

Finite Remote Write completion now fences the SummaryStore append boundary,
not just the receiver queue. After all admitted outputs are published, the store
records the greatest published window end for each physical SeriesId. Sketch and
exact-state writes ending at or before that boundary are rejected, including
writes arriving through other producers. A later window remains writable. Observed SDS inventory reports only these frozen
instances as `Complete`; ordinary emitted panes remain `Unknown`.

The boundary is monotone in the existing SeriesId metadata sidecar and is restored
before recovered identities become writable. A stale background metadata flush
cannot reopen a completed window. The guard belongs to the physical lifetime;
a catalog-authorized replacement SeriesId has its own boundary.

With persistence enabled, completion explicitly requests the existing flusher to
make the completed prefix durable, even if it is still inside the hot tier.
Completion waits until the corresponding epochs have been evicted after part and
manifest publication; only then does it persist the immutable boundary. An
in-memory deployment provides no restart guarantee. Maintenance consumers still
must atomically publish their output identity before claiming replay-safe consumption.
The existing finite-source completeness proof still rejects untracked writes or
pending admitted work. Continuous producer watermarks and derived-state commit
transactions are separate from this finite-input boundary.
### Executing an immutable maintenance sink

`precompute_engine::maintenance_runtime::execute_completed_maintenance` executes
one installed semantic subDAG from a physical source whose required base windows
are durably complete. SummaryStore validates the catalog generation, physical
SeriesId, population, exact window coverage, and each part read. Missing, corrupt,
or duplicate source windows are errors; this path cannot silently omit a pane as
a query fallback helper might.

The existing maintenance operator registry preserves a collection of source
states until the DAG explicitly merges or finalizes it. Exact Sum/Count
finalization with a declared Float64 output produces one row per source window; an unkeyed SummaryAgg consumes
those rows together. Consequently `Finalize -> SummaryAgg` does not accidentally
become one complete DAG evaluation per correction fragment. Live worker fragments
remain ineligible for finalization.

The engine resumes a matching durable pending part and looks up the stored input
digest before computing a potentially randomized sketch. The existing flusher publishes a new result through its part
reservation protocol; SummaryStore fences query reads and physical lifetime
changes during publication. A concurrent identical completion reuses the durable
result instead of comparing newly randomized bytes. Both pending recovery and a
committed lookup restore the live completion boundary. Catalog-derived definitions
reject additive sketch/precompute writes even beyond that boundary; only reserved
publication may create their output state. The latest committed window can be
retried after restart without adding another part.

Backend-local remote-write plans can bind a selected exact accumulator followed
by an explicit maintenance-time Finalize and outer unkeyed SummaryAgg. Initial
automatic installation requires one raw source definition and identical full,
non-overlapping source/output windows. The finite drain barrier flushes source
state and schedules complete retained windows through this same entry point;
raw routing never feeds samples directly into the derived accumulator.

Finite completion closes all raw store writes for that catalog generation, not
only its HTTP receiver. The existing admission lock issues a private publication
writer; a receipt carried in an output is not evidence that this lock is held.
The metadata writer persists one generation checkpoint before completion becomes
usable, and restores it before accepting writes after restart. A failed close
stays closed to writers until its persistence retry succeeds. Installing a new
catalog generation starts a new admission lifetime. Derived state from a previous
generation is excluded from query candidates and inventory; recomputation receives
a fresh physical SID through the existing resolver. Raw state remains independently
reusable, and retained old source populations cannot be omitted from a singleton proof.

A per-entity source can feed global Reduce([]) only when the store proves that
its entire finite population contains exactly one physical source SID and one
stored label population. This proof unions live bindings with all nonremoved
strict durable metadata for the same summary definition, across catalog generations,
before reserving any output. The
reduction then removes source labels according to the installed output grouping.
Multiple source SIDs or stored groups fail closed; this is not general shuffle support. Physical
SID metadata retains observed per-entity label names for durable decoding while
the catalog retains the logical partitioning contract. SQL backfill job status
alone is not this all-producer completion proof and does not trigger this path.

Synchronized multiple sources, general row operators, overlapping output-window
replacement, and continuous producer watermarks remain unsupported. In particular, the SQL
subquery's timestamp grouping and sampling predicate must not be replaced with an
arbitrary tumbling aggregate. Historical completion-metadata GC and pinning source
parts for recovery before a reserved output part exists remain lifecycle work.
