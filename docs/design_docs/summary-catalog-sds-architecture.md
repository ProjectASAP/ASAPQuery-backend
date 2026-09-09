# Summary Catalog and Self-Describing Summary Architecture

This design defines three logical layers for summary producers and consumers.

| Layer | Describes | Changes when |
| --- | --- | --- |
| **Summary Descriptor** | Summary operator and fidelity guarantees | Algorithm, configuration or guarantee contract changes |
| **Data Descriptor** | Summarized source and population | Source binding or population definition changes |
| **Summary Instance** | Instance metadata and summary state | A concrete materialization is created or updated |

Separating these layers lets many materialized instances reuse the same operator
configuration and data scope. A new time interval creates a new instance without
copying or redefining either descriptor.

## Proposed ownership

The descriptor vocabulary is a shared contract in `asap_types`. The control
plane owns the authoritative `SummaryCatalog`; Collector and backend receive the
same immutable catalog snapshot. Planner reasons about operators, fidelity,
source and population semantics, while runtime components bind catalog identities
to producers and stored instances.

| Layer | Responsibility |
| --- | --- |
| Summary Descriptor | Shared semantic definition used by Planner and backend |
| Data Descriptor | Shared source/population definition; backend resolves concrete runtime bindings |
| Summary Instance | Backend owns metadata, state, updates, storage and retirement |

Planner may observe instance availability, covered time ranges and descriptor
references as planning evidence. It does not need the encoded summary state.
SDS describes summaries; an installed QueryPlan specifies how to execute a query
using them. The current backend fields are an incremental implementation of this
model. They must converge on the identities and invariants below rather than add
operator-specific stores beside `SketchStore`.

## Target semantic model

The target model has descriptor registries plus pane instances. Descriptor IDs
are derived from canonical semantic content; display names and runtime SIDs are
not descriptor identities. The current implementation uses the canonical string
itself as the ID. A future hashed representation must preserve the same content
identity and handle collisions explicitly.

```rust
struct SummaryDescriptor {
    id: SummaryDescriptorId,
    operator: SummaryOperator,
    fidelity: Vec<FidelityGuarantee>,
    state_schema: StateSchema,
}

struct DataDescriptor {
    id: DataDescriptorId,
    source: MetricSource,
    population: PopulationDefinition,
    observation_semantics: ObservationSemantics,
}

struct SummaryInstance {
    id: SummaryInstanceId,
    materialization_id: MaterializationId,
    summary_descriptor_id: SummaryDescriptorId,
    data_descriptor_id: DataDescriptorId,
    interval: HalfOpenInterval,
    group_values: BTreeMap<String, String>,
    completeness: Completeness,
    catalog_generation: CatalogGeneration,
    placement: SummaryPlacement,
    state_reference: SummaryStateReference,
    status: SummaryInstanceStatus,
    lifecycle: Persistent | Ephemeral(EphemeralLease),
}
```

The instance contract contains no payload bytes. `SummaryStateReference` is an
opaque storage-engine locator with state-schema version, generation, sequence
and optional checksum. `ObservedSummaryInventory` is a versioned data-plane
report keyed by `SummaryInstanceId`; it is observed state and never part of the
desired catalog snapshot.

## Authoritative SummaryCatalog and execution plans

The control-plane `SummaryCatalog` is the metadata authority. It stores immutable
Summary and Data Descriptors plus stable materialization identities. It does not
store pane payloads, watermarks, completeness, or observed availability; those
are data-plane instance/runtime metadata.

The control plane reconciles two explicitly separate views:

- **Desired SummaryCatalog:** persistent materializations selected through
  workload feedback and Planner decisions.
- **Observed Summary Inventory:** instances actually building or stored,
  including placement, time coverage, state reference, status and generation.

Reconciliation creates missing desired materializations, updates instances from
old catalog generations, recovers failed or missing payloads, and retires then
garbage-collects materializations removed from desired state. A data-plane fast
path may create only an ephemeral instance with a finite lease and must report
it immediately. A matching desired materialization promotes it; otherwise it
expires and is collected. The data plane cannot promote an ephemeral instance
or create persistent desired state by itself.

```text
                       ASAPPlanner post-ASAP DAG
                                  |
                                  v
                    Control-plane SummaryCatalog
       SummaryDescriptor + DataDescriptor + MaterializationIdentity
                                  |
              catalog references | shared snapshot
          +-----------------------+-----------------------+
          |                       |                       |
          v                       v                       v
    CollectorPlan           PrecomputePlan           QueryPlan DAG
 producer placement,       backend-ingest build,     readout, combine,
 input routing, build       update and lifecycle     Prometheus fallback
          |                       |
          +-----------+-----------+
                      v
              TransmissionPlan (when remote producers exist)
        full/delta/checkpoint transport, sequence and encoding
                      |
                      v
        Backend/Collector catalog replicas and SummaryStore
             pane instances, completeness and lineage
```

All four execution plans carry catalog references and use catalog materialization
IDs for cross-plan identity. During the compatibility migration, producer and
precompute DTOs still repeat fields needed by existing runtimes, including
operator parameters, source/filter/grouping, window, and state schema. Install
validation requires those fields to agree exactly with the catalog; they are not
independent semantic definitions. New interfaces should resolve them from the
catalog, allowing the copied fields to be removed as consumers migrate.

| Component | Responsibility |
| --- | --- |
| `SummaryCatalog` | Canonical descriptor definitions, stable IDs and catalog schema/version |
| `CollectorPlan` | Collector placement, input routing, producer identity and collector-side build operations |
| `PrecomputePlan` | Backend-ingest placement, window updates, retention and lifecycle |
| `TransmissionPlan` | Optional producer-to-backend full state, delta, checkpoint, sequence and encoding contract |
| `QueryPlan` | Materialization references, readout, DAG composition and exact Prometheus boundaries |
| SummaryStore (`SketchStore` today) | Instance state, concrete intervals/groups, completeness, lineage and rebuildable rollups |

The former `BackendPlan` has been removed. `SummaryCatalog` owns materialization
metadata, `PrecomputePlan` owns update/placement/lifecycle, `QueryPlan` owns
readout and fallback routing, and the common deployment envelope carries their
shared plan identity. Consumers atomically install one catalog snapshot with
the plans that reference it.

The implemented ownership split is:

1. Move the SDS catalog contract into `asap_types`.
2. Make the control plane own the authoritative `SummaryCatalog`.
3. Make `PrecomputePlan` reference catalog descriptors and own update, placement and lifecycle.
4. Make `QueryPlan::MaterializationBinding` reference catalog/materialization IDs directly.
5. Distribute the same catalog snapshot to Collector and backend.
6. `BackendPlan`, its protobuf and install endpoint, and duplicate validation are removed.

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
implemented `DataDescriptor` contains metric name, canonical population filter,
grouping keys and versioned observation semantics. The shared contract now also
defines `SummaryInstance`, `ObservedSummaryInventory`, placement, completeness,
state references, catalog generation and ephemeral leases. The control-plane
reconciler emits create, update, recover, retire, garbage-collect, promote and
expire actions. Summary payloads and the application of those actions remain in
the SummaryStore runtime.

The durable `sid_metadata.json` format is versioned independently. Version 2
contains `summary_descriptors`, `data_descriptors`, and `bindings` tables. A
binding stores only both descriptor IDs plus SID-local timestamps. Version-1
flat SID records remain readable and are rewritten in normalized version-2 form
on the next metadata update.

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

A Summary Instance combines **instance metadata** with **the actual summary
state**, referencing one Summary Descriptor and one Data Descriptor.

| Field | Type | Definition |
| --- | --- | --- |
| `instance_id` | `QualifiedId` | Materialized instance identity |
| `summary_descriptor_id` | `QualifiedId` | Referenced operator/fidelity descriptor |
| `data_descriptor_id` | `QualifiedId` | Referenced source/population descriptor |
| `metadata` | `InstanceMetadata` | Concrete extent, population binding, completeness and provenance |
| `state` | `SummaryState` | Materialized state encoded according to the Summary Descriptor |

`InstanceMetadata` contains the concrete time range or dataset extent, any group
values needed by the population rule, completeness (`Complete`, `Partial` or
`Unknown`), producer/generation/sequence provenance and instance-specific fidelity
evidence. Time ranges specify their clock, units and interval boundaries.
Completeness is separate from mathematical approximation error.

`SummaryState` is the state itself, not a quantile readout or other query result.
If a transport carries a delta, it must identify its base instance/version and
the descriptor's supported apply operation; it cannot be interpreted as a full
state without that context.

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
    state: S0
  - instance_id: example:login-cpu-1
    summary_descriptor_id: example:kll-200-v1
    data_descriptor_id: example:login-cpu-v1
    metadata: {time_range: "[10,20)", clock: example:seconds}
    state: S1
  - instance_id: example:login-cpu-2
    summary_descriptor_id: example:kll-200-v1
    data_descriptor_id: example:login-cpu-v1
    metadata: {time_range: "[20,30)", clock: example:seconds}
    state: S2
```

`S0`, `S1` and `S2` denote separate encoded KLL states. The example omits concrete
payload bytes and producer evidence; it makes no completeness or numerical error
claim. Descriptor references must resolve within the supplied context or a
durably retained descriptor registry.

Changing `k` creates a new Summary Descriptor. Changing the source or population
creates a new Data Descriptor. Advancing the time range creates a new Summary
Instance. Merge compatibility additionally requires the operator's merge rules,
compatible data scopes and valid instance coverage; sharing descriptors alone
does not authorize merging overlapping observations.
