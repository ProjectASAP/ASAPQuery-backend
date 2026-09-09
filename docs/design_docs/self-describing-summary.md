# Self-Describing Summary (SDS)

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

The descriptor vocabulary belongs in shared semantic contracts, suitable for
Planner's shared types. Planner reasons about operators, fidelity, source and
population semantics. The backend binds those descriptions to actual series,
filters and grouping, and owns materialized instance state and its lifecycle.

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

## Concrete backend model

The durable model has descriptor registries plus pane instances. IDs are hashes
of canonical semantic content; display names and runtime SIDs are not identities.

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
    summary_descriptor_id: SummaryDescriptorId,
    data_descriptor_id: DataDescriptorId,
    interval: HalfOpenInterval,
    group_values: BTreeMap<String, String>,
    completeness: Completeness,
    lineage: Lineage,
    state: AggPayload,
}
```

The backend maps this model onto its execution components as follows:

| Component | SDS responsibility |
| --- | --- |
| Physical-plan compiler | Canonicalize descriptor content, assign descriptor references and declare build/readout operations |
| Precompute engine | Route matching observations and update the instance for one descriptor pair, group and pane |
| SummaryStore (`SketchStore` today) | Store descriptor registries, instance metadata, state, completeness and lineage |
| Query engine | Resolve plan references, select complete instances, merge/read out their state and combine exact Prometheus subquery results |
| `rollups` | Hold typed, rebuildable indexes derived from canonical instances |

An ingest record is never an SDS instance. Raw samples can be transient inputs to
the precompute engine, but the backend does not retain them as a second exact
query store. Exact residual subtrees run in Prometheus.

The store enforces these invariants:

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
