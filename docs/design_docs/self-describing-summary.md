# Self-Describing Summary (SDS)

This proposal defines three logical layers for summary producers and consumers.
It does not change the current runtime or wire format.

| Layer | Describes | Changes when |
| --- | --- | --- |
| **Summary Descriptor** | Summary operator and fidelity guarantees | Algorithm, configuration or guarantee contract changes |
| **Data Descriptor** | Summarized source and population | Source binding or population definition changes |
| **Summary Instance** | Instance metadata and summary state | A concrete materialization is created or updated |

Separating these layers lets many materialized instances reuse the same operator
configuration and data scope. A new time interval creates a new instance without
copying or redefining either descriptor.

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
