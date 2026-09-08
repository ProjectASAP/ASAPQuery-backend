# Self-Describing Summary (SDS)

Proposed logical structures; not an implemented serialization format.

## SDS

| Field | Type | Definition |
| --- | --- | --- |
| `schemas` | `SdsSchema[]` | Immutable summary descriptors |
| `dictionary` | `SdsIdentity[]` | Reusable population identities |
| `records` | `SdsRecord[]` | Summary states or readout results |

References are namespace-qualified. Every record's descriptor references must
resolve within the SDS or a durably retained descriptor context.

## SdsSchema

| Field | Type | Definition |
| --- | --- | --- |
| `schema_id` | `QualifiedId` | Immutable descriptor identity |
| `schema_version` | `Version` | Descriptor format version |
| `input` | `InputContract` | Summarized input and its interpretation |
| `grouping` | `GroupingContract` | Population partitioning and group-key types |
| `summary` | `SummaryTypeContract` | Summary-specific state and operation semantics |
| `representation` | `RepresentationContract` | Payload layout and encoding |
| `guarantees` | `GuaranteeContract[]` | Guarantees available for specified operations |

### InputContract

| Field | Type | Definition |
| --- | --- | --- |
| `source` | `CanonicalSourceBinding` | Versioned source definition; concrete snapshot/partition is identified by record coverage |
| `fields` | `FieldDefinition[]` | Stable field IDs, data types, nullability and optional units |
| `projection` | `TypedExpression[]` | Expressions supplying the operation's inputs |
| `filter` | `Optional<TypedPredicate>` | Input qualification; absent means no additional filter |
| `observation_semantics` | `VersionedSemanticContract` | Null, NaN, duplicate and ordering interpretation |

### GroupingContract

| Field | Type | Definition |
| --- | --- | --- |
| `mode` | `Global \| PerEntity \| ByKeys` | One population, preserved entity populations, or explicit grouping |
| `keys` | `TypedExpression[]` | Group-key expressions; empty for Global |
| `key_semantics` | `VersionedSemanticContract` | Equality, canonicalization and absent/null handling |

### SummaryTypeContract

| Field | Type | Definition |
| --- | --- | --- |
| `type_id` | `QualifiedId` | Summary semantic type |
| `type_version` | `Version` | State and operation semantics version |
| `parameters` | `TypedParameterRecord` | Parameters validated against this summary type's parameter schema |
| `state_schema` | `TypeDefinition` | Logical state structure |
| `operations` | `OperationContract[]` | Supported state construction, modification, combination and readout |

`parameters` and operation signatures are type-specific; `item` and `weight`
are not common SDS fields.

| Summary type | Type-specific parameter fields | Update input signature |
| --- | --- | --- |
| Exact aggregate | Aggregate components, numeric representation and overflow policy | Typed observations with component-specific qualification |
| DDSketch | Relative accuracy and supported value-domain policy | Numeric observation |
| KLL | Capacity and compaction configuration | Ordered observation |
| HLL | Precision, element encoding and hash configuration | Element |
| CMS | Width, depth, key encoding, hash configuration and increment-domain policy | Key, increment |
| CountSketch | Width, depth, key encoding, hash configuration and increment-domain policy | Key, increment |
| Heap-bearing frequency summary | Base frequency contract, candidate policy and heap capacity | Base frequency update input |

### OperationContract

| Field | Type | Definition |
| --- | --- | --- |
| `operation_id` | `QualifiedId` | Versioned operation definition |
| `kind` | `Build \| Update \| Merge \| Retract \| Subtract \| Readout` | Operation category; only supported operations are listed |
| `inputs` | `TypeDefinition[]` | Ordered input/state signatures |
| `arguments` | `ParameterSchema` | Typed operation arguments |
| `output` | `TypeDefinition` | Output state or result type |
| `preconditions` | `RuleRef[]` | Compatibility, coverage, ordering and provenance requirements |
| `guarantee_rules` | `RuleRef[]` | Applicable guarantee derivation/composition rules |

### RepresentationContract

| Field | Type | Definition |
| --- | --- | --- |
| `representation_id` | `QualifiedId` | Concrete state-layout identity |
| `codec` | `QualifiedId` | Payload codec |
| `codec_version` | `Version` | Codec version |
| `layout` | `TypeDefinition` | Encoded payload layout |
| `payload_kinds` | `Set<FullState \| StateDelta \| ReadoutResult>` | Supported payload forms |

### GuaranteeContract

| Field | Type | Definition |
| --- | --- | --- |
| `guarantee_id` | `QualifiedId` | Versioned guarantee definition |
| `operation` | `QualifiedId` | Operation/readout to which the guarantee applies |
| `kind` | `Exact \| DeterministicBound \| ProbabilisticBound \| Unknown` | Guarantee category |
| `error_quantity` | `Optional<TypedErrorDefinition>` | Quantity, units and normalization being bounded |
| `bound` | `Optional<TypedBound>` | Bound or versioned bound derivation |
| `failure_probability` | `Optional<Probability>` | Required for a probabilistic bound; not invented for other categories |
| `scope` | `GuaranteeScope` | Population/readout and evaluation set covered by the claim |
| `assumptions` | `RuleRef[]` | Required input, algorithm and evidence conditions |

## SdsIdentity

| Field | Type | Definition |
| --- | --- | --- |
| `identity_id` | `QualifiedId` | Population identity; may have a compact dictionary alias |
| `schema_id` | `QualifiedId` | Referenced SDS Schema |
| `source_identity` | `TypedRecord` | Concrete source identity, including metric name where applicable |
| `group_values` | `TypedTuple` | Values matching the grouping key/entity schema |

## SdsRecord

| Field | Type | Definition |
| --- | --- | --- |
| `record_id` | `QualifiedId` | Record identity |
| `identity_id` | `QualifiedId` | Referenced dictionary identity |
| `coverage` | `Coverage` | Actual summarized input extent and completeness |
| `payload` | `SdsPayload` | Exactly one state or result variant |
| `provenance` | `ProvenanceRef` | Resolvable producer, generation and sequence metadata |
| `guarantee_evidence` | `GuaranteeEvidence[]` | Instance-specific evidence for applicable guarantees |

### SdsPayload

| Variant | Fields |
| --- | --- |
| `FullState` | `state: bytes` |
| `StateDelta` | `base_record: QualifiedId`, `apply_operation: QualifiedId`, `delta: bytes` |
| `ReadoutResult` | `operation: QualifiedId`, `arguments: TypedParameterRecord`, `value: TypedValue` |

State bytes use the referenced representation contract. A readout result uses
its operation's output type and is not implicitly mergeable state.

### Coverage

| Field | Type | Definition |
| --- | --- | --- |
| `extent` | `TimeExtent \| DatasetExtent` | Time interval or dataset snapshot/partition extent |
| `completeness` | `Complete \| Partial \| Unknown` | Coverage status, separate from mathematical accuracy |
| `evidence` | `EvidenceRef[]` | Evidence establishing coverage/freshness |

| Extent | Fields |
| --- | --- |
| `TimeExtent` | `clock: ClockDefinition`, `start: Timestamp`, `end: Timestamp`, `bounds: IntervalBounds` |
| `DatasetExtent` | `snapshot: QualifiedId`, `partitions: TypedSet`, `selection: Optional<TypedPredicate>` |

### GuaranteeEvidence

| Field | Type | Definition |
| --- | --- | --- |
| `guarantee_id` | `QualifiedId` | Schema guarantee being evaluated |
| `scope` | `GuaranteeScope` | Concrete population/readout/evaluation scope |
| `status` | `Established \| Unverified \| Invalid` | Whether the conditions for this instance are established |
| `evidence` | `EvidenceRef[]` | Resolvable evidence and its validity/provenance |

## Referenced types

| Type | Definition |
| --- | --- |
| `QualifiedId` | Namespace plus immutable identifier |
| `Version` | Version identifier with an explicit compatibility definition |
| `TypeDefinition` | Resolvable, versioned scalar/tuple/collection/state type |
| `TypedValue / TypedTuple / TypedRecord / TypedSet` | Values whose types and field identities resolve through a TypeDefinition |
| `ParameterSchema / TypedParameterRecord` | Versioned parameter definition and values validated against it |
| `CanonicalSourceBinding` | Resolvable source definition with stable field bindings |
| `FieldDefinition` | Field ID, name, type, nullability and optional unit |
| `TypedExpression / TypedPredicate` | Canonical expression with resolved input/output types |
| `VersionedSemanticContract / RuleRef` | Versioned semantic definition or compatibility rule; not executable code supplied by a record |
| `TypedErrorDefinition / TypedBound` | Error quantity and bound with defined types, units and interpretation |
| `Probability` | Finite number in [0, 1] |
| `GuaranteeScope` | Population selector, operation arguments and covered evaluation set; distinguishes per-row, whole-result and repeated-evaluation claims |
| `ClockDefinition / Timestamp / IntervalBounds` | Clock/time unit, time value and inclusive/exclusive interval boundaries |
| `ProvenanceRef` | Reference to existing frame/materialization metadata: producer, epoch, sequence and plan generation |
| `EvidenceRef` | Immutable evidence reference including issuer, observation time, validity and subject scope |
