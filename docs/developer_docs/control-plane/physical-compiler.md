# Developing the Planner adapter and physical compiler

> Interface status: target public API. Existing migration modules must converge
> on this boundary.

## Current implementation boundary

The production path has not yet converged on the public interfaces below. It
currently consumes ASAPPlanner types pinned in `control_plane/Cargo.toml`, then
uses `physical::colored_dag::StageAllocator`, `ThreeStageEmitter`, and
`backend_plan::from_stage_config` to produce agent YAML and `BackendPlan`.
Callers must not treat the target `PlanningRequest`, `PhysicalCompiler`, or
`CompiledPlanBundle` examples below as implemented Rust APIs.

ASAPPlanner owns logical semantics and summary selection. In particular, a
deployment override may choose only a family compatible with the selected
statistic; the physical compiler must reject or ignore an incompatible
override, never change the statistic to make the override fit. Upgrading the
Planner pin is a whole-interface migration because newer Planner revisions
change the post-ASAP family, reduction, grouping, and maintenance types.

## 1. Code architecture

The control plane has three public layers:

```text
PlanningRequest
      |
      v
PlannerAdapter ----------> SelectedLogicalPlan
                                  |
                                  v
PhysicalCompiler -------> CompiledPlanBundle
                            |             |
                            v             v
                       CollectorPlan   BackendPlan
```

- **Planner adapter** owns the typed call to ASAPPlanner. It supplies the whole
  workload and receives one selected logical plan without copying Planner IR.
- **Physical compiler** adds backend-owned placement, windows, transport, and
  runtime capabilities without changing logical semantics.
- **Plan bundle** is the only output passed to publication. CollectorPlan and
  BackendPlan are created together and share identities.

Logical query parsing, summary alternatives, guarantees, and candidate search
remain public ASAPPlanner interfaces. Runtime publication is documented in
[Runtime plan publication](plan-publication.md).

## 2. Public interfaces and definitions

### Planner adapter

```rust
pub trait PlannerAdapter {
    type Error;

    fn select(
        &self,
        request: PlanningRequest,
    ) -> Result<SelectedLogicalPlan, Self::Error>;
}
```

`PlanningRequest` is backend-owned request context around Planner's canonical
workload value:

```rust
pub struct PlanningRequest {
    pub workload: asap_planner::Workload,
    pub schema: asap_planner::SchemaCatalog,
    pub constraints: asap_planner::PlanningConstraints,
    pub cost_inputs: asap_planner::CostInputs,
    pub planner_revision: String,
}
```

Input definitions:

| Field | Definition |
| --- | --- |
| `workload` | Complete workload; shared queries must not be split into independent calls. |
| `schema` | Source/label/type information required to bind queries. |
| `constraints` | Accuracy and logical requirements supplied by the caller. |
| `cost_inputs` | Measured/declared logical cost inputs; unknown values stay unknown. |
| `planner_revision` | Immutable Planner build/revision used for reproducibility. |

`SelectedLogicalPlan` wraps Planner's public selected post-ASAP workload plan
and correlation metadata; it does not define another DAG:

```rust
pub struct SelectedLogicalPlan {
    pub workload_plan: asap_planner::SelectedWorkloadPlan,
    pub planner_revision: String,
    pub query_ids: Vec<String>,
}
```

Why this interface exists: it prevents adapters, protocols, and physical
planning from each implementing their own query-to-summary mapping.

### Physical compiler

```rust
pub trait PhysicalCompiler {
    type Error;

    fn compile(
        &self,
        selected: SelectedLogicalPlan,
        environment: DeploymentEnvironment,
        policy: RuntimePolicy,
    ) -> Result<CompiledPlanBundle, Self::Error>;
}
```

```rust
pub struct DeploymentEnvironment {
    pub topology: DeploymentTopology,
    pub collectors: Vec<CollectorTarget>,
    pub backend: BackendTarget,
    pub capability_snapshot_id: String,
}

pub struct RuntimePolicy {
    pub activation: Timestamp,
    pub expiry: Option<Timestamp>,
    pub freshness: FreshnessPolicy,
    pub retention: RetentionPolicy,
    pub transmission: TransmissionPolicy,
}

pub struct CompiledPlanBundle {
    pub envelope: PlanEnvelope,
    pub collector_plans: Vec<CollectorPlan>,
    pub backend_plan: BackendPlan,
}
```

Supporting public types:

| Type | Definition |
| --- | --- |
| `DeploymentTopology` | Runtime stages, network relationships, and isolation boundaries available for placement. |
| `CollectorTarget` | Collector identity, edge assignment, endpoint reference, and advertised capability snapshot. |
| `BackendTarget` | Data-plane identity, endpoint reference, storage routes, and advertised capabilities. |
| `FreshnessPolicy` | Maximum readiness lag, watermark, and allowed-lateness requirements. |
| `RetentionPolicy` | Duration and lifecycle rules for active/draining materializations. |
| `TransmissionPolicy` | Allowed raw/full/delta modes, cadence, encoding, and checkpoint limits. |
| `PlanEnvelope` | Shared `plan_id`, `plan_version`, activation/expiry, backend compatibility, and Planner revision. |
| `CollectorPlan` | Versioned public YAML execution contract owned by ASAPCollector. |
| `BackendPlan` | Versioned public data-plane materialization/routing contract defined in this repository. |

Output definitions:

| Output | Definition |
| --- | --- |
| `envelope` | Shared plan/version/activation/compatibility identity. |
| `collector_plans` | One plan per targeted collector, following ASAPCollector's public CollectorPlan schema. |
| `backend_plan` | Matching data-plane materialization and routing contract. |

### What the compiler puts in CollectorPlan

Each targeted Collector receives a complete executable projection of the
physical decision. It is not a Planner DAG and not a pointer requiring the
Collector to recover missing semantics.

| CollectorPlan section | Required content | Consuming ASAPCollector component |
| --- | --- | --- |
| envelope | Schema version, kind, shared plan ID/version, generation/activation/expiry, backend compatibility, Planner revision, candidate/query traceability | OpAMP receiver and plan validator |
| target | Collector instance UID, edge assignment, capability snapshot hash | Control-plane agent and capability validator |
| materialization reference | Content fingerprint describing maintained semantics plus logical-node traceability | Plan applier and materialization registry |
| input | Source metric, canonical matchers, and whether the summarized input is sample value or a named label | OTLP/OTAP metric adapter and observation router |
| summary | Raw, exact-aggregation, or sketch kind; concrete algorithm; typed parameters; requested accuracy | Precompute factory and accumulator/sketch implementation |
| reduction/grouping | Per-entity or reduction semantics, retained/excluded labels, and independent or shared grouping layout | Series-key builder and aggregation state manager |
| window | Kind, size, slide/anchor, allowed lateness, flush behavior | Window manager |
| placement | Collector stage and local shard assignment | Processor topology/runtime |
| transmission | Raw/full/delta mode, encoding and schema version, emission cadence, checkpoint interval and sequence scope | Envelope encoder and delta sender |
| output/lifecycle | Backend endpoint reference, unsupported behavior, drain/retirement policy | Exporter, plan lifecycle manager, and application reporter |

For a Collector-side DDSketch materialization, the relevant projection is
conceptually:

```yaml
materializations:
  - id: mat:sha256:98f1...
    input:
      metric: request_duration_seconds
      matchers: [{label: region, op: eq, value: us-east}]
      value: sample_value
    summary:
      family: sketch
      algorithm: ddsketch
      parameters: {alpha: 0.01}
      accuracy: {kind: epsilon, epsilon: 0.01}
    reduction: {kind: reduce, by: [service], without: false}
    grouping: {kind: per_subpopulation_instance}
    window: {kind: tumbling, size: 1m, slide: 1m, allowed_lateness: 10s}
    placement: {stage: collector, shards: 1}
    transmission:
      mode: delta
      encoding: ddsketch-v1
      schema_version: 1
      emit_every: 10s
      full_checkpoint_every: 1m
      sequence_scope: materialization_window_producer
    output: {endpoint_ref: asapquery-primary}
```

The materialization `id` above identifies the maintained summary definition.
It is not the SID. During collection and ingest, each concrete canonical label
set under that definition resolves to its own SID.

### What the compiler puts in BackendPlan

The matching BackendPlan is the consumer and query-serving projection of the
same decision:

| BackendPlan section | Required content | Consuming ASAPQuery-backend component |
| --- | --- | --- |
| envelope | The same plan ID/version, activation/expiry, compatibility identity and Planner revision | Backend plan manager |
| materialization descriptor | Same materialization fingerprint, canonical metric, retained grouping keys, capability, aggregation kind/parameters, accuracy, policy fingerprint and lifecycle | SID resolver and instance-metadata registry |
| producers/ingest | Expected Collector/producer IDs, input payload kind, state schema, full/delta lineage and optional backend-precompute placement | OTLP ingest engine and precompute engine |
| window/coverage | Pane/window compatibility, lateness, expected coverage and merge rules | Ingest validation and summary store |
| storage | Warm/archive/remote route, retention, retirement and expiry | Summary store engine and archive adapter |
| query routes | Query IDs or canonical capability match, materialization reference, readout/operator, grouping/window composition and remaining backend operators | Query classifier, router and readout engine |
| guarantee/fallback | Selected guarantee and explicit exact/archive fallback behavior | Query engine and response metadata |

The backend declaration does not contain a pre-enumerated SID for every label
value. It installs immutable metadata for the materialization; the ingest/SID
resolver then binds each canonical materialized series to an SID and the store
indexes its windows under that SID.

### One decision, two component graphs

```text
CollectorPlan
  -> plan validator
  -> source matcher / collection router
  -> windowed raw, exact-aggregation, or sketch state
  -> full/delta encoder + SID dictionary
  -> exporter

BackendPlan
  -> plan installer
  -> ingest validator / optional backend precompute
  -> SID resolver + instance metadata
  -> summary store
  -> query route + readout / explicit exact fallback
```

Cross-plan validation proves that every Collector-produced materialization has
one compatible backend ingest/storage declaration and that every planned query
route references a declared materialization. A Backend-only precompute has a
BackendPlan producer but no Collector materialization; a raw pass-through has
matching raw transmission and ingest/archive declarations.

The compiler error must identify an unsupported capability, invalid placement,
window incompatibility, identity conflict, or invalid selected guarantee. It
must not silently substitute another logical summary.

Why this interface exists: one compile guarantees that producer and consumer
receive the same family, parameters, grouping, window, and materialization
identity.

## 3. Adding and verifying functionality

### Add a deployment topology

1. Add a public `DeploymentTopology` variant and its required target fields.
2. Teach `PhysicalCompiler::compile` how selected operators can be placed on it.
3. Reject plans requiring an unavailable stage/capability.
4. Verify the output contains one complete CollectorPlan for every producer and
   one BackendPlan referencing all produced materializations.

Interpretation: a successful bundle means both runtime views are complete and
cross-consistent; it does not mean they have been activated.

### Add a transmission mode

1. Extend public `TransmissionPolicy` and capability declarations.
2. Define representation, sequencing, checkpoint, and fallback requirements.
3. Emit the same compatibility identity into producer and consumer plans.
4. Verify unsupported endpoint combinations return a compile error.

For delta, verify duplicate, missing, reordered, and recovery-checkpoint cases.

### Add a physical window policy

1. Add the public policy variant with anchor, size, slide, and lateness.
2. Prove it covers the selected logical range without changing semantics.
3. Include it in materialization identity.
4. Verify generated collector/backend windows are identical and incompatible
   query ranges fail compilation.

### Required output checks

- deterministic inputs produce deterministic plan/materialization identities;
- shared logical producers remain shared materializations;
- every referenced materialization has a producer and backend declaration;
- all query IDs remain traceable; and
- unsupported inputs return structured errors, never partial bundles.
