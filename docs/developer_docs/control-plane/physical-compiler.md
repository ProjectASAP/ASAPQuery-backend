# Developing the Planner adapter and physical compiler

> Interface status: implemented MVP API in
> `control_plane::physical::compiler`.

## Current implementation boundary

The compiler consumes ASAPPlanner types pinned to the revision exposed as
`physical::compiler::PLANNER_REVISION`, selects
from Planner's legal candidate space with backend-owned cost and evidence
inputs, and emits one `PhysicalPlan`. The plan contains CollectorPlan,
PrecomputePlan, BackendPlan, and QueryPlan projections compiled from the same decision
for every target collector. Legacy
`StageAllocator`/`ThreeStageEmitter` paths remain for older publication flows;
they are not a second semantic planner.

ASAPPlanner owns abstract semantics and selection: summary family and
parameters, summary-maintenance lifecycle, and summary-window framework. The
backend enumerates executor-feasible concrete implementations and supplies
complete workload-scoped cost evidence to Planner. It then retains the
concrete identity corresponding to Planner's selected abstract framework.
Missing or stale implementation evidence makes the candidate unavailable; the
compiler never invents a framework or assigns it an optimistic zero cost.

## 1. Code architecture

The control plane has three public layers:

```text
PlanningRequest + DataWorkload + concrete implementation evidence
      |                                      ^
      | abstract candidates                  | complete physical costs
      v                                      |
ASAPPlanner selection <---------- PhysicalCompiler
      |
      v
PhysicalCompiler -------> PhysicalPlan
                            |       |       |      |
                            v       v       v      v
                      Collector  Precompute Backend Query
                         Plan       Plan     Plan   DAG
```

- **Planner selection boundary** is
  `planner_selection::select_summary_with_evidence`. It enumerates Planner's
  candidates and commits only a legal candidate.
- **Physical compiler** enumerates concrete window/pane/state-layout,
  placement, transport, and runtime implementations without changing the
  Planner-owned abstract framework.
- **PhysicalPlan** is the only output passed to publication. Its CollectorPlan,
  PrecomputePlan, BackendPlan, and QueryPlan projections are created together and share identities.

Logical query parsing, summary alternatives, guarantees, and candidate search
remain public ASAPPlanner interfaces. Runtime publication is documented in
[Runtime plan publication](plan-publication.md).

## 2. Public interfaces and definitions

### Planning request

`PlanningRequest` is backend-owned request context around Planner's canonical
per-query IR:

```rust
pub struct PlanningRequest {
    pub queries: Vec<PlanningQuery>,
    pub evidence: HashMap<String, TopKMembershipEvidence>,
    pub planner_revision: String,
}
```

Input definitions:

| Field | Definition |
| --- | --- |
| `queries` | Canonical `QueryExpr`, source, grouping labels, accuracy, stable query ID, `DataWorkload`, and executor-feasible `window_implementations`. |
| `evidence` | Optional typed TopK membership certificates keyed by query ID. |
| `planner_revision` | Immutable Planner build/revision used for reproducibility. |

TopK evidence is accepted only when its selected lower bound is strictly above
the excluded upper bound, its failure probability is valid, its source is
non-empty, and its observation is fresh under `DeploymentEnvironment`.

```rust
pub struct TopKMembershipEvidence {
    pub selected_lower_bound: f64,
    pub excluded_upper_bound: f64,
    pub interval_failure_probability: f64,
    pub observed_at_unix_ms: u64,
    pub source: String,
}
```

Why this interface exists: it prevents adapters, protocols, and physical
planning from each implementing their own query-to-summary mapping.

Each `WindowImplementationCandidate` carries a backend-owned implementation
identity, its Planner `SummaryWindowFramework`, concrete window/pane/state
layout, and versioned workload-specific CPU, peak-memory, network, storage,
scan, and calibrated weighted-cost evidence. The compiler collapses several
physical realizations of one framework to the cheapest complete one before
calling Planner, then resolves Planner's result back to that retained concrete
identity. Physical identities never enter post-ASAP IR.

### Physical compiler

```rust
impl PhysicalCompiler {
    pub fn compile(
        &self,
        request: PlanningRequest,
        environment: DeploymentEnvironment,
    ) -> Result<PhysicalPlan, CompileError>;
}
```

```rust
pub struct DeploymentEnvironment {
    pub collector_ids: Vec<String>,
    pub capability_snapshot_id: String,
    pub observed_at_unix_ms: u64,
    pub max_evidence_age_ms: u64,
    pub plan_version: u64,
    pub activation_unix_ms: u64,
    pub expiry_unix_ms: Option<u64>,
    pub backend_compat: String,
}

pub struct PhysicalPlan {
    pub envelope: PlanEnvelope,
    pub collector_plans: Vec<CollectorPlan>, // complete per-target projections
    pub precompute_plan: PrecomputePlan,      // backend streaming materializations
    pub transmission_plan: TransmissionPlan,  // producer/frame wire contract
    pub backend_plan: BackendPlan,
    pub query_plan: QueryPlan,                // node-ID physical serving DAG
}
```

Supporting public types:

| Type | Definition |
| --- | --- |
| `DeploymentEnvironment` | Target collector IDs, capability snapshot identity, planning time, and evidence freshness policy. |
| `PlanEnvelope` | Shared deterministic `plan_id`, generation time, capability snapshot, and Planner revision. |
| `CollectorPlan` | Serializable execution projection consumed by ASAPCollector. |
| `PrecomputePlan` | Authoritative materialization, ingest, state-schema, and producer contract consumed directly by the backend runtime. |
| `TransmissionPlan` | Exact per-producer mode, encoding, cadence, destination, checkpoint policy, and frame identity contract. |
| `BackendPlan` | Versioned public data-plane materialization/routing contract defined in this repository. |
| `QueryPlan` | Canonical-query keyed executable DAG with exact materialization bindings and fallback policy. |

Current MVP limits are explicit: time-series sources and supported summary
materializations are accepted; table sources and unexecutable selected families
return `CompileError`. Versioned stage/activate/drain/retire lifecycle is part
of publication; richer topology placement remains follow-up work.

Output definitions:

| Output | Definition |
| --- | --- |
| `envelope` | Shared plan/version/activation/compatibility identity. |
| `collector_plans` | One plan per targeted collector, following ASAPCollector's public CollectorPlan schema. |
| `precompute_plan` | Materializations plus `/v1/metrics` ingest semantics, typed state schemas/encodings, and allowed producers; it is installed directly and contains no query-string jobs. |
| `transmission_plan` | One rule per producer/materialization/schema binding plus the mandatory frame identity and sequencing scope. |
| `backend_plan` | Matching data-plane materialization and routing contract. |
| `query_plan` | Canonical query identity, explicit fallback policy, node-ID DAG, and exact per-node materialization bindings. |

### QueryPlan execution boundary

`QueryPlan` is physical and executable; it is not a serialized copy of
post-ASAP IR. Each `ReadMaterialization` node binds one policy fingerprint,
metric, family/parameters, stored SID grouping layout, output reduction, and
window. The data plane resolves only that fingerprint through the
`policy_fp -> SID` reverse index and verifies SID metadata exactly. It never
scans the catalog for a serving-time candidate.

Graph traversal is separate from node definitions and store semantics.
Activation validates roots, edges, bindings, reachability, and cycles.
Execution uses the validated topological order and memoizes every node result,
so a shared node in a diamond DAG performs one store/operator execution. A
typed node failure follows the entry's explicit fallback route.

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

### Summary frame identity

Until modified OTLP has dedicated identity fields, Collector attaches reserved
data-point attributes under `asap.frame.*`: identity version, plan ID/version,
backend compatibility, materialization and schema IDs, producer ID/epoch,
canonical series fingerprint, sequence, full/delta kind, encoding, and
checkpoint/base IDs. Window start/end
remain the typed data-point timestamps. The backend removes reserved attributes
before building the series label key and rejects the complete request before
writing any frame when one identity, schema, encoding, materialization, or full
payload does not match the active TransmissionPlan. HTTP 2xx / gRPC OK is the
delivery acknowledgement. Retrying the same full frame is idempotent because
the identity selects the same SID, label set, and window replacement; no second
application-level ACK or transport WAL is part of this contract.

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

### Add a window implementation

1. Use an existing Planner-owned `SummaryWindowFramework`; new abstract
   frameworks must first be added to Planner.
2. Advertise an executor-feasible concrete implementation with complete,
   fresh `DataWorkload` evidence.
3. Prove its panes and state layout implement the framework and cover the
   selected query-time range.
4. Retain the concrete implementation identity in the physical plan and
   materialization identity, never in Planner IR.
5. Verify missing evidence and incompatible executor semantics fail closed.

### Required output checks

- deterministic inputs produce deterministic plan/materialization identities;
- shared logical producers remain shared materializations;
- every referenced materialization has a producer and backend declaration;
- all query IDs remain traceable; and
- unsupported inputs return structured errors, never partial bundles.
