# Developing the Planner adapter and physical compiler

> Interface status: target public API. Existing migration modules must converge
> on this boundary.

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
