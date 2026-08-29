# Physical planning for collector and backend execution

> Status: proposed
>
> MVP relation: required to turn one Planner selection into matching collector
> and backend runtime plans.
>
> Scope: the ASAPQuery-backend physical-planning step between ASAPPlanner's
> selected post-ASAP workload DAG and the two runtime executors:
> ASAPCollector and the ASAPQuery data plane.

Developer guides:
[Planner adapter and physical compiler](../developer_docs/control-plane-physical-compiler.md)
and [runtime plan publication](../developer_docs/control-plane-plan-publication.md).

## TL;DR

ASAPPlanner selects a logical plan. That plan says which summaries and exact
operations answer a workload, but it intentionally does not choose machines,
shards, runtime windows, transport modes, or storage routes.

ASAPQuery-backend performs one physical compile that produces both runtime
views of the decision:

```text
selected post-ASAP workload DAG
              |
              v
      physical compilation
         /             \
        v               v
CollectorSubplan     BackendSubplan
CollectorPlan(s)     BackendPlan
```

The two subplans share the same plan and materialization identities. They are
never derived independently and are never considered active unless both sides
confirm a compatible decision.

## 1. Why this layer exists

ASAPPlanner owns logical choices such as:

- exact accumulator versus approximate summary;
- summary family, algorithm, and parameters;
- reduction and grouping strategy;
- logical sharing and composition;
- summary readout; and
- exact fallback.

The runtime still needs deployment decisions that do not belong in Planner:

- which collector or backend stage runs each operation;
- how logical state is sharded or shared;
- which streaming panes materialize a query time range;
- whether state is sent as raw observations, full summaries, or deltas;
- where state is stored and queried;
- when a new plan becomes active; and
- how incompatible or failed updates are rolled back.

Skipping this layer causes two common failures:

1. treating a logical `SummaryAgg` as though it already names a collector
   process and runtime configuration; and
2. deriving collector and backend plans separately, allowing family,
   parameters, grouping, windows, or identities to drift.

The physical compiler closes both gaps in one operation.

## 2. Input contract

The compiler receives:

- the selected post-ASAP DAG for the whole workload;
- stable workload/query correlation information;
- collector and backend capability snapshots;
- deployment topology and stage boundaries;
- workload statistics and resource constraints;
- runtime window, freshness, and retention policy; and
- transmission and storage policy.

Shared logical nodes remain shared at this boundary. The compiler must not
first flatten the workload into independent per-query or per-metric rows.

The selected DAG may contain summary producers, estimates, merges,
subtractions, deletes, joins, exact operations, shared sub-DAGs, and
`KeepPreAsap` fallback. A pinned Planner revision may also carry explicit
execution phases and result guarantees. The compiler consumes those canonical
values rather than defining local equivalents.

## 3. Output contract

One compile returns one `CompiledPlan` with:

- a collector subplan containing one `CollectorPlan` for every targeted
  collector;
- a backend subplan containing one `BackendPlan` for the ASAPQuery data
  plane;
- a shared plan envelope;
- content-addressed materialization identities; and
- a validation record showing that both subplans were produced from the same
  selected DAG and capability snapshots.

### Shared plan envelope

| Field | Meaning |
| --- | --- |
| `plan_id` | Content-addressed identity of the logical selection, topology identity, and semantic constraints. |
| `plan_version` | Ordered update within the same plan identity, such as changed sizing or lifecycle policy. |
| `activation` | Earliest time at which both subplans may become authoritative. |
| `expiry` | Optional time after which the plan may no longer produce or serve state. |
| `backend_compat` | Compatibility identity for BackendPlan and emitted summary-state schemas. |
| `planner_revision` | Immutable ASAPPlanner revision used for the selection. |

Mutable lifecycle or sizing fields do not silently change the identity of an
existing version. Reusing the same `(plan_id, plan_version)` for different
content is invalid.

### Materialization identity

A materialization is the physical state built for one selected logical
summary producer. Its identity includes every property required for safe
reuse and merge, including:

- bound source and filters;
- summarized value/item;
- summary family, algorithm, and parameters;
- reduction and grouping layout;
- logical/physical window contract; and
- state schema compatibility.

Placement, transport cadence, or storage location may change without
pretending a semantically different summary is the same materialization.

Explain or viewer node IDs are traceability metadata, not materialization
identity.

## 4. Partitioning the selected DAG

The baseline partition follows data availability:

- operations that consume new observations and construct maintained state
  belong on the update side;
- operations that read, merge, or transform maintained state into query
  answers belong on the readout side; and
- `KeepPreAsap` remains exact backend/archive execution.

In the current Planner vocabulary, `SummaryAgg` is the principal update-side
boundary and `SummaryEstimate` is a readout. Summary merge and other state
composition are placed according to topology, capabilities, and transport
cost without changing their logical semantics.

If the pinned Planner revision provides explicit execution availability or
phase assignments, those validated phases are authoritative. The compiler
must not infer a conflicting phase from node names.

An operation may be assigned only to an executor that advertises its full
semantics. A nameable Planner alternative is not automatically deployable.

## 5. Collector subplan

The collector subplan follows ASAPCollector's
[collection-plan interface](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/developer_docs/opamp-config-push.md).

For each assigned collector, it specifies:

- target collector and capability snapshot;
- the shared plan envelope;
- source metrics and matchers;
- materialization identities;
- summary family, algorithm, parameters, and accuracy requirement;
- reduction and grouping semantics;
- concrete streaming windows and lateness;
- local sharding;
- raw, full, or delta transmission; and
- backend endpoint/schema compatibility.

OpAMP is the delivery transport. The payload is the versioned
`asap-collector-plan.yaml` document, not ASAPPlanner's Rust IR and not the
collector's complete bootstrap configuration.

## 6. Backend subplan

The backend subplan follows
[`backend-plan.md`](control-plane-backend-plan.md).

It specifies:

- the same plan envelope;
- every expected materialization;
- the collector assignments producing its state;
- exact summary family, algorithm, parameters, grouping, and windows;
- ingestion/storage destination;
- query capabilities and readouts satisfied by the materialization;
- remaining backend-side operators; and
- selected result guarantees when provided by Planner.

The backend does not reconstruct the chosen summary from query text or stored
state. It installs and executes the control plane's exact decision.

## 7. Sharing, sharding, and merge

One shared logical producer remains one logical materialization even when it
serves several queries. The backend may associate several routing/readout
entries with that one materialization.

A materialization may have several physical producers when sharded across
collectors. The compiler records all producers and inserts a compatible merge
at the selected boundary. A merge is legal only when every input agrees on
the materialization contract and the selected family supports merge.

Sharding does not create several unrelated logical summaries, and sharing
does not allow consumers with incompatible filters, reductions, grouping,
windows, parameters, or guarantees to reuse state.

## 8. Window and transmission decisions

Planner time ranges express query semantics. The physical compiler chooses
streaming panes capable of answering those ranges.

For the MVP:

- panes are anchored and tumbling;
- pane composition must exactly cover each claimed query range;
- allowed lateness and watermark behavior are explicit;
- incompatible alignment is rejected; and
- freshness policy is shared with the backend readiness check.

Transmission is independent of logical summary choice:

- `raw` forwards selected observations for exact/backend execution;
- `full` sends complete summary state; and
- `delta` sends ordered state changes plus periodic full checkpoints.

Delta is legal only when collector and backend advertise the same state,
sequence, and checkpoint semantics. Every payload identifies its plan,
materialization, producer, window, sequence, and base/checkpoint.

### Aggregation placement

Planner's reduction and grouping are logical requirements. The physical
compiler decides where that reduction runs without changing them. For example,
`sum by (region) (rate(http_requests_total[5m]))` may maintain one summary per
`region` at collectors, while a query with no grouping may use one
whole-workload materialization. A per-series or per-group result must never be
silently collapsed into a global result.

### State representation

Dense versus sparse state is a physical representation choice, not a new
summary choice. For example, an HLL materialization may use sparse state for
low-cardinality groups and promote to dense state as cardinality grows, but
both representations must preserve the same HLL parameters, merge semantics,
wire compatibility, and accuracy contract.

The compiler may select a representation only when both producer and consumer
advertise compatible support. Otherwise it uses the declared fallback or
rejects the plan. Representation details such as collector configuration field
names belong in the collector interface, not in this design.

## 9. Compile and activation sequence

1. Validate the selected DAG against both capability snapshots.
2. Allocate update/readout operators and physical producers.
3. Choose compatible windows, transmission, and storage routes.
4. Construct materialization identities.
5. Emit both subplans from the same in-memory decision.
6. Validate cross-subplan equality for all shared contracts.
7. Stage both subplans before `activation`.
8. Require backend installation and collector semantic application reports.
9. Route queries to the new plan only after both sides report compatible
   active identities.
10. Retire old state after its readers and lateness horizon drain.

If any step fails, the previous unexpired plan remains authoritative. A
partial push, OpAMP delivery acknowledgement, file write, or process restart
does not constitute plan activation.

## 10. Fail-closed rules

The compiler or runtime rejects the plan when:

- an assigned executor lacks a required family, algorithm, grouping, phase,
  readout, window, or transmission capability;
- collector and backend materialization contracts differ;
- a required accuracy guarantee is unknown or insufficient;
- a merge combines incompatible state;
- delta sequencing/checkpoint semantics do not match;
- plan versions conflict or lifecycle conditions disallow activation; or
- semantic application evidence is missing.

It must never substitute another family, parameter, grouping layout,
accuracy target, or transmission semantics to make an invalid plan appear
deployable.

## 11. Example

For:

```promql
quantile_over_time(0.95, request_duration_seconds{region="us-east"}[5m])
```

Planner may select a per-entity DDSketch summary and a p95 readout. The
physical compiler may then:

- assign DDSketch construction to selected collectors;
- choose one-minute panes that compose into the five-minute query range;
- transmit deltas every ten seconds with periodic full checkpoints;
- declare one shared DDSketch materialization in BackendPlan; and
- route the p95 query readout to that materialization.

The compiler does not change DDSketch to KLL, change the accuracy parameter,
or aggregate series together merely because another physical layout would be
cheaper. Such a change requires selection of a different valid Planner
candidate.

## 12. Non-goals

This document does not define:

- query parsing or summary selection;
- internal Planner serialization;
- exact protobuf field numbers;
- summary-state byte encoding;
- collector bootstrap configuration;
- storage-engine implementation; or
- query-engine implementation details.

## 13. Definition of done

The compiled-plan design is satisfied when:

- one compile produces both subplans;
- all shared identities and semantic fields match;
- shared Planner nodes remain shared materializations;
- sharded producers merge only under a valid contract;
- unsupported shapes fail before activation;
- both plans stage and activate atomically from the user's perspective;
- emitted state carries the active identities;
- the data plane serves without replanning; and
- a deliberately mismatched or partially applied plan is rejected in an
  end-to-end test.
