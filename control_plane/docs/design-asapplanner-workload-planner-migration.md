# Replace the legacy query planner with ASAPPlanner workload planning

> Status: proposed, 2026-08-26
>
> Scope: migrate ASAPQuery-backend from its overlapping, per-query
> query/replacement planning paths to ASAPPlanner's workload-wide replacement
> search. This document does **not** propose removing ASAPQuery's
> deployment-specific placement, materialization, routing, or serving logic.

## 1. Decision

Retire the legacy ASAPQuery query/replacement planner after the new path has
passed shadow and end-to-end validation.

ASAPPlanner becomes the single owner of:

```text
query text
  -> pre-ASAP IR
  -> workload common-subexpression sharing
  -> replacement candidate search
  -> cost-based strategy selection
  -> selected post-ASAP DAG
```

ASAPQuery-backend remains the owner of:

```text
selected post-ASAP DAG
  -> deployment placement
  -> collector/backend stage allocation
  -> BackendPlan
  -> materialization and routing
  -> data-plane execution and archive fallback
```

The boundary is intentional: ASAPPlanner decides *what a query sub-DAG may be
replaced with*; ASAPQuery decides *where the selected replacement runs, how it
is represented on the wire, and how it is served*.

## 2. Why the legacy query planner should be removed

ASAPQuery currently has several overlapping planning paths:

- `query_planning.rs` reduces a set of query strings into per-metric
  capabilities and then unions the required sketch families.
- `asap_tier_implement.rs` finds independently realizable aggregate roots and
  implements each root separately.
- `sketch_algebra/lower.rs` and parts of `optimizer/rules` make additional
  summary-family or replacement decisions.
- serving-time lowering can reconstruct or infer decisions after the control
  plane has already planned them.

Flattening a query into `(metric, capability, sketch family)` loses the IR
relationships needed for workload optimizations. In particular, it cannot
faithfully represent:

- common subexpressions shared by multiple queries;
- one shared scan or shared summary sub-DAG with several consumers;
- `AvgToSumOverCountStrategy`;
- group-by rollup reuse;
- shared versus independent grouped summaries, including Hydra layouts;
- deriving a smaller compatible top-k result from a larger one;
- post-ASAP node provenance and strategy explanations.

Keeping two implementations for these decisions would also let the control
plane, data plane, and ASAPPlanner silently select different physical summary
families or parameters for the same query.

## 3. Target architecture

```text
Protocol/source adapters
  -> ASAPQuery O11yMetricsQuery
  -> ASAPPlanner QueryWorkload
     BatchEntry / RepeatingEntry / QueryRequirements
                    |
                    v
          parse and schema-bind every query
                    |
                    v
       Vec<(CallerId, Rc<QueryExpr>)>
                    |
                    v
       ASAPPlanner search_workload_with
                    |
                    v
        PlanSpace::global_selection
                    |
                    v
       ASAPQuery SelectedWorkloadPlan
              /                 \
             v                   v
       ExplainPlan      deployment placement
                               |
                               v
                          BackendPlan
                               |
                               v
                           data_plane
```

The selected workload and its shared `Rc<QueryExpr>`/`Rc<SummaryNode>`
identities must remain intact until placement and materialization are complete.
It must not be flattened into independent per-query or per-metric plans before
that point.

## 4. What is removed, retained, and changed

### 4.1 Remove after cutover

- The capability-union allocation algorithm in `query_planning.rs` as a
  production planner.
- Per-aggregate-root `implement_tree` behavior in
  `asap_tier_implement.rs`.
- Replacement/sketch-family choice duplicated in `optimizer/rules` and
  `sketch_algebra/lower.rs`.
- Serving-time use of a cost model to guess a decision already made by the
  control plane.
- `BackendStageConfig -> BackendPlan` as the canonical source of planning
  decisions. It can remain temporarily as a legacy compatibility adapter.
- The placeholder `types_v2::WorkloadPlan`, `BindingName`, and
  `QueryExprPlaceholder`; ASAPPlanner already represents the workload as
  canonical roots and performs real `Rc`-identity CSE without `Ref` or
  `LetBinding` placeholders.
- The backend-local `types_v2::QueryLanguage`; use ASAPPlanner's workload
  language type.
- Backend-local one-shot/periodic query-shape modeling where it duplicates
  `BatchEntry` versus `RepeatingEntry`.
- The duplicate `DataDistribution` and overlapping statistical fields in
  `WorkloadCharacteristics`; populate ASAPPlanner `DataCharacteristics` and
  keep only deployment-only constraints such as collector memory budget in
  the placement layer.

Delete these paths only after the new workload path is the production source
of `BackendPlan`. During migration they remain available for shadow comparison
and rollback.

### 4.2 Retain and adapt

- The HTTP API and workload registry.
- Replanning triggers and runtime telemetry.
- ASAPQuery's cost model, implemented through ASAPPlanner's `CostModel` trait.
- Deployment constraints and resource budgets.
- Stage splitting and placement across collector, backend, and archive.
- Collector configuration generation.
- `PolicyFingerprint` and persistent materialization identity.
- `BackendPlan`, `RoutingIndex`, push, hot reload, and plan versioning.
- Data-plane summary execution and cold/archive fallback.
- Monitoring and epsilon-allocation behavior that is independent of logical
  replacement search.

### 4.3 Add

- An adapter-neutral ASAPQuery `O11yMetricsQuery` ingestion model.
- Independent source/protocol adapters, beginning with Prometheus query and
  rule adapters, that produce `O11yMetricsQuery` values.
- A thin `O11yMetricsQuery -> ASAPPlanner QueryWorkload` conversion.
- A deployment-owned `SelectedWorkloadPlan` representation.
- A materializer from ASAPPlanner's global selection into the selected
  post-ASAP DAG and then into `BackendPlan`.
- An explain/debug endpoint over ASAPPlanner's existing DAG export and
  replacement-explanation types.
- Planning phase timings, search-size metrics, deadlines, and cancellation.

## 5. Migration stack

Each phase should land as a separate, buildable PR. Later PRs may be stacked
while earlier ones are under review.

### PR 1: ASAPPlanner pin and API compatibility

Move every ASAPPlanner dependency to the same immutable revision. The first
target containing the workload planner and DAG-viewer changes is
`747c66a8958409afd727b4d8046e16c653d228f6` (ASAPPlanner PR #283).

Update together:

- `planner-types` in `control_plane`, `data_plane`, and the local shared-types
  crate;
- `asap-aware-mapping`;
- `asap-frontend-promql`;
- `Cargo.lock`.

The new mapping API removes the former `bind` and `boundary` modules. Migrate
call sites to `asap_aware_mapping::replacement` and its public re-exports:

- `Implementation` and candidate enumeration;
- `ReplacementStrategy` and `ReplacementSubDAG`;
- `search_workload_with` and `default_strategies_with`;
- `PlanSpace::global_selection`;
- `replacement::default_size_params`.

Adapt the ASAPQuery cost model:

- rank `SketchAlgorithm` values and return an exact permutation of the input;
- size `SketchParams` for the selected algorithm;
- preserve deployment-specific extension realization;
- supply CSE recompute and shared-maintenance costs;
- supply subpopulation estimates and grouping-state costs when statistics are
  available;
- expose numeric `estimate_cost` values for observability.

Use ASAPPlanner's `AccuracyTarget` as the only correctness/accuracy input
model. During compatibility migration, `control_plane::types_v2` may re-export
that upstream type, but ASAPQuery must not define a second semantic equivalent.
Delete after API migration:

- the legacy `accuracy_sla: f64` fields and their `1.0 - accuracy_sla`
  conversions;
- the data-plane routing `AccuracyTarget::{Exact, Approximate}` enum;
- any backend-local `CorrectnessPolicy` proposal.

Routing, placement, and execution should consume the selected plan and the
original upstream `AccuracyTarget`, not collapse it to an exact/approximate
boolean.

Acceptance criteria:

- `cargo build --workspace` succeeds;
- existing control-plane and data-plane tests pass;
- no ASAPPlanner crate is pinned to a different revision;
- the compatibility path does not change production output yet.

### PR 2: Adopt ASAPPlanner's canonical workload input

Add a new API such as `POST /api/v1/workloads/plan`. Its JSON is decoded by a
source adapter into ASAPQuery's adapter-neutral `O11yMetricsQuery`, then
converted into ASAPPlanner's existing workload types. Neither layer is a
second planner-domain model:

```json
{
  "queries": [
    {
      "id": "q1",
      "language": "promql",
      "text": "sum by (service) (rate(requests_total[5m]))",
      "accuracy": { "epsilon": 0.01 },
      "schema_refs": []
    }
  ],
  "table_schemas": []
}
```

Requirements:

- stable, caller-visible query IDs;
- map every accuracy field directly into
  `QueryRequirements.accuracy: Option<AccuracyTarget>`;
- map one-shot queries into `BatchEntry` and scheduled queries into
  `RepeatingEntry`;
- use ASAPPlanner's `QueryLanguage` rather than the backend's local language
  enum;
- group mixed-language API requests into one ASAPPlanner `QueryWorkload` per
  language until upstream supports mixed languages in one workload;
- pass table schemas through ASAPPlanner's existing `SchemaCatalog` binder
  before enabling SQL;
- request limits and validation;
- old single-query endpoints adapt into a one-query workload rather than
  maintaining a second planner.

Use a module boundary such as:

```text
control_plane/src/o11y_query/
  mod.rs                    # O11yMetricsQuery and generic conversion
  adapters/
    mod.rs                  # adapter trait/error contract
    prometheus/
      query.rs              # Prometheus instant/range request adapter
      rules.rs              # Prometheus rule-file adapter
      runtime.rs            # Prometheus-only scheduling/alert metadata
```

Keep this as an independent dependency boundary even if it initially lives in
the `control_plane` crate:

```text
adapters/prometheus -> O11yMetricsQuery -> ASAPPlanner workload types
ASAPPlanner          -X-> adapters/prometheus
planner/search       -X-> Prometheus rule/runtime types
```

It can move into a workspace adapter crate later without changing planner or
deployment APIs.

`O11yMetricsQuery` contains only source-independent query information and
reuses ASAPPlanner vocabulary for language, requirements, accuracy, and repeat
interval. It must not copy `QueryExpr`, `AccuracyTarget`, or `QueryLanguage`
into new local enums:

```rust
struct O11yMetricsQuery<Id> {
    id: Id,
    query: Query,
    language: QueryLanguage,
    requirements: Option<QueryRequirements>,
    repetition: Option<RepetitionInterval>,
}
```

The generic converter maps `repetition: None` to `BatchEntry` and
`Some(interval)` to `RepeatingEntry`. Caller IDs remain beside the workload
entries and become the generic IDs passed to CSE/search; they do not require a
second planner `QueryId` type.

ASAPPlanner already owns canonicalization and workload CSE. The backend should
invoke those existing paths and preserve the resulting named
`Rc<QueryExpr>` roots; it must not implement its own AST normalization,
semantic equality, or `QuerySetPlan` capability flattening.

Acceptance criteria:

- one and multiple queries enter the same code path;
- query IDs survive lowering and error reporting;
- equivalent subtrees remain shareable across roots;
- malformed queries and schemas return per-query diagnostics.

### PR 3: Workload-wide search and selection

Run one planning operation for the complete workload:

```rust
let strategies = default_strategies_with(&backend_cost_model);
let space = search_workload_with(roots, &strategies);
let selection = space.global_selection(&backend_cost_model);
```

Build `SelectedWorkloadPlan` from the result while preserving shared node
identity. It must support selected `Replacement::Summary` and
`Replacement::Rewrite` candidates and recursively compose choices for
descendant targets.

This phase should activate and test:

- `SketchAlgorithmStrategy`;
- `SharedSubtreeStrategy`;
- `HydraGroupingStrategy`;
- `AvgToSumOverCountStrategy`;
- `RollupStrategy`;
- `TopKLimitReuseStrategy`.

ASAPPlanner deliberately owns candidate search but not ASAPQuery deployment
placement. Do not copy `dag_export`'s JSON into the runtime contract and do
not infer mappings from labels, hashes, strategy rationale, or viewer node
signatures.

Acceptance criteria:

- compatible scans and sub-DAGs have one shared node identity;
- incompatible filters, windows, groupings, orderings, and schemas do not
  share;
- every chosen replacement records its real strategy name and target;
- selection is deterministic for identical workload, statistics, and cost
  model inputs.

### PR 4: Selected workload to BackendPlan

Introduce a direct conversion:

```text
SelectedWorkloadPlan
  -> deployment placement
  -> materializations and readouts
  -> BackendPlan
```

Continue using `PolicyFingerprint` for persistent runtime identity. Exporter
IDs such as DAG node IDs or `workload_node_id` are scoped to an explain result
and must not become materialization keys.

Review whether the wire needs additive fields for:

- exact versus sketch summary family;
- sketch algorithm and parameters;
- independent versus Hydra grouping layout;
- shared materialization dependencies;
- multiple query/readout consumers of one materialization;
- derived readouts such as `avg = sum / count`, rollups, and top-k prefix
  reuse.

Use additive protobuf fields and retain backward decoding during rollout.

Acceptance criteria:

- two queries sharing one summary produce one materialization and multiple
  legal routes/readouts;
- materialization fingerprints are stable across replans;
- protobuf encode/decode and hot reload preserve the chosen plan;
- the data plane never has to run a cost model to reconstruct the choice.

### PR 5: Data-plane execution coverage

Make the data plane execute or explicitly reject every post-ASAP shape the
control plane may select. Cover at least:

- exact accumulators;
- sketch aggregation and estimate;
- sum/count composition for rewritten Avg;
- reading top-k `k_small` from a compatible `k_large` summary;
- rollup derivation;
- Hydra grouping layouts;
- shared summary dependencies and merges.

Unsupported candidates must be removed before selection or fail planning with
a clear capability diagnostic. They must never be accepted by the control
plane and fail later during query serving.

Acceptance criteria:

- control plane selects once and the data plane consumes that exact choice;
- family, parameters, grouping, and accuracy are not re-derived at serving
  time;
- archive fallback remains available for unsupported queries;
- end-to-end tests prove plan push, hot reload, and serving.

### PR 6: Explain and planner observability

Add an optional explain response or endpoint containing:

- the original pre-ASAP DAG;
- the selected post-ASAP DAG;
- explicit pre-target -> decision -> post-node mappings;
- strategy, concise rationale, rank, and estimated cost;
- query ownership of shared nodes;
- edge schemas;
- phase timings.

Use ASAPPlanner's existing `DagGraph`, `DagDecision`, `TargetReplacement`,
`WorkloadGraph`, `export_post_asap`, and replacement-explanation APIs. Do not
define a second backend graph/decision schema. Production planning calls these
Rust APIs directly; it must not start the Python viewer server or invoke the
`dag_export` binary as a subprocess.

Record at least:

- parse and schema-binding time;
- pre-ASAP IR and CSE time;
- replacement search time;
- global-selection time;
- post-ASAP materialization time;
- `BackendPlan` construction and push time;
- target, candidate, shared-node, and selected-materialization counts.

### PR 7: Shadow rollout and legacy deletion

Introduce a feature flag such as `ASAP_WORKLOAD_PLANNER_V2` and use three
rollout modes:

1. `legacy`: legacy planner emits; new planner is disabled.
2. `shadow`: legacy planner emits; new planner runs and differences are
   recorded.
3. `selected`: new planner emits; legacy planner is available only for
   rollback.

Compare:

- selected summary family and parameters;
- materialization count and fingerprints;
- routing coverage;
- estimated resource cost;
- warm versus archive placement;
- planning latency and errors.

After one release cycle without unexplained mismatches:

- delete the capability-union production planner;
- delete per-root implementation code;
- delete duplicate replacement decisions in optimizer/lowering modules;
- delete serving-time cost-based reconstruction;
- remove the legacy `BackendStageConfig -> BackendPlan` path once no other
  caller requires it;
- keep only the single-query API adapters, not a single-query planner.

## 6. Recurring workloads and Prometheus rules

Repeated execution is a workload property, not an execution loop inside the
query planner. ASAPPlanner should compile a recurring query into a reusable
temporal plan. A scheduler, ruler, or materializer triggers that plan at each
evaluation timestamp.

The recommended first deployment is a **materializer**: ASAP accelerates and
materializes the expensive numeric expression while Prometheus remains the
authority for rule scheduling, pending/firing state, `for`,
`keep_firing_for`, and Alertmanager integration.

```text
Prometheus instant/range queries ----+
                                     |
Prometheus rule files ---------------+--> adapters/prometheus
                                             |          |
                                             |          +--> Prometheus-only
                                             |               runtime metadata
                                             |               (adapter boundary)
                                             v
                                      O11yMetricsQuery
                                             |
                                             v
                               ASAPPlanner QueryWorkload
                                             |
                                             v
ASAPQuery deployment ----------> shared temporal aggregations
        |                    window, retention, labels, summary policy
        v
ASAP QueryEngine
        |
        +----> optional ASAP-aware ruler
        |
        +----> materializer writes derived series to Prometheus
                          |
                          v
                Prometheus evaluates alert state
                (`for`, `keep_firing_for`, Alertmanager)
```

Prometheus rule groups have semantics that a simple cron loop does not
provide: rules in a group use the same evaluation timestamp, execute
sequentially, and skip scheduled evaluations while the previous group
evaluation is still running. The rule importer and trigger service must
preserve these semantics. See the
[Prometheus recording and alerting rule documentation](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/).

Historical ASAPQuery planner code is useful migration input here: it infers
`repetition_delay_ms` from the median query-log inter-arrival time
([frequency.rs](https://github.com/ProjectASAP/ASAPQuery/blob/1fc18be81ec2e0f44fa0ded85151b513ee5312b7/asap-planner-rs/src/query_log/frequency.rs))
and exposes per-query-group repetition configuration
([input.rs](https://github.com/ProjectASAP/ASAPQuery/blob/1fc18be81ec2e0f44fa0ded85151b513ee5312b7/asap-planner-rs/src/config/input.rs)).
That inference remains useful for ad hoc dashboards. A declared rule schedule,
however, is authoritative and must not be replaced by frequency inference.

### 6.1 Use ASAPPlanner's existing repeating-workload model unchanged

ASAPPlanner already defines `QueryRequirements.accuracy: Option<AccuracyTarget>`,
`RepeatingEntry`, and `RepetitionInterval`. They already contain everything
replacement planning needs: the expression, repetition interval, accuracy,
and optional latency requirement. Do not add a parallel ASAPQuery workload
type and do not add `phase`, `query_offset`, missed-tick behavior, or rule-group
identity to ASAPPlanner's model.

```rust
struct RepeatingEntry {
    query: Query,
    interval: RepetitionInterval,
    requirements: Option<QueryRequirements>,
}
```

The Prometheus rules adapter returns two linked outputs:

1. generic `O11yMetricsQuery` values, subsequently converted into existing
   ASAPPlanner `RepeatingEntry` values;
2. scheduler-only rule metadata containing group identity/order,
   `query_offset`, missed-tick behavior, `for`, and `keep_firing_for`.

Both the Prometheus query adapter and rules adapter live under the same
independent `adapters/prometheus` boundary. Adapter output (1) crosses into
ASAPQuery core only as `O11yMetricsQuery`; output (2) remains in the
Prometheus adapter/runtime integration. It never enters `O11yMetricsQuery`,
`QueryWorkload`, lowering, replacement search, post-ASAP IR, or `PlanSpace`.
ASAPPlanner receives no Prometheus query-protocol or rule-group concept at any
layer. Normalized expression text is derived from ASAPPlanner's canonical
pre-ASAP IR and must not be stored as a second caller-controlled truth.

Example normalized input:

```yaml
query_groups:
  - id: high-error-rate
    schedule:
      interval_ms: 30000
      phase_ms: 0
      query_offset_ms: 15000
      missed_tick_policy: skip

    rules:
      - alert: HighErrorRate
        expr: |
          sum by (service) (rate(http_requests_total{status=~"5.."}[5m]))
            /
          sum by (service) (rate(http_requests_total[5m]))
            > 0.05
        for_ms: 300000
        keep_firing_for_ms: 60000
        accuracy:
          epsilon: 0.002
          delta: 0.01
```

Keep these time concepts separate:

- `interval_ms` controls how often the instant expression is evaluated.
- `[5m]` in PromQL is the data lookback.
- `query_offset_ms` changes the logical evaluation timestamp, not the
  interval.
- `for_ms` and `keep_firing_for_ms` belong to alert-state management, not
  aggregation planning.
- A rule evaluation is an instant query even when its expression contains
  range vectors such as `[5m]`.

### 6.2 Temporal alignment, watermark, and retention

The deployment plan, rather than ASAPPlanner's workload model, records runtime
alignment metadata:

```rust
struct WindowPlan {
    size_ms: u64,
    anchor_epoch_ms: i64,
    allowed_lateness_ms: u64,
    retention_buckets: u64,
}
```

For a scheduler trigger at timestamp `T`, the scheduler invokes QueryEngine at
the already-offset logical timestamp:

```text
logical evaluation time = T - query_offset
QueryEngine receives logical evaluation time
runtime reads only buckets whose watermark covers that time
```

The common anchor is required for correctness. Equal-duration buckets with
different boundaries cannot be merged as though they represented the same
logical interval.

The historical window planner chooses tumbling windows from the repeat
interval and range-query step, and explicitly disables sliding windows because
they crash Arroyo
([window.rs](https://github.com/ProjectASAP/ASAPQuery/blob/1fc18be81ec2e0f44fa0ded85151b513ee5312b7/asap-planner-rs/src/planner/window.rs)).
Keep the first recurring-rule implementation conservative:

- use anchored tumbling panes;
- require pane size to divide the evaluation interval;
- require pane size to divide the range-query step, when present;
- require pane size to be at least the scrape interval;
- require pane size not to exceed the expression lookback;
- retain enough closed panes for the maximum lookback plus allowed lateness.

When several rules need the same aggregation at different compatible
intervals, materialize the smallest compatible pane and merge panes for the
slower rule. Do not allocate one streaming aggregation per alert interval.

### 6.3 AccuracyTarget and KeepPreAsap are sufficient

Do not add `CorrectnessPolicy::{Exact, Approximate, ExactOrValidate}`.
ASAPPlanner's existing `AccuracyTarget` is the single source of truth:

```rust
AccuracyTarget::Exact
AccuracyTarget::Epsilon(epsilon)
AccuracyTarget::EpsilonDelta { epsilon, delta }
```

`Exact` excludes approximate candidates. A node for which no valid exact ASAP
replacement exists remains `KeepPreAsap`, which means ASAPQuery executes the
original pre-ASAP subtree from raw/archive data. `Epsilon` and
`EpsilonDelta` allow ASAPPlanner to choose a summary sized to that target.

No `ResidualExpr`, `GuardedResult`, interval-propagation layer, or conditional
exact-fallback policy is needed for this migration. ASAPQuery consumes
`SummaryNode` replacements and executes `KeepPreAsap` exactly; it must not
reinterpret the accuracy requirement or define a competing correctness enum.

### 6.4 Keep scheduling outside ASAPPlanner

The planner remains deterministic and free of timers:

```text
plan(workload, schemas, existing_plan, statistics) -> PlanDiff
```

A scheduler, ruler, or materializer owns time:

1. Determine the group evaluation timestamp.
2. Apply `query_offset_ms` to obtain logical evaluation time.
3. Prevent overlapping evaluations for the same Prometheus rule group.
4. Invoke QueryEngine with the selected plan and timestamp.
5. Execute selected summary nodes and any `KeepPreAsap` subtree.
6. Materialize the result or update alert state.

#### Materialize into Prometheus first

At each scheduled timestamp, evaluate the accelerated numeric expression and
remote-write a derived series such as:

```promql
asap:high_error_rate:ratio{service="checkout"} 0.073
```

Generate or configure the Prometheus alert as:

```yaml
- alert: HighErrorRate
  expr: asap:high_error_rate:ratio > 0.05
  for: 5m
  keep_firing_for: 1m
```

This preserves Prometheus rule reload, group ordering, labels, annotations,
pending/firing state, limits, and Alertmanager integration. The materialized
sample must be committed before the corresponding Prometheus evaluation. A
group `query_offset` can provide a data-availability margin; Prometheus
documents this as a use case for
[rule query offset](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#rule-query-offset).

#### ASAP-aware ruler later

An ASAP-aware ruler can call an instant-query endpoint directly and avoid
intermediate series. It must first implement or reuse Prometheus-compatible
group scheduling, missed-iteration handling, label/annotation templates,
state persistence, `for`, `keep_firing_for`, limits, reload behavior, and
Alertmanager delivery. This is intentionally not the first milestone.

### 6.5 Incremental replanning and rule reload

The historical input model already contains `existing_streaming_config` and
`existing_inference_config`, although they are reserved rather than acted on
([input.rs](https://github.com/ProjectASAP/ASAPQuery/blob/1fc18be81ec2e0f44fa0ded85151b513ee5312b7/asap-planner-rs/src/config/input.rs)).
Use the equivalent current-plan input to produce a versioned diff:

```rust
struct PlanDiff {
    reuse: Vec<LogicalAggregationId>,
    add: Vec<AggregationPlan>,
    resize: Vec<AggregationMigration>,
    retire: Vec<MaterializationFingerprint>,
}
```

Use two identities rather than conflating logical reuse with one physical
revision:

```text
LogicalAggregationId = hash(
  normalized leaf expression,
  metric and filters,
  grouping labels,
  statistic,
  window size and anchor
)

MaterializationFingerprint = hash(
  LogicalAggregationId,
  summary family and parameters,
  grouping layout,
  physical format version
)
```

This lets a parameter change be represented as `resize` instead of appearing
as an unrelated logical aggregation, while each physical state still has a
stable content-addressed fingerprint.

Safe rule reload is:

1. Parse and validate the complete new rule set.
2. Generate `PlanDiff` against the active version.
3. Reuse unchanged aggregations and start additions/resizes.
4. Warm new aggregations for their maximum lookback.
5. Atomically switch query/rule mappings at a group evaluation boundary.
6. Retire unreferenced physical states after their retention horizon.

### 6.6 Recurring-workload delivery track

Build this after the base workload planner in Section 5 can produce and serve
a selected `BackendPlan`:

1. Add the adapter-neutral `O11yMetricsQuery` model and its direct conversion
   into ASAPPlanner `BatchEntry`/`RepeatingEntry` workloads.
2. Add an independent `adapters/prometheus` module with query and rule-file
   adapters. Emit `O11yMetricsQuery` plus separately contained Prometheus
   runtime metadata; make no ASAPPlanner Prometheus model change.
3. Feed adapted expressions through ASAPPlanner's existing canonicalization
   and workload CSE.
4. Add deployment window anchoring, watermark, lateness, and retention
   metadata.
5. Materialize exact or selected approximate results back into Prometheus.
6. Implement current-plan diffs, warm cutovers, and evaluation-boundary swaps.
7. Only then consider an ASAP-native ruler.

The conceptual change is:

```text
Current:
query string + inferred repetition delay
  -> static aggregation config

Target:
ASAPPlanner RepeatingEntry + AccuracyTarget
  -> shared temporal aggregation plan
  + incremental deployment plan
```

The same model generalizes to recurring dashboards, scheduled SQL reports,
SLO evaluation, and periodic anomaly detection.

## 7. Cross-repository golden workload

Use the ASAPPlanner DAG-viewer demo queries as shared regression fixtures:

- `q1`: grouped count;
- `q2`: grouped Avg rewritten to sum/count while sharing input with `q1`;
- `q3`: top-5 over a rate/frequency summary;
- `q4`: compatible top-10, allowing `q3` to derive from the larger result;
- `q6`: join query sharing a compatible input scan with other queries.

Add negative fixtures for:

- different filter predicates;
- incompatible group-by reductions;
- different windows;
- different sort keys;
- incompatible schemas or table bindings;
- top-k inputs that differ below the limit.

Tests are required at four boundaries:

1. query workload -> selected strategies;
2. selected workload -> `BackendPlan`;
3. protobuf -> data-plane hot reload and `RoutingIndex`;
4. ingest -> plan push -> warm query response, including archive fallback.

## 8. Complexity and safety limits

ASAPPlanner stores alternatives in memo groups rather than enumerating the
Cartesian product of whole plans. This avoids exponential copying of complete
workload DAGs, but it does not make every strategy linear:

- common-subexpression discovery is approximately linear in reachable IR
  nodes, subject to hashing/equality checks;
- ordinary candidate generation is proportional to discovered targets,
  registered strategies, and candidates per target;
- rollup sibling discovery may compare aggregate pairs;
- top-k reuse may compare compatible limit pairs;
- candidate sorting adds per-group sorting cost;
- global selection is a topological dynamic-programming pass over the
  discovered reference graph.

Protect the control plane with:

- maximum query, schema, IR-node, and candidate counts;
- a planning deadline and cancellation token;
- ASAPPlanner's search-iteration cap plus a deployment-level deadline;
- normalized-workload caching keyed by query, schema, accuracy, planner
  revision, statistics epoch, and cost-model version;
- benchmarks for 1, 10, 50, and 100-query workloads;
- alerts for planning latency, candidate growth, timeout, and fallback rate.

## 9. Non-goals

- Moving deployment placement into ASAPPlanner.
- Making the DAG-viewer JSON the control-plane/data-plane protocol.
- Using viewer/export node IDs as persistent materialization identity.
- Removing archive fallback.
- Enabling SQL without a real schema catalog and binder.
- Reimplementing Prometheus scheduling or alert state in ASAPPlanner.
- Deleting the legacy planner before shadow validation and rollback are in
  place.

## 10. Completion criteria

The migration is complete when:

- all production queries enter one workload-aware ASAPPlanner path;
- the selected post-ASAP plan is the sole source of `BackendPlan` decisions;
- shared sub-DAGs remain shared through materialization and serving;
- the data plane does not independently select summary families or params;
- ASAPPlanner's `AccuracyTarget` is the only accuracy model and legacy
  `accuracy_sla`/local exact-vs-approximate enums have been removed;
- q1/q2/q3/q4/q6 pass cross-repository end-to-end tests;
- explain output maps every selected post-ASAP replacement explicitly to its
  pre-ASAP target;
- the legacy capability-union and per-root implementation planners have been
  removed;
- deployment placement, collector configuration, routing, execution, and
  archive fallback remain owned by ASAPQuery-backend.

Recurring-rule support is complete only when:

- rule schedules override frequency inference;
- only the generic evaluation interval enters ASAPPlanner; query offset and
  alert-state durations remain scheduler metadata;
- temporal panes have an explicit common anchor and watermark contract;
- `AccuracyTarget::Exact` plans either select exact ASAP summaries or execute
  `KeepPreAsap` from raw/archive data;
- rule reload uses a warm, versioned `PlanDiff` cutover;
- the initial production path materializes into Prometheus while Prometheus
  retains alert-state authority.
