# BackendPlan: a typed control-plane → data-plane contract, and the ASAPController IR merge that feeds it

> Status: design draft
> Scope: ASAPCollector + ASAPQuery-backend only. The public `ASAPQuery` repo
> is out of scope and requires no changes. `deployment-model-asapfusion` and
> a hypothetical standalone `deployment-model-asapquery` are referenced only
> to justify which pieces of this design are safe to generalize into
> ASAPController core.

## 1. Motivation

Three findings, taken together, motivate this design:

1. **ASAPController's L1-L3 has outgrown control_plane's.** A direct diff of
   the two `agg_intent.rs` files (not the stale snapshot in
   `ASAPController/docs/intent-algebra-reconciliation.md`, which its own
   addendum admits is out of date) shows ASAPController now has ~40+
   `AggIntent` variants after issues #43-#51 (native-histogram accessors,
   math/trig transforms, time/calendar accessors, extended range-vector
   reducers), versus control_plane's 25. `intent-algebra-reconciliation.md`'s
   original recommendation ("base = control_plane's L3") is superseded by
   this growth.
2. **control_plane already *is* an unextracted implementation of
   ASAPController's own target architecture.** `control_plane/docs/design.md`
   is the same document as `ASAPController/docs/design.md` (self-referential:
   "this file", "the companion file `migration-plan.md`"). control_plane's
   §5 refactor note explicitly maps its current single-crate module layout
   onto the target `core::*` / `deployment-model-*` split — it just hasn't
   been extracted, and it currently mixes `core`-worthy code,
   `deployment-model-asapquery`-flavored code, and
   `deployment-model-asaplifecycle`-flavored code (edge/gateway/OpAMP
   emitters) in one crate.
3. **The current control-plane → data-plane wire format has a documented,
   currently-unfixed parity gap.** `control_plane/docs/analyzer-parity-matrix.md`
   enumerates five query-shape categories (D1-D5: `sum by (...)`, `topk`,
   `count_over_time`, `rate`/`increase`, `sum(rate(...))`) that data_plane's
   legacy duplicate analyzer (`ASAPQueryEngine::parse_and_match_promql`)
   answers, but which control_plane's canonical planner
   (`sketch_algebra::capability::capability_for`) currently rejects as
   `UnsupportedAggIntent` — because these are *exact* shapes, and
   `capability_for` today treats "exact" as "give up" rather than "plan an
   exact materialization." The wire format itself (`AggregationType`
   already has `Sum`/`Increase`/`MinMax` alongside the sketch kinds) is not
   the blocker; the planning-time rejection is.

This document proposes: merge ASAPController's L1-L3 into control_plane as
the canonical IR (base = ASAPController, per finding 1); replace
`StreamingConfig`'s pattern-match-shaped schema with a typed `BackendPlan`
that makes exact materializations first-class (closing D1-D5 at the wire
level); and give data_plane a structural query-time index
(`RoutingIndex`) that shares control_plane's IR and capability vocabulary
instead of maintaining a second, divergent analyzer.

## 2. Goals / non-goals

**Goals:**
- One canonical L1-L3 IR, shared by control_plane's planner and
  data_plane's query-time matcher.
- A typed wire contract (`BackendPlan`) between control_plane and
  data_plane that can express both sketch-based and exact materializations.
- A query-time index in data_plane that resolves "which materialization
  answers this query" structurally, not via a second hand-maintained
  pattern table.
- Close the D1-D5 gap by construction, not by patching the duplicate
  analyzer to match the canonical one field-by-field.

**Non-goals (this document):**
- Extracting `core` / `deployment-model-*` into separate crates or repos.
  Per prior scoping decision: `Topology` and `CostModel` are pure
  interfaces in ASAPController core; concrete topologies, cost functions,
  edge/gateway emitters, OpAMP, and stage allocation stay wherever
  ASAPCollector + ASAPQuery-backend's code already lives today
  (logically "deployment-model-asaplifecycle", physically unchanged).
- Any change to the public `ASAPQuery` repo.
- Any change to OpAMP, the edge agent YAML, or the gateway YAML wire
  formats.

## 3. What stays where (recap)

| | Owner | Why |
|---|---|---|
| L1-L3 parsers + intent algebra IR | ASAPController `core` | Validated by ≥2 real deployment shapes (time-series via ASAPCollector+backend, tabular via asap-fusion) |
| Generic sketch-binding L4 rules (`BindKllOnQuantile`, etc.) | ASAPController `core` | Same — asap-fusion's `SketchConfigRule` already maps onto these |
| `Topology` / `CostModel` | `trait` only in `core` | Deliberately no shared implementation — each deployment scenario's shape differs too much (0/1/3-stage; bandwidth-cost vs in-process-selectivity-cost) to share concretely |
| Stage allocation, edge/gateway emitters, OpAMP, bandwidth cost models (`tco`/`pareto`/`online`/`delta`) | ASAPCollector + ASAPQuery-backend (logically `deployment-model-asaplifecycle`) | Only scenario that has >1 stage or a network-transmission cost concern |
| `BackendPlan` schema, `Materialization`, `RoutingIndex` (this doc) | ASAPQuery-backend | Backend-specific wire contract and query-time structure; not shared with any other deployment model today |

## 4. L1-L3 IR merge

**Base = ASAPController.** Per the audit in §1, this is now the richer,
faster-growing side; the reconciliation doc's original "base = control_plane"
recommendation is stale.

Known reconciliation items, audited directly against current source (not
the reconciliation doc's cached numbers):

| Item | control_plane | ASAPController | Resolution |
|---|---|---|---|
| `Frequency` | Standalone `AggIntent::Frequency { accuracy }` | Folded into `TopK`'s `RankingMeasure::Frequency` | **TBD** — pick one representation; capability exists on both sides, only the shape differs |
| `irate` vs `rate` | Distinct intents: `Rate{window}`, `Irate{window}` | Both lower to the same `AggIntent::Rate`; the avg-vs-last-two-samples distinction is deliberately pushed to L4 | **Adopt ASAPController's approach** — consistent with the L3 design rule "intent at L3, estimation method at L4" |
| `Project` node (SQL) | Present (`relational.rs`) | Previously flagged missing by `intent-algebra-reconciliation.md`; **confirmed present** in current source (`l2::relational::Project`, `ir::intent_algebra::query_expr::ProjectItem`) | No action — already closed |

No other gaps identified in the current audit; re-diff `schema.rs` /
`binder.rs` / `column_resolution.rs` at merge time rather than trusting
`intent-algebra-reconciliation.md`'s line-count table, which predates
ASAPController's #43-#51 growth.

## 5. `BackendPlan`: the new control-plane → data-plane wire contract

Replaces `StreamingConfig` (`crates/asap_types/src/streaming_config.rs`) and
`AggregationConfig` (`crates/asap_types/src/aggregation_config.rs`).

```rust
/// Pushed by control_plane to data_plane's backend process.
pub struct BackendPlan {
    pub plan_id: PlanId,              // observability only, not identity
    pub generated_at: DateTime<Utc>,

    /// Materialized policies this backend should build/maintain,
    /// keyed by content-addressed identity (unchanged from today).
    pub materializations: HashMap<PolicyFingerprint, Materialization>,

    /// Query-time capability routing — see §6 for why this is a
    /// separate table rather than 1:1 with `materializations`.
    pub routing: Vec<RoutingEntry>,

    pub monitors: Vec<MonitorSpec>,   // CDM, unchanged concept
}

pub struct Materialization {
    pub fingerprint: PolicyFingerprint,

    // Reuses intent_algebra types directly instead of re-flattening
    // into ad-hoc string fields.
    pub source: Source,               // metric + label filter
    pub window: WindowSpec,           // kind / size / slide
    pub group_by: Vec<GroupKey>,
    pub rollup: Vec<GroupKey>,

    pub payload: MaterializationPayload,
    pub retention: Option<RetentionPolicy>,
}

pub enum MaterializationPayload {
    Sketch {
        kind: SketchKind,             // reuses sketch_algebra::params::SketchKind
        params: SketchParams,
        col: ColumnRef,
    },
    /// New. See §7 — this is what makes D1-D5 representable once
    /// capability_for() is fixed to plan them instead of rejecting them.
    ExactAggregate {
        agg: ExactAggType,            // Sum / Count / MinMax / Rate / Increase / ...
        col: Option<ColumnRef>,
    },
}

pub struct RoutingEntry {
    pub satisfies: Capability,        // reuses sketch_algebra::capability::Capability
    pub materialization: PolicyFingerprint,
    pub storage_backend: StorageBackend,   // unchanged type
}
```

**Why `routing` is a separate table from `materializations`, not a 1:1
map:** one materialization can satisfy several query capabilities — e.g. a
single KLL(k=200) sketch built for p99 can also answer p50/p95, and
`capability_for` already does family-wildcard matching
(`QuantileApprox(Any)` vs `QuantileApprox(DDSketch)`). Binding "what's
built" to "what it can answer" 1:1 can't express that reuse; a separate
routing table can, and adding a new answerable shape for an existing
materialization is one new `RoutingEntry`, not a change to the
materialization itself.

**Why `ExactAggregate` is a first-class payload variant, not a fallback:**
the old schema *could* already express exact aggregation
(`AggregationType::Sum`/`Increase`/`MinMax` already exist), but nothing in
the planning path ever emitted them, because `capability_for` short-circuits
on "exact" before reaching the point where an `AggregationConfig` would be
built. Making `ExactAggregate` structurally equal in status to `Sketch`
inside `MaterializationPayload` forces the planner to make an explicit
choice for every intent, closing the loophole where "exact" silently means
"unplanned."

### 5.1 Transport

**Proto, not YAML/JSON.** control_plane and data_plane are built and
released atomically (confirmed — no independent rollout, no coordinated
upgrade window), so `design.md` §10's "`POST /api/v1/streaming-config` is
an unchanged hard contract" constraint does not apply here: that constraint
was written for a lift-and-shift extraction plan (`migration-plan.md`) that
has already diverged from reality (control_plane absorbed the DC controller
in-tree independently of that plan), and in any case exists to protect
independently-upgraded services — which these two are not. `parameters:
HashMap<String, Value>` — the concrete "pattern-match smell" in the old
schema — has no proto equivalent to fall back into; `SketchParams` /
`ExactAggType` become proper `oneof`s, checked at compile time on both
ends. The project already has proto precedent (`asap_otel_proto` for OTLP
ingest); `design.md` §10 itself earmarks `proto/asap_control.proto` for
exactly this, just deferred ("initial migration skips this").

## 6. `RoutingIndex`: query-time structural matching in data_plane

Two-tier lookup, built fresh (and atomically swapped, `arc-swap`) each time
a new `BackendPlan` is pushed.

**Tier 1 — exact fingerprint, O(1).** Compute the incoming query's own
shape fingerprint using the *same* hash function as `PolicyFingerprint`
(metric + agg_type/sub_type + params + grouping/aggregated/rollup labels +
window + normalized filter), and look it up directly in
`materializations`. Covers the common case — a query that's structurally
identical to something control_plane already planned (the majority of
fixed-dashboard traffic).

**Tier 2 — structural/capability match, on Tier-1 miss.**

```rust
pub struct RoutingIndex {
    exact: HashMap<PolicyFingerprint, PolicyFingerprint>,   // Tier 1

    by_metric: HashMap<MetricId, MetricBucket>,             // Tier 2
}

struct MetricBucket {
    // Columnar, not Vec<CandidateEntry> nested in a HashMap — see §6.1.
    group_shape_ids:  Vec<GroupShapeId>,
    filter_ids:       Vec<FilterId>,
    windows:          Vec<WindowSpec>,
    capabilities:     Vec<Capability>,
    fingerprints:     Vec<PolicyFingerprint>,
    storage_backends: Vec<StorageBackend>,
}
```

Match algorithm on Tier-1 miss:
1. `by_metric.get(metric_id)`.
2. Filter `group_shape_ids` for rows equal to, or a rollup-derivable parent
   of, the query's requested group-by.
3. Filter remaining rows by window compatibility — exact match, or reuse
   the existing window-merge/closest-pane logic in
   `storage_engines/sketch_db/query/{window_merger,sketch_reducer}.rs`
   rather than reimplementing it.
4. Filter remaining rows for filter *subsumption* (materialization's
   baked-in filter ⊆ query's filter — not equality).
5. Call the existing `Capability::is_satisfied_by(query_agg_intent,
   accuracy)` on what's left.
6. If more than one candidate survives, rank by error bound / storage-tier
   cost (same tie-break principle as the existing "closest-pane" logic
   noted in `TODO.md`'s 2026-05-01 entry).
7. No survivors → existing archive/Thanos fallback routing, unchanged.

### 6.1 Why columnar + interned ids, not nested `HashMap<Vec<LabelName>, Vec<_>>`

This is not a novel choice — it's consistency with an existing, already
production-validated pattern one layer down.
`data_plane/src/storage_engines/sketch_db/index/epoch_columnar.rs`
already does exactly this for the physical sketch index:
`LabelValuesId = u32` interns each group-by label-values vector so the hot
loop compares a 4-byte int instead of walking a `BTreeMap<String,
String>`, and the epoch store itself is three parallel arrays ("range scan
touches only `windows_col`"). This is the same design OTAP (OpenTelemetry
Protocol with Apache Arrow) uses dictionary encoding for — repeated,
high-cardinality values get stored once and referenced by a compact id.
`RoutingIndex` should use `GroupShapeId`/`FilterId` interning and
parallel-array storage per `MetricBucket` for the same reason
`epoch_columnar.rs` does: at the scale of thousands of control-plane-planned
materializations per metric, Tier-2 lookup is effectively a small
in-memory OLAP scan, not a handful of hash lookups, and it should be built
that way rather than reinvented as nested hash maps.

(A separate, open-ended finding from the same investigation: `emit/otap.rs`
— the OTAP *dataflow DAG* config, unrelated to the Arrow schema point above
— identifies plugin node types by URN string rather than a closed enum.
That pattern is relevant only if `MaterializationPayload` ever needs
third-party-pluggable sketch kinds; until that need is real, keep it a
closed enum for the compile-time exhaustiveness checking, which is the
whole point of retiring the untyped `parameters` bag in the first place.)

## 7. Closing D1-D5

The wire format (§5) makes exact materializations representable; it does
not by itself fix the gap. Two more changes are required:

1. **Fix `capability_for()`** (`control_plane/src/sketch_algebra/capability.rs`)
   to return a valid capability/materialization choice for `Sum`, `Count`,
   `Rate`, `Increase`, `TopK` at "exact" accuracy — routing to
   `MaterializationPayload::ExactAggregate` — instead of
   `UnsupportedAggIntent`. This is the actual fix; the wire format is what
   makes the fix expressible.
2. **Data_plane's query-time path must parse and lower through the same L1-L3
   code control_plane uses** (post IR-merge, §4), and match via
   `RoutingIndex` (§6) against the same `Capability` vocabulary
   `capability_for()` produces. This makes control-plane-time and
   query-time disagreement structurally impossible, rather than something
   `analyzer-parity-matrix.md`'s α→ε manual-parity process has to keep
   proving by hand. Once `RoutingIndex` is live and proven, the legacy
   `ASAPQueryEngine::parse_and_match_promql` / `Statistic` /
   `QueryPatternType` code is deleted — this supersedes that document's
   planned γ/δ phases with a structural fix instead of a shim-then-delete
   sequence.

## 8. Sequencing

1. Merge L1-L3 (base = ASAPController); resolve `Frequency` / `irate`
   representation (§4).
2. Fix `capability_for()` to plan exact aggregates instead of rejecting
   them (§7.1) — this alone, even before the wire format changes, proves
   the planning-side half of D1-D5 is fixed.
3. Land the `BackendPlan` proto + `Materialization` / `RoutingEntry` types
   (§5), reusing `PolicyFingerprint`, `Capability`, `SketchKind` /
   `SketchParams` as-is — no reinvention of identity or capability
   semantics, only the envelope around them.
4. Implement `RoutingIndex` in data_plane (§6), wired to the shared
   frontend/lowering crates so the query-time path and the control-plane
   planning path are provably the same code, not just field-compatible.
5. Delete the duplicate legacy analyzer once `RoutingIndex` reaches parity
   on the existing 18-query corpus in `analyzer-parity-matrix.md` (reuse
   that corpus as the acceptance gate; extend per its own planned ε phase
   if useful, but the parity *mechanism* it was building toward is now
   structural, not maintained by hand).
6. Cut `control_plane` and `data_plane` over to `BackendPlan` in a single
   joint release (safe per the atomic-deploy premise in §5.1); delete
   `StreamingConfig`/`AggregationConfig`.

## 9. Open questions

- **Query-side fingerprinting.** Does Tier-1 matching reuse
  `PolicyFingerprint::from_config` verbatim, or does an incoming query need
  a distinct fingerprint function (it has no `original_yaml` or
  `num_aggregates_to_retain`, both already excluded from the hash per
  `policy_fingerprint.rs`'s doc comment — likely fine as-is, but should be
  verified rather than assumed).
- **Rollup algebra.** §6 step 2 assumes "a materialization grouped by
  `(zone, region)` can answer a query grouped by `(zone)` alone" is a
  known-safe operation gated by the `rollup` field, but the precise algebra
  (which `AggIntent`s roll up safely, e.g. `Sum`/`Count` yes, `Quantile`
  generally no without re-estimation error) needs its own short design pass
  before `RoutingIndex` step 2 can be implemented as described.
- **`MaterializationPayload` extensibility.** Deferred per §6.1 — revisit
  only if a real need for pluggable, non-recompiled sketch kinds emerges.
