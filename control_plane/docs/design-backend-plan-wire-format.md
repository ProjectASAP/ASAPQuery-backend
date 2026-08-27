# `BackendPlan`: the control-plane → data-plane wire contract

> Scope: the wire format and query-time structure connecting `control_plane`
> (planning) to `data_plane` (serving) in this deployment. This document
> designs the target shape directly — it does not narrate what currently
> exists or what a migration path looks like; see PR history for that.

## 0. One-sentence version

`BackendPlan` is the typed message `control_plane` pushes to `data_plane`
describing every materialization it has decided on and what query
capabilities each one answers; `RoutingIndex` is the structure `data_plane`
builds from it and consults at query time — both planning time and serving
time end up reading the *same* decision, instead of serving time
re-deriving one independently.

## 1. Why this exists: planning decides once, serving must reuse it exactly

Planning (`control_plane`) and serving (`data_plane`) are different
processes with different jobs — planning picks a summary family and sizes
its parameters from a query shape and an accuracy target, symbolically;
serving walks already-materialized state and answers queries against it.
Those two steps must agree on **exactly** which `SummaryFamilyType` a given
metric's materialization uses, including algorithm, parameters, and grouping
layout, because `SummaryExecutor::find_candidates` matches on that state type *by strict
equality*, on purpose: this deployment chose exact agreement over silently
serving an answer under a looser accuracy guarantee than what was
actually planned (see `summary_executor.rs::summary_params_match`'s
rationale).

The wire format is the mechanism that keeps those two sides honest. If
serving time ever has to *guess* what was planned — e.g. re-deriving a
family/params choice from a hardcoded default accuracy, independent of
what a specific metric's workload actually requested — that guess will
occasionally be wrong, and the two sides drift apart silently (a query
that should be servable warm capability-misses to archive for no
query-shape reason, only because the guess didn't match reality). The
correct shape is: `control_plane` decides once, writes the decision into
`BackendPlan`, and `data_plane` (via `RoutingIndex`) reads that same
decision back — never re-derives it.

This has one direct implication for `data_plane`'s serving-time L4
lowering: it should resolve a query's selected `SummaryFamilyType` by
looking it up in `RoutingIndex` (built from the `BackendPlan` `control_plane`
already pushed), not by invoking a `CostModel` a second time at query time.
`CostModel::rank_candidates`/`size_params` are a **planning-time-only**
concern — they run once, when `BackendPlan` is built, never again per
query.

## 2. Goals

- One wire message, typed end to end, that both sketch-based and exact
  materializations populate identically — no "exact means unplanned"
  loophole.
- A query-time structure in `data_plane` that resolves "which
  materialization answers this query" by direct lookup against what was
  actually planned, not by independently re-classifying the query and
  hoping the classification matches reality.
- Reuse this deployment's existing canonical vocabulary
  (ASAPPlanner's `QueryExpr`/`AggIntent`/`SummaryFamilyType`, and
  `control_plane`'s own `Capability`/`PolicyFingerprint`)
  directly. No parallel, wire-specific re-encoding of concepts that already
  have a canonical type.

## 3. `Materialization`: exact and approximate are already the same shape

ASAPPlanner's current `SummaryFamilyType` is the canonical union of plain,
exact-aggregate, sketch, sample, wavelet, and statistical-model state. A
sketch-valued family carries its concrete `SketchKind` and
`GroupingStrategy`; `SketchKind` carries category, algorithm, and parameters.
`BackendPlan` preserves that selected type instead of flattening it into the
obsolete local `(SummaryKind, SummaryParams)` pair.

```rust
pub struct BackendPlan {
    // Cross-subplan identity as of design-compiled-plan-collector-backend-
    // split.md: shared verbatim with this plan's CollectorSubplan, so the
    // two can be checked for agreement. `PolicyFingerprint` below remains
    // the identity of one materialization; `plan_id` answers "were the
    // collector and backend subplans compiled together," which no
    // per-materialization fingerprint can answer on its own.
    pub plan_id: PlanId,
    pub plan_version: u64,
    pub activation: DateTime<Utc>,
    pub expiry: Option<DateTime<Utc>>,
    pub backend_compat: BackendCompatId,
    pub generated_at: DateTime<Utc>,

    /// Every materialization this backend should build/maintain,
    /// keyed by content-addressed identity.
    pub materializations: HashMap<PolicyFingerprint, Materialization>,

    /// Query-time capability routing — see §4 for why this is a
    /// separate table rather than 1:1 with `materializations`.
    pub routing: Vec<RoutingEntry>,

    pub monitors: Vec<MonitorSpec>,   // CDM, unchanged concept
}

pub struct Materialization {
    pub fingerprint: PolicyFingerprint,

    // Reuses asap_ir/asap_sketch types directly — no re-flattening into
    // ad-hoc string/HashMap fields.
    pub source: Source,                // metric + label filter
    pub window: WindowSpec,            // kind / size / slide
    pub group_by: Vec<GroupKey>,
    pub rollup: Vec<GroupKey>,

    /// The exact selected Planner state type, including concrete sketch
    /// algorithm/parameters and grouping layout where applicable.
    pub family: SummaryFamilyType,
    pub col: ColumnRef,

    /// Collector assignments that produce this state under the matching
    /// CollectorPlan.
    pub sources: Vec<EdgeSourceRef>,

    /// Present when the pinned Planner revision computes a guarantee for
    /// this selected result. Unknown is not represented as exact.
    pub guarantee: Option<ResultGuarantee>,

    pub retention: Option<RetentionPolicy>,
}

pub struct RoutingEntry {
    pub satisfies: Capability,         // control_plane's own coarse,
                                        // family-level routing vocabulary
    pub materialization: PolicyFingerprint,
    pub storage_backend: StorageBackend,
}
```

**Why `routing` is a separate table from `materializations`, not a 1:1
map:** one materialization can satisfy several query capabilities — a
single KLL(k=200) sketch built for p99 can also answer p50/p95, and
`Capability::is_satisfied_by` already does family-wildcard matching
(`QuantileApprox(Any)` vs `QuantileApprox(DDSketch)`). Binding "what's
built" to "what it can answer" 1:1 can't express that reuse; a separate
routing table can — adding a new answerable shape for an existing
materialization is one new `RoutingEntry`, not a change to the
materialization itself.

**Why `Capability` still exists as its own type, distinct from
`SummaryFamilyType`:** `Capability` is the *family-level*
question ("does anything at all answer a `QuantileApprox` shape for this
metric") used for coarse routing decisions — miss detection, archive
fallback, "should I even try the sketch tier." `SummaryFamilyType` is the
*exact* question `SummaryExecutor::find_candidates`
needs — because two candidates must agree exactly to be legally mergeable
via `merge_states`, family-level compatibility alone isn't enough to
decide that. These are genuinely different match precisions for genuinely
different callers (see §4) — collapsing them into one would either make
routing too strict (rejecting a real family match on an incidental params
difference) or make serving too loose (merging things that don't actually
agree).

## 4. `RoutingIndex`: query-time structure in `data_plane`

Built fresh from each `BackendPlan` push, atomically swapped
(`arc-swap`) so a config update never serves from a half-updated index.

**Tier 1 — exact fingerprint, O(1).** Compute the incoming query's own
shape fingerprint using the same hash function as `PolicyFingerprint`
(metric + agg shape + params + grouping/rollup labels + window +
normalized filter) and look it up directly in `materializations`. Covers
the common case — a query structurally identical to something
`control_plane` already planned (the majority of fixed-dashboard traffic).

**Tier 2 — structural/capability match, on Tier-1 miss.**

```rust
pub struct RoutingIndex {
    exact: HashMap<PolicyFingerprint, PolicyFingerprint>,   // Tier 1
    by_metric: HashMap<MetricId, MetricBucket>,             // Tier 2
}

/// Columnar, not `Vec<CandidateEntry>` nested in a `HashMap` — see §4.1.
struct MetricBucket {
    group_shape_ids:  Vec<GroupShapeId>,
    filter_ids:       Vec<FilterId>,
    windows:          Vec<WindowSpec>,
    capabilities:     Vec<Capability>,
    families:         Vec<SummaryFamilyType>,
    guarantees:       Vec<Option<ResultGuarantee>>,
    fingerprints:     Vec<PolicyFingerprint>,
    storage_backends: Vec<StorageBackend>,
}
```

Match algorithm on Tier-1 miss:

1. `by_metric.get(metric_id)`.
2. Filter `group_shape_ids` for rows equal to, or a rollup-derivable parent
   of, the query's requested group-by.
3. Filter remaining rows by window compatibility — exact match, or the
   existing window-merge/closest-pane logic
   (`storage_engines/sketch_db/query/window_merger.rs`).
4. Filter remaining rows for filter *subsumption* (materialization's
   baked-in filter ⊆ query's filter — not equality).
5. **Match precision depends on the caller** — this is the one place
   `RoutingIndex` has two genuinely different read modes, not a single
   shared one:
   - **Whole-query resolution** (deciding whether this query can be
     served warm at all, and which storage backend to route to): match
     `Capability::is_satisfied_by(query_agg_intent, accuracy)` —
     family-level.
   - **`SummaryExecutor::find_candidates`** (per-`L4Node`-leaf, called
     during `asap_sketch::exec::execute()`'s walk): match exact
     exact `SummaryFamilyType` equality — required for anything that can feed
     a `SummaryMerge`, and the reason `l4_lowering.rs` no longer needs to
     independently observe or guess this (see §5).
6. **Whole-query resolution only:** if more than one candidate survives,
   rank by error bound / storage-tier cost and return the single winner.
   **`find_candidates` only:** skip ranking — return every row that
   survived step 5's exact match, so `execute()` can fold them all via
   `merge_states`.
7. No survivors → archive/Thanos fallback routing (whole-query mode), or
   `ExecError::NoCandidates` (`find_candidates` mode) — two callers of the
   same "empty" outcome.

### 4.1 Why columnar + interned ids, not nested `HashMap<Vec<LabelName>, Vec<_>>`

Consistency with an existing, already production-validated pattern one
layer down: `storage_engines/sketch_db/index/epoch_columnar.rs` already
does exactly this for the physical sketch index — `LabelValuesId = u32`
interns each group-by label-values vector so the hot loop compares a
4-byte int instead of walking a `BTreeMap<String, String>`, and the epoch
store itself is parallel arrays ("range scan touches only `windows_col`").
`RoutingIndex` should use `GroupShapeId`/`FilterId` interning and
parallel-array storage per `MetricBucket` for the same reason: at the
scale of thousands of planned materializations per metric, Tier-2 lookup
is effectively a small in-memory OLAP scan, not a handful of hash lookups,
and should be built that way rather than reinvented as nested hash maps.

## 5. What this replaces at serving time

Today, `data_plane`'s serving-time L4 lowering (`l4_lowering.rs`) parses
the raw query string down to a canonical `QueryExpr`, then has to
*independently reconstruct* which `SummaryFamilyType` a
metric's registered sid actually uses by inspecting the `SketchStore`'s
own metadata (`ObservedFamilyCostModel`) before it can bind an `L4Node`
that `find_candidates` will actually match. That's a real, working
mechanism, but it's inherently a *reconstruction* — it infers the plan
from its side effect (what got registered), rather than reading the plan
directly.

Once `RoutingIndex` exists, serving-time lowering simplifies to: parse to
`QueryExpr` (L1-L3, still genuinely needed — a query's *shape* has to be
recovered from its text regardless of any wire format), then resolve the
query's selected `SummaryFamilyType` via `RoutingIndex`'s
`find_candidates`-mode lookup directly, and construct the `L4Node` from
that pair — no `CostModel::rank_candidates`/`size_params` call at serving
time at all. `CostModel` becomes exactly what its name says: a
planning-time cost model, invoked once when `BackendPlan` is built, never
re-invoked per query. `ObservedFamilyCostModel`'s SketchStore-introspection
approach was always a stopgap for the absence of this lookup, not a
replacement for it.

## 6. Transport

**Proto, not YAML/JSON.** `BackendPlan` is a typed protobuf contract.
`SummaryFamilyType` and its family-specific values become proper protobuf
`oneof`s, with invalid family/parameter combinations rejected during decode.
`backend_compat` explicitly protects coordinated use with the independently
delivered CollectorPlan and emitted summary-state schema; deployment timing
must not be treated as an implicit compatibility guarantee. Additive fields
and backward decoding support controlled rollout, with unknown required
variants rejected rather than placed in an untyped
`HashMap<String, Value>` bag.

## 7. Open questions

- **Query-side fingerprinting.** Does Tier-1 matching reuse
  `PolicyFingerprint::from_config` verbatim, or does an incoming query
  need a distinct fingerprint function (it carries no `original_yaml` or
  `num_aggregates_to_retain` — both already excluded from the hash per
  `policy_fingerprint.rs`'s own doc comment, so likely fine as-is, but
  worth verifying rather than assuming).
- **Rollup algebra.** Step 2 above assumes "a materialization grouped by
  `(zone, region)` can answer a query grouped by `(zone)` alone" is a
  known-safe operation gated by the `rollup` field. The precise algebra —
  which summary families roll up safely (`Sum`/`Count`-family: yes;
  `Quantile`: generally no without re-estimation error) — needs its own
  short design pass before this step can be implemented as described.
- **Exact-requested top-k policy.** ASAPPlanner PR #293 permits a deployment
  cost model to offer `CmsWithHeap`/`CountSketchWithHeap` for
  `TopK { accuracy: Exact }` only with an explicit effective approximation
  target, while retaining pass-through. If the pinned revision contains that
  hook and ASAPQuery opts in, `BackendPlan` records the approximate family,
  effective target, and selected guarantee; it must not describe the result as
  exact. Without the opt-in, the query routes to exact raw/archive execution.
- **`RoutingIndex` performance.** New query-time hot path in a
  latency-sensitive service — needs a benchmark pass against the current
  lookup, not just a correctness pass, before it can replace anything.
