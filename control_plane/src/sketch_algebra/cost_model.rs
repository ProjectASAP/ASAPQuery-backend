//! `ControlPlaneCostModel` — plugs control_plane's own sketch-family
//! selection + parameter-sizing policy into
//! `asap_aware_mapping::bind::implement_tree_in_with` via the two `CostModel`
//! extension points (`rank_candidates` for family choice, `size_params`
//! for parameter sizing — the latter added by ASAPController PR #146
//! specifically to support this migration).
//!
//! Ports the decisions previously made by the `bind_kll_quantile` /
//! `bind_ddsketch_quantile` / `bind_hll_cardinality` / `bind_cms_count` /
//! `bind_cms_topk` `Rule`s verbatim — same accuracy-bound citations, same
//! priority order, same recall-tier logic — just re-homed behind the
//! `CostModel` trait instead of a bespoke `Rule` dispatcher, so the L3→L4
//! walk itself (schema derivation, `col`/`by` computation, DAG
//! construction) can be `asap_aware_mapping::bind`'s rather than a forked copy.
//!
//! `AggIntent::Extension` (control_plane's `Frequency` point-query) used
//! to be one of two shapes `implement_tree_in_with` couldn't realize even
//! with this `CostModel` plugged in — `boundary::implementation_for_with`
//! now consults [`ControlPlaneCostModel::realize_extension`]/
//! [`readout_extension`](CostModel::readout_extension) for it instead of
//! hardcoding `PassThrough` (ASAPController#150).
//!
//! One shape remains genuinely unreachable via this `CostModel`, because
//! the decision of *whether* to call into `rank_candidates`/`size_params`
//! at all is made upstream, before the `CostModel` is ever consulted:
//!
//! - `AggIntent::TopK { accuracy: AccuracyTarget::Exact, .. }` — routes to
//!   `exact_realization`, which has no accumulator form for `TopK` and
//!   returns `PassThrough`, so `implement_tree_in_with` falls through to
//!   its own `Logical` fallback for this shape unchanged. There is no
//!   local pre-pass binding it: the `BindCountSketchOnTopK` rule that
//!   once did was deleted (see `lower.rs`'s module doc) — this is a
//!   genuine, still-open `asap-plan` coverage gap (ASAPController#151),
//!   not something this deployment routes around locally.

#![allow(dead_code)]

use asap_aware_mapping::CostModel;
use asap_aware_mapping::Implementation;
use planner_types::post_asap::{SketchAlgorithm as SketchKind, SketchParams, SketchQuery};
use planner_types::pre_asap::expr_ir::ColumnRef;

use crate::intent_algebra::agg_intent::FREQUENCY_EXT_KIND;
use crate::intent_algebra::AggIntent;
use crate::optimizer::cost::wire::WireCostTable;
use crate::types_v2::AccuracyTarget;

/// See module docs.
pub struct ControlPlaneCostModel {
    /// Workload-level accuracy policy — combined (tighter-of) with each
    /// intent's own accuracy field, matching every `bind_*.rs` rule's old
    /// `accuracy: &AccuracyTarget` parameter.
    pub workload_accuracy: AccuracyTarget,
}

impl ControlPlaneCostModel {
    pub fn new(workload_accuracy: AccuracyTarget) -> Self {
        Self { workload_accuracy }
    }

    /// The tighter (lower) of the workload policy and an intent's own
    /// accuracy target, as `(eps, delta)`. `None` when either side is
    /// `Exact` — mirrors `bind_kll_quantile.rs` / `bind_ddsketch_quantile.rs`
    /// / `bind_hll_cardinality.rs` / `bind_cms_count.rs`'s identical
    /// `match (accuracy, &intent_accuracy) {...}` block. (`TopK` has its
    /// own combination rule — an `Exact` side there picks the *other*
    /// side's budget rather than bailing — see [`Self::topk_eps_delta`].)
    fn combined_eps_delta(&self, intent_accuracy: &AccuracyTarget) -> Option<(f64, f64)> {
        match (&self.workload_accuracy, intent_accuracy) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => None,
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => Some((a.min(*b), 0.01)),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { epsilon, delta })
            | (AccuracyTarget::EpsilonDelta { epsilon, delta }, AccuracyTarget::Epsilon(a)) => {
                Some((a.min(*epsilon), *delta))
            }
            (
                AccuracyTarget::EpsilonDelta {
                    epsilon: a,
                    delta: da,
                },
                AccuracyTarget::EpsilonDelta {
                    epsilon: b,
                    delta: db,
                },
            ) => Some((a.min(*b), da.min(*db))),
        }
    }

    /// `TopK`'s own `(eps, delta)` combination — verbatim port of
    /// `bind_cms_topk.rs`'s `bind` match. Unlike
    /// [`Self::combined_eps_delta`], an `Exact` side does not bail: it
    /// picks the *other* side's budget (falling back to the catalog
    /// default `(0.01, 0.01)` only when both sides are `Exact`). Public
    /// (within the crate) because [`crate::sketch_algebra::lower`]'s
    /// `TopK { accuracy: Exact }` pre-pass needs the same combination.
    pub(crate) fn topk_eps_delta(&self, intent_accuracy: &AccuracyTarget) -> (f64, f64) {
        match (&self.workload_accuracy, intent_accuracy) {
            (AccuracyTarget::Exact, AccuracyTarget::Exact) => (0.01, 0.01),
            (AccuracyTarget::Exact, other) | (other, AccuracyTarget::Exact) => match other {
                AccuracyTarget::Epsilon(a) => (*a, 0.01),
                AccuracyTarget::EpsilonDelta { epsilon, delta } => (*epsilon, *delta),
                AccuracyTarget::Exact => (0.01, 0.01),
            },
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => (a.min(*b), 0.01),
            (AccuracyTarget::Epsilon(a), AccuracyTarget::EpsilonDelta { epsilon, delta })
            | (AccuracyTarget::EpsilonDelta { epsilon, delta }, AccuracyTarget::Epsilon(a)) => {
                (a.min(*epsilon), *delta)
            }
            (
                AccuracyTarget::EpsilonDelta {
                    epsilon: a,
                    delta: da,
                },
                AccuracyTarget::EpsilonDelta {
                    epsilon: b,
                    delta: db,
                },
            ) => (a.min(*b), da.min(*db)),
        }
    }

    /// Recall-tier-filtered, wire-cost-ordered top-k family candidates.
    /// Verbatim port of `bind_cms_topk.rs`'s `TopkRecallTier` +
    /// `candidate_families` + `cheapest_family`. Public (within the
    /// crate) for the same reason as [`Self::topk_eps_delta`].
    pub(crate) fn topk_family_order(&self, intent_accuracy: &AccuracyTarget) -> Vec<SketchKind> {
        let tight = matches!(self.workload_accuracy, AccuracyTarget::Exact)
            || matches!(intent_accuracy, AccuracyTarget::Exact);
        let allowed: &[SketchKind] = if tight {
            &[SketchKind::CountSketchWithHeap]
        } else {
            &[SketchKind::CmsWithHeap, SketchKind::CountSketchWithHeap]
        };
        let table = WireCostTable::default();
        let mut ranked: Vec<SketchKind> = allowed.to_vec();
        ranked.sort_by_key(|k| table.for_kind(k).per_flush());
        ranked
    }

    /// `(width, depth)` for a CMS-family sketch under `(eps, delta)`.
    /// Verbatim: `w = ⌈e/eps⌉` clamped to `≥2`, `d = ⌈ln(1/delta)⌉`
    /// clamped to `≥1`.
    fn cms_width_depth(eps: f64, delta: f64) -> (u32, u32) {
        let w = (std::f64::consts::E / eps).ceil().max(2.0) as u32;
        let d = (1.0 / delta).ln().ceil().max(1.0) as u32;
        (w, d)
    }
}

/// The workload-level accuracy target combined with an intent's own —
/// pulls the intent's `accuracy` field out of whichever variant carries
/// one. `_` covers every non-approximate-capable variant, unreachable in
/// practice (`rank_candidates`/`size_params` are only ever called for
/// `Quantile`/`Cardinality`/`Count`/`TopK` — the shapes
/// `boundary::bind_summary_with` handles).
fn intent_accuracy(intent: &AggIntent) -> AccuracyTarget {
    match intent {
        AggIntent::Quantile { accuracy, .. }
        | AggIntent::Cardinality { accuracy, .. }
        | AggIntent::TopK { accuracy, .. } => accuracy.clone(),
        AggIntent::Count { accuracy } => accuracy.clone(),
        _ => AccuracyTarget::Exact,
    }
}

impl CostModel for ControlPlaneCostModel {
    fn rank_candidates(&self, intent: &AggIntent, candidates: &[SketchKind]) -> Vec<SketchKind> {
        match intent {
            // bind_ddsketch_quantile (priority 6) always wins the old
            // dispatcher's tie-break over bind_kll_quantile (priority 5)
            // whenever both can bind (see bind_kll_quantile.rs's
            // `priority()` doc — no eps-dependent split actually exists
            // between the two rules today, both read the same combined
            // eps). Pure static reorder: DDSketch before Kll.
            AggIntent::Quantile { .. } => {
                let mut v = candidates.to_vec();
                if let Some(pos) = v.iter().position(|k| *k == SketchKind::DDSketch) {
                    let dd = v.remove(pos);
                    v.insert(0, dd);
                }
                v
            }
            AggIntent::TopK { .. } => {
                let preferred = self.topk_family_order(&intent_accuracy(intent));
                let mut ranked = Vec::with_capacity(candidates.len());
                for kind in preferred {
                    if candidates.contains(&kind) && !ranked.contains(&kind) {
                        ranked.push(kind);
                    }
                }
                for kind in candidates {
                    if !ranked.contains(kind) {
                        ranked.push(kind.clone());
                    }
                }
                ranked
            }
            // Cardinality → Hll, Count → Cms: control_plane only ever
            // binds one family for each; asap-plan's static order already
            // puts it first (`summary_candidates`), nothing to reorder.
            _ => candidates.to_vec(),
        }
    }

    fn size_params(
        &self,
        kind: SketchKind,
        intent: &AggIntent,
        eps: f64,
        delta: f64,
    ) -> SketchParams {
        match intent {
            AggIntent::TopK { k, .. } => {
                let (eps, delta) = self.topk_eps_delta(&intent_accuracy(intent));
                let (w, d) = Self::cms_width_depth(eps, delta);
                // CountSketch/CMS columns MUST be a power of two: the
                // agent (asapedgeprocessor config_validate) rejects
                // non-pow2 cols. Round up — this only tightens the
                // additive bound (ε ≤ e/w).
                let w = w.next_power_of_two();
                let heap_size = *k as u32;
                match kind {
                    SketchKind::CmsWithHeap => SketchParams::CmsWithHeap {
                        width: w,
                        depth: d,
                        heap_size,
                    },
                    _ => SketchParams::CountSketchWithHeap {
                        width: w,
                        depth: d,
                        heap_size,
                    },
                }
            }
            _ => {
                let Some((eps, delta)) = self.combined_eps_delta(&intent_accuracy(intent)) else {
                    // Either side Exact: unreachable in practice for
                    // Quantile/Cardinality/Count (an Exact intent never
                    // reaches `bind_summary_with` upstream — see
                    // `implementation_for_with`'s `Exact => exact_realization`
                    // arm), kept as a safe fallback rather than a panic.
                    return asap_aware_mapping::DefaultCostModel
                        .size_params(kind, intent, eps, delta);
                };
                match kind {
                    SketchKind::Kll => SketchParams::Kll {
                        k: kll_k_for_eps(eps),
                    },
                    SketchKind::DDSketch if (0.0..1.0).contains(&eps) => {
                        SketchParams::DDSketch { alpha: eps }
                    }
                    SketchKind::Hll => SketchParams::Hll {
                        precision: hll_precision_for_eps(eps),
                    },
                    SketchKind::Cms => {
                        let (w, d) = Self::cms_width_depth(eps, delta);
                        SketchParams::Cms { width: w, depth: d }
                    }
                    other => {
                        asap_aware_mapping::DefaultCostModel.size_params(other, intent, eps, delta)
                    }
                }
            }
        }
    }

    /// Realize control_plane's `Frequency` (`ext_kind: "frequency"`)
    /// point-query intent (ASAPController#150). `SketchKind::Cms` (heap-
    /// less — a point lookup needs no heap, unlike `TopK`) matches
    /// `capability_matching::pick_family`'s own `Frequency → Cms` mapping
    /// (and its `is_valid_pair` truth table, which declares `(Cms,
    /// Frequency)` valid and has no `(CountSketch, Frequency)` entry) —
    /// the newer, tested, currently-authoritative source of truth for this
    /// choice, not the older `sketch_catalog::sketch_type_for_op`'s
    /// `SketchType::CountSketch` (a genuinely different sketch algorithm
    /// under a same-ish name — `SketchType` has separate `CountSketch`
    /// and `CountMinSketch` variants; that mapping predates
    /// `capability_matching` and disagrees with it). Sized the same
    /// `e/eps` width / `ln(1/delta)` depth way every other CMS-family kind
    /// here is. `PassThrough` for any other `ext_kind` (none exist yet)
    /// or an unparseable/`Exact` accuracy, matching every other
    /// approximate-capable intent's `Exact ⇒ no sketch form` policy.
    fn realize_extension(&self, ext_kind: &str, payload: &serde_json::Value) -> Implementation {
        if ext_kind != FREQUENCY_EXT_KIND {
            return Implementation::PassThrough;
        }
        let Some(accuracy) = payload
            .get("accuracy")
            .and_then(|v| serde_json::from_value::<AccuracyTarget>(v.clone()).ok())
        else {
            return Implementation::PassThrough;
        };
        let Some((eps, delta)) = self.combined_eps_delta(&accuracy) else {
            return Implementation::PassThrough;
        };
        let (width, depth) = Self::cms_width_depth(eps, delta);
        Implementation::Sketch(planner_types::post_asap::SketchKind::new(
            SketchKind::Cms,
            SketchParams::Cms {
                width: width.next_power_of_two(),
                depth,
            },
        ))
    }

    /// Build the `SketchQuery` readout for `Frequency`. `item_label`/
    /// `item_value` are populated by `intent_algebra::agg_intent::frequency`
    /// once the filter value is threaded through (ASAPQuery-backend Phase
    /// 3 — not yet); until then `payload` never has them, so this
    /// correctly falls back to the bare bucket total
    /// (`key: SampleValue, value: None`) — the same answer a `Frequency`
    /// intent with no item filter should give either way.
    fn readout_extension(
        &self,
        ext_kind: &str,
        payload: &serde_json::Value,
        _col: &ColumnRef,
    ) -> SketchQuery {
        debug_assert_eq!(
            ext_kind, FREQUENCY_EXT_KIND,
            "readout_extension called for an ext_kind realize_extension never realizes as Sketch"
        );
        let item_label = payload.get("item_label").and_then(|v| v.as_str());
        let item_value = payload
            .get("item_value")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        match item_label {
            Some(label) => SketchQuery::PointCount {
                key: ColumnRef::Named(label.to_string()),
                value: item_value,
            },
            None => SketchQuery::PointCount {
                key: ColumnRef::SampleValue,
                value: None,
            },
        }
    }
}

/// A `CostModel` that forces a single family for whichever intent it's
/// asked to rank, delegating parameter sizing to an inner
/// [`ControlPlaneCostModel`]. Used by `optimizer::rules::bind_workload_typed`,
/// which already has a definitive family pick from the capability matrix
/// (or a `sketch_type_override`) and just needs the matching binding, not
/// a fresh selection decision.
pub struct ForcedFamilyCostModel {
    inner: ControlPlaneCostModel,
    forced: SketchKind,
}

impl ForcedFamilyCostModel {
    pub fn new(workload_accuracy: AccuracyTarget, forced: SketchKind) -> Self {
        Self {
            inner: ControlPlaneCostModel::new(workload_accuracy),
            forced,
        }
    }
}

impl CostModel for ForcedFamilyCostModel {
    fn rank_candidates(&self, intent: &AggIntent, candidates: &[SketchKind]) -> Vec<SketchKind> {
        let mut ranked = self.inner.rank_candidates(intent, candidates);
        if let Some(pos) = ranked.iter().position(|kind| kind == &self.forced) {
            let forced = ranked.remove(pos);
            ranked.insert(0, forced);
        }
        ranked
    }

    fn size_params(
        &self,
        kind: SketchKind,
        intent: &AggIntent,
        eps: f64,
        delta: f64,
    ) -> SketchParams {
        // Latest Planner validates the selected candidate against its own
        // accuracy algebra.  Reuse its sizing formula for an explicitly
        // forced family so the override changes only algorithm preference,
        // never weakens the requested guarantee.
        asap_aware_mapping::DefaultCostModel.size_params(kind, intent, eps, delta)
    }

    // `realize_extension`/`readout_extension` delegate to `inner` rather
    // than falling back to the trait's default `PassThrough` — otherwise
    // `bind_workload_typed`'s `Frequency` contract row (which binds via
    // `ForcedFamilyCostModel`, already knowing its family pick from the
    // capability matrix) would still decline pending #150 even after
    // `ControlPlaneCostModel` itself learned to realize it.
    fn realize_extension(&self, ext_kind: &str, payload: &serde_json::Value) -> Implementation {
        self.inner.realize_extension(ext_kind, payload)
    }

    fn readout_extension(
        &self,
        ext_kind: &str,
        payload: &serde_json::Value,
        col: &ColumnRef,
    ) -> SketchQuery {
        self.inner.readout_extension(ext_kind, payload, col)
    }
}

/// A `CostModel` that forces both the family AND the exact parameters
/// for whichever intent it's asked to rank/size, falling back to an
/// inner accuracy-driven [`ControlPlaneCostModel`] when nothing was
/// observed for the candidates on offer.
///
/// This is the seam `data_plane`'s live-serving re-binding path
/// (`l4_lowering.rs`) needs: planning already decided a family + params
/// for a metric (that decision is what's actually registered in the
/// `SketchStore`), so serving-time re-parsing the same query must
/// reproduce EXACTLY that plan, not size a fresh one from a guessed
/// accuracy target (`ForcedFamilyCostModel` above forces the family but
/// still re-derives params from `eps`/`delta` — the wrong tool here,
/// since re-deriving is exactly what caused the mismatch this type
/// exists to avoid; see `control_plane/docs/design-target-architecture.md`'s
/// "planning vs serving" split). `observed` is `None` whenever this
/// query's metric has no registered sid at all — `rank_candidates`/
/// `size_params` then fall back to the accuracy-driven default, which
/// won't match anything registered either way, so the outcome
/// (`find_candidates` finds nothing) is unchanged.
///
/// **Fallback status (design-backend-plan-wire-format.md §5):**
/// `l4_lowering.rs` prefers reading planning's decision directly off an
/// installed `BackendPlan`'s materializations (no reconstruction needed
/// there — `Materialization.kind`/`.params` already ARE the pair
/// `observed` needs). This type's caller
/// (`observed_family_for_metric`, the `SketchStore`-metadata
/// reconstruction) is the fallback for deploys with no `BackendPlan`
/// installed yet, or for metrics a partial/stale plan doesn't cover.
pub struct ObservedFamilyCostModel {
    inner: ControlPlaneCostModel,
    observed: Option<(SketchKind, SketchParams)>,
}

impl ObservedFamilyCostModel {
    pub fn new(
        workload_accuracy: AccuracyTarget,
        observed: Option<(SketchKind, SketchParams)>,
    ) -> Self {
        Self {
            inner: ControlPlaneCostModel::new(workload_accuracy),
            observed,
        }
    }
}

impl CostModel for ObservedFamilyCostModel {
    fn rank_candidates(&self, intent: &AggIntent, candidates: &[SketchKind]) -> Vec<SketchKind> {
        match &self.observed {
            Some((kind, _)) if candidates.contains(kind) => {
                let mut ranked = self.inner.rank_candidates(intent, candidates);
                let pos = ranked
                    .iter()
                    .position(|candidate| candidate == kind)
                    .expect("observed candidate was present before ranking");
                let observed = ranked.remove(pos);
                ranked.insert(0, observed);
                ranked
            }
            _ => self.inner.rank_candidates(intent, candidates),
        }
    }

    fn size_params(
        &self,
        kind: SketchKind,
        intent: &AggIntent,
        eps: f64,
        delta: f64,
    ) -> SketchParams {
        match &self.observed {
            Some((okind, oparams)) if *okind == kind => oparams.clone(),
            _ => self.inner.size_params(kind, intent, eps, delta),
        }
    }

    fn realize_extension(&self, ext_kind: &str, payload: &serde_json::Value) -> Implementation {
        self.inner.realize_extension(ext_kind, payload)
    }

    fn readout_extension(
        &self,
        ext_kind: &str,
        payload: &serde_json::Value,
        col: &ColumnRef,
    ) -> SketchQuery {
        self.inner.readout_extension(ext_kind, payload, col)
    }
}

/// Map an ε rank-error budget to a KLL stream-size `k`. Verbatim port of
/// `bind_kll_quantile.rs::kll_k_for_eps` — power-of-two rungs (200, 400,
/// 800, 2048, 8192) so the in-tree `algebra::directory` continues to
/// recognise the parameter.
fn kll_k_for_eps(eps: f64) -> u32 {
    if eps <= 0.0 {
        return 8192;
    }
    if eps >= 0.01 {
        200
    } else if eps >= 0.005 {
        400
    } else if eps >= 0.0025 {
        800
    } else if eps >= 0.001 {
        2048
    } else {
        8192
    }
}

/// Map an ε standard-error budget to the HLL `precision`. Verbatim port
/// of `bind_hll_cardinality.rs::hll_precision_for_eps`.
fn hll_precision_for_eps(eps: f64) -> u8 {
    if eps <= 0.0 {
        return 16;
    }
    if eps >= 0.03 {
        10
    } else if eps >= 0.015 {
        12
    } else if eps >= 0.008 {
        14
    } else {
        16
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::{default_cardinality, default_quantile};

    fn eps(e: f64) -> AccuracyTarget {
        AccuracyTarget::Epsilon(e)
    }

    #[test]
    fn quantile_always_prefers_ddsketch_over_kll() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.1));
        let ranked = model.rank_candidates(
            &default_quantile(0.99),
            &[SketchKind::Kll, SketchKind::DDSketch],
        );
        assert_eq!(ranked, vec![SketchKind::DDSketch, SketchKind::Kll]);
    }

    #[test]
    fn kll_k_matches_bind_kll_quantile_rungs() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(1.0));
        let params = model.size_params(SketchKind::Kll, &default_quantile(0.99), 0.01, 0.01);
        assert_eq!(params, SketchParams::Kll { k: 200 });

        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(1.0));
        let tight = crate::intent_algebra::AggIntent::Quantile {
            col: None,
            q: 0.99,
            accuracy: eps(0.001),
        };
        let params = model.size_params(SketchKind::Kll, &tight, 0.01, 0.01);
        assert_eq!(params, SketchParams::Kll { k: 2048 });
    }

    #[test]
    fn hll_precision_matches_bind_hll_cardinality_rungs() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(1.0));
        let params = model.size_params(SketchKind::Hll, &default_cardinality(), 0.01, 0.01);
        assert_eq!(params, SketchParams::Hll { precision: 14 });
    }

    #[test]
    fn topk_tight_tier_forces_countsketch_even_when_ranked_from_cms() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Exact);
        let intent = crate::intent_algebra::AggIntent::TopK {
            k: 5,
            accuracy: eps(0.01),
        };
        let ranked = model.rank_candidates(
            &intent,
            &[SketchKind::CmsWithHeap, SketchKind::CountSketchWithHeap],
        );
        assert_eq!(
            ranked,
            vec![SketchKind::CountSketchWithHeap, SketchKind::CmsWithHeap]
        );
    }

    #[test]
    fn topk_loose_tier_prefers_cheaper_cms_heap() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.1));
        let intent = crate::intent_algebra::AggIntent::TopK {
            k: 5,
            accuracy: eps(0.01),
        };
        let ranked = model.rank_candidates(
            &intent,
            &[SketchKind::CmsWithHeap, SketchKind::CountSketchWithHeap],
        );
        assert_eq!(ranked[0], SketchKind::CmsWithHeap);
    }

    #[test]
    fn topk_width_is_rounded_up_to_a_power_of_two() {
        let model = ControlPlaneCostModel::new(AccuracyTarget::Epsilon(0.1));
        let intent = crate::intent_algebra::AggIntent::TopK {
            k: 5,
            accuracy: eps(0.01),
        };
        let params = model.size_params(SketchKind::CmsWithHeap, &intent, 0.0, 0.0);
        let SketchParams::CmsWithHeap {
            width, heap_size, ..
        } = params
        else {
            panic!("expected CmsWithHeap params");
        };
        assert!(width.is_power_of_two());
        assert_eq!(heap_size, 5);
    }
}
