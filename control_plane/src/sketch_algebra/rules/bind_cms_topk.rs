//! `BindCountSketchOnTopK` — `Aggregate{TopK{k, accuracy}}` → a
//! heavy-hitter sketch, picked **recall-SLA-aware**.
//!
//! Reference: `control_plane/docs/design.md` §6 line ~419 — "`SketchAgg
//! { intent, col }` … L4 emits `PhysicalExpr::SketchAgg`" — and
//! `intent_algebra::AggIntent::TopK` (heavy-hitter intent) maps to a
//! heavy-hitter sketch primitive. Two families answer the intent:
//!
//! * **CMS-with-heap** (Count-Min + size-`k` heap; Cormode-Muthukrishnan
//!   2005). One-sided over-estimate; ~4 KB of wire state. Recovers the
//!   top-`k` heavy hitters w.h.p. — perfectly adequate for the common
//!   "who are the top-k" question under a **loose recall SLA**.
//! * **CountSketch-with-heap** (Charikar-Chen-Farach-Colton; median-of-
//!   rows). Unbiased / two-sided / signed estimates, supports exact-rank
//!   reconstruction — but ~250 KB of wire state, ~66× the CMS-heap cost
//!   (see `optimizer::cost::wire`: `count_sketch_delta` 250 KB vs
//!   `count_min_delta` 4 KB).
//!
//! ## The recall-aware binding (Fig-12 cost-gap fix)
//!
//! The original rule hard-bound CountSketch for every non-exact top-k.
//! That paid the 250 KB CountSketch price even when a loose recall SLA
//! (`recall@k ≥ 0.9`, approximate heavy hitters — the common case) would
//! be satisfied by the 4 KB CMS-heap, blowing the P95 cost-gap tail the
//! Fig-12 harness measured.
//!
//! The fix makes the family choice [`TopkRecallTier`]-driven:
//!
//! | Tier | When | Bound family | Wire cost |
//! |---|---|---|---|
//! | [`TopkRecallTier::Loose`]  | approximate heavy hitters, `recall@k ≥ ~0.9`, no signed/exact-rank need | **CMS-with-heap** | ~4 KB |
//! | [`TopkRecallTier::Tight`]  | exact rank, very-high recall, or signed/two-sided estimate required | **CountSketch-with-heap** | ~250 KB |
//!
//! The tie-break between "both meet the SLA" picks the cheaper family by
//! the [`optimizer::cost::wire`] cost table — the same "min cost s.t. SLA"
//! the oracle uses. For a loose SLA both families clear the recall bar, so
//! CMS-heap (the cheaper one) wins; for a tight SLA only CountSketch
//! clears it, so it wins regardless of price.
//!
//! ## Where the recall SLA comes from
//!
//! There is no per-query recall field today (`AccuracyTarget` is
//! `Exact` / `Epsilon` / `EpsilonDelta` — a *frequency*-error budget, not
//! a *recall* budget). Until one is threaded through (the follow-up), the
//! tier is inferred conservatively from the accuracy target:
//!
//! * `AccuracyTarget::Exact` (on either the query policy or the intent) →
//!   exact rank required → **Tight** (CountSketch) — preserving the old
//!   exact-bail behaviour but as a *family* pick rather than a `None`.
//!   (Note: a true exact top-k still needs HashAgg+Heap; CountSketch is
//!   the closest sketch-tier approximation and the unbiased estimator.)
//! * everything else → **Loose** (CMS-heap) — the cheap common-case
//!   default.
//!
//! See [`TopkRecallTier::from_accuracy`] for the mapping and the
//! module-level follow-up note.
//!
//! Accuracy → `(w, d)` mapping: `AccuracyTarget::EpsilonDelta { eps,
//! delta }` → `(w, d) = (⌈e/eps⌉, ⌈ln(1/delta)⌉)`, identical for both CMS
//! and CountSketch. The heap size is the requested `k`. See
//! `accuracy_profile.rs` (ASAPQuery-backend) for the formal heavy-hitter
//! recall guarantee (a frequency sketch + size-`k` heap recovers all
//! heavy hitters with frequency `≥ ‖f‖₁ / k` w.h.p.).

#![allow(dead_code)]

use crate::intent_algebra::{AggIntent, QueryExpr};
use crate::optimizer::cost::wire::WireCostTable;
use crate::sketch_algebra::params::{CmsParams, CountSketchParams, SketchKind, SketchParams};
use crate::sketch_algebra::physical_expr::{EstimateOp, PhysicalExpr};
use crate::sketch_algebra::rules::Rule;
use crate::types_v2::AccuracyTarget;

/// Recall tier for a top-k binding — drives the family pick.
///
/// "Recall" here is `recall@k` of the heavy-hitter set: the fraction of
/// the true top-`k` items the sketch's heap recovers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopkRecallTier {
    /// Approximate heavy hitters are fine (`recall@k ≥ ~0.9`), and no
    /// signed / two-sided / exact-rank estimate is needed. CMS-with-heap
    /// (one-sided over-estimate) is the cheap, correct choice.
    Loose,
    /// Exact rank, very-high recall, or a signed / two-sided estimate is
    /// required. CountSketch-with-heap (unbiased median-of-rows) is the
    /// fit despite its ~66× wire cost.
    Tight,
}

impl TopkRecallTier {
    /// Conservative recall tier inferred from the (policy, intent)
    /// accuracy targets. No per-query recall field exists today, so:
    ///
    /// * either side `Exact` → exact-rank intent → [`Tight`].
    /// * otherwise → [`Loose`] (the cheap common-case default).
    ///
    /// **Follow-up:** thread a real per-query `recall@k` target (or a
    /// `signed`/`two_sided` flag) through `AccuracyTarget` / the query
    /// policy and pivot here on that instead of inferring from `Exact`.
    ///
    /// [`Tight`]: TopkRecallTier::Tight
    /// [`Loose`]: TopkRecallTier::Loose
    pub fn from_accuracy(policy: &AccuracyTarget, intent: &AccuracyTarget) -> Self {
        match (policy, intent) {
            (AccuracyTarget::Exact, _) | (_, AccuracyTarget::Exact) => TopkRecallTier::Tight,
            _ => TopkRecallTier::Loose,
        }
    }
}

pub struct BindCountSketchOnTopK;

impl BindCountSketchOnTopK {
    /// Families that satisfy a top-k recall tier, cheapest-first.
    ///
    /// * `Loose`: both CMS-heap (cheap) and CountSketch-heap clear the
    ///   recall bar, so the cost model is free to pick the cheaper one.
    /// * `Tight`: only CountSketch-heap (unbiased / signed / exact-rank)
    ///   clears the bar.
    fn candidate_families(tier: TopkRecallTier) -> &'static [SketchKind] {
        match tier {
            TopkRecallTier::Loose => &[SketchKind::Cms, SketchKind::CountSketch],
            TopkRecallTier::Tight => &[SketchKind::CountSketch],
        }
    }

    /// Pick the SLA-meeting family with the lowest per-flush wire cost.
    /// `candidates` is already filtered to the families that meet the
    /// recall SLA (see [`Self::candidate_families`]); this is the
    /// "min cost s.t. SLA" tie-break the oracle uses.
    fn cheapest_family(candidates: &[SketchKind], table: &WireCostTable) -> SketchKind {
        candidates
            .iter()
            .min_by_key(|k| table.for_kind(k).per_flush())
            .cloned()
            // (above: k is &&SketchKind; for_kind autoderefs to &SketchKind)
            // candidate_families never returns empty.
            .unwrap_or(SketchKind::CountSketch)
    }

    /// Bind a top-k under an explicit recall tier — bypasses the
    /// accuracy-inferred tier in [`Rule::apply`]. Used by
    /// `optimizer::rules::bind_workload_typed`, which has already pinned
    /// the family from the capability matrix / a `sketch_family_override`
    /// and just needs the matching heap-bearing binding:
    ///
    /// * a CountSketch family pick → [`TopkRecallTier::Tight`]
    ///   (CountSketch-with-heap, the unbiased canonical pick).
    /// * a CMS family pick on a top-k → [`TopkRecallTier::Loose`]
    ///   (CMS-with-heap).
    pub fn apply_with_tier(
        &self,
        expr: &QueryExpr,
        accuracy: &AccuracyTarget,
        tier: TopkRecallTier,
    ) -> Option<PhysicalExpr> {
        self.bind(expr, accuracy, Some(tier))
    }

    /// Core binding. When `forced_tier` is `Some`, that tier is used;
    /// otherwise the tier is inferred from the accuracy targets via
    /// [`TopkRecallTier::from_accuracy`].
    fn bind(
        &self,
        expr: &QueryExpr,
        accuracy: &AccuracyTarget,
        forced_tier: Option<TopkRecallTier>,
    ) -> Option<PhysicalExpr> {
        let (k_topk, intent_accuracy, child) = match expr {
            QueryExpr::Aggregate {
                aggs, child, by, ..
            } if aggs.len() == 1 && by.is_empty() => match &aggs[0] {
                AggIntent::TopK { k, accuracy } => (*k, accuracy.clone(), child),
                _ => return None,
            },
            _ => return None,
        };

        if k_topk == 0 {
            return None;
        }

        // Recall tier first — it decides which families are viable, and
        // (for the Tight tier) it is the only thing that keeps CountSketch
        // in play. Note we no longer bail to `None` on `Exact`: an exact
        // top-k intent picks the unbiased CountSketch family (the closest
        // sketch-tier approximation) rather than declining the binding.
        let tier = forced_tier
            .unwrap_or_else(|| TopkRecallTier::from_accuracy(accuracy, &intent_accuracy));

        // Derive (w, d) from the frequency-error budget. `Exact` on either
        // side leaves the ε/δ unspecified (it's a *recall* tier signal,
        // not a frequency budget), so fall back to the catalog defaults
        // (eps=0.01, delta=0.01) used elsewhere for heavy-hitter sketches.
        let (eps, delta) = match (accuracy, &intent_accuracy) {
            (AccuracyTarget::Exact, AccuracyTarget::Exact) => (0.01, 0.01),
            (AccuracyTarget::Exact, other) | (other, AccuracyTarget::Exact) => match other {
                AccuracyTarget::Epsilon(a) => (*a, 0.01),
                AccuracyTarget::EpsilonDelta {
                    epsilon: eps,
                    delta,
                } => (*eps, *delta),
                AccuracyTarget::Exact => (0.01, 0.01),
            },
            (AccuracyTarget::Epsilon(a), AccuracyTarget::Epsilon(b)) => (a.min(*b), 0.01),
            (
                AccuracyTarget::Epsilon(a),
                AccuracyTarget::EpsilonDelta {
                    epsilon: eps,
                    delta,
                },
            )
            | (
                AccuracyTarget::EpsilonDelta {
                    epsilon: eps,
                    delta,
                },
                AccuracyTarget::Epsilon(a),
            ) => (a.min(*eps), *delta),
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
        };

        if eps <= 0.0 || delta <= 0.0 || delta >= 1.0 {
            return None;
        }

        let w = (std::f64::consts::E / eps).ceil() as u32;
        let d = (1.0 / delta).ln().ceil() as u32;
        // CountSketch columns MUST be a power of two: the agent
        // (asapedgeprocessor config_validate) rejects non-pow2 cols because
        // sketchlib bit-slices the hash with a pow2 column mask. Round the
        // ε-derived width UP to the next power of two — this only tightens
        // the additive bound (ε ≤ e/w) and prevents an agent-side
        // "cols must be a power of two" crash on config apply. CMS does not
        // require pow2 cols, but using the same width keeps the two
        // families' accuracy comparable for the cost-model tie-break.
        let w = w.max(2).next_power_of_two();
        let d = d.max(1);

        // Cost-aware tie-break: among the families that meet the recall
        // SLA for this tier, pick the cheapest by the wire cost model
        // (the same "min cost s.t. SLA" the oracle uses).
        let table = WireCostTable::default();
        let family = Self::cheapest_family(Self::candidate_families(tier), &table);

        let (kind, params) = match family {
            SketchKind::Cms => (
                SketchKind::Cms,
                // CMS-Heap pattern: pair the CMS matrix with a size-k
                // heavy-hitter heap. The streaming-config emit promotes
                // this to `CountMinSketchWithHeap` (servable as
                // FrequencyTopk per `asap_tier_analysis`).
                SketchParams::Cms(CmsParams {
                    w,
                    d,
                    with_heap: true,
                }),
            ),
            // Tight tier (and any future family) → CountSketch-with-heap.
            _ => (
                SketchKind::CountSketch,
                SketchParams::CountSketch(CountSketchParams {
                    w,
                    d,
                    with_heap: true,
                }),
            ),
        };

        Some(PhysicalExpr::estimate_over_agg(
            EstimateOp::TopK { k: k_topk },
            kind,
            params,
            (**child).clone(),
        ))
    }
}

impl Rule for BindCountSketchOnTopK {
    fn name(&self) -> &'static str {
        "bind_cms_topk"
    }

    fn priority(&self) -> u16 {
        5
    }

    fn apply(&self, expr: &QueryExpr, accuracy: &AccuracyTarget) -> Option<PhysicalExpr> {
        // Recall-aware default: tier is inferred from the accuracy targets
        // (loose → CMS-heap, tight/exact → CountSketch). Callers that have
        // already pinned a family use [`Self::apply_with_tier`] instead.
        self.bind(expr, accuracy, None)
    }
}
