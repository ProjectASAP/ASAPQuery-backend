//! Layer 4 sketch parameters — typed per family.
//!
//! Mirrors `control_plane/docs/design.md` §6 `core::sketch_algebra` (around
//! line ~580). The L3 IR carries an `AggIntent` + `AccuracyTarget`; L4
//! `Bind*` rules read those, consult the catalog, and emit a typed
//! [`SketchParams`] payload alongside the chosen [`SketchType`].
//!
//! Why a parallel typed enum rather than reusing `crate::types::SketchParams`?
//! The legacy `SketchParams` (in `crate::types`) is shaped for the wire
//! format the OTel-collector consumes: it carries pre-baked `quantiles`
//! grids, a `metric_name` field for the CMS partition operator, etc. —
//! all of which are L5 emitter concerns. L4 needs only the parameters
//! that affect cost / accuracy: the KLL sketch's `k`, DDSketch's `alpha`,
//! HLL's `precision`, CMS's `(w, d)`. Keeping the L4 parameter shape
//! minimal makes the `Bind*` rule signatures narrow and the typed
//! `PhysicalExpr` IR independent of wire-format drift.
//!
//! Convertibility — see [`SketchParams::to_legacy`]. The legacy form is
//! what the existing `algebra::directory::build_sketch_params` produces;
//! this `to_legacy` adapter is what lets the typed-path rewrite of
//! `planner::rules` (Phase E) land additively.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};

use crate::types::{SketchDefaults, SketchParams as LegacySketchParams, SketchType};

/// L4 sketch family selector. Mirrors `crate::types::SketchType` but with
/// the variants L4 sketch-binding rules emit. Naming kept distinct from
/// the legacy enum so the typed path is unambiguous in error messages /
/// diagnostics.
///
/// The values are 1:1 convertible to the legacy `SketchType` via
/// [`SketchType::from`]; all rule outputs are round-trippable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SketchKind {
    /// Karnin-Lang-Liberty quantile sketch. Mergeable, exact rank-error
    /// bound. Picked for `Quantile` intents when relative-error budget is
    /// not specified or when the quantile target is in the tail.
    Kll,
    /// DDSketch — log-bucketed quantile sketch with a relative-error
    /// guarantee. Picked for `Quantile` intents when an explicit ε is
    /// supplied and tail-relative error matters more than rank-error.
    DDSketch,
    /// HyperLogLog — approximate cardinality. Picked for `Cardinality`.
    Hll,
    /// Count-Min sketch. Picked for `Count` and `Frequency` intents when
    /// approximation is allowed.
    Cms,
    /// Count-Sketch (with optional heavy-hitter heap). Picked for `TopK`
    /// when paired with a Misra-Gries / heap-of-counters extractor; also
    /// the substrate for general `Frequency` sketching when balanced
    /// (zero-mean) error is preferable to CMS's one-sided bias.
    CountSketch,
}

impl From<SketchKind> for SketchType {
    fn from(k: SketchKind) -> Self {
        match k {
            SketchKind::Kll => SketchType::KLL,
            SketchKind::DDSketch => SketchType::DDSketch,
            SketchKind::Hll => SketchType::HLL,
            SketchKind::Cms => SketchType::CountMinSketch,
            SketchKind::CountSketch => SketchType::CountSketch,
        }
    }
}

/// Inverse of `From<SketchKind> for SketchType`. Round-trippable:
/// `SketchKind::from(SketchType::from(k)) == k` for every variant. Used
/// by `planner::rules::bind_workload_typed` to translate the legacy
/// `QueryWorkload::sketch_type_override` field into the typed `SketchKind`
/// the capability matrix consumes.
impl From<SketchType> for SketchKind {
    fn from(t: SketchType) -> Self {
        match t {
            SketchType::KLL => SketchKind::Kll,
            SketchType::DDSketch => SketchKind::DDSketch,
            SketchType::HLL => SketchKind::Hll,
            SketchType::CountMinSketch => SketchKind::Cms,
            SketchType::CountSketch => SketchKind::CountSketch,
        }
    }
}

/// L4 sketch parameters per family. Parameter names match the canonical
/// sketch literature: `k` for KLL stream size, `alpha` for DDSketch
/// relative-accuracy bound, `precision` for HLL register width, `(w, d)`
/// for CMS / CountSketch dimensions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "family", rename_all = "snake_case")]
pub enum SketchParams {
    /// KLL stream size. `k=200` ≈ ε≈0.01 rank error, `k=2048` ≈ ε≈0.0035
    /// — see `accuracy_profile.rs` (ASAPQuery-backend) for the formal
    /// rank-error bound.
    Kll(KllParams),
    /// DDSketch relative-accuracy. `alpha=0.01` is the catalog default
    /// for `Epsilon(0.01)` quantile budgets.
    DDSketch(DDSketchParams),
    /// HLL register width. `precision=14` ≈ ε≈0.81%/√m; `precision=10` is
    /// the coarse default.
    Hll(HllParams),
    /// Count-Min: `w` columns × `d` rows. Error bound: ε ≤ e/w with prob.
    /// 1 − 2^(−d) (Cormode-Muthukrishnan).
    Cms(CmsParams),
    /// Count-Sketch: `w` columns × `d` rows; balanced (zero-mean) error.
    /// When `with_heap` is set, pairs with a heavy-hitter heap to extract
    /// `TopK`.
    CountSketch(CountSketchParams),
}

/// KLL parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct KllParams {
    /// Stream-size parameter. Higher k → tighter rank-error / more memory.
    pub k: u32,
}

/// DDSketch parameters.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct DDSketchParams {
    /// Relative-error bound — DDSketch guarantees |estimate − true| ≤
    /// alpha · true on every quantile.
    pub alpha: f64,
}

/// HLL parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HllParams {
    /// log2 of the register count. `precision=p` → 2^p registers, error
    /// ≈ 1.04 / √(2^p).
    pub precision: u32,
}

/// Count-Min parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CmsParams {
    /// Number of columns (width). Drives the additive ε bound (≤ e/w).
    pub w: u32,
    /// Number of rows (depth). Drives the failure probability (≤ 2^−d).
    pub d: u32,
    /// Whether to pair the CMS matrix with a heavy-hitter heap (CMS-Heap
    /// pattern from Cormode & Muthukrishnan 2005). Set by
    /// `bind_cms_with_heap_on_topk` when the planner picks CMS for a
    /// TopK statistic. The streaming-config emit consults this flag to
    /// pick `CountMinSketchWithHeap` vs `CountMinSketch` for the
    /// `aggregationType` string the backend's `policy_capability` keys
    /// on.
    #[serde(default)]
    pub with_heap: bool,
}

/// Count-Sketch parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountSketchParams {
    /// Number of columns.
    pub w: u32,
    /// Number of rows.
    pub d: u32,
    /// Whether to pair with a heavy-hitter heap (for TopK extraction).
    pub with_heap: bool,
}

impl SketchParams {
    /// The matching [`SketchKind`] for this parameter payload.
    pub fn kind(&self) -> SketchKind {
        match self {
            SketchParams::Kll(_) => SketchKind::Kll,
            SketchParams::DDSketch(_) => SketchKind::DDSketch,
            SketchParams::Hll(_) => SketchKind::Hll,
            SketchParams::Cms(_) => SketchKind::Cms,
            SketchParams::CountSketch(_) => SketchKind::CountSketch,
        }
    }

    /// Convert to the legacy wire-shaped `crate::types::SketchParams`.
    /// `quantile_grid` is read from defaults — Phase C does not yet plumb
    /// query-specific quantile grids into the typed path; that's an L5
    /// emitter concern picked up in Phase E (stage_split + emitter
    /// migration).
    pub fn to_legacy(&self, defaults: &SketchDefaults) -> LegacySketchParams {
        match self {
            SketchParams::Kll(p) => LegacySketchParams::KLL {
                k: p.k,
                quantiles: defaults.quantile_grid.clone(),
            },
            SketchParams::DDSketch(p) => LegacySketchParams::DDSketch {
                relative_accuracy: p.alpha,
                quantiles: defaults.quantile_grid.clone(),
            },
            SketchParams::Hll(p) => LegacySketchParams::HLL {
                precision: p.precision,
            },
            SketchParams::Cms(p) => LegacySketchParams::CountMinSketch {
                rows: p.d,
                cols: p.w,
                metric_name: defaults.count_min_sketch.metric_name.clone(),
            },
            SketchParams::CountSketch(p) => LegacySketchParams::CountSketch {
                // Translate (w, d) back into the legacy (epsilon, delta)
                // surface — `epsilon ≈ e/w`, `delta ≈ 2^−d`. The legacy
                // CountSketch processor expects these directly.
                epsilon: std::f64::consts::E / (p.w as f64),
                delta: 2f64.powi(-(p.d as i32)),
            },
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sketch_kind_to_legacy_roundtrip() {
        let cases = [
            (SketchKind::Kll, SketchType::KLL),
            (SketchKind::DDSketch, SketchType::DDSketch),
            (SketchKind::Hll, SketchType::HLL),
            (SketchKind::Cms, SketchType::CountMinSketch),
            (SketchKind::CountSketch, SketchType::CountSketch),
        ];
        for (k, expected) in cases {
            let legacy: SketchType = k.into();
            assert_eq!(legacy, expected);
        }
    }

    #[test]
    fn sketch_params_kind_matches() {
        assert_eq!(
            SketchParams::Kll(KllParams { k: 200 }).kind(),
            SketchKind::Kll
        );
        assert_eq!(
            SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }).kind(),
            SketchKind::DDSketch
        );
        assert_eq!(
            SketchParams::Hll(HllParams { precision: 14 }).kind(),
            SketchKind::Hll
        );
        assert_eq!(
            SketchParams::Cms(CmsParams { w: 2048, d: 5, with_heap: false }).kind(),
            SketchKind::Cms
        );
        assert_eq!(
            SketchParams::CountSketch(CountSketchParams {
                w: 2048,
                d: 5,
                with_heap: true,
            })
            .kind(),
            SketchKind::CountSketch
        );
    }

    #[test]
    fn to_legacy_kll_carries_k_and_grid() {
        let defaults = SketchDefaults::default();
        let p = SketchParams::Kll(KllParams { k: 200 }).to_legacy(&defaults);
        match p {
            LegacySketchParams::KLL { k, quantiles } => {
                assert_eq!(k, 200);
                assert_eq!(quantiles, defaults.quantile_grid);
            }
            other => panic!("expected KLL legacy, got {other:?}"),
        }
    }

    #[test]
    fn to_legacy_ddsketch_carries_alpha() {
        let defaults = SketchDefaults::default();
        let p = SketchParams::DDSketch(DDSketchParams { alpha: 0.005 }).to_legacy(&defaults);
        match p {
            LegacySketchParams::DDSketch {
                relative_accuracy, ..
            } => {
                assert!((relative_accuracy - 0.005).abs() < 1e-12);
            }
            other => panic!("expected DDSketch legacy, got {other:?}"),
        }
    }

    #[test]
    fn params_serde_roundtrip() {
        let cases = [
            SketchParams::Kll(KllParams { k: 200 }),
            SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
            SketchParams::Hll(HllParams { precision: 14 }),
            SketchParams::Cms(CmsParams { w: 2048, d: 5, with_heap: false }),
            SketchParams::CountSketch(CountSketchParams {
                w: 2048,
                d: 5,
                with_heap: true,
            }),
        ];
        for c in cases {
            let json = serde_json::to_string(&c).unwrap();
            let back: SketchParams = serde_json::from_str(&json).unwrap();
            assert_eq!(c, back);
        }
    }
}
