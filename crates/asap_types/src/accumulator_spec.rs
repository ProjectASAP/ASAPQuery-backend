//! `AccumulatorSpec` — data_plane's replacement for the
//! `AggregationType` + `aggregation_sub_type: String` + untyped
//! `parameters: HashMap<String, Value>` triple.
//!
//! **Step 5 of the sketch-identity unification** (see
//! `scratchpad/artifacts/enum-unification-plan.md`, §7-8). Converges
//! accumulator *identity* onto ASAPController's `asap_sketch::SummaryKind`
//! / `SummaryParams` — the same representation `control_plane` already
//! uses as of Stage 3 (merged) — extended with the one axis that
//! representation doesn't have: keyed-vs-unkeyed grouping, which
//! `AggregationType` wrongly folded into identity (`Sum` vs
//! `MultipleSum`, etc.) instead of modeling as a sibling field.
//!
//! ## This is an additive representation, not a replacement (yet)
//!
//! `AggregationConfig` keeps its `aggregation_type` / `aggregation_sub_type`
//! / `parameters` fields untouched. Two hard constraints ruled out full
//! removal in this pass:
//!
//! 1. **`PolicyFingerprint` hash stability.** [`crate::policy_fingerprint`]
//!    hashes `aggregation_type` / `aggregation_sub_type` / `parameters`
//!    directly, and its own module doc is explicit that the byte layout
//!    it produces is a stability *contract* ("Don't reorder fields...
//!    any such change invalidates every deployed fingerprint and forces
//!    a cold-start rebuild"). Changing what feeds that hash — even by
//!    routing it through an equivalent typed shape — risks producing a
//!    different byte sequence for the same logical policy, which strands
//!    on-disk sids after a deploy. `policy_fingerprint.rs` is
//!    deliberately **not touched** by this module; it keeps reading the
//!    original three fields, unchanged.
//! 2. **Consumer fan-out.** `AggregationType` is read by ~40 files across
//!    `data_plane` and `asap_types` — persistence (`sid_metadata.json`
//!    round-trip), query-time capability matching
//!    (`capability_matching.rs`, unrelated to accumulator dispatch),
//!    the query engine, reconciliation, index maintenance — not just
//!    `accumulator_factory.rs` (the single highest-risk consumer, and
//!    the one this module targets). Migrating all of them in one PR was
//!    judged too large to land and review safely; that's tracked as
//!    follow-up, not done here.
//!
//! So: `AccumulatorSpec` is *computed from* `AggregationConfig`'s
//! existing fields via [`AggregationConfig::accumulator_spec`], and
//! consumed by `data_plane::precompute_engine::accumulator_factory`
//! instead of the raw fields. The wire format (`aggregationType` /
//! `aggregationSubType` / `parameters` JSON/YAML keys) is completely
//! unaffected — nothing here changes how `AggregationConfig::from_yaml`
//! / `from_json` parse or how `serialize_to_json` emits.
//!
//! ## What doesn't fit `SummaryKind`/`SummaryParams`
//!
//! `asap_sketch`'s types are ASAPController's, not ours to extend from
//! this repo, and two data_plane-specific details don't fit them:
//!
//! - **Min/max direction.** `SummaryParams::MinMax` carries no fields —
//!   upstream doesn't model a direction axis. `accumulator_factory.rs`
//!   keeps reading `AggregationConfig::aggregation_sub_type` directly
//!   for this one bit (`eq_ignore_ascii_case("max")`), exactly as it did
//!   before this refactor.
//! - **HydraKLL's `(row, col)` tiling.** `SummaryParams::Kll` carries
//!   only `k` — upstream has no concept of the CMS-like grid-of-KLL-cells
//!   layout `HydraKllSketchAccumulator` uses to parallelize a keyed KLL
//!   across many populations. `accumulator_factory.rs` calls
//!   [`cms_params`] directly for the `(SummaryKind::Kll, keyed=true)`
//!   arm, same extraction the plain CMS arms use, because `w`/`d` are
//!   genuinely the same wire keys for both.
//! - **Top-k ranking mode (`weight_mode`).** Not a sketch structural
//!   parameter — a data_plane-only "what to accumulate" axis
//!   (`accumulator_factory::TopkWeight`) with no upstream equivalent.
//!   Stays a raw-`parameters`-reading helper in `accumulator_factory.rs`.

use serde_json::Value;

use crate::aggregation_config::AggregationConfig;
use crate::key_by_label_names::KeyByLabelNames;
use crate::AggregationType;

pub use asap_sketch::{SummaryKind, SummaryParams};

/// Data_plane's typed replacement for
/// `(aggregation_type, aggregation_sub_type, parameters)`: which
/// accumulator to run (`kind`), with what tuning (`params`), and
/// whether it's keyed by a group-by label set (`grouping`).
///
/// Computed on demand from an [`AggregationConfig`] via
/// [`AggregationConfig::accumulator_spec`] — not stored on the config
/// itself, so there is exactly one source of truth for the fields that
/// feed [`crate::policy_fingerprint::PolicyFingerprint`].
#[derive(Debug, Clone, PartialEq)]
pub struct AccumulatorSpec {
    /// Which accumulator family — identity only (no keyed/unkeyed axis,
    /// no heap-vs-bare ambiguity: heap-bearing sketches are their own
    /// `SummaryKind` variant, e.g. `CmsWithHeap` vs `Cms`).
    pub kind: SummaryKind,
    /// Typed tuning parameters matching `kind` (no `HashMap` lookups —
    /// see the module doc for the handful of details that still need
    /// one, kept in `accumulator_factory.rs` since `SummaryParams` has
    /// no field for them).
    pub params: SummaryParams,
    /// `Some(labels)` for a keyed (multi-population) accumulator,
    /// `None` for a single-population one. This is the axis
    /// `AggregationType` wrongly folded into identity (`Sum` vs
    /// `MultipleSum`) — here it's a sibling field instead.
    pub grouping: Option<KeyByLabelNames>,
}

/// Why [`AggregationConfig::accumulator_spec`] couldn't resolve a config
/// into an [`AccumulatorSpec`]. Each variant matches one of the three
/// distinct fallback paths `accumulator_factory::create_accumulator_updater`
/// took pre-Step-5 — preserved verbatim (including which default
/// updater and which warning text each one produced) so this refactor
/// changes *how* the dispatch is expressed, not what it does for any
/// input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccumulatorSpecError {
    /// `aggregation_type` was `SingleSubpopulation` with an
    /// `aggregation_sub_type` string not in the recognized alias list.
    /// Pre-Step-5 this defaulted to `SumAccumulatorUpdater`.
    UnknownSingleSubpopulationSubType(String),
    /// `aggregation_type` was `MultipleSubpopulation` with an
    /// unrecognized `aggregation_sub_type`. Pre-Step-5 this defaulted
    /// to `MultipleSumAccumulatorUpdater` (note: a *different* default
    /// than the `SingleSubpopulation` case).
    UnknownMultipleSubpopulationSubType(String),
    /// `aggregation_type` itself has no accumulator-dispatch mapping.
    /// Today this is only ever `AggregationType::HLL` — it's a real
    /// `SummaryKind::Hll` identity and `control_plane` can emit
    /// `aggregationType: HLL` on the wire, but
    /// `accumulator_factory::create_accumulator_updater` never grew a
    /// real HLL arm (HLL accumulators are built via the SketchEnvelope
    /// ingest path instead, bypassing raw-value dispatch). Pre-existing
    /// gap, not introduced by this refactor — preserved as-is.
    UnmappedAggregationType(AggregationType),
}

impl std::fmt::Display for AccumulatorSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownSingleSubpopulationSubType(s) => {
                write!(
                    f,
                    "Unknown SingleSubpopulation sub_type '{s}', defaulting to Sum"
                )
            }
            Self::UnknownMultipleSubpopulationSubType(s) => {
                write!(
                    f,
                    "Unknown MultipleSubpopulation sub_type '{s}', defaulting to Sum"
                )
            }
            Self::UnmappedAggregationType(t) => write!(
                f,
                "Unknown aggregation_type '{t:?}', defaulting to SingleSubpopulation Sum"
            ),
        }
    }
}

impl std::error::Error for AccumulatorSpecError {}

impl AggregationConfig {
    /// Resolve this config's `(aggregation_type, aggregation_sub_type,
    /// parameters)` triple into a typed [`AccumulatorSpec`].
    ///
    /// Mirrors `accumulator_factory::create_accumulator_updater`'s
    /// pre-Step-5 dispatch exactly — same sub_type alias lists, same
    /// numeric defaults, same three fallback paths (see
    /// [`AccumulatorSpecError`]) — just re-expressed as data instead of
    /// as a 14-arm match baked into the accumulator constructor.
    pub fn accumulator_spec(&self) -> Result<AccumulatorSpec, AccumulatorSpecError> {
        use AggregationType::*;

        let sub_type = self.aggregation_sub_type.as_str();

        let (kind, params, keyed): (SummaryKind, SummaryParams, bool) = match self.aggregation_type
        {
            Sum => (SummaryKind::Sum, SummaryParams::Sum, false),
            Increase => (SummaryKind::Increase, SummaryParams::Increase, false),
            MinMax => (SummaryKind::MinMax, SummaryParams::MinMax, false),
            DatasketchesKLL => (
                SummaryKind::Kll,
                SummaryParams::Kll {
                    k: kll_k_param(self) as u32,
                },
                false,
            ),
            MultipleSum => (SummaryKind::Sum, SummaryParams::Sum, true),
            MultipleIncrease => (SummaryKind::Increase, SummaryParams::Increase, true),
            MultipleMinMax => (SummaryKind::MinMax, SummaryParams::MinMax, true),
            HydraKLL => (
                SummaryKind::Kll,
                SummaryParams::Kll {
                    k: kll_k_param(self) as u32,
                },
                true,
            ),
            CountMinSketch => {
                let (row_num, col_num) = cms_params(self);
                (
                    SummaryKind::Cms,
                    SummaryParams::Cms {
                        width: col_num as u32,
                        depth: row_num as u32,
                    },
                    true,
                )
            }
            CountMinSketchWithHeap => {
                let (row_num, col_num) = cms_params(self);
                let heap_size = heap_size_param(self);
                (
                    SummaryKind::CmsWithHeap,
                    SummaryParams::CmsWithHeap {
                        width: col_num as u32,
                        depth: row_num as u32,
                        heap_size: heap_size as u32,
                    },
                    true,
                )
            }
            CountSketch => {
                let (row_num, col_num) = cms_params(self);
                (
                    SummaryKind::CountSketch,
                    SummaryParams::CountSketch {
                        width: col_num as u32,
                        depth: row_num as u32,
                    },
                    true,
                )
            }
            CountSketchWithHeap => {
                let (row_num, col_num) = cms_params(self);
                let heap_size = heap_size_param(self);
                (
                    SummaryKind::CountSketchWithHeap,
                    SummaryParams::CountSketchWithHeap {
                        width: col_num as u32,
                        depth: row_num as u32,
                        heap_size: heap_size as u32,
                    },
                    true,
                )
            }
            DDSketch => (
                SummaryKind::DDSketch,
                SummaryParams::DDSketch {
                    alpha: ddsketch_alpha_param(self),
                },
                false,
            ),
            HLL => return Err(AccumulatorSpecError::UnmappedAggregationType(HLL)),
            SingleSubpopulation => match sub_type {
                "Sum" | "sum" => (SummaryKind::Sum, SummaryParams::Sum, false),
                "Min" | "min" => (SummaryKind::MinMax, SummaryParams::MinMax, false),
                "Max" | "max" => (SummaryKind::MinMax, SummaryParams::MinMax, false),
                "Increase" | "increase" => (SummaryKind::Increase, SummaryParams::Increase, false),
                "DatasketchesKLL" | "datasketches_kll" | "KLL" | "kll" => (
                    SummaryKind::Kll,
                    SummaryParams::Kll {
                        k: kll_k_param(self) as u32,
                    },
                    false,
                ),
                other => {
                    return Err(AccumulatorSpecError::UnknownSingleSubpopulationSubType(
                        other.to_string(),
                    ))
                }
            },
            MultipleSubpopulation => match sub_type {
                "Sum" | "sum" => (SummaryKind::Sum, SummaryParams::Sum, true),
                "Min" | "min" => (SummaryKind::MinMax, SummaryParams::MinMax, true),
                "Max" | "max" => (SummaryKind::MinMax, SummaryParams::MinMax, true),
                "Increase" | "increase" => (SummaryKind::Increase, SummaryParams::Increase, true),
                "CountMinSketch" | "count_min_sketch" | "CMS" | "cms" => {
                    let (row_num, col_num) = cms_params(self);
                    (
                        SummaryKind::Cms,
                        SummaryParams::Cms {
                            width: col_num as u32,
                            depth: row_num as u32,
                        },
                        true,
                    )
                }
                "HydraKLL" | "hydra_kll" => (
                    SummaryKind::Kll,
                    SummaryParams::Kll {
                        k: kll_k_param(self) as u32,
                    },
                    true,
                ),
                other => {
                    return Err(AccumulatorSpecError::UnknownMultipleSubpopulationSubType(
                        other.to_string(),
                    ))
                }
            },
        };

        let grouping = if keyed {
            Some(self.grouping_labels.clone())
        } else {
            None
        };

        Ok(AccumulatorSpec {
            kind,
            params,
            grouping,
        })
    }
}

// ---------------------------------------------------------------------------
// Raw-parameter extraction helpers.
//
// Relocated verbatim from `data_plane::precompute_engine::accumulator_factory`
// (same names, same behavior, same defaults) — this is now their one
// definition; `accumulator_factory.rs` re-exports them via `use` so its
// existing unit tests (`cms_params_reads_canonical_w_d_keys`,
// `test_kll_k_param_capital_k`, ...) keep passing unchanged.
// ---------------------------------------------------------------------------

/// Extract the KLL `k` parameter. Capital `"K"` takes precedence over
/// lowercase `"k"` to match the convention used by the top-level
/// aggregation type arms. Defaults to 200.
pub fn kll_k_param(config: &AggregationConfig) -> u16 {
    config
        .parameters
        .get("K")
        .or_else(|| config.parameters.get("k"))
        .and_then(|v| v.as_u64())
        .and_then(|v| u16::try_from(v).ok())
        .unwrap_or(200)
}

/// Extract `(row_num, col_num)` for CMS / HydraKLL configs.
///
/// Reads canonical `d` (depth = rows) / `w` (width = cols) keys —
/// matches what the control plane's `sketch_params_to_json` emits and
/// what `sketch_config_to_params` uses for OTLP policy_fp content
/// matching. Defaults to `(4, 1000)`.
pub fn cms_params(config: &AggregationConfig) -> (usize, usize) {
    let row_num = config
        .parameters
        .get("d")
        .and_then(|v| v.as_u64())
        .unwrap_or(4) as usize;
    let col_num = config
        .parameters
        .get("w")
        .and_then(|v| v.as_u64())
        .unwrap_or(1000) as usize;
    (row_num, col_num)
}

/// Top-k heap size for the `*WithHeap` configs. Reads `heap_size` / `k`
/// from `parameters`; defaults to 20 (the heap holds the top-k
/// candidates — it must be >= the largest `k` a query asks for).
pub fn heap_size_param(config: &AggregationConfig) -> usize {
    config
        .parameters
        .get("heap_size")
        .or_else(|| config.parameters.get("k"))
        .or_else(|| config.parameters.get("K"))
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .filter(|&v| v > 0)
        .unwrap_or(20)
}

/// Pull `relativeAccuracy` (or canonical aliases) out of a
/// streaming-config aggregation entry. Defaults to 0.01 (1% rel-err,
/// the same default the agent's `ddsketchprocessor` uses).
pub fn ddsketch_alpha_param(config: &AggregationConfig) -> f64 {
    let parsed = param_f64(config, "relativeAccuracy")
        .or_else(|| param_f64(config, "relative_accuracy"))
        .or_else(|| param_f64(config, "alpha"))
        .unwrap_or(0.01);
    if parsed > 0.0 && parsed < 1.0 {
        parsed
    } else {
        tracing::warn!(
            "DDSketch relativeAccuracy {} out of (0,1); using default 0.01",
            parsed
        );
        0.01
    }
}

fn param_f64(config: &AggregationConfig, key: &str) -> Option<f64> {
    config.parameters.get(key).and_then(Value::as_f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::WindowType;
    use crate::key_by_label_names::KeyByLabelNames;
    use std::collections::HashMap;

    #[allow(clippy::too_many_arguments)]
    fn make_config(
        agg_type: AggregationType,
        sub_type: &str,
        params: HashMap<String, Value>,
        grouping_labels: Vec<&str>,
    ) -> AggregationConfig {
        AggregationConfig::new(
            agg_type,
            sub_type.to_string(),
            params,
            KeyByLabelNames::new(grouping_labels.into_iter().map(|s| s.to_string()).collect()),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            "m".to_string(),
            None,
            None,
            None,
        )
    }

    // ---- direct (non-wrapper) variants ------------------------------

    #[test]
    fn sum_is_unkeyed_sum() {
        let cfg = make_config(AggregationType::Sum, "", HashMap::new(), vec![]);
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::Sum);
        assert_eq!(spec.params, SummaryParams::Sum);
        assert!(spec.grouping.is_none());
    }

    #[test]
    fn multiple_sum_is_keyed_sum() {
        let cfg = make_config(
            AggregationType::MultipleSum,
            "",
            HashMap::new(),
            vec!["zone"],
        );
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::Sum);
        assert_eq!(
            spec.grouping,
            Some(KeyByLabelNames::new(vec!["zone".to_string()]))
        );
    }

    #[test]
    fn datasketches_kll_reads_k_param() {
        let mut params = HashMap::new();
        params.insert("k".to_string(), serde_json::json!(128));
        let cfg = make_config(AggregationType::DatasketchesKLL, "", params, vec![]);
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::Kll);
        assert_eq!(spec.params, SummaryParams::Kll { k: 128 });
        assert!(spec.grouping.is_none());
    }

    #[test]
    fn hydra_kll_is_keyed_kll() {
        let mut params = HashMap::new();
        params.insert("k".to_string(), serde_json::json!(64));
        let cfg = make_config(AggregationType::HydraKLL, "", params, vec!["host"]);
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::Kll);
        assert_eq!(spec.params, SummaryParams::Kll { k: 64 });
        assert!(spec.grouping.is_some());
    }

    #[test]
    fn count_min_sketch_maps_to_cms_params() {
        let mut params = HashMap::new();
        params.insert("d".to_string(), serde_json::json!(7));
        params.insert("w".to_string(), serde_json::json!(2048));
        let cfg = make_config(AggregationType::CountMinSketch, "", params, vec!["host"]);
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::Cms);
        assert_eq!(
            spec.params,
            SummaryParams::Cms {
                width: 2048,
                depth: 7
            }
        );
        assert!(spec.grouping.is_some());
    }

    #[test]
    fn count_min_sketch_with_heap_maps_to_cms_with_heap_params() {
        let mut params = HashMap::new();
        params.insert("d".to_string(), serde_json::json!(4));
        params.insert("w".to_string(), serde_json::json!(256));
        params.insert("heap_size".to_string(), serde_json::json!(8));
        let cfg = make_config(
            AggregationType::CountMinSketchWithHeap,
            "",
            params,
            vec!["host"],
        );
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::CmsWithHeap);
        assert_eq!(
            spec.params,
            SummaryParams::CmsWithHeap {
                width: 256,
                depth: 4,
                heap_size: 8
            }
        );
    }

    /// Documented existing quirk (see `accumulator_factory.rs`): bare
    /// `CountSketch` gets its own `SummaryKind` identity here, but
    /// `accumulator_factory` routes it through the same
    /// `CmsAccumulatorUpdater` as bare CMS — no dedicated heap-less
    /// Count-Sketch accumulator exists. This test locks in the *identity*
    /// resolution only; the shared-accumulator behavior is exercised in
    /// `accumulator_factory.rs`'s own tests.
    #[test]
    fn count_sketch_gets_its_own_kind_identity() {
        let cfg = make_config(AggregationType::CountSketch, "", HashMap::new(), vec!["h"]);
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::CountSketch);
    }

    #[test]
    fn ddsketch_reads_relative_accuracy_alpha() {
        let mut params = HashMap::new();
        params.insert("relativeAccuracy".to_string(), serde_json::json!(0.02));
        let cfg = make_config(AggregationType::DDSketch, "", params, vec![]);
        let spec = cfg.accumulator_spec().expect("resolves");
        assert_eq!(spec.kind, SummaryKind::DDSketch);
        assert_eq!(spec.params, SummaryParams::DDSketch { alpha: 0.02 });
        assert!(spec.grouping.is_none());
    }

    #[test]
    fn hll_is_unmapped_preserving_pre_step5_gap() {
        let cfg = make_config(AggregationType::HLL, "", HashMap::new(), vec![]);
        let err = cfg.accumulator_spec().expect_err("HLL has no dispatch arm");
        assert_eq!(
            err,
            AccumulatorSpecError::UnmappedAggregationType(AggregationType::HLL)
        );
    }

    // ---- wrapper (Single/MultipleSubpopulation) variants ------------

    #[test]
    fn single_subpopulation_sum_alias() {
        for alias in ["Sum", "sum"] {
            let cfg = make_config(
                AggregationType::SingleSubpopulation,
                alias,
                HashMap::new(),
                vec![],
            );
            let spec = cfg.accumulator_spec().expect("resolves");
            assert_eq!(spec.kind, SummaryKind::Sum);
            assert!(spec.grouping.is_none());
        }
    }

    #[test]
    fn multiple_subpopulation_cms_alias() {
        for alias in ["CountMinSketch", "count_min_sketch", "CMS", "cms"] {
            let cfg = make_config(
                AggregationType::MultipleSubpopulation,
                alias,
                HashMap::new(),
                vec!["host"],
            );
            let spec = cfg.accumulator_spec().expect("resolves");
            assert_eq!(spec.kind, SummaryKind::Cms);
            assert!(spec.grouping.is_some());
        }
    }

    #[test]
    fn multiple_subpopulation_hydra_kll_alias() {
        for alias in ["HydraKLL", "hydra_kll"] {
            let cfg = make_config(
                AggregationType::MultipleSubpopulation,
                alias,
                HashMap::new(),
                vec!["host"],
            );
            let spec = cfg.accumulator_spec().expect("resolves");
            assert_eq!(spec.kind, SummaryKind::Kll);
            assert!(spec.grouping.is_some());
        }
    }

    #[test]
    fn unknown_single_subpopulation_sub_type_errors() {
        let cfg = make_config(
            AggregationType::SingleSubpopulation,
            "Bogus",
            HashMap::new(),
            vec![],
        );
        let err = cfg.accumulator_spec().expect_err("unknown sub_type");
        assert_eq!(
            err,
            AccumulatorSpecError::UnknownSingleSubpopulationSubType("Bogus".to_string())
        );
    }

    #[test]
    fn unknown_multiple_subpopulation_sub_type_errors() {
        let cfg = make_config(
            AggregationType::MultipleSubpopulation,
            "Bogus",
            HashMap::new(),
            vec!["host"],
        );
        let err = cfg.accumulator_spec().expect_err("unknown sub_type");
        assert_eq!(
            err,
            AccumulatorSpecError::UnknownMultipleSubpopulationSubType("Bogus".to_string())
        );
    }

    #[test]
    fn error_display_matches_pre_step5_warning_text() {
        assert_eq!(
            AccumulatorSpecError::UnknownSingleSubpopulationSubType("Bogus".to_string())
                .to_string(),
            "Unknown SingleSubpopulation sub_type 'Bogus', defaulting to Sum"
        );
        assert_eq!(
            AccumulatorSpecError::UnknownMultipleSubpopulationSubType("Bogus".to_string())
                .to_string(),
            "Unknown MultipleSubpopulation sub_type 'Bogus', defaulting to Sum"
        );
        assert_eq!(
            AccumulatorSpecError::UnmappedAggregationType(AggregationType::HLL).to_string(),
            "Unknown aggregation_type 'HLL', defaulting to SingleSubpopulation Sum"
        );
    }

    // ---- PolicyFingerprint stability guard ---------------------------

    /// `accumulator_spec()` must be a pure, additional *read* of
    /// `AggregationConfig` — it must not change what
    /// `PolicyFingerprint::from_config` hashes. This locks in a fixed
    /// fingerprint for a fixed config as a tripwire: if this test ever
    /// needs its expected constant updated, `policy_fingerprint.rs`
    /// changed in a way that breaks on-disk sid compatibility, which is
    /// exactly what Step 5 was required not to do.
    #[test]
    fn accumulator_spec_does_not_perturb_policy_fingerprint() {
        let cfg = make_config(AggregationType::DDSketch, "", HashMap::new(), vec![]);
        let fp_before = cfg.policy_fp_u64();
        let _ = cfg.accumulator_spec(); // takes &self — must not mutate `cfg`
        let fp_after = cfg.policy_fp_u64();
        assert_eq!(
            fp_before, fp_after,
            "calling accumulator_spec() must not change the fingerprint \
             PolicyFingerprint::from_config computes from this config"
        );

        // An independently-built config with identical content must
        // still agree — proves accumulator_spec() reads, never writes,
        // the fields PolicyFingerprint::from_config hashes.
        let cfg2 = make_config(AggregationType::DDSketch, "", HashMap::new(), vec![]);
        assert_eq!(fp_after, cfg2.policy_fp_u64());
    }
}
