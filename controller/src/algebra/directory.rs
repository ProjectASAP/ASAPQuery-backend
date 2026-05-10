//! Sketch directory — single source of truth for mapping aggregation
//! operations to sketch types, parameters, and memory estimates.
//!
//! Previously this logic was duplicated across:
//! - `planner/rules.rs` (`select_sketch_type`)
//! - `planner/stage_split.rs` (`agg_op_to_sketch_type`, `agg_op_to_sketch_params`,
//!    `estimated_sketch_memory_bytes`)
//! - `algebra/allocator.rs` (`sketch_type_for_op`)
//!
//! All callers now go through this module.

use crate::algebra::expr::{AggIntent, ExactAgg};
use crate::types::{
    AggType, SketchDefaults,
    SketchParams, SketchType,
};

// ── AggType → candidate SketchTypes ──────────────────────────────────────────

/// Candidate sketch families per aggregation type.
///
/// Each aggregation type has multiple viable sketch implementations.
/// The first entry is the default; the cost-model planner scores all
/// candidates and picks the cheapest that meets the accuracy SLA.
///
/// | AggType     | Candidates (default first)  |
/// |-------------|---------------------------- |
/// | Quantile    | DDSketch, KLL               |
/// | Cardinality | HLL                         |
/// | Frequency   | CountSketch, CountMinSketch  |
pub fn candidates_for_agg(agg: &AggType) -> &'static [SketchType] {
    match agg {
        AggType::Quantile    => &[SketchType::DDSketch, SketchType::KLL],
        AggType::Cardinality => &[SketchType::HLL],
        AggType::Frequency   => &[SketchType::CountSketch, SketchType::CountMinSketch],
    }
}

/// All candidate sketch types for a workload (deduped, stable order).
pub fn candidates_for_workload(aggs: &[AggType]) -> Vec<SketchType> {
    let mut out = Vec::new();
    for agg in aggs {
        for st in candidates_for_agg(agg) {
            if !out.contains(st) {
                out.push(st.clone());
            }
        }
    }
    out
}

/// Pick the default sketch family from a list of aggregation types.
///
/// Returns the first candidate for the highest-priority aggregation.
/// Priority: Quantile → Cardinality → Frequency.  Falls back to DDSketch.
///
/// For cost-optimised selection, use [`candidates_for_workload`] and score
/// each candidate via the cost model.
pub fn sketch_type_for_agg(aggs: &[AggType]) -> SketchType {
    for agg in aggs {
        let candidates = candidates_for_agg(agg);
        if !candidates.is_empty() {
            return candidates[0].clone();
        }
    }
    SketchType::DDSketch
}

// ── AggIntent → SketchType ───────────────────────────────────────────────────

/// Resolve the concrete [`SketchType`] for an [`AggIntent`] IR node.
pub fn sketch_type_for_op(op: &AggIntent) -> SketchType {
    match op {
        AggIntent::Quantile { .. } | AggIntent::Extrema { .. } => SketchType::DDSketch,
        AggIntent::Cardinality { .. } => SketchType::HLL,
        AggIntent::Frequency { .. } => SketchType::CountSketch,
        AggIntent::PerPartition { inner, .. } => sketch_type_for_op(inner),
        AggIntent::Exact(_) => SketchType::DDSketch,
    }
}

// ── AggIntent → SketchParams ────────────────────────────────────────────────

/// Derive [`SketchParams`] from an [`AggIntent`] IR node.
pub fn sketch_params_for_op(op: &AggIntent) -> SketchParams {
    match op {
        AggIntent::Quantile { quantiles, accuracy } => SketchParams::DDSketch {
            relative_accuracy: *accuracy,
            quantiles: quantiles.clone(),
        },
        AggIntent::Cardinality { accuracy } => {
            // registers ≈ (1.04/accuracy)^2, precision = log2(registers)
            let registers = ((1.04 / accuracy).powi(2) as u32).next_power_of_two();
            let precision = (registers as f64).log2() as u32;
            SketchParams::HLL { precision }
        },
        AggIntent::Frequency { accuracy } => {
            let width = (std::f64::consts::E / accuracy) as u32;
            SketchParams::CountSketch {
                epsilon: *accuracy,
                delta: 0.01,
            }
        },
        AggIntent::PerPartition { inner, .. } => sketch_params_for_op(inner),
        AggIntent::Extrema { .. } => SketchParams::DDSketch {
            relative_accuracy: 0.01,
            quantiles: vec![0.0, 1.0],
        },
        AggIntent::Exact(_) => SketchParams::default(),
    }
}

/// Combined (type, params) lookup — convenience for callers that need both.
pub fn sketch_type_and_params(op: &AggIntent) -> (SketchType, SketchParams) {
    (sketch_type_for_op(op), sketch_params_for_op(op))
}

// ── AggIntent → memory estimate ─────────────────────────────────────────────

/// Estimated sketch memory footprint per series (bytes).
///
/// Used by `split_expr_by_stage` to decide whether to defer an operation
/// to a later pipeline stage when the budget is exceeded.
pub fn estimated_sketch_memory_bytes(op: &AggIntent) -> u64 {
    match op {
        AggIntent::Quantile { .. } => 4_096,
        AggIntent::Cardinality { accuracy } => {
            // HLL: registers ≈ (1.04/accuracy)^2, memory = registers
            let registers = ((1.04 / accuracy).powi(2) as u64).next_power_of_two();
            registers.max(16)
        },
        AggIntent::Frequency { accuracy } => {
            // CMS: width ≈ e/accuracy, depth ≈ 5, memory = width*depth*8
            let width = (std::f64::consts::E / accuracy) as u64;
            width * 5 * 8
        },
        AggIntent::Extrema { .. } => 16,
        AggIntent::PerPartition { inner, keys } => {
            let factor = 1u64 << keys.len().min(10);
            estimated_sketch_memory_bytes(inner).saturating_mul(factor)
        },
        AggIntent::Exact(_) => 8,
    }
}

// ── SketchType + accuracy SLA → SketchParams (configurable defaults) ─────────

/// Build default [`SketchParams`] from a [`SketchDefaults`] config and accuracy SLA.
///
/// Query-specific quantiles override the configured grid when non-empty.
pub fn build_sketch_params(
    defaults: &SketchDefaults,
    st: &SketchType,
    accuracy_sla: f64,
    query_quantiles: &[f64],
) -> SketchParams {
    let acc = if accuracy_sla <= 0.0 {
        defaults.ddsketch.relative_accuracy
    } else {
        accuracy_sla
    };
    let quantiles: Vec<f64> = if !query_quantiles.is_empty() {
        query_quantiles.to_vec()
    } else {
        defaults.quantile_grid.clone()
    };
    match st {
        SketchType::DDSketch => SketchParams::DDSketch {
            relative_accuracy: acc,
            quantiles,
        },
        SketchType::KLL => {
            let k = ((1.0 / acc) as u32).max(defaults.kll.min_k);
            SketchParams::KLL { k, quantiles }
        }
        SketchType::HLL => {
            let d = &defaults.hll;
            let precision = if acc > d.precision_threshold { d.precision_coarse } else { d.precision_fine };
            SketchParams::HLL { precision }
        }
        SketchType::CountSketch => SketchParams::CountSketch {
            epsilon: defaults.count_sketch.epsilon,
            delta: defaults.count_sketch.delta,
        },
        SketchType::CountMinSketch => SketchParams::CountMinSketch {
            rows: defaults.count_min_sketch.rows,
            cols: defaults.count_min_sketch.cols,
            metric_name: defaults.count_min_sketch.metric_name.clone(),
        },
    }
}

/// Convenience: build default params using compiled-in defaults.
pub fn default_sketch_params(st: &SketchType, accuracy_sla: f64) -> SketchParams {
    build_sketch_params(&SketchDefaults::default(), st, accuracy_sla, &[])
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agg_type_quantile_maps_to_ddsketch() {
        assert_eq!(sketch_type_for_agg(&[AggType::Quantile]), SketchType::DDSketch);
    }

    #[test]
    fn agg_type_cardinality_maps_to_hll() {
        assert_eq!(sketch_type_for_agg(&[AggType::Cardinality]), SketchType::HLL);
    }

    #[test]
    fn agg_type_frequency_maps_to_countsketch() {
        assert_eq!(sketch_type_for_agg(&[AggType::Frequency]), SketchType::CountSketch);
    }

    #[test]
    fn empty_aggs_default_to_ddsketch() {
        assert_eq!(sketch_type_for_agg(&[]), SketchType::DDSketch);
    }

    #[test]
    fn op_quantile_yields_ddsketch_type_and_params() {
        let op = AggIntent::Quantile { quantiles: vec![0.5], accuracy: 0.01 };
        let (st, p) = sketch_type_and_params(&op);
        assert_eq!(st, SketchType::DDSketch);
        assert!(matches!(p, SketchParams::DDSketch { .. }));
    }

    #[test]
    fn op_cardinality_yields_hll_type() {
        let op = AggIntent::default_cardinality();
        assert_eq!(sketch_type_for_op(&op), SketchType::HLL);
    }

    #[test]
    fn op_frequency_yields_countsketch() {
        let op = AggIntent::default_frequency();
        assert_eq!(sketch_type_for_op(&op), SketchType::CountSketch);
    }

    #[test]
    fn per_partition_delegates_to_inner() {
        let op = AggIntent::PerPartition {
            inner: Box::new(AggIntent::default_cardinality()),
            keys: vec!["k".into()],
        };
        assert_eq!(sketch_type_for_op(&op), SketchType::HLL);
    }

    #[test]
    fn memory_quantile() {
        let op = AggIntent::Quantile { quantiles: vec![0.5], accuracy: 0.01 };
        assert_eq!(estimated_sketch_memory_bytes(&op), 4096);
    }

    #[test]
    fn memory_per_partition_scales_by_keys() {
        let inner = AggIntent::default_cardinality();
        let base_mem = estimated_sketch_memory_bytes(&inner);
        let op = AggIntent::PerPartition {
            inner: Box::new(inner),
            keys: vec!["a".into(), "b".into()],
        };
        assert_eq!(estimated_sketch_memory_bytes(&op), base_mem * 4);
    }

    #[test]
    fn configurable_defaults_override_quantile_grid() {
        let mut d = SketchDefaults::default();
        d.quantile_grid = vec![0.5, 0.99];
        let p = build_sketch_params(&d, &SketchType::DDSketch, 0.01, &[]);
        assert_eq!(p.quantiles(), &[0.5, 0.99]);
    }

    #[test]
    fn query_quantiles_override_grid() {
        let d = SketchDefaults::default();
        let p = build_sketch_params(&d, &SketchType::DDSketch, 0.01, &[0.1, 0.9]);
        assert_eq!(p.quantiles(), &[0.1, 0.9]);
    }

    #[test]
    fn quantile_candidates_include_ddsketch_and_kll() {
        let c = candidates_for_agg(&AggType::Quantile);
        assert!(c.contains(&SketchType::DDSketch));
        assert!(c.contains(&SketchType::KLL));
    }

    #[test]
    fn frequency_candidates_include_cs_and_cms() {
        let c = candidates_for_agg(&AggType::Frequency);
        assert!(c.contains(&SketchType::CountSketch));
        assert!(c.contains(&SketchType::CountMinSketch));
    }

    #[test]
    fn candidates_for_workload_dedupes() {
        let c = candidates_for_workload(&[AggType::Quantile, AggType::Quantile]);
        assert_eq!(c.iter().filter(|s| **s == SketchType::DDSketch).count(), 1);
    }
}
