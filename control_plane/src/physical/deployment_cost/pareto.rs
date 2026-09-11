//! SP-6 Pareto frontier reporting for physical plans.
//!
//! The cost-model planner minimises bandwidth while meeting the accuracy SLA.
//! This module extends it by enumerating **all** Pareto-optimal plans across
//! three objectives — bandwidth, CPU and memory — so operators can choose the
//! trade-off that best fits their infrastructure.
//!
//! # Algorithm
//!
//! For each sketch candidate the planner generates one `ParetoPoint` (a plan
//! annotated with its scored objectives).  A point is Pareto-optimal if no
//! other point is better on *every* objective simultaneously.  The frontier is
//! returned sorted by weighted-sum score so that `select_best` returns the
//! single plan that best matches caller-supplied objective weights.
//!
//! # Usage
//!
//! ```ignore
//! let weights = ObjectiveWeights { bandwidth: 0.6, cpu: 0.3, memory: 0.1 };
//! let frontier = pareto_frontier(&planner, &workload, &wc, weights);
//! let best = select_best(&frontier, weights);
//! ```

use std::collections::HashMap;

use crate::physical::deployment_cost::delta::decide_delta;
use crate::physical::deployment_cost::online;
use crate::physical::deployment_cost::{benchmark_table_pub, score_with, SketchCosts};
use crate::physical::workload_planner::{
    default_sketch_params, select_window_strategy, DeploymentPlanCompiler,
};
use crate::types::*;

// ── Types ─────────────────────────────────────────────────────────────────────

/// Caller-supplied relative weights for the three objectives.
/// Values are normalised internally so they need not sum to 1.
#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize)]
pub struct ObjectiveWeights {
    /// Weight given to minimising bandwidth (bytes/sec).
    pub bandwidth: f64,
    /// Weight given to minimising CPU (µs/sample).
    pub cpu: f64,
    /// Weight given to minimising memory (bytes).
    pub memory: f64,
}

impl Default for ObjectiveWeights {
    fn default() -> Self {
        Self {
            bandwidth: 1.0,
            cpu: 0.0,
            memory: 0.0,
        }
    }
}

impl ObjectiveWeights {
    fn normalised(self) -> Self {
        let total = self.bandwidth + self.cpu + self.memory;
        if total <= 0.0 {
            return Self {
                bandwidth: 1.0,
                cpu: 0.0,
                memory: 0.0,
            };
        }
        Self {
            bandwidth: self.bandwidth / total,
            cpu: self.cpu / total,
            memory: self.memory / total,
        }
    }
}

/// A single candidate on the Pareto frontier.
#[derive(Debug, Clone)]
pub struct ParetoPoint {
    pub plan: CollectionPlan,
    pub sketch_type: SketchType,
    pub bandwidth_bytes_per_sec: f64,
    pub cpu_micros_per_sample: f64,
    pub memory_bytes: f64,
    pub estimated_error: f64,
    pub meets_sla: bool,
}

// ── Frontier computation ──────────────────────────────────────────────────────

/// Enumerate all Pareto-optimal plans for the given workload.
///
/// Only plans that meet the accuracy SLA are included.  The returned slice is
/// sorted by weighted-sum score (lowest = best) according to `weights`.
///
/// If `online_store` is `Some` the scoring uses EMA-blended costs; otherwise
/// it falls back to the static benchmark table.
pub fn pareto_frontier(
    workload: &QueryWorkload,
    wc: &WorkloadCharacteristics,
    weights: ObjectiveWeights,
    online_store: Option<&online::OnlineMetricsStore>,
) -> Vec<ParetoPoint> {
    if !matches!(workload.accuracy, crate::types_v2::AccuracyTarget::Epsilon(epsilon) if epsilon > 0.0)
    {
        return Vec::new();
    }
    let table: HashMap<SketchType, SketchCosts> = match online_store {
        Some(s) => online::effective_table(s),
        None => benchmark_table_pub(),
    };

    let candidates = all_sketch_types();
    let rules = DeploymentPlanCompiler::new();
    let mut points: Vec<ParetoPoint> = Vec::new();

    for st in candidates {
        let params = default_sketch_params(&st, workload.error_bound());
        let (mode, window_duration) = select_window_strategy(workload);

        let mut plan = rules.plan(workload);
        plan.agent_config.sketch_type = st.clone();
        plan.agent_config.sketch_params = params;
        plan.agent_config.mode = mode;
        plan.agent_config.window_duration = window_duration;

        // Apply delta decision using the cost table.
        apply_delta(st.clone(), &mut plan, workload, wc, &table);

        let s = score_with(&plan, workload, &table);
        if !s.meets_sla {
            continue;
        }

        points.push(ParetoPoint {
            sketch_type: st,
            bandwidth_bytes_per_sec: s.bandwidth_bytes_per_sec,
            cpu_micros_per_sample: s.cpu_micros_per_sample,
            memory_bytes: s.memory_bytes,
            estimated_error: s.estimated_error,
            meets_sla: s.meets_sla,
            plan,
        });
    }

    // Filter to Pareto-optimal subset.
    let optimal = pareto_filter(&points);

    // Sort by weighted score.
    let w = weights.normalised();
    let mut sorted = optimal;
    sorted.sort_by(|a, b| {
        weighted_score(a, w)
            .partial_cmp(&weighted_score(b, w))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    sorted
}

/// Returns the single plan from `frontier` with the lowest weighted-sum score.
/// Returns `None` if the frontier is empty.
pub fn select_best(frontier: &[ParetoPoint], weights: ObjectiveWeights) -> Option<&ParetoPoint> {
    let w = weights.normalised();
    frontier.iter().min_by(|a, b| {
        weighted_score(a, w)
            .partial_cmp(&weighted_score(b, w))
            .unwrap_or(std::cmp::Ordering::Equal)
    })
}

// ── Private helpers ───────────────────────────────────────────────────────────

fn all_sketch_types() -> Vec<SketchType> {
    vec![
        SketchType::DDSketch,
        SketchType::KLL,
        SketchType::HLL,
        SketchType::CountSketch,
        SketchType::CountMinSketch,
    ]
}

fn apply_delta(
    st: SketchType,
    plan: &mut CollectionPlan,
    w: &QueryWorkload,
    wc: &WorkloadCharacteristics,
    table: &HashMap<SketchType, SketchCosts>,
) {
    let bytes_per_series_per_sec = table
        .get(&st)
        .map(|c| c.bytes_per_series_per_sec)
        .unwrap_or(200.0);

    let (decision, summary) = decide_delta(plan, w, wc, bytes_per_series_per_sec);

    match &decision {
        DeltaDecision::UseDelta { threshold, .. } => {
            plan.agent_config.delta_transmission = true;
            plan.agent_config.delta_threshold = *threshold;
            plan.agent_config.gos = (plan.agent_config.sketch_type == SketchType::CountSketch)
                .then(|| GosKnobs::derive(w.error_bound(), 1, 0.0, 1.0, false));
        }
        _ => {
            plan.agent_config.delta_transmission = false;
            plan.agent_config.delta_threshold = 0.0;
            plan.agent_config.gos = None;
        }
    }
    plan.delta_decision = decision;
    plan.transmission_cost_summary = summary;
}

fn pareto_filter(points: &[ParetoPoint]) -> Vec<ParetoPoint> {
    points
        .iter()
        .filter(|p| {
            !points.iter().any(|q| {
                q.bandwidth_bytes_per_sec <= p.bandwidth_bytes_per_sec
                    && q.cpu_micros_per_sample <= p.cpu_micros_per_sample
                    && q.memory_bytes <= p.memory_bytes
                    && (q.bandwidth_bytes_per_sec < p.bandwidth_bytes_per_sec
                        || q.cpu_micros_per_sample < p.cpu_micros_per_sample
                        || q.memory_bytes < p.memory_bytes)
            })
        })
        .cloned()
        .collect()
}

fn weighted_score(p: &ParetoPoint, w: ObjectiveWeights) -> f64 {
    w.bandwidth * p.bandwidth_bytes_per_sec
        + w.cpu * p.cpu_micros_per_sample
        + w.memory * p.memory_bytes
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    fn quantile_workload() -> QueryWorkload {
        QueryWorkload {
            metric_name: "latency".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![AggType::Quantile],
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.02,
            accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.02),
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    fn default_wc() -> WorkloadCharacteristics {
        WorkloadCharacteristics::default()
    }

    #[test]
    fn quantile_frontier_non_empty_and_kll_present() {
        // KLL has lower BW, CPU, and memory than DDSketch, so DDSketch is
        // dominated and excluded.  KLL and HLL form the frontier (HLL has
        // lower BW/CPU but higher memory than KLL).
        let f = pareto_frontier(
            &quantile_workload(),
            &default_wc(),
            ObjectiveWeights::default(),
            None,
        );
        assert!(
            !f.is_empty(),
            "frontier should not be empty for quantile workload"
        );
        let types: Vec<_> = f.iter().map(|p| &p.sketch_type).collect();
        assert!(
            types.contains(&&SketchType::KLL),
            "KLL expected in frontier"
        );
    }

    #[test]
    fn all_frontier_points_meet_sla() {
        let f = pareto_frontier(
            &quantile_workload(),
            &default_wc(),
            ObjectiveWeights::default(),
            None,
        );
        for p in &f {
            assert!(
                p.meets_sla,
                "{} does not meet SLA (error={})",
                p.sketch_type, p.estimated_error
            );
        }
    }

    #[test]
    fn bandwidth_weight_selects_lowest_bw() {
        let f = pareto_frontier(
            &quantile_workload(),
            &default_wc(),
            ObjectiveWeights {
                bandwidth: 1.0,
                cpu: 0.0,
                memory: 0.0,
            },
            None,
        );
        let best = select_best(
            &f,
            ObjectiveWeights {
                bandwidth: 1.0,
                cpu: 0.0,
                memory: 0.0,
            },
        )
        .unwrap();
        for p in &f {
            assert!(
                p.bandwidth_bytes_per_sec >= best.bandwidth_bytes_per_sec,
                "best ({}) should have minimum bandwidth",
                best.sketch_type
            );
        }
    }

    #[test]
    fn memory_weight_selects_lowest_memory() {
        let f = pareto_frontier(
            &quantile_workload(),
            &default_wc(),
            ObjectiveWeights {
                bandwidth: 0.0,
                cpu: 0.0,
                memory: 1.0,
            },
            None,
        );
        let best = select_best(
            &f,
            ObjectiveWeights {
                bandwidth: 0.0,
                cpu: 0.0,
                memory: 1.0,
            },
        )
        .unwrap();
        for p in &f {
            assert!(
                p.memory_bytes >= best.memory_bytes,
                "best ({}) should have minimum memory",
                best.sketch_type
            );
        }
    }

    #[test]
    fn select_best_empty_returns_none() {
        let result = select_best(&[], ObjectiveWeights::default());
        assert!(result.is_none());
    }

    #[test]
    fn tight_sla_excludes_inaccurate_sketches() {
        let w = QueryWorkload {
            accuracy_sla: 0.001, // very tight — only DDSketch at 0.1% accuracy can meet this
            accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.001),
            ..quantile_workload()
        };
        let f = pareto_frontier(&w, &default_wc(), ObjectiveWeights::default(), None);
        for p in &f {
            assert!(
                p.estimated_error <= 0.001,
                "{} error {} exceeds 0.001 SLA",
                p.sketch_type,
                p.estimated_error
            );
        }
    }

    #[test]
    fn pareto_no_dominated_points() {
        let f = pareto_frontier(
            &quantile_workload(),
            &default_wc(),
            ObjectiveWeights::default(),
            None,
        );
        // Verify no point in f is dominated by another.
        for i in 0..f.len() {
            for j in 0..f.len() {
                if i == j {
                    continue;
                }
                let a = &f[i];
                let b = &f[j];
                let b_dominates_a = b.bandwidth_bytes_per_sec <= a.bandwidth_bytes_per_sec
                    && b.cpu_micros_per_sample <= a.cpu_micros_per_sample
                    && b.memory_bytes <= a.memory_bytes
                    && (b.bandwidth_bytes_per_sec < a.bandwidth_bytes_per_sec
                        || b.cpu_micros_per_sample < a.cpu_micros_per_sample
                        || b.memory_bytes < a.memory_bytes);
                assert!(
                    !b_dominates_a,
                    "{} dominates {} but both are in frontier",
                    b.sketch_type, a.sketch_type
                );
            }
        }
    }
}
