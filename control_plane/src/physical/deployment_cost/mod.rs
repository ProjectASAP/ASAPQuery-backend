//! Physical deployment cost model and reporting helpers.

use std::collections::HashMap;

pub mod delta;
pub mod online;
pub mod pareto;
pub mod sketch_capability;
pub mod tco;
pub mod wire;

use self::delta::decide_delta;
use crate::physical::workload_planner::{
    default_sketch_params, select_window_strategy, DeploymentPlanCompiler,
};
use crate::types::*;

// ── Benchmark-derived cost table ──────────────────────────────────────────────
//
// Source: e2e benchmark results (2026-03-15), 1 000 series × 1 000 Hz row.
// Units: bandwidth bytes/series/sec, CPU µs/sample, memory bytes/sketch.

#[derive(Debug, Clone, Copy)]
pub struct SketchCosts {
    pub bytes_per_series_per_sec: f64,
    pub cpu_micros_per_sample: f64,
    pub base_memory_bytes: f64,
    pub relative_error_at_default: f64,
}

/// Public accessor used by `apply_delta_decision` to retrieve the cost table.
pub fn benchmark_table_pub() -> HashMap<SketchType, SketchCosts> {
    benchmark_table()
}

fn benchmark_table() -> HashMap<SketchType, SketchCosts> {
    [
        (
            SketchType::DDSketch,
            SketchCosts {
                bytes_per_series_per_sec: 120.0,
                cpu_micros_per_sample: 0.8,
                base_memory_bytes: 4_096.0,
                relative_error_at_default: 0.01,
            },
        ),
        (
            SketchType::KLL,
            SketchCosts {
                bytes_per_series_per_sec: 80.0,
                cpu_micros_per_sample: 0.5,
                base_memory_bytes: 2_048.0,
                relative_error_at_default: 0.02,
            },
        ),
        (
            SketchType::HLL,
            SketchCosts {
                bytes_per_series_per_sec: 40.0,
                cpu_micros_per_sample: 0.3,
                base_memory_bytes: 16_384.0, // precision=14 → 16 KB
                relative_error_at_default: 0.008,
            },
        ),
        (
            SketchType::CountSketch,
            SketchCosts {
                bytes_per_series_per_sec: 200.0,
                cpu_micros_per_sample: 1.2,
                base_memory_bytes: 40_960.0,
                relative_error_at_default: 0.01,
            },
        ),
        (
            SketchType::CountMinSketch,
            SketchCosts {
                bytes_per_series_per_sec: 200.0,
                cpu_micros_per_sample: 1.0,
                base_memory_bytes: 40_960.0,
                relative_error_at_default: 0.01,
            },
        ),
    ]
    .into()
}

// ── Scoring ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PlanScore {
    pub bandwidth_bytes_per_sec: f64,
    pub cpu_micros_per_sample: f64,
    pub memory_bytes: f64,
    pub estimated_error: f64,
    pub meets_sla: bool,
}

/// Estimates resource costs for a given plan + workload using the provided cost table.
pub fn score_with(
    plan: &CollectionPlan,
    w: &RegisteredWorkload,
    table: &HashMap<SketchType, SketchCosts>,
) -> PlanScore {
    let st = &plan.agent_config.sketch_type;
    let Some(&costs) = table.get(st) else {
        return PlanScore {
            bandwidth_bytes_per_sec: f64::MAX,
            cpu_micros_per_sample: f64::MAX,
            memory_bytes: f64::MAX,
            estimated_error: 1.0,
            meets_sla: false,
        };
    };

    let dim_multiplier = (plan.agent_config.aggregate_by.len() + 1) as f64;
    let bandwidth = costs.bytes_per_series_per_sec * dim_multiplier;
    let memory = costs.base_memory_bytes * dim_multiplier;
    let err = estimate_error(st, &plan.agent_config.sketch_params, costs);
    let sla = if w.error_bound() <= 0.0 {
        0.01
    } else {
        w.error_bound()
    };

    PlanScore {
        bandwidth_bytes_per_sec: bandwidth,
        cpu_micros_per_sample: costs.cpu_micros_per_sample,
        memory_bytes: memory,
        estimated_error: err,
        meets_sla: matches!(w.accuracy(), crate::types::AccuracyTarget::Epsilon(epsilon) if epsilon > 0.0)
            && err <= sla,
    }
}

/// Estimates resource costs for a given plan + workload.
pub fn score(plan: &CollectionPlan, w: &RegisteredWorkload) -> PlanScore {
    let table = benchmark_table();
    let st = &plan.agent_config.sketch_type;

    let Some(&costs) = table.get(st) else {
        return PlanScore {
            bandwidth_bytes_per_sec: f64::MAX,
            cpu_micros_per_sample: f64::MAX,
            memory_bytes: f64::MAX,
            estimated_error: 1.0,
            meets_sla: false,
        };
    };

    // More preserved dimensions → more distinct sketches in flight.
    let dim_multiplier = (plan.agent_config.aggregate_by.len() + 1) as f64;

    let bandwidth = costs.bytes_per_series_per_sec * dim_multiplier;
    let memory = costs.base_memory_bytes * dim_multiplier;
    let err = estimate_error(st, &plan.agent_config.sketch_params, costs);

    let sla = if w.error_bound() <= 0.0 {
        0.01
    } else {
        w.error_bound()
    };

    PlanScore {
        bandwidth_bytes_per_sec: bandwidth,
        cpu_micros_per_sample: costs.cpu_micros_per_sample,
        memory_bytes: memory,
        estimated_error: err,
        meets_sla: matches!(w.accuracy(), crate::types::AccuracyTarget::Epsilon(epsilon) if epsilon > 0.0)
            && err <= sla,
    }
}

fn estimate_error(_st: &SketchType, p: &SketchParams, costs: SketchCosts) -> f64 {
    match p {
        SketchParams::DDSketch {
            relative_accuracy, ..
        } if *relative_accuracy > 0.0 => *relative_accuracy,
        SketchParams::KLL { k, .. } if *k > 0 => 1.0 / *k as f64,
        SketchParams::HLL { precision } if *precision > 0 => {
            1.04 / (2.0f64.powi(*precision as i32)).sqrt()
        }
        _ => costs.relative_error_at_default,
    }
}

// ── DeploymentCostPlanner ──────────────────────────────────────────────────────────

/// Extends the rule-based planner by scoring all valid sketch candidates and
/// choosing the one with the lowest bandwidth that still meets the AccuracySLA.
///
/// When an [`OnlineMetricsStore`] is attached (via [`DeploymentCostPlanner::with_online_store`])
/// the planner blends live EMA observations into the cost table used for scoring,
/// so that real-world behaviour gradually supersedes the static benchmark defaults.
pub struct DeploymentCostPlanner {
    inner: DeploymentPlanCompiler,
    online_store: Option<online::OnlineMetricsStore>,
}

impl Default for DeploymentCostPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl DeploymentCostPlanner {
    pub fn new() -> Self {
        Self {
            inner: DeploymentPlanCompiler::new(),
            online_store: None,
        }
    }

    pub fn with_sketch_defaults(mut self, defaults: SketchDefaults) -> Self {
        self.inner.sketch_defaults = defaults;
        self
    }

    /// Attach a live EMA store so scoring uses blended benchmark + observed costs.
    pub fn with_online_store(mut self, store: online::OnlineMetricsStore) -> Self {
        self.online_store = Some(store);
        self
    }

    /// Returns the effective cost table: online-blended when available, benchmark otherwise.
    fn cost_table(&self) -> HashMap<SketchType, SketchCosts> {
        match &self.online_store {
            Some(s) => online::effective_table(s),
            None => benchmark_table_pub(),
        }
    }

    /// Produces a [`CollectionPlan`] optimised for the given query workload
    /// and data characteristics.
    ///
    /// `wc` drives the delta transmission decision: fill rate, flush rate,
    /// CPU / memory overhead, and raw vs. sketch bandwidth comparison.
    /// `None` means no usable data evidence: retain the legal rule-based plan
    /// without estimating rate-dependent costs from fabricated defaults.
    pub fn plan(
        &self,
        w: &RegisteredWorkload,
        wc: Option<&WorkloadCharacteristics>,
    ) -> CollectionPlan {
        if !matches!(w.accuracy(), crate::types::AccuracyTarget::Epsilon(epsilon) if epsilon > 0.0)
        {
            return self.inner.plan(w);
        }
        let Some(wc) = wc else {
            return self.inner.plan(w);
        };

        let table = self.cost_table();

        // If a specific sketch type is pinned, use it directly.
        if let Some(st) = &w.deployment.sketch_type_override {
            let params = default_sketch_params(st, w.error_bound());
            let (mode, window_duration) = select_window_strategy(w);
            let mut plan = self.inner.plan(w);
            plan.agent_config.sketch_type = st.clone();
            plan.agent_config.sketch_params = params;
            plan.agent_config.mode = mode;
            plan.agent_config.window_duration = window_duration;
            apply_delta_decision_with(&mut plan, w, wc, &table);
            return plan;
        }

        let candidates =
            crate::physical::sketch_catalog::candidates_for_workload(&w.aggregations());

        // Start with the rule-based plan as the baseline.
        let baseline = self.inner.plan(w);
        let mut best_plan = baseline;
        let mut best_score = score_with(&best_plan, w, &table);

        for st in candidates {
            let params = default_sketch_params(&st, w.error_bound());
            let (mode, window_duration) = select_window_strategy(w);

            let mut trial = self.inner.plan(w);
            trial.agent_config.sketch_type = st.clone();
            trial.agent_config.sketch_params = params;
            trial.agent_config.mode = mode;
            trial.agent_config.window_duration = window_duration;

            let s = score_with(&trial, w, &table);
            if !s.meets_sla {
                continue;
            }

            if s.bandwidth_bytes_per_sec < best_score.bandwidth_bytes_per_sec
                || !best_score.meets_sla
            {
                best_plan = trial;
                best_score = s;
            }
        }

        apply_delta_decision_with(&mut best_plan, w, wc, &table);
        best_plan
    }
}

/// Runs the delta cost model and writes the decision into the plan using a provided cost table.
fn apply_delta_decision_with(
    plan: &mut CollectionPlan,
    w: &RegisteredWorkload,
    wc: &WorkloadCharacteristics,
    table: &HashMap<SketchType, SketchCosts>,
) {
    let bytes_per_series_per_sec = table
        .get(&plan.agent_config.sketch_type)
        .map(|c| c.bytes_per_series_per_sec)
        .unwrap_or(200.0);

    let (decision, summary) = decide_delta(plan, w, wc, bytes_per_series_per_sec);

    // Propagate into agent config.
    match &decision {
        DeltaDecision::UseDelta { threshold, .. } => {
            plan.agent_config.delta_transmission = true;
            plan.agent_config.delta_threshold = *threshold;
            // GOS relative delta gating: the edge replaces the fixed threshold
            // with the norm-adaptive one for Count-Sketch. ε_st = the staleness
            // share of the accuracy budget (w_edge=0 → all to thresholds).
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

// candidates_for_workload delegated to algebra::directory.

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::collections::HashMap;
    use std::time::Duration;

    fn workload(aggs: Vec<AggType>) -> RegisteredWorkload {
        crate::registered_workload::fixtures::WorkloadFixture {
            metric_name: "test".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: aggs,
            time_window: Duration::from_secs(300),
            repeat_every: None,

            accuracy: crate::types::AccuracyTarget::Epsilon(0.01),
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
        .build()
    }

    fn dummy_plan(st: SketchType) -> CollectionPlan {
        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type: st.clone(),
                sketch_params: default_sketch_params(&st, 0.01),
                aggregate_by: vec![],
                label_matchers: vec![],
                window_duration: Some(Duration::from_secs(300)),
                mode: ProcessorMode::Window,
                enable_self_monitoring: true,
                transmit_sketch: true,
                drop_original: true,
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: false,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            valid_until: Utc::now(),
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    #[test]
    fn ddsketch_meets_sla_at_1pct() {
        let w = workload(vec![AggType::Quantile]);
        let s = score(&dummy_plan(SketchType::DDSketch), &w);
        assert!(
            s.meets_sla,
            "DDSketch at 1% should meet 1% SLA, error={}",
            s.estimated_error
        );
    }

    #[test]
    fn ddsketch_fails_tight_sla() {
        let w = {
            let mut w = workload(vec![AggType::Quantile]);
            w.set_accuracy(crate::types::AccuracyTarget::Epsilon(0.001));
            w
        };
        // Force 1% params despite tighter SLA.
        let mut plan = dummy_plan(SketchType::DDSketch);
        plan.agent_config.sketch_params = SketchParams::DDSketch {
            relative_accuracy: 0.01,
            quantiles: vec![0.5, 0.99],
        };
        let s = score(&plan, &w);
        assert!(!s.meets_sla, "DDSketch at 1% should NOT meet 0.1% SLA");
    }

    #[test]
    fn hll_lower_bandwidth_than_ddsketch() {
        let w = workload(vec![AggType::Quantile]);
        let s_dd = score(&dummy_plan(SketchType::DDSketch), &w);
        let s_hll = score(&dummy_plan(SketchType::HLL), &w);
        assert!(s_hll.bandwidth_bytes_per_sec < s_dd.bandwidth_bytes_per_sec);
    }

    #[test]
    fn dim_multiplier_increases_bandwidth() {
        let w_few = {
            let mut w = workload(vec![AggType::Quantile]);
            w.deployment.retained_labels = vec!["host".into()];
            w
        };
        let w_many = {
            let mut w = workload(vec![AggType::Quantile]);
            w.deployment.retained_labels = vec![
                "host".into(),
                "service".into(),
                "zone".into(),
                "region".into(),
            ];
            w
        };
        let pl = DeploymentPlanCompiler::new();
        let s_few = score(&pl.plan(&w_few), &w_few);
        let s_many = score(&pl.plan(&w_many), &w_many);
        assert!(s_many.bandwidth_bytes_per_sec > s_few.bandwidth_bytes_per_sec);
    }

    #[test]
    fn kll_error_formula() {
        let w = {
            let mut w = workload(vec![AggType::Quantile]);
            w.set_accuracy(crate::types::AccuracyTarget::Epsilon(0.02));
            w
        };
        let mut plan = dummy_plan(SketchType::KLL);
        plan.agent_config.sketch_params = SketchParams::KLL {
            k: 100, // error ≈ 1/100 = 1%
            quantiles: vec![0.5, 0.99],
        };
        let s = score(&plan, &w);
        assert!(s.meets_sla, "KLL k=100 (error~1%) should meet 2% SLA");
    }

    #[test]
    fn cost_model_planner_meets_sla_for_all_agg_types() {
        let pl = DeploymentCostPlanner::new();
        for (agg, sla) in [
            (AggType::Quantile, 0.01),
            (AggType::Cardinality, 0.01),
            (AggType::Frequency, 0.02),
        ] {
            let w = {
                let mut w = workload(vec![agg]);
                w.set_accuracy(crate::types::AccuracyTarget::Epsilon(sla));
                w
            };
            let plan = pl.plan(&w, None);
            let s = score(&plan, &w);
            assert!(
                s.meets_sla,
                "agg={} sla={sla}: plan does not meet SLA (error={})",
                w.aggregations()[0],
                s.estimated_error
            );
        }
    }

    #[test]
    fn cost_model_prefers_lower_bandwidth_for_cardinality() {
        let w = {
            let mut w = workload(vec![AggType::Cardinality]);
            w.set_accuracy(crate::types::AccuracyTarget::Epsilon(0.02));
            w
        };
        let plan = DeploymentCostPlanner::new().plan(&w, None);
        assert_eq!(
            plan.agent_config.sketch_type,
            SketchType::HLL,
            "HLL should win for cardinality (lowest bandwidth)"
        );
    }

    #[test]
    fn cost_model_valid_until_in_future() {
        let plan = DeploymentCostPlanner::new().plan(&workload(vec![AggType::Quantile]), None);
        assert!(plan.valid_until > Utc::now());
    }
}
