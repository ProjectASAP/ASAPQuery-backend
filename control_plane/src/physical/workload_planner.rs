//! Compatibility compiler from legacy flat workloads to physical deployment
//! plans. Summary selection delegates to ASAPPlanner.

use chrono::Utc;
use std::time::Duration;

use crate::types::*;

pub const DEFAULT_VALID_FOR: Duration = Duration::from_secs(10 * 60);

/// MVP fixture classification used only to translate the demo workload into
/// Planner intents. Sketch legality and candidate enumeration remain owned by
/// ASAPPlanner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeploymentIntent {
    Quantile,
    Cardinality,
    TopK,
    Frequency,
    RawPassthrough,
}

fn mvp_deployment_policy(
    metric_name: &str,
) -> Option<(DeploymentIntent, planner_types::post_asap::SketchAlgorithm)> {
    use planner_types::post_asap::SketchAlgorithm;

    Some(match metric_name {
        "http_requests_total" => (DeploymentIntent::RawPassthrough, SketchAlgorithm::DDSketch),
        "http_latency_ms" => (DeploymentIntent::Quantile, SketchAlgorithm::DDSketch),
        "request_size_bytes" => (DeploymentIntent::Quantile, SketchAlgorithm::Kll),
        "unique_users_per_min" => (DeploymentIntent::Cardinality, SketchAlgorithm::Hll),
        "top_endpoint_qps" => (DeploymentIntent::TopK, SketchAlgorithm::CountSketch),
        "endpoint_request_freq" => (DeploymentIntent::Frequency, SketchAlgorithm::Cms),
        _ => return None,
    })
}

/// Bind a flat workload to a typed physical expression for stage emission.
///
/// An explicit sketch override takes precedence when valid for the statistic.
/// Otherwise deployment contract rows select the family, with aggregation-type
/// defaults for other metrics. Unsupported and raw-passthrough workloads
/// return `None`.
pub fn bind_workload_typed(w: &QueryWorkload) -> Option<crate::physical::post_asap::PhysicalExpr> {
    bind_workload_typed_with_evidence(w, None, None)
}

/// Bind a legacy workload with an explicit Top-K membership certificate.
/// Approximate Top-K fails closed through [`bind_workload_typed`] when this
/// evidence is absent; callers that have validated a fresh certificate use
/// this entry point instead.
pub fn bind_workload_typed_with_topk_evidence(
    w: &QueryWorkload,
    evidence: &crate::physical::compiler::TopKMembershipEvidence,
) -> Option<crate::physical::post_asap::PhysicalExpr> {
    bind_workload_typed_with_evidence(w, None, Some(evidence))
}

/// Like [`bind_workload_typed`], but for a `Frequency` statistic, `item_filter`
/// (a `(label, value)` pair, e.g. `("item", "checkout")`) threads the
/// query's actual per-item filter value through to the bound
/// `SketchQuery::PointCount` -- `None` (what `bind_workload_typed` itself
/// passes) gives the bare bucket total, same as before this parameter
/// existed. `QueryWorkload` itself carries no `item_label` field (adding
/// one would break its 30+ struct-literal construction sites across the
/// crate), so callers that know a metric's item_label -- e.g.
/// `emit::collect_metric_to_family`'s loop, which already has `entry:
/// &WorkloadEntry` and `workload.label_filters` in scope -- pass it in
/// directly instead.
pub fn bind_workload_typed_with_item_filter(
    w: &QueryWorkload,
    item_filter: Option<(&str, &str)>,
) -> Option<crate::physical::post_asap::PhysicalExpr> {
    bind_workload_typed_with_evidence(w, item_filter, None)
}

fn bind_workload_typed_with_evidence(
    w: &QueryWorkload,
    item_filter: Option<(&str, &str)>,
    topk_evidence: Option<&crate::physical::compiler::TopKMembershipEvidence>,
) -> Option<crate::physical::post_asap::PhysicalExpr> {
    use crate::physical::post_asap::cost_model::ForcedFamilyCostModel;
    use planner_types::post_asap::SketchAlgorithm;
    use planner_types::pre_asap::{AggIntent as L3AggIntent, QueryExpr, Schema, Source};
    use planner_types::pre_asap::{Column, DataType};

    // Contract-row metrics (`classify_demo_metric` returns `Some`) and
    // operator-supplied overrides both signal "this metric must be
    // sketched". The parser's `exact_required` flag — set when a query
    // bottoms out at a bare VectorSelector → `AggFunc::Sum`, or carries
    // a `Sum`/`Rate`/`Increase`/`Delta` (e.g. `rate(metric[5m])`,
    // `count(metric)` whose inner walk synthesizes a `Sum` over the
    // VectorSelector) — must not short-circuit those signals. Without
    // this carve-out, MVP §46 entries 5–8 (`unique_users_per_min` /
    // `top_endpoint_qps` / `endpoint_request_freq`) parse to
    // `exact_required: true` and the typed binder declines, so the
    // 5-sketch routing emitter never sees them.
    let metric_is_contract_row = mvp_deployment_policy(&w.metric_name).is_some();
    let operator_pinned_sketch = w.sketch_type_override.is_some();
    if w.exact_required && !metric_is_contract_row && !operator_pinned_sketch {
        return None;
    }
    if w.aggregations.len() != 1 {
        return None;
    }

    // ── Pick the (statistic class, accuracy preference) ──────────────
    //
    // Priority: workload-spec metric-name match → AggType-driven
    // default. The metric-name match owns the demo contract rows; the
    // AggType fallback covers everything else.
    let (statistic, default_kind) =
        mvp_deployment_policy(&w.metric_name).unwrap_or_else(|| match w.aggregations[0] {
            AggType::Quantile => (DeploymentIntent::Quantile, SketchAlgorithm::DDSketch),
            AggType::Cardinality => (DeploymentIntent::Cardinality, SketchAlgorithm::Hll),
            AggType::Frequency => (DeploymentIntent::Frequency, SketchAlgorithm::Cms),
        });

    // The query/operator owns the statistic. An override may select a
    // compatible implementation (for example KLL instead of DDSketch for a
    // quantile), but must never rewrite query semantics merely to make an
    // incompatible family fit. Contract-row classification above handles the
    // MVP's count/topk/frequency metrics before this compatibility check.

    // SumRateCount → no sketch (raw passthrough). Decline the typed
    // binding so the caller falls back to the legacy raw plan.
    if statistic == DeploymentIntent::RawPassthrough {
        return None;
    }

    // ── Resolve the SketchAlgorithm (override > capability-matched default) ─
    //
    // The workload-spec's `sketch_type_override` (= the spec's
    // `sketch_family_override` per orchestrator contract) wins over the
    // capability-matched pick, *provided* the override is valid for the
    // statistic class. An invalid override (e.g. HLL for a Quantile
    // workload) is silently dropped — the catalog-default family runs
    // instead so the binding never produces a nonsense (sketch, stat)
    // pair.
    let override_kind: Option<SketchAlgorithm> = w
        .sketch_type_override
        .as_ref()
        .map(|st| SketchAlgorithm::from(st.clone()));
    // ASAPPlanner is the authority on whether this forced family is legal for
    // the intent. An invalid override produces no candidate below.
    let kind = override_kind.unwrap_or(default_kind);

    let accuracy = w.accuracy.clone();
    let intent_accuracy = accuracy.clone();

    // Build the matching L3 `AggIntent` for the picked statistic class.
    // TopK lacks an `AggType` enum entry today (the MVP-contract
    // top_endpoint_qps metric is name-classified, not AggType-derived),
    // so we synthesize a default k=10 — the same value the legacy
    // PromQL `topk(10, …)` lowering uses.
    let intent = match statistic {
        DeploymentIntent::Quantile => L3AggIntent::Quantile {
            col: None,
            q: w.quantiles.first().copied().unwrap_or(0.99),
            accuracy: intent_accuracy,
        },
        DeploymentIntent::Cardinality => L3AggIntent::Cardinality {
            col: None,
            accuracy: intent_accuracy,
        },
        DeploymentIntent::Frequency => crate::planner_selection::frequency(
            intent_accuracy,
            item_filter.map(|(label, value)| (label.to_string(), value.to_string())),
        ),
        DeploymentIntent::TopK => L3AggIntent::TopK {
            k: 10,
            accuracy: intent_accuracy,
        },
        // SumRateCount handled above (early return).
        DeploymentIntent::RawPassthrough => unreachable!(),
    };

    let scan = QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: w.metric_name.clone(),
        },
        // This synthetic scan only exists to drive `Bind*` rule dispatch
        // against a representative `Aggregate` shape — the rules key off
        // the `AggIntent`/accuracy/window, not the scan's predicates, and
        // `w.label_filters`' labels aren't columns in the synthetic
        // `(ts, value)` schema below anyway (`label_filter_to_predicate`
        // would resolve every one of them to `None`).
        predicates: Vec::new(),
        schema: Schema::with_time_index(
            vec![
                Column {
                    name: "ts".into(),
                    dtype: DataType::Timestamp,
                    nullable: false,
                    table: None,
                },
                Column {
                    name: "value".into(),
                    dtype: DataType::Float64,
                    nullable: false,
                    table: None,
                },
                Column {
                    name: "endpoint".into(),
                    dtype: DataType::Utf8,
                    nullable: false,
                    table: None,
                },
            ],
            0,
            vec![vec![0]],
        ),
    };
    let windowed = QueryExpr::TimeRange {
        range: w.time_window,
        child: Box::new(scan).into(),
    };
    // Planner's weighted Top-K contract deliberately accepts only an
    // additive ranking input.  The legacy workload vocabulary has no TopK
    // aggregation variant: the `top_endpoint_qps` contract row arrives as
    // `Frequency`, meaning that each observed series occurrence contributes
    // one to its rank.  Make that previously implicit update semantics
    // explicit as an inner Count aggregate.  Besides satisfying the typed
    // contract, this supplies the PromQL label-set entity identity used as
    // the heap item.
    let aggregate_child = if statistic == DeploymentIntent::TopK {
        QueryExpr::Aggregate {
            reduction: planner_types::pre_asap::Reduction::by(vec![2]),
            measures: vec![L3AggIntent::Count {
                accuracy: accuracy.clone(),
            }],
            output_names: Vec::new(),
            having: None,
            child: Box::new(windowed).into(),
        }
    } else {
        windowed
    };
    let aggregate = QueryExpr::Aggregate {
        // Top-K is a genuine full reduction over the per-item counts above;
        // other synthetic probes retain the representative per-entity shape.
        reduction: if statistic == DeploymentIntent::TopK {
            planner_types::pre_asap::Reduction::by(vec![])
        } else {
            planner_types::pre_asap::Reduction::PerEntity
        },
        measures: vec![intent],
        output_names: Vec::new(),
        having: None,
        child: Box::new(aggregate_child).into(),
    };

    // ── Drive the picked family directly, bypassing selection ─────────
    //
    // We have a definitive family pick from the capability matrix (or the
    // `sketch_type_override`), so force it via `ForcedFamilyCostModel`
    // instead of letting `ControlPlaneCostModel::rank_candidates` choose.
    // This keeps the contract-row mapping deterministic — the normal
    // dispatcher's tie-break (DDSketch p=6 vs KLL p=5) cannot accidentally
    // flip `request_size_bytes`'s KLL pick to DDSketch.
    //
    // CMS+TopK note: when the picker selected `SketchAlgorithm::Cms` for a
    // TopK statistic (only reachable today via a `sketch_family_override:
    // CountMinSketch` on a TopK metric), this forces the CMS-with-heap
    // variant. The CMS-Heap pattern (Cormode & Muthukrishnan 2005) gives
    // a valid heavy-hitter sketch; the unbiased CountSketch remains the
    // canonical pick when no override is supplied. The backend's "top-K
    // from CountMin state" readout path is a separate workstream — see
    // Planner validates the forced family before this physical adapter commits it.
    //
    // `StatisticClass::Frequency` (the `endpoint_request_freq` contract
    // row) is `AggIntent::Extension`-shaped. This used to always decline
    // (return `None`) here, because `asap_aware_mapping::boundary::implementation_for`
    // mapped every `Extension` to `PassThrough` unconditionally (a core-
    // vs-deployment-specific-shape gap, ASAPController#150). Now that
    // `ForcedFamilyCostModel::realize_extension`/`readout_extension`
    // delegate to `ControlPlaneCostModel`'s own `"frequency"` handling,
    // this contract row commits like any other.
    let forced = match kind {
        SketchAlgorithm::CountSketch => SketchAlgorithm::CountSketchWithHeap,
        SketchAlgorithm::Cms if statistic == DeploymentIntent::TopK => SketchAlgorithm::CmsWithHeap,
        other => other,
    };
    let cost_model = ForcedFamilyCostModel::new(accuracy.clone(), forced);
    struct Evidence<'a>(Option<&'a crate::physical::compiler::TopKMembershipEvidence>);
    impl asap_aware_mapping::AccuracyEvidenceProvider for Evidence<'_> {
        fn propagation_stats(
            &self,
            op: &planner_types::post_asap::CompositionOperator,
            _family: &planner_types::post_asap::SummaryFamilyType,
            _query: Option<&planner_types::post_asap::SketchQuery>,
        ) -> asap_aware_mapping::PropagationStats {
            match (op, self.0) {
                (planner_types::post_asap::CompositionOperator::TopKSelection, Some(e)) => {
                    asap_aware_mapping::PropagationStats {
                        topk_selected_lower_bound: Some(e.selected_lower_bound),
                        topk_excluded_upper_bound: Some(e.excluded_upper_bound),
                        topk_interval_failure_probability: Some(e.interval_failure_probability),
                        ..Default::default()
                    }
                }
                _ => Default::default(),
            }
        }
    }
    let node = crate::planner_selection::select_summary_with_evidence(
        &aggregate,
        &cost_model,
        &asap_aware_mapping::DefaultAccuracyModel,
        &asap_aware_mapping::EqualSplitAllocator,
        &Evidence(topk_evidence),
    )
    .ok()?;
    // `implement_tree_with` never *errors* on "nothing bound" — an
    // intent `boundary::implementation_for`/`CostModel::realize_extension`
    // can't realize (e.g. `TopK { accuracy: Exact }`, ASAPController#151,
    // still open) still returns `Ok(Rc<SummaryNode>)`, just wrapping the input
    // as `SummaryExpr::Logical` unchanged. `bind_workload_typed`'s own
    // contract is `None` for "typed path doesn't support this shape yet"
    // — translate the two by checking whether anything actually got
    // committed.
    if matches!(
        node.expr,
        planner_types::post_asap::SummaryExpr::KeepPreAsap(_)
    ) {
        return None;
    }
    Some(crate::physical::post_asap::deployment_expr::PhysicalExpr::committed(node))
}

pub struct DeploymentPlanCompiler {
    pub valid_for: Duration,
    pub sketch_defaults: SketchDefaults,
}

impl Default for DeploymentPlanCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl DeploymentPlanCompiler {
    pub fn new() -> Self {
        Self {
            valid_for: DEFAULT_VALID_FOR,
            sketch_defaults: SketchDefaults::default(),
        }
    }

    pub fn with_defaults(defaults: SketchDefaults) -> Self {
        Self {
            valid_for: DEFAULT_VALID_FOR,
            sketch_defaults: defaults,
        }
    }

    pub fn plan(&self, w: &QueryWorkload) -> CollectionPlan {
        // This legacy scalar cost path cannot certify a failure probability.
        // Exact/zero-error and EpsilonDelta use raw; the typed binder independently
        // checks the full requirement against Planner's family guarantees.
        if w.exact_required
            || !matches!(w.accuracy, crate::types_v2::AccuracyTarget::Epsilon(epsilon) if epsilon > 0.0)
        {
            return self.raw_passthrough_plan(w);
        }

        let sketch_type = crate::physical::sketch_catalog::sketch_type_for_agg(&w.aggregations);
        let sketch_params = crate::physical::sketch_catalog::build_sketch_params(
            &self.sketch_defaults,
            &sketch_type,
            w.error_bound(),
            &w.quantiles,
        );
        let (mode, window_duration) = select_window_strategy(w);

        let mut aggregate_by = w.group_by_labels.clone();
        aggregate_by.sort();

        let mut label_matchers: Vec<String> = w
            .label_filters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        label_matchers.sort();

        let valid_until = Utc::now() + chrono::Duration::seconds(self.valid_for.as_secs() as i64);

        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Sketch,
                sketch_type,
                sketch_params,
                aggregate_by,
                label_matchers,
                window_duration,
                mode,
                enable_self_monitoring: true,
                transmit_sketch: false,
                drop_original: true,
                // Delta fields are left as disabled defaults here; the
                // DeploymentCostPlanner overwrites them via decide_delta().
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: true,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            valid_until,
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }

    /// Returns a raw-passthrough plan for queries that require exact per-sample
    /// computation (RSI, MACD, stochastic oscillator, etc.).
    fn raw_passthrough_plan(&self, w: &QueryWorkload) -> CollectionPlan {
        let valid_until = Utc::now() + chrono::Duration::seconds(self.valid_for.as_secs() as i64);

        let mut label_matchers: Vec<String> = w
            .label_filters
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        label_matchers.sort();

        CollectionPlan {
            agent_config: AgentCollectorConfig {
                output_mode: OutputMode::Raw,
                sketch_type: SketchType::DDSketch, // unused for raw mode
                sketch_params: SketchParams::default(),
                aggregate_by: vec![],
                label_matchers,
                window_duration: None,
                mode: ProcessorMode::Batch,
                enable_self_monitoring: true,
                transmit_sketch: false,
                drop_original: false,
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: true,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            valid_until,
            delta_decision: DeltaDecision::default(),
            transmission_cost_summary: TransmissionCostSummary::default(),
        }
    }
}

// ── Sketch selection (delegated to algebra::directory) ───────────────────────

pub use crate::physical::sketch_catalog::{build_sketch_params, default_sketch_params};

// ── Window strategy ───────────────────────────────────────────────────────────

/// Decides processor mode.
///
/// Rule: if `latency_sla >= time_window` (or unset) → window mode.
///       otherwise → batch mode (gateway/backend merges on query).
pub fn select_window_strategy(w: &QueryWorkload) -> (ProcessorMode, Option<Duration>) {
    match w.latency_sla {
        None => (ProcessorMode::Window, Some(w.time_window)),
        Some(ls) if ls >= w.time_window => (ProcessorMode::Window, Some(w.time_window)),
        _ => (ProcessorMode::Batch, None),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn workload(aggs: Vec<AggType>) -> QueryWorkload {
        QueryWorkload {
            metric_name: "test".into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: aggs,
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    #[test]
    fn quantile_selects_ddsketch() {
        let plan = DeploymentPlanCompiler::new().plan(&workload(vec![AggType::Quantile]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::DDSketch);
    }

    #[test]
    fn cardinality_selects_hll() {
        let plan = DeploymentPlanCompiler::new().plan(&workload(vec![AggType::Cardinality]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::HLL);
    }

    #[test]
    fn frequency_selects_countsketch() {
        let plan = DeploymentPlanCompiler::new().plan(&workload(vec![AggType::Frequency]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::CountSketch);
    }

    /// An override chooses an implementation; it cannot change a query's
    /// statistic just to make an incompatible family appear valid.
    #[test]
    fn incompatible_override_does_not_rewrite_query_semantics() {
        use crate::emit::extract_root_sketch_algorithm;
        use planner_types::post_asap::SketchAlgorithm;
        for (ov, expect) in [
            (SketchType::DDSketch, SketchAlgorithm::DDSketch),
            (SketchType::KLL, SketchAlgorithm::Kll),
            (SketchType::HLL, SketchAlgorithm::DDSketch),
            (SketchType::CountSketch, SketchAlgorithm::DDSketch),
            (SketchType::CountMinSketch, SketchAlgorithm::DDSketch),
        ] {
            let mut w = workload(vec![AggType::Quantile]);
            w.sketch_type_override = Some(ov.clone());
            let pe = bind_workload_typed(&w)
                .unwrap_or_else(|| panic!("bind declined for override {ov:?}"));
            assert_eq!(
                extract_root_sketch_algorithm(&pe),
                Some(expect.clone()),
                "override {ov:?} must not change Quantile semantics",
            );
        }
    }

    #[test]
    fn quantile_priority_wins() {
        let plan = DeploymentPlanCompiler::new()
            .plan(&workload(vec![AggType::Quantile, AggType::Cardinality]));
        assert_eq!(
            plan.agent_config.sketch_type,
            SketchType::DDSketch,
            "quantile should take priority over cardinality"
        );
    }

    #[test]
    fn window_mode_when_latency_geq_time_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = Some(Duration::from_secs(600)); // 10m >= 5m
        let plan = DeploymentPlanCompiler::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Window);
        assert_eq!(
            plan.agent_config.window_duration,
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn batch_mode_when_latency_lt_time_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = Some(Duration::from_secs(60)); // 1m < 5m
        let plan = DeploymentPlanCompiler::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Batch);
        assert_eq!(plan.agent_config.window_duration, None);
    }

    #[test]
    fn no_latency_sla_defaults_to_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = None;
        let plan = DeploymentPlanCompiler::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Window);
    }

    #[test]
    fn aggregate_by_sorted() {
        let mut w = workload(vec![AggType::Quantile]);
        w.group_by_labels = vec!["zone".into(), "host.name".into(), "service".into()];
        let plan = DeploymentPlanCompiler::new().plan(&w);
        assert_eq!(
            plan.agent_config.aggregate_by,
            vec!["host.name", "service", "zone"]
        );
    }

    #[test]
    fn label_matchers_from_filters() {
        let mut w = workload(vec![AggType::Quantile]);
        w.label_filters = [
            ("env".into(), "prod".into()),
            ("service".into(), "web".into()),
        ]
        .into();
        let plan = DeploymentPlanCompiler::new().plan(&w);
        assert_eq!(plan.agent_config.label_matchers.len(), 2);
    }

    #[test]
    fn ddsketch_accuracy_params() {
        let mut w = workload(vec![AggType::Quantile]);
        w.accuracy = crate::types_v2::AccuracyTarget::Epsilon(0.005);
        let plan = DeploymentPlanCompiler::new().plan(&w);
        match &plan.agent_config.sketch_params {
            SketchParams::DDSketch {
                relative_accuracy, ..
            } => assert_eq!(*relative_accuracy, 0.005),
            other => panic!("expected DDSketch, got {:?}", other),
        }
    }

    #[test]
    fn hll_precision_coarse_sla() {
        let mut w = workload(vec![AggType::Cardinality]);
        w.accuracy = crate::types_v2::AccuracyTarget::Epsilon(0.03);
        let plan = DeploymentPlanCompiler::new().plan(&w);
        match &plan.agent_config.sketch_params {
            SketchParams::HLL { precision } => {
                assert_eq!(*precision, 10, "coarse SLA should use lower precision")
            }
            other => panic!("expected HLL, got {:?}", other),
        }
    }

    #[test]
    fn valid_until_in_future() {
        let plan = DeploymentPlanCompiler::new().plan(&workload(vec![AggType::Quantile]));
        assert!(
            plan.valid_until > Utc::now(),
            "valid_until should be in the future"
        );
    }

    #[test]
    fn gateway_passthrough() {
        let plan = DeploymentPlanCompiler::new().plan(&workload(vec![AggType::Quantile]));
        assert!(plan.gateway_config.passthrough);
    }

    // ── Family-per-metric tests (issue #46 MVP demo contract) ─────────────────
    //
    // The shared MVP demo contract pins six metric→family rows. These tests
    // drive each row through `bind_workload_typed` and assert the bound
    // `PhysicalExpr` carries the expected sketch family. The contract:
    //
    // | metric                  | family       |
    // |-------------------------|--------------|
    // | `http_requests_total`   | raw (None)   |
    // | `http_latency_ms`       | DDSketch     |
    // | `request_size_bytes`    | KLL          |
    // | `unique_users_per_min`  | HLL          |
    // | `top_endpoint_qps`      | CountSketch  |
    // | `endpoint_request_freq` | CMS          |

    use crate::physical::post_asap::deployment_expr::PhysicalExpr;
    use planner_types::post_asap::SketchAlgorithm;
    use planner_types::pre_asap::expr_ir::ColumnRef;

    /// Walk the L4 binding output and pull out the approximate sketch
    /// family. Returns `None` if no sketch node is present (raw / pure
    /// logical pass-through, or an exact accumulator — see
    /// `emit::extract_root_sketch_algorithm`, whose logic this mirrors).
    fn extract_family(expr: &PhysicalExpr) -> Option<SketchAlgorithm> {
        crate::emit::extract_root_sketch_algorithm(expr)
    }

    /// Pull the `SketchQuery` out of a bound `PhysicalExpr`'s top-level
    /// `SummaryEstimate` -- unlike `extract_family`, this needs the
    /// readout itself (to check `PointCount`'s `key`/`value`), not just
    /// the sketch family underneath it.
    fn extract_query(expr: &PhysicalExpr) -> Option<planner_types::post_asap::SketchQuery> {
        let PhysicalExpr::Committed(
            crate::physical::post_asap::deployment_expr::PostAsapPlan::Summary(node),
        ) = expr
        else {
            return None;
        };
        match &node.expr {
            planner_types::post_asap::SummaryExpr::SummaryEstimate { query, .. } => {
                Some(query.clone())
            }
            _ => None,
        }
    }

    /// Build a workload with the given metric name + reasonable
    /// AggType-driven default for the contract row. The metric-name match
    /// in `classify_demo_metric` overrides the AggType for the
    /// contract rows; the AggType still has to be a valid one (the enum
    /// has no `TopK` variant, so for `top_endpoint_qps` we pass
    /// `Frequency` and rely on the metric-name reclassification).
    fn workload_for(metric: &str, agg: AggType) -> QueryWorkload {
        QueryWorkload {
            metric_name: metric.into(),
            label_filters: HashMap::new(),
            group_by_labels: vec![],
            aggregations: vec![agg],
            time_window: Duration::from_secs(300),
            repeat_every: None,
            accuracy_sla: 0.01,
            accuracy: crate::types_v2::AccuracyTarget::Epsilon(0.01),
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    fn topk_evidence() -> crate::physical::compiler::TopKMembershipEvidence {
        crate::physical::compiler::TopKMembershipEvidence {
            selected_lower_bound: 101.0,
            excluded_upper_bound: 100.0,
            interval_failure_probability: 0.001,
            observed_at_unix_ms: 1,
            source: "workload-planner-test".into(),
        }
    }

    #[test]
    fn typed_binding_http_requests_total_is_raw_passthrough() {
        // Contract: `http_requests_total` → raw passthrough (no sketch).
        // The typed path declines (`bind_workload_typed` returns `None`)
        // so the caller falls back to the legacy raw plan.
        let w = workload_for("http_requests_total", AggType::Frequency);
        let bound = bind_workload_typed(&w);
        assert!(
            bound.is_none(),
            "http_requests_total should bind to None (raw passthrough); got {bound:?}",
        );
    }

    #[test]
    fn typed_binding_http_latency_ms_picks_ddsketch() {
        // Contract: `http_latency_ms` → DDSketch (Quantile, rel-err).
        let w = workload_for("http_latency_ms", AggType::Quantile);
        let bound = bind_workload_typed(&w).expect("http_latency_ms must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::DDSketch),
            "http_latency_ms should bind to DDSketch (Quantile, rel-err)",
        );
    }

    #[test]
    fn typed_binding_request_size_bytes_picks_kll() {
        // Contract: `request_size_bytes` → KLL (Quantile, rank-err).
        // Note: this is the rank-err preference flip — without the
        // metric-name reclassification, the priority-based dispatcher
        // would pick DDSketch (priority 6 > KLL priority 5).
        let w = workload_for("request_size_bytes", AggType::Quantile);
        let bound = bind_workload_typed(&w).expect("request_size_bytes must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::Kll),
            "request_size_bytes should bind to KLL (Quantile, rank-err)",
        );
    }

    #[test]
    fn typed_binding_unique_users_per_min_picks_hll() {
        // Contract: `unique_users_per_min` → HLL (Cardinality).
        let w = workload_for("unique_users_per_min", AggType::Cardinality);
        let bound = bind_workload_typed(&w).expect("unique_users_per_min must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::Hll),
            "unique_users_per_min should bind to HLL (Cardinality)",
        );
    }

    #[test]
    fn typed_binding_top_endpoint_qps_picks_count_sketch_with_heap() {
        // Contract: `top_endpoint_qps` → CountSketch (TopK).
        // The metric-name reclassification reroutes from the AggType
        // default (Frequency → CMS) to the contract row (TopK →
        // CountSketch).
        let w = workload_for("top_endpoint_qps", AggType::Frequency);
        assert!(bind_workload_typed(&w).is_none());
        let bound = bind_workload_typed_with_topk_evidence(&w, &topk_evidence())
            .expect("evidenced top_endpoint_qps must bind");
        // Count-ranked producers must not revert to value-weighted runtime defaults.
        let configs = crate::physical::stage_split::split_typed_three_stage(&bound).unwrap();
        let backend = configs
            .get(&crate::physical::colored_dag::StageId::Backend)
            .unwrap();
        let crate::physical::colored_dag::StageConfig::Backend(backend) = backend else {
            panic!("expected backend config");
        };
        assert_eq!(backend.aggregations[0].heap_update_mode, Some("count"));
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::CountSketchWithHeap),
        );
        assert!(matches!(
            extract_query(&bound),
            Some(planner_types::post_asap::SketchQuery::TopK { k: 10, .. })
        ));
    }

    #[test]
    fn typed_binding_endpoint_request_freq_binds_cms() {
        // Contract: `endpoint_request_freq` → CMS (Frequency). `Frequency`
        // is `AggIntent::Extension`-shaped; this used to decline the typed
        // path entirely (`asap_aware_mapping::boundary::implementation_for` mapped
        // every `Extension` to `PassThrough` unconditionally — core has no
        // realization opinion for a deployment-specific shape it doesn't
        // know, ASAPController#150). Now that
        // `ControlPlaneCostModel::realize_extension`/`readout_extension`
        // handle `"frequency"`, this contract row binds like any other.
        let w = workload_for("endpoint_request_freq", AggType::Frequency);
        let bound = bind_workload_typed(&w).expect("endpoint_request_freq must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::Cms),
            "endpoint_request_freq should bind to Cms (Frequency)",
        );
    }

    #[test]
    fn bind_workload_typed_with_item_filter_threads_the_actual_value() {
        // `bind_workload_typed` itself (no item filter) must still read
        // out as the bare bucket total -- unchanged behavior.
        let w = workload_for("endpoint_request_freq", AggType::Frequency);
        let bound = bind_workload_typed(&w).expect("must bind");
        assert!(
            matches!(
                extract_query(&bound),
                Some(planner_types::post_asap::SketchQuery::PointCount {
                    key: ColumnRef::SampleValue,
                    value: None
                })
            ),
            "no item filter given -> bare bucket total, got {:?}",
            extract_query(&bound)
        );

        // With an item filter, the SAME workload must read out as a
        // per-item point lookup carrying the actual value.
        let bound_filtered =
            bind_workload_typed_with_item_filter(&w, Some(("endpoint", "checkout")))
                .expect("must bind");
        match extract_query(&bound_filtered) {
            Some(planner_types::post_asap::SketchQuery::PointCount {
                key: ColumnRef::Named(label),
                value: Some(value),
            }) => {
                assert_eq!(label, "endpoint");
                assert_eq!(value, "checkout");
            }
            other => panic!("expected PointCount{{key: Named(\"endpoint\"), value: Some(\"checkout\")}}, got {other:?}"),
        }
    }

    // ── sketch_type_override (= sketch_family_override) wins ──────────────────

    #[test]
    fn sketch_type_override_pins_kll_for_quantile_metric() {
        // `http_latency_ms`'s contract row is DDSketch, but a workload
        // override of `KLL` must win — both KLL and DDSketch are valid
        // for Quantile per the capability matrix, so the override is
        // honoured.
        let mut w = workload_for("http_latency_ms", AggType::Quantile);
        w.sketch_type_override = Some(SketchType::KLL);
        let bound = bind_workload_typed(&w).expect("override should still bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::Kll),
            "sketch_type_override=KLL should pin KLL despite the contract's DDSketch default",
        );
    }

    #[test]
    fn sketch_type_override_pins_ddsketch_for_quantile_metric() {
        // `request_size_bytes`'s contract row is KLL (rank-err); a
        // workload override of `DDSketch` flips it back to DDSketch.
        let mut w = workload_for("request_size_bytes", AggType::Quantile);
        w.sketch_type_override = Some(SketchType::DDSketch);
        let bound = bind_workload_typed(&w).expect("override should still bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::DDSketch),
            "sketch_type_override=DDSketch should pin DDSketch despite the contract's KLL default",
        );
    }

    #[test]
    fn planner_honors_cms_topk_override() {
        // CMS-Heap pattern (Cormode & Muthukrishnan 2005): when a
        // workload's `sketch_family_override` (=
        // `sketch_type_override`) selects CountMinSketch for a TopK
        // metric, the planner should accept it instead of falling
        // back to the canonical CountSketch default.
        let mut w = workload_for("top_endpoint_qps", AggType::Frequency);
        w.sketch_type_override = Some(SketchType::CountMinSketch);
        let bound = bind_workload_typed_with_topk_evidence(&w, &topk_evidence())
            .expect("CMS Top-K override must bind with evidence");
        assert_eq!(extract_family(&bound), Some(SketchAlgorithm::CmsWithHeap));
    }

    #[test]
    fn planner_default_topk_uses_count_sketch_with_heap() {
        // Without any override, the canonical pick for a TopK metric
        // stays CountSketch(-with-heap) — CMS-Heap is opt-in via
        // override only.
        let w = workload_for("top_endpoint_qps", AggType::Frequency);
        let bound = bind_workload_typed_with_topk_evidence(&w, &topk_evidence())
            .expect("default Top-K must bind with evidence");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::CountSketchWithHeap),
        );
    }

    #[test]
    fn invalid_sketch_type_override_falls_back_to_default() {
        // HLL is NOT valid for a Quantile statistic — the capability
        // matrix rejects the override, and the planner falls back to
        // the contract-row default (DDSketch for `http_latency_ms`).
        let mut w = workload_for("http_latency_ms", AggType::Quantile);
        w.sketch_type_override = Some(SketchType::HLL);
        let bound = bind_workload_typed(&w).expect("fallback should bind");
        assert_eq!(
            extract_family(&bound),
            Some(SketchAlgorithm::DDSketch),
            "invalid (HLL, Quantile) override should be rejected; planner falls back to DDSketch",
        );
    }

    // ── Combined sweep: all 6 contract rows in one shot ───────────────────────

    #[test]
    fn all_six_contract_metrics_produce_expected_family() {
        // Single test that drives the full contract row set through
        // `bind_workload_typed` — this is the per-task acceptance test
        // ("verify each produces the expected `PhysicalExpr` family").
        let cases: Vec<(&str, AggType, Option<SketchAlgorithm>)> = vec![
            ("http_requests_total", AggType::Frequency, None),
            (
                "http_latency_ms",
                AggType::Quantile,
                Some(SketchAlgorithm::DDSketch),
            ),
            (
                "request_size_bytes",
                AggType::Quantile,
                Some(SketchAlgorithm::Kll),
            ),
            (
                "unique_users_per_min",
                AggType::Cardinality,
                Some(SketchAlgorithm::Hll),
            ),
            ("top_endpoint_qps", AggType::Frequency, None),
            // `Extension`/Frequency now binds via `ControlPlaneCostModel`'s
            // `realize_extension` (ASAPController#150) — see
            // `typed_binding_endpoint_request_freq_binds_cms`.
            (
                "endpoint_request_freq",
                AggType::Frequency,
                Some(SketchAlgorithm::Cms),
            ),
        ];
        for (metric, agg, expected) in cases {
            let w = workload_for(metric, agg);
            let bound = bind_workload_typed(&w);
            let got = bound.as_ref().and_then(extract_family);
            assert_eq!(
                got, expected,
                "metric {metric}: expected family {expected:?}, got {got:?}",
            );
        }
    }
}
