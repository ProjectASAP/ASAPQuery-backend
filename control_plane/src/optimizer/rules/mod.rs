use chrono::Utc;
use std::time::Duration;

use crate::types::*;

pub const DEFAULT_VALID_FOR: Duration = Duration::from_secs(10 * 60);

/// Env-var that opts the planner into the typed L4 binding path
/// (`sketch_algebra::bind_query_expr`). Additive — when unset, the
/// existing untyped `algebra::directory::sketch_type_for_agg` path runs
/// unchanged. Phase E (stage_split refactor) is the natural migration
/// point at which the typed path becomes the only path.
///
/// Set `USE_TYPED_SKETCH_ALGEBRA=1` to opt in.
#[allow(dead_code)]
pub const ENV_USE_TYPED_SKETCH_ALGEBRA: &str = "USE_TYPED_SKETCH_ALGEBRA";

/// Whether the typed L4 binding path is enabled for this process.
/// Reads the env var once per call (cheap; called per `plan()` invocation
/// at most). Phase C is additive — both code paths produce the same
/// `CollectionPlan` shape; the typed path is a *parallel* binding that
/// the planner can compare against the legacy path during development.
#[allow(dead_code)]
pub fn typed_sketch_algebra_enabled() -> bool {
    matches!(
        std::env::var(ENV_USE_TYPED_SKETCH_ALGEBRA).as_deref(),
        Ok("1") | Ok("true") | Ok("yes")
    )
}

/// Bind a `QueryWorkload` into the typed L4 [`crate::sketch_algebra::PhysicalExpr`]
/// IR, when callers want to inspect the typed binding alongside the
/// legacy `CollectionPlan` output.
///
/// **Family-per-metric picker** (issue #46, MVP demo). The contract pins
/// six metric→family rows (`http_latency_ms` → DDSketch, `request_size_bytes`
/// → KLL, `unique_users_per_min` → HLL, `top_endpoint_qps` → CountSketch,
/// `endpoint_request_freq` → CMS, `http_requests_total` → raw). The
/// matching is done by [`sketch_algebra::capability_matching::
/// classify_demo_metric`] for the contract rows, and falls back to the
/// `AggType`-driven default (Quantile→DDSketch, Cardinality→HLL,
/// Frequency→CMS) for any other metric name.
///
/// **`sketch_type_override` wins.** When the workload-spec carries a
/// `sketch_type_override`, that field bypasses the capability-matched
/// default and pins the family directly (modulo `(sketch, statistic)`
/// validity — an override that violates the catalog is rejected and the
/// fallback path runs). This is the spec's `sketch_family_override`
/// behaviour.
///
/// **`SumRateCount` declines.** Metrics whose contract row is "raw
/// passthrough" (`http_requests_total`) return `None` — the typed path
/// has no sketch to bind, and the legacy `plan()` path produces the
/// raw-passthrough `CollectionPlan`.
///
/// Returns `None` when the workload shape is not yet supported by the
/// typed path (multi-intent, exact-required, raw-passthrough metric, or
/// no aggregations) — the caller should then fall back to the legacy
/// `plan()` output.
///
/// Phase B (MVP v6) wires `main::handle_plan` to call this whenever
/// the parallel `USE_TYPED_STAGE_SPLIT` gate is enabled — the bound
/// `PhysicalExpr` is then fed into `planner::stage_split::split_typed_three_stage`
/// + the per-stage emitters in `config::stage_config`.
pub fn bind_workload_typed(w: &QueryWorkload) -> Option<crate::sketch_algebra::PhysicalExpr> {
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::intent_algebra::{AggIntent as L3AggIntent, QueryExpr, Schema, Source, WindowKind};
    use crate::sketch_algebra::capability_matching::{
        classify_demo_metric, is_valid_pair, pick_family, AccuracyPreference, StatisticClass,
    };
    use crate::sketch_algebra::rules::{
        bind_cms_count::BindCmsOnCount, bind_cms_topk::BindCountSketchOnTopK,
        bind_ddsketch_quantile::BindDDSketchOnQuantile, bind_hll_cardinality::BindHllOnCardinality,
        bind_kll_quantile::BindKllOnQuantile, Rule,
    };
    use crate::types_v2::AccuracyTarget;
    use asap_sketch::SummaryKind;

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
    let metric_is_contract_row =
        crate::sketch_algebra::capability_matching::classify_demo_metric(&w.metric_name).is_some();
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
    let (mut statistic, mut accuracy_pref) =
        classify_demo_metric(&w.metric_name).unwrap_or_else(|| match w.aggregations[0] {
            AggType::Quantile => (StatisticClass::Quantile, AccuracyPreference::RelativeError),
            AggType::Cardinality => (StatisticClass::Cardinality, AccuracyPreference::default()),
            AggType::Frequency => (StatisticClass::Frequency, AccuracyPreference::default()),
        });

    // An explicit `sketch_family_override` is authoritative for the FAMILY
    // — and therefore for the STATISTIC CLASS it answers. The query-derived
    // statistic above only covers Quantile/Cardinality/Frequency from
    // `AggType`; a `count(...)` / `topk(...)` / `count_over_time(...)`
    // query can classify as the wrong class, so without this an HLL /
    // CountMinSketch / CountSketch override would mismatch the derived
    // statistic, be rejected by `is_valid_pair` below, and silently fall
    // back to the catalog default (DDSketch) — the controller would then
    // emit `family: ddsketch` for an HLL/CMS/CountSketch metric. Re-derive
    // the statistic from the override whenever the derived one is
    // incompatible, so the override drives both family and statistic.
    if let Some(st) = w.sketch_type_override.as_ref() {
        let ov = SummaryKind::from(st.clone());
        if !is_valid_pair(ov.clone(), statistic) {
            let (s, ap) = match ov {
                SummaryKind::DDSketch | SummaryKind::Kll => {
                    (StatisticClass::Quantile, AccuracyPreference::RelativeError)
                }
                SummaryKind::Hll => (StatisticClass::Cardinality, AccuracyPreference::default()),
                SummaryKind::Cms => (StatisticClass::Frequency, AccuracyPreference::default()),
                SummaryKind::CountSketch => (StatisticClass::TopK, AccuracyPreference::default()),
                // `ov` always comes from `SummaryKind::from(SketchType)`
                // (`w.sketch_type_override` is the legacy 5-family enum),
                // so only these 5 canonical families are ever reachable.
                other => unreachable!(
                    "sketch_type_override resolved to an unsupported SummaryKind {other:?}"
                ),
            };
            statistic = s;
            accuracy_pref = ap;
        }
    }

    // SumRateCount → no sketch (raw passthrough). Decline the typed
    // binding so the caller falls back to the legacy raw plan.
    if statistic == StatisticClass::SumRateCount {
        return None;
    }

    // ── Resolve the SummaryKind (override > capability-matched default) ─
    //
    // The workload-spec's `sketch_type_override` (= the spec's
    // `sketch_family_override` per orchestrator contract) wins over the
    // capability-matched pick, *provided* the override is valid for the
    // statistic class. An invalid override (e.g. HLL for a Quantile
    // workload) is silently dropped — the catalog-default family runs
    // instead so the binding never produces a nonsense (sketch, stat)
    // pair.
    let override_kind: Option<SummaryKind> = w
        .sketch_type_override
        .as_ref()
        .map(|st| SummaryKind::from(st.clone()));
    let kind = match override_kind {
        Some(k) if is_valid_pair(k.clone(), statistic) => k,
        _ => pick_family(statistic, accuracy_pref)?,
    };

    // QueryWorkload::accuracy_sla in the legacy planner is interpreted
    // directly as the ε bound (e.g. `0.01` ⇒ ε=0.01). The L3/L4 typed
    // form is `AccuracyTarget::Epsilon(eps)` with the same semantic.
    let accuracy = if w.accuracy_sla > 0.0 {
        AccuracyTarget::Epsilon(w.accuracy_sla)
    } else {
        AccuracyTarget::Exact
    };
    let intent_accuracy = accuracy.clone();

    // Build the matching L3 `AggIntent` for the picked statistic class.
    // TopK lacks an `AggType` enum entry today (the MVP-contract
    // top_endpoint_qps metric is name-classified, not AggType-derived),
    // so we synthesize a default k=10 — the same value the legacy
    // PromQL `topk(10, …)` lowering uses.
    let intent = match statistic {
        StatisticClass::Quantile => L3AggIntent::Quantile {
            col: None,
            q: w.quantiles.first().copied().unwrap_or(0.99),
            accuracy: intent_accuracy,
        },
        StatisticClass::Cardinality => L3AggIntent::Cardinality {
            col: None,
            accuracy: intent_accuracy,
        },
        StatisticClass::Frequency => crate::intent_algebra::frequency(intent_accuracy),
        StatisticClass::TopK => L3AggIntent::TopK {
            k: 10,
            accuracy: intent_accuracy,
        },
        // SumRateCount handled above (early return).
        StatisticClass::SumRateCount => unreachable!(),
    };

    let scan = QueryExpr::Scan {
        source: Source::TimeSeries {
            metric: w.metric_name.clone(),
        },
        label_filters: w
            .label_filters
            .iter()
            .map(|(k, v)| crate::intent_algebra::LabelFilter {
                label: k.clone(),
                equals: v.clone(),
            })
            .collect(),
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
            ],
            0,
            vec![vec![0]],
        ),
    };
    let windowed = QueryExpr::Window {
        kind: WindowKind::Sliding,
        size: w.time_window,
        slide: None,
        child: Box::new(scan),
    };
    let aggregate = QueryExpr::Aggregate {
        by: vec![],
        aggs: vec![intent],
        having: None,
        child: Box::new(windowed),
    };

    // ── Drive the picked family-specific `Bind*` rule ─────────────────
    //
    // Bypass the priority-based dispatcher: we have a definitive family
    // pick from the capability matrix (or the `sketch_type_override`),
    // so route directly to the rule that produces that family. This
    // keeps the contract-row mapping deterministic — the dispatcher's
    // tie-break (DDSketch p=6 vs KLL p=5) cannot accidentally flip
    // `request_size_bytes`'s KLL pick to DDSketch.
    //
    // CMS+TopK note: when the picker selected `SummaryKind::Cms` for a
    // TopK statistic (only reachable today via a `sketch_family_override:
    // CountMinSketch` on a TopK metric), we emit a CMS-with-heap binding
    // inline. The CMS-Heap pattern (Cormode & Muthukrishnan 2005) gives
    // a valid heavy-hitter sketch; the unbiased CountSketch remains the
    // canonical pick when no override is supplied. The backend's "top-K
    // from CountMin state" readout path is a separate workstream — see
    // `sketch_algebra::capability_matching` module docs for the gap note.
    match (kind, statistic) {
        (SummaryKind::DDSketch, _) => BindDDSketchOnQuantile.apply(&aggregate, &accuracy),
        (SummaryKind::Kll, _) => BindKllOnQuantile.apply(&aggregate, &accuracy),
        (SummaryKind::Hll, _) => BindHllOnCardinality.apply(&aggregate, &accuracy),
        // The capability matrix already pinned the family here, so force
        // the matching recall tier rather than re-inferring it: a
        // CountSketch pick is the unbiased canonical top-k (Tight); a CMS
        // pick on a top-k is the cheap CMS-with-heap (Loose). This keeps
        // `bind_workload_typed`'s contract-row mapping deterministic — the
        // recall-aware default lives in `dispatch()` / `Rule::apply`.
        (SummaryKind::CountSketch, _) => BindCountSketchOnTopK.apply_with_tier(
            &aggregate,
            &accuracy,
            crate::sketch_algebra::rules::bind_cms_topk::TopkRecallTier::Tight,
        ),
        (SummaryKind::Cms, StatisticClass::TopK) => BindCountSketchOnTopK.apply_with_tier(
            &aggregate,
            &accuracy,
            crate::sketch_algebra::rules::bind_cms_topk::TopkRecallTier::Loose,
        ),
        (SummaryKind::Cms, _) => BindCmsOnCount.apply(&aggregate, &accuracy),
        // `kind` always comes from `pick_family`/`sketch_type_override`,
        // exhaustively the 5 canonical families.
        (other, _) => unreachable!("dispatch: unsupported SummaryKind {other:?}"),
    }
}

pub struct RulesPlanner {
    pub valid_for: Duration,
    pub sketch_defaults: SketchDefaults,
}

impl RulesPlanner {
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
        // When exact computation is required (RSI, MACD, stateful indicators),
        // skip sketch selection and return a raw-passthrough plan.
        if w.exact_required {
            return self.raw_passthrough_plan(w);
        }

        let sketch_type = crate::physical::sketch_catalog::sketch_type_for_agg(&w.aggregations);
        let sketch_params = crate::physical::sketch_catalog::build_sketch_params(
            &self.sketch_defaults,
            &sketch_type,
            w.accuracy_sla,
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
                // CostModelPlanner overwrites them via decide_delta().
                delta_transmission: false,
                delta_threshold: 0.0,
                gos: None,
                enable_series_id: true,
                series_id_ttl_secs: 0,

                data_sink: AgentDataSink::default(),
            },
            gateway_config: GatewayCollectorConfig { passthrough: true },
            precompute: vec![],
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
            precompute: vec![],
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
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
        }
    }

    #[test]
    fn quantile_selects_ddsketch() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::DDSketch);
    }

    #[test]
    fn cardinality_selects_hll() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Cardinality]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::HLL);
    }

    #[test]
    fn frequency_selects_countsketch() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Frequency]));
        assert_eq!(plan.agent_config.sketch_type, SketchType::CountSketch);
    }

    /// Regression: an explicit `sketch_family_override` must pin the family
    /// (and statistic) even when the query's `AggType` classifies as a
    /// different/incompatible class. Without the override re-deriving the
    /// statistic, `is_valid_pair` rejected HLL/CMS/CountSketch overrides
    /// against a Quantile-classified query and fell back to DDSketch, so
    /// the controller emitted `family: ddsketch` for those metrics.
    #[test]
    fn override_pins_nonquantile_family_over_misclassified_query() {
        use crate::emit::extract_root_sketch_kind;
        use asap_sketch::SummaryKind;
        for (ov, expect) in [
            (SketchType::DDSketch, SummaryKind::DDSketch),
            (SketchType::KLL, SummaryKind::Kll),
            (SketchType::HLL, SummaryKind::Hll),
            (SketchType::CountMinSketch, SummaryKind::Cms),
            // CountSketch override re-derives statistic as TopK (see the
            // override arm below), and `bind_cms_topk` always binds the
            // heap-bearing kind for a top-k intent — matches this
            // fixture's pre-`SummaryKind`-split expectation, when
            // `with_heap: true` was a params flag rather than kind
            // identity.
            (SketchType::CountSketch, SummaryKind::CountSketchWithHeap),
        ] {
            // Query classifies as Quantile (the mis-derived case observed
            // live for count()/topk()/count_over_time()); the override must win.
            let mut w = workload(vec![AggType::Quantile]);
            w.sketch_type_override = Some(ov.clone());
            let pe = bind_workload_typed(&w)
                .unwrap_or_else(|| panic!("bind declined for override {ov:?}"));
            assert_eq!(
                extract_root_sketch_kind(&pe),
                Some(expect.clone()),
                "override {ov:?} should pin family {expect:?}, not fall back to DDSketch",
            );
        }
    }

    #[test]
    fn quantile_priority_wins() {
        let plan =
            RulesPlanner::new().plan(&workload(vec![AggType::Quantile, AggType::Cardinality]));
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
        let plan = RulesPlanner::new().plan(&w);
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
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Batch);
        assert_eq!(plan.agent_config.window_duration, None);
    }

    #[test]
    fn no_latency_sla_defaults_to_window() {
        let mut w = workload(vec![AggType::Quantile]);
        w.latency_sla = None;
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.mode, ProcessorMode::Window);
    }

    #[test]
    fn aggregate_by_sorted() {
        let mut w = workload(vec![AggType::Quantile]);
        w.group_by_labels = vec!["zone".into(), "host.name".into(), "service".into()];
        let plan = RulesPlanner::new().plan(&w);
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
        let plan = RulesPlanner::new().plan(&w);
        assert_eq!(plan.agent_config.label_matchers.len(), 2);
    }

    #[test]
    fn ddsketch_accuracy_params() {
        let mut w = workload(vec![AggType::Quantile]);
        w.accuracy_sla = 0.005;
        let plan = RulesPlanner::new().plan(&w);
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
        w.accuracy_sla = 0.03;
        let plan = RulesPlanner::new().plan(&w);
        match &plan.agent_config.sketch_params {
            SketchParams::HLL { precision } => {
                assert_eq!(*precision, 10, "coarse SLA should use lower precision")
            }
            other => panic!("expected HLL, got {:?}", other),
        }
    }

    #[test]
    fn valid_until_in_future() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
        assert!(
            plan.valid_until > Utc::now(),
            "valid_until should be in the future"
        );
    }

    #[test]
    fn gateway_passthrough() {
        let plan = RulesPlanner::new().plan(&workload(vec![AggType::Quantile]));
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

    use crate::sketch_algebra::physical_expr::PhysicalExpr;
    use asap_sketch::SummaryKind;

    /// Walk the L4 binding output and pull out the `SketchAgg`'s family.
    /// Returns `None` if no `SketchAgg` node is present (raw / pure
    /// logical pass-through).
    fn extract_family(expr: &PhysicalExpr) -> Option<SummaryKind> {
        match expr {
            PhysicalExpr::SketchAgg { sketch_type, .. } => Some(sketch_type.clone()),
            PhysicalExpr::SketchEstimate { child, .. } => extract_family(child),
            PhysicalExpr::SketchMerge { children, .. } => children.iter().find_map(extract_family),
            PhysicalExpr::LetBinding { expr, child, .. } => {
                extract_family(expr).or_else(|| extract_family(child))
            }
            PhysicalExpr::Logical(_) | PhysicalExpr::Ref { .. } => None,
            PhysicalExpr::RawAtEdgeSketchAtBackend { family, .. } => Some(family.clone()),
            PhysicalExpr::RawAtEdgePrometheusArchive { .. } => None,
            // ExactAgg has no sketch family — it produces an exact
            // aggregation accumulator, not a sketch state.
            PhysicalExpr::ExactAgg { .. } => None,
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
            latency_sla: None,
            sketch_type_override: None,
            exact_required: false,
            quantiles: vec![],
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
            Some(SummaryKind::DDSketch),
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
            Some(SummaryKind::Kll),
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
            Some(SummaryKind::Hll),
            "unique_users_per_min should bind to HLL (Cardinality)",
        );
    }

    #[test]
    fn typed_binding_top_endpoint_qps_picks_countsketch() {
        // Contract: `top_endpoint_qps` → CountSketch (TopK).
        // The metric-name reclassification reroutes from the AggType
        // default (Frequency → CMS) to the contract row (TopK →
        // CountSketch).
        let w = workload_for("top_endpoint_qps", AggType::Frequency);
        let bound = bind_workload_typed(&w).expect("top_endpoint_qps must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SummaryKind::CountSketchWithHeap),
            "top_endpoint_qps should bind to CountSketch-with-heap (TopK)",
        );
    }

    #[test]
    fn typed_binding_endpoint_request_freq_picks_cms() {
        // Contract: `endpoint_request_freq` → CMS (Frequency).
        // The legacy AggType default for Frequency is *also* CMS via
        // BindCmsOnCount, but the metric-name path goes through the
        // capability-matching picker first — both produce CMS, the
        // contract row ratifies it.
        let w = workload_for("endpoint_request_freq", AggType::Frequency);
        let bound = bind_workload_typed(&w).expect("endpoint_request_freq must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SummaryKind::Cms),
            "endpoint_request_freq should bind to CMS (Frequency)",
        );
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
            Some(SummaryKind::Kll),
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
            Some(SummaryKind::DDSketch),
            "sketch_type_override=DDSketch should pin DDSketch despite the contract's KLL default",
        );
    }

    #[test]
    fn planner_accepts_countmin_override_for_topk_metric() {
        // CMS-Heap pattern (Cormode & Muthukrishnan 2005): when a
        // workload's `sketch_family_override` (=
        // `sketch_type_override`) selects CountMinSketch for a TopK
        // metric, the planner should accept it instead of falling
        // back to the canonical CountSketch default.
        let mut w = workload_for("top_endpoint_qps", AggType::Frequency);
        w.sketch_type_override = Some(SketchType::CountMinSketch);
        let bound =
            bind_workload_typed(&w).expect("CountMin override on a TopK metric should still bind");
        assert_eq!(
            extract_family(&bound),
            Some(SummaryKind::CmsWithHeap),
            "sketch_type_override=CountMinSketch on a TopK metric should pin CMS-with-heap",
        );
    }

    #[test]
    fn planner_default_for_topk_remains_countsketch() {
        // Without any override, the canonical pick for a TopK metric
        // stays CountSketch(-with-heap) — CMS-Heap is opt-in via
        // override only.
        let w = workload_for("top_endpoint_qps", AggType::Frequency);
        let bound = bind_workload_typed(&w).expect("top_endpoint_qps must bind");
        assert_eq!(
            extract_family(&bound),
            Some(SummaryKind::CountSketchWithHeap),
            "default TopK pick must remain CountSketch-with-heap (unbiased estimator)",
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
            Some(SummaryKind::DDSketch),
            "invalid (HLL, Quantile) override should be rejected; planner falls back to DDSketch",
        );
    }

    // ── Combined sweep: all 6 contract rows in one shot ───────────────────────

    #[test]
    fn all_six_contract_metrics_produce_expected_family() {
        // Single test that drives the full contract row set through
        // `bind_workload_typed` — this is the per-task acceptance test
        // ("verify each produces the expected `PhysicalExpr` family").
        let cases: Vec<(&str, AggType, Option<SummaryKind>)> = vec![
            ("http_requests_total", AggType::Frequency, None),
            (
                "http_latency_ms",
                AggType::Quantile,
                Some(SummaryKind::DDSketch),
            ),
            (
                "request_size_bytes",
                AggType::Quantile,
                Some(SummaryKind::Kll),
            ),
            (
                "unique_users_per_min",
                AggType::Cardinality,
                Some(SummaryKind::Hll),
            ),
            (
                "top_endpoint_qps",
                AggType::Frequency,
                Some(SummaryKind::CountSketchWithHeap),
            ),
            (
                "endpoint_request_freq",
                AggType::Frequency,
                Some(SummaryKind::Cms),
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
