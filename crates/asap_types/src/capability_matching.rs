use std::cmp::Ordering;
use std::collections::HashMap;

use promql_utilities::data_model::KeyByLabelNames;
use promql_utilities::query_logics::enums::Statistic;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::aggregation_config::{AggregationConfig, AggregationIdInfo};
use crate::enums::WindowType;
use crate::query_requirements::QueryRequirements;
use crate::utils::normalize_spatial_filter;
use promql_utilities::query_logics::enums::AggregationType;

pub const ENGINE_ID_ASAP_QUERY: &str = "asap_query";
pub const ENGINE_ID_THANOS_QUERY: &str = "thanos_query";

pub const CANONICAL_QUERY_ENGINE_IDS: &[&str] = &[ENGINE_ID_ASAP_QUERY, ENGINE_ID_THANOS_QUERY];

// ---------------------------------------------------------------------------
// Phase-5: storage-backend capability axis
//
// Today's `find_compatible_aggregation` matches on
// `(metric, statistic, sub_type, window_size, grouping_labels, spatial_filter)`
// — there is no axis for "which storage tier serves this query." The Phase-5
// `GorillaQueryEngine` (PR #85) introduces a parallel exact tier; the planner /
// router needs to disambiguate between ASAP-tier sketches and Gorilla-S3
// chunks. See `docs/design-gorilla-s3-cold-engine.md` §8.
// ---------------------------------------------------------------------------

/// Which physical storage tier a query (or a metric configuration) routes to.
///
/// `SketchStore` is the default — every existing `AggregationConfig` and
/// `StreamingConfig` decodes into this variant via `#[serde(default)]`, so
/// pre-Phase-5 deploys keep dispatching to `ASAPQueryEngine` unchanged.
///
/// **Step-1 of the JSONL deprecation refactor** removed the
/// `ColdJsonlFallback` variant. The legacy local-FS JSONL leg
/// (`LocalFsColdStore`, `parse_jsonl`, the §5.2 raw-store
/// fallback) was deleted at the same commit; the surviving
/// failover surface is ASAP-tier sketch ↔ Thanos archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StorageBackend {
    /// Warm-tier sketch DB (today's `SketchStore` + accumulators).
    /// Served by `ASAPQueryEngine`. Default for unconfigured metrics.
    #[default]
    SketchStore,

    /// Thanos archive over MinIO/S3. The enum name is kept for
    /// serde/back-compat with existing configs, but its canonical
    /// query-engine identity is `thanos_query`. Gorilla is an
    /// archive chunk format/storage detail, not a public query engine.
    GorillaObjectStore,

    /// Double-write: the metric is written to both ASAP-tier sketches AND the
    /// Gorilla-S3 archive. Capability matching surfaces both options and the
    /// cost-aware dispatcher picks per query (typically ASAP-tier for low-
    /// latency approximate, archive for exact).
    DoubleWrite,

    /// Prometheus-remote: the metric's data is shipped raw to a
    /// Prometheus instance via the native OTLP receiver. Phase ε.2
    /// registers a `PrometheusForwardEngine` (HTTP-forwarder to
    /// Prometheus's `/api/v1/query`) under this slot so the
    /// controller's `RawAtEdgePrometheusArchive` mode can route a
    /// metric's queries to Prometheus directly. Mirrors the
    /// `GorillaObjectStore` slot's "single backend, no failover"
    /// semantics — there is no ASAP-tier sketch to fall back on for a
    /// Prometheus-remote metric.
    PrometheusRemote,
}

impl StorageBackend {
    /// Canonical string tag pinned for byte-comparable dispatch on the wire (mirrors
    /// the `data_source: <tag>` info-line on `QueryResult`). Engines
    /// register themselves under these IDs in the router.
    pub const fn data_source_id(self) -> &'static str {
        match self {
            StorageBackend::SketchStore => ENGINE_ID_ASAP_QUERY,
            StorageBackend::GorillaObjectStore => ENGINE_ID_THANOS_QUERY,
            StorageBackend::DoubleWrite => "double_write",
            StorageBackend::PrometheusRemote => "prometheus_remote",
        }
    }
}

pub fn parse_storage_backend_engine_id(s: &str) -> Option<StorageBackend> {
    match s {
        ENGINE_ID_ASAP_QUERY => Some(StorageBackend::SketchStore),
        ENGINE_ID_THANOS_QUERY => Some(StorageBackend::GorillaObjectStore),
        "double_write" => Some(StorageBackend::DoubleWrite),
        "prometheus_remote" => Some(StorageBackend::PrometheusRemote),
        _ => None,
    }
}

/// Accuracy hint pushed by the controller at intent-binding time
/// (`controller/docs/design.md` §6 `core::workload`). The Phase-5 capability
/// router consults this to decide whether a metric configured for both warm-
/// tier and Gorilla-S3 should answer from the archive (Exact) or the
/// approximate ASAP-tier sketch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AccuracyTarget {
    /// Caller demands an exact answer; ASAP-tier sketches are not eligible
    /// unless they happen to be exact accumulators (Sum, MinMax, Increase).
    Exact,
    /// Caller accepts ε/δ-bounded approximate answers. Default.
    #[default]
    Approximate,
}

// ---------------------------------------------------------------------------
// Pure compatibility helpers
// ---------------------------------------------------------------------------

/// Returns the aggregation types that can serve this statistic.
///
/// This list is the **superset of compatibility**: every `AggregationType`
/// that the planner's canonical map (`promql_utilities::query_logics::logics::
/// map_statistic_to_precompute_operator`) may legally produce for this
/// statistic — across both `Exact` and `Approximate` treatment types — must
/// appear here. The agreement is enforced by
/// `capability_canonical_map_agreement` in the test module: any future
/// divergence between this table and `map_statistic_to_precompute_operator`
/// will be caught at test-time.
///
/// The runtime caller (`find_compatible_aggregation`) has no
/// `QueryTreatmentType` to consult — `QueryRequirements` is treatment-agnostic
/// — so this list intentionally enumerates *every* type that could serve the
/// statistic. Selection between e.g. `Sum` (exact) and `CountMinSketch`
/// (approximate) for `Statistic::Sum` is made downstream via
/// `aggregation_priority` (largest window size wins).
pub fn compatible_agg_types(stat: Statistic) -> &'static [AggregationType] {
    match stat {
        // Sum: exact via Sum / MultipleSum; approximate via CountMinSketch
        // (the canonical approximator picked by `map_statistic_to_precompute_operator`).
        // Pre-fix this list omitted CountMinSketch, so a `sum_over_time(...)`
        // query against a CMS-only config fell through capability matching
        // and onto the cold tier.
        Statistic::Sum => &[
            AggregationType::Sum,
            AggregationType::MultipleSum,
            AggregationType::CountMinSketch,
            // Counters: ASAP-tier ingest stores counter metrics
            // (OTel `Sum` / monotonic=true) as Increase /
            // MultipleIncrease accumulators, whose `query`
            // implementation answers `Statistic::Sum` with the
            // latest cumulative value per series — matching
            // Prometheus' instant `sum(<counter>)` semantics.
            // Without these here, `sum by (zone) (http_requests_total)`
            // capability-misses (issue ProjectASAP/ASAPCollector#46;
            // diagnosis in PR #108).
            AggregationType::Increase,
            AggregationType::MultipleIncrease,
        ],
        // Count: exact via MultipleSum (the planner's canonical pick for
        // Count-Exact uses `MultipleSum` with sub_type="count"); approximate
        // via CountMinSketch / CountMinSketchWithHeap.
        //
        // HLL is also valid here: the ASAP-tier MVP demo
        // (ProjectASAP/ASAPCollector#46) plans `unique_users_per_min`
        // as an HLL agg and the replay client queries it with
        // `count(unique_users_per_min)`. `HllSketchAccumulator`
        // answers `Statistic::Count` as a cardinality alias —
        // see `precompute_operators/hll_sketch_accumulator.rs:220`.
        // Without HLL listed here the warm engine returns `status=error`
        // for every count-of-HLL replay row.
        Statistic::Count => &[
            AggregationType::MultipleSum,
            AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap,
            AggregationType::HLL,
        ],
        Statistic::Min | Statistic::Max => {
            &[AggregationType::MinMax, AggregationType::MultipleMinMax]
        }
        // Quantile: KLL (planner-emitted canonical pick) plus the
        // sketch types whose accumulators answer `Statistic::Quantile`
        // natively but that the planner's canonical map does not
        // emit. The MVP demo's controller (`ASAPCollector/controller`)
        // plans `http_requests_total_latency_ms` as a `DDSketch`
        // directly from `mvp-workload.yaml` and routes the resulting
        // delta payloads through the modified-OTLP wire format
        // (DDSketch state landing in `DDSketchAccumulator`, which
        // supports `Statistic::Quantile` — see
        // `precompute_operators/dd_sketch_accumulator.rs`). Without
        // DDSketch enumerated here, capability matching for an
        // out-of-YAML query like `quantile_over_time(0.99,
        // http_requests_total_latency_ms[1m])` would miss and the
        // ASAP-tier engine returns `EngineError::CapabilityMiss`.
        Statistic::Quantile => &[
            AggregationType::DatasketchesKLL,
            AggregationType::HydraKLL,
            AggregationType::DDSketch,
        ],
        // Rate / Increase: the canonical exact accumulators are the
        // counter-shaped Increase / MultipleIncrease, but `rate(...)`
        // and `increase(...)` over a CountMinSketch-backed agg are
        // also valid — CMS records every insert and answers
        // `Statistic::Rate` natively (events / range_ms when the
        // engine passes `range_ms` in query_kwargs; raw event count
        // as a units-of-events/window fallback otherwise — see
        // `precompute_operators/count_min_sketch_accumulator.rs`).
        // Without CMS / CMSWithHeap listed here, `rate(metric[5m])`
        // against a CMS-only config — the canonical MVP demo
        // CountMin path — capability-misses and the warm engine
        // returns `status=error`. Closes the PR #111 honest-gap
        // call-out for `Statistic::Rate` not implemented.
        Statistic::Rate | Statistic::Increase => &[
            AggregationType::Increase,
            AggregationType::MultipleIncrease,
            AggregationType::CountMinSketch,
            AggregationType::CountMinSketchWithHeap,
        ],
        // Cardinality: SetAggregator / DeltaSetAggregator are the
        // exact key trackers; HLL is the canonical approximator
        // whose accumulator answers `Statistic::Cardinality` (and
        // `Statistic::Count` as a cardinality alias) — see
        // `precompute_operators/hll_sketch_accumulator.rs`. HLL is
        // not in the planner's canonical map (it's wired in via
        // modified-OTLP from the agent processors) but the runtime
        // accumulator surface still resolves it, so list it here so
        // capability matching can pick it up.
        Statistic::Cardinality => &[
            AggregationType::SetAggregator,
            AggregationType::DeltaSetAggregator,
            AggregationType::HLL,
        ],
        // Topk: `CountMinSketchWithHeap` is the canonical CMS-Heap
        // pattern. CountSketch is the second-tier reservoir-style
        // approximator the MVP demo's controller plans for
        // `top_endpoint_qps` (median-of-row estimator over a
        // signed-counter matrix). `CountSketchAccumulator` answers
        // `Statistic::Topk` directly — see
        // `precompute_operators/count_sketch_accumulator.rs:284`.
        // Without CountSketch listed here, `topk(K, top_endpoint_qps)`
        // capability-misses and the warm engine returns `status=error`.
        Statistic::Topk => &[
            AggregationType::CountMinSketchWithHeap,
            AggregationType::CountSketch,
        ],
    }
}

/// Returns the storage backends that can serve a `(statistic, accuracy)`
/// query when the metric is configured for `metric_storage_config`.
///
/// The returned list is **ordered by preference**: the router walks it in
/// order and dispatches to the first backend whose engine is registered.
///
/// **Step-1 of the JSONL deprecation refactor**: the legacy
/// `ColdJsonlFallback` failover slot was removed. Surviving
/// failover surface is ASAP-tier sketch ↔ Gorilla-S3 archive.
///
/// Routing rules (mirrors `docs/design-gorilla-s3-cold-engine.md` §8):
///
/// * Metric configured for `GorillaObjectStore`: always
///   `[GorillaObjectStore]`. Exact-on-archive subsumes approximate-on-warm,
///   so even a `Statistic::Quantile` with an `Approximate` target still
///   routes to the archive when the metric is Gorilla-only — there is no
///   ASAP-tier sketch to fall back to in that deploy shape.
/// * Metric configured for `SketchStore` (or unconfigured / default):
///   `[SketchStore]`. A capability miss in the ASAP tier surfaces
///   as a 404 — the previous JSONL fallback path has been deleted.
/// * Metric configured for `DoubleWrite`: head depends on accuracy hint,
///   tail is the failover sequence (the cost-aware `EngineRouter` picks
///   the head, walks the tail on failure):
///   - `Exact` → `[GorillaObjectStore, SketchStore]`
///   - `Approximate` → `[SketchStore, GorillaObjectStore]`
pub fn compatible_storage_backends(
    _stat: Statistic,
    accuracy: AccuracyTarget,
    metric_storage_config: StorageBackend,
) -> Vec<StorageBackend> {
    match metric_storage_config {
        StorageBackend::GorillaObjectStore => {
            vec![StorageBackend::GorillaObjectStore]
        }
        StorageBackend::SketchStore => {
            vec![StorageBackend::SketchStore]
        }
        StorageBackend::DoubleWrite => match accuracy {
            AccuracyTarget::Exact => vec![
                StorageBackend::GorillaObjectStore,
                StorageBackend::SketchStore,
            ],
            AccuracyTarget::Approximate => vec![
                StorageBackend::SketchStore,
                StorageBackend::GorillaObjectStore,
            ],
        },
        // Phase ε.2: Prometheus-remote metrics route only to the
        // Prometheus forwarder. There is no ASAP-tier sketch to fall
        // back on (the metric's raw samples never landed in
        // ASAP-managed storage), so the failover sequence is the
        // single backend itself; a missing engine surfaces as a
        // `NoEngineRegistered` 503 from the HTTP handler, which is
        // the correct fail-loud behaviour for a misconfigured deploy.
        StorageBackend::PrometheusRemote => {
            vec![StorageBackend::PrometheusRemote]
        }
    }
}

/// Returns the required aggregation_sub_type for this statistic, if any.
/// `Min` requires `"min"`, `Max` requires `"max"`. All others are unconstrained.
pub fn required_sub_type(stat: Statistic) -> Option<&'static str> {
    match stat {
        Statistic::Min => Some("min"),
        Statistic::Max => Some("max"),
        _ => None,
    }
}

/// Whether this value aggregation type requires a paired key aggregation
/// (`SetAggregator` or `DeltaSetAggregator`).
pub fn is_multi_population_value_type(agg_type: AggregationType) -> bool {
    agg_type.is_multi_population_value_type()
}

/// Whether this type is a key aggregation (tracks which label-value combinations exist).
fn is_key_agg_type(agg_type: AggregationType) -> bool {
    agg_type.is_key_agg_type()
}

/// Window compatibility: can `config` serve a query needing `data_range_ms`?
///
/// - `None` (spatial-only): always compatible.
/// - Tumbling: `data_range_ms` must be a positive integer multiple of `window_size_ms`.
/// - Sliding: `data_range_ms` must equal `window_size_ms` exactly (a sliding window
///   precomputes one fixed range per timestamp; overlapping windows cannot be merged).
pub fn window_compatible(config: &AggregationConfig, data_range_ms: Option<u64>) -> bool {
    let Some(range) = data_range_ms else {
        return true;
    };
    let window_ms = config.window_size * 1000;
    if window_ms == 0 || range == 0 {
        return false;
    }
    match config.window_type {
        WindowType::Sliding => range == window_ms,
        WindowType::Tumbling => range % window_ms == 0,
    }
}

/// Label compatibility: config can serve a query whose grouping is a
/// **subset** (including equality) of the config's grouping_labels.
///
/// Pre-fix this was strict-exact: `config_labels == req_labels`. The
/// MVP demo (ProjectASAP/ASAPCollector#46) replays
/// `count(unique_users_per_min)` / `topk(5, top_endpoint_qps)` with
/// no `by (...)` modifier, which translates to `req.grouping_labels =
/// []`. The corresponding agg configs are per-zone (`[zone]` grouping).
/// Pre-fix every such replay row capability-missed and the warm engine
/// returned `status=error`. Post-fix the engine accepts the agg, runs
/// the per-zone accumulators through the merge path
/// (`execute_and_merge_store_queries` produces a per-key map; the
/// downstream merge collapses them to the requested `[]` grouping —
/// HLL/CMS/CountSketch all support natural across-key merge, and
/// scalar accumulators like Sum / Increase reduce by addition).
///
/// Direction is asymmetric: `config ⊇ req` is OK (engine merges away
/// the extra labels), but `req ⊃ config` is NOT — the engine cannot
/// invent a label that the materialised agg never partitioned by.
pub fn labels_compatible(config_labels: &KeyByLabelNames, req_labels: &KeyByLabelNames) -> bool {
    let req: std::collections::HashSet<&String> = req_labels.labels.iter().collect();
    let cfg: std::collections::HashSet<&String> = config_labels.labels.iter().collect();
    req.is_subset(&cfg)
}

/// Spatial filter compatibility.
/// - Both empty → compatible.
/// - Config non-empty and matches query → compatible.
/// - Config non-empty and query differs (or is empty) → incompatible.
pub fn spatial_filter_compatible(config_filter: &str, req_filter: &str) -> bool {
    let config_norm = normalize_spatial_filter(config_filter);
    let req_norm = normalize_spatial_filter(req_filter);
    if config_norm.is_empty() {
        // Config has no filter — compatible with any query filter.
        return true;
    }
    config_norm == req_norm
}

/// Aggregation priority comparator: prefer larger `window_size` (descending).
/// This is a separate function so callers can swap the policy without touching matching logic.
pub fn aggregation_priority(a: &AggregationConfig, b: &AggregationConfig) -> Ordering {
    b.window_size.cmp(&a.window_size)
}

// ---------------------------------------------------------------------------
// Core matching function
// ---------------------------------------------------------------------------

/// Find a compatible aggregation (or pair of aggregations for multi-population queries)
/// given all available aggregation configs and a set of query requirements.
///
/// Returns `None` if no fully compatible match exists.
///
/// Algorithm:
/// 1. For each statistic, collect and sort compatible candidates.
/// 2. For multi-statistic requirements (e.g. avg = [Sum, Count]), all must be
///    served by configs sharing the same `window_size` and `grouping_labels`.
/// 3. If the selected value aggregation type is multi-population, also find a
///    paired key aggregation (`SetAggregator` / `DeltaSetAggregator`) on the same metric.
pub fn find_compatible_aggregation(
    configs: &HashMap<u64, AggregationConfig>,
    requirements: &QueryRequirements,
) -> Option<AggregationIdInfo> {
    if requirements.statistics.is_empty() {
        return None;
    }

    debug!(
        metric = %requirements.metric,
        statistics = ?requirements.statistics,
        data_range_ms = ?requirements.data_range_ms,
        grouping_labels = ?requirements.grouping_labels.labels,
        "capability matching: searching {} aggregation config(s)",
        configs.len(),
    );

    // For each statistic, collect configs that pass all filters, sorted by priority.
    let mut per_stat_candidates: Vec<Vec<&AggregationConfig>> = Vec::new();

    for &stat in &requirements.statistics {
        let types = compatible_agg_types(stat);
        let sub_type = required_sub_type(stat);

        let mut candidates: Vec<&AggregationConfig> = configs
            .values()
            .filter(|c| {
                let ok = c.metric == requirements.metric
                    && types.contains(&c.aggregation_type)
                    && sub_type.is_none_or(|st| c.aggregation_sub_type == st)
                    && window_compatible(c, requirements.data_range_ms)
                    && labels_compatible(&c.grouping_labels, &requirements.grouping_labels)
                    && spatial_filter_compatible(
                        &c.spatial_filter_normalized,
                        &requirements.spatial_filter_normalized,
                    );
                if !ok {
                    debug!(
                        agg_id = c.aggregation_id(),
                        agg_type = %c.aggregation_type,
                        metric = %c.metric,
                        window_size_s = c.window_size,
                        "capability matching: rejected config for {:?}",
                        stat,
                    );
                }
                ok
            })
            .collect();

        candidates.sort_by(|a, b| aggregation_priority(a, b));

        if candidates.is_empty() {
            warn!(
                metric = %requirements.metric,
                statistic = ?stat,
                "capability matching: no compatible aggregation found for statistic",
            );
            return None;
        }

        debug!(
            statistic = ?stat,
            num_candidates = candidates.len(),
            chosen_agg_id = candidates[0].aggregation_id(),
            chosen_agg_type = %candidates[0].aggregation_type,
            chosen_window_size_s = candidates[0].window_size,
            "capability matching: found candidates, chose best",
        );

        per_stat_candidates.push(candidates);
    }

    // Pick the best candidate for the first statistic.
    let value_agg = per_stat_candidates[0][0];

    // For multi-statistic requirements, the remaining statistics must be served by a
    // config that agrees on window_size and grouping_labels with the chosen value agg.
    for (i, candidates) in per_stat_candidates.iter().enumerate().skip(1) {
        let found = candidates.iter().any(|c| {
            c.window_size == value_agg.window_size && c.grouping_labels == value_agg.grouping_labels
        });
        if !found {
            warn!(
                metric = %requirements.metric,
                statistic = ?requirements.statistics[i],
                required_window_size_s = value_agg.window_size,
                "capability matching: no matching window/labels for multi-statistic requirement",
            );
            return None;
        }
    }

    // If value type is multi-population, find the paired key aggregation.
    let key_agg: &AggregationConfig = if is_multi_population_value_type(value_agg.aggregation_type)
    {
        let ka = configs
            .values()
            .find(|c| c.metric == requirements.metric && is_key_agg_type(c.aggregation_type));
        if ka.is_none() {
            warn!(
                metric = %requirements.metric,
                value_agg_type = %value_agg.aggregation_type,
                "capability matching: multi-population value agg requires a key agg (SetAggregator/DeltaSetAggregator) but none found",
            );
        }
        ka?
    } else {
        value_agg
    };

    debug!(
        metric = %requirements.metric,
        value_agg_id = value_agg.aggregation_id(),
        value_agg_type = %value_agg.aggregation_type,
        key_agg_id = key_agg.aggregation_id(),
        key_agg_type = %key_agg.aggregation_type,
        "capability matching: resolved",
    );

    Some(AggregationIdInfo {
        aggregation_id_for_value: value_agg.aggregation_id(),
        aggregation_type_for_value: value_agg.aggregation_type,
        aggregation_id_for_key: key_agg.aggregation_id(),
        aggregation_type_for_key: key_agg.aggregation_type,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::normalize_spatial_filter;
    use promql_utilities::data_model::KeyByLabelNames;
    use std::collections::HashMap;

    #[allow(clippy::too_many_arguments)]
    fn make_config(
        _id: u64,
        metric: &str,
        agg_type: &str,
        sub_type: &str,
        window_size_s: u64,
        window_type: &str,
        grouping: &[&str],
        spatial_filter: &str,
    ) -> AggregationConfig {
        // `_id` is unused after PR 5 — identity is content-addressed
        // via `PolicyFingerprint::from_config`. Kept as a parameter so
        // the wide-coverage assertions in this module's test cases
        // don't churn.
        let grouping_labels =
            KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect());
        let spatial_filter_normalized = normalize_spatial_filter(spatial_filter);
        AggregationConfig {
            aggregation_type: agg_type.parse::<AggregationType>().expect("valid agg type"),
            aggregation_sub_type: sub_type.to_string(),
            parameters: HashMap::new(),
            grouping_labels,
            aggregated_labels: KeyByLabelNames::new(vec![]),
            rollup_labels: KeyByLabelNames::new(vec![]),
            original_yaml: String::new(),
            window_size: window_size_s,
            slide_interval: window_size_s,
            window_type: window_type.parse::<WindowType>().unwrap_or_default(),
            spatial_filter: spatial_filter.to_string(),
            spatial_filter_normalized,
            metric: metric.to_string(),
            num_aggregates_to_retain: None,
            table_name: None,
            value_column: None,
        }
    }

    fn req(
        metric: &str,
        stats: &[Statistic],
        data_range_ms: Option<u64>,
        grouping: &[&str],
        spatial_filter: &str,
    ) -> QueryRequirements {
        QueryRequirements {
            metric: metric.to_string(),
            statistics: stats.to_vec(),
            data_range_ms,
            grouping_labels: KeyByLabelNames::new(grouping.iter().map(|s| s.to_string()).collect()),
            spatial_filter_normalized: normalize_spatial_filter(spatial_filter),
        }
    }

    fn single_config(config: AggregationConfig) -> HashMap<u64, AggregationConfig> {
        let mut m = HashMap::new();
        m.insert(config.aggregation_id(), config);
        m
    }

    // --- basic type matching ---

    #[test]
    fn basic_sum_match() {
        let cfg = make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], "");
        let expected = cfg.aggregation_id();
        let configs = single_config(cfg);
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
        assert_eq!(result.unwrap().aggregation_id_for_value, expected);
    }

    #[test]
    fn quantile_any_value_finds_kll() {
        let cfg = make_config(2, "lat", "DatasketchesKLL", "", 300, "tumbling", &[], "");
        let expected = cfg.aggregation_id();
        let configs = single_config(cfg);
        // quantile value (0.5 or 0.9) is NOT part of QueryRequirements — both should find the same config
        let r1 = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], Some(300_000), &[], ""),
        );
        let r2 = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], Some(300_000), &[], ""),
        );
        assert_eq!(r1.unwrap().aggregation_id_for_value, expected);
        assert_eq!(r2.unwrap().aggregation_id_for_value, expected);
    }

    #[test]
    fn quantile_matches_hydrarkll() {
        let cfg = make_config(3, "lat", "HydraKLL", "", 300, "tumbling", &[], "");
        let expected = cfg.aggregation_id();
        let configs = single_config(cfg);
        let result = find_compatible_aggregation(
            &configs,
            &req("lat", &[Statistic::Quantile], Some(300_000), &[], ""),
        );
        assert_eq!(result.unwrap().aggregation_id_for_value, expected);
    }

    #[test]
    fn no_match_wrong_metric() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("mem", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn no_match_wrong_type() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "DatasketchesKLL",
            "",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    // --- window compatibility ---

    #[test]
    fn window_tumbling_exact() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn window_tumbling_divisible() {
        // 900_000 ms / 300 s = 3 buckets — valid merge
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(900_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn window_tumbling_not_divisible() {
        // 600_000 ms / 900 s is not a whole number
        let configs = single_config(make_config(1, "cpu", "Sum", "", 900, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(600_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn window_sliding_exact() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "sliding", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn window_sliding_too_large() {
        // Query range 600 s but sliding window only covers 300 s
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "sliding", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(600_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn window_priority_largest_wins() {
        let small = make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], "");
        let large = make_config(2, "cpu", "Sum", "", 900, "tumbling", &[], "");
        let expected = large.aggregation_id();
        let mut configs = HashMap::new();
        configs.insert(small.aggregation_id(), small);
        configs.insert(large.aggregation_id(), large);
        // 900_000 ms is divisible by both 300 s and 900 s — prefer 900 s
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(900_000), &[], ""),
        );
        assert_eq!(result.unwrap().aggregation_id_for_value, expected);
    }

    #[test]
    fn spatial_only_no_range() {
        // data_range_ms = None → any window size is compatible
        let configs = single_config(make_config(1, "cpu", "Sum", "", 900, "tumbling", &[], ""));
        let result =
            find_compatible_aggregation(&configs, &req("cpu", &[Statistic::Sum], None, &[], ""));
        assert!(result.is_some());
    }

    // --- label compatibility ---

    #[test]
    fn label_strict_exact() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["job"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &["job"], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn label_superset_config_accepts_subset_query() {
        // Config has `{job, instance}`, query wants only `{job}`.
        //
        // Pre-fix `labels_compatible` did strict-eq and rejected this,
        // which broke the MVP demo (ProjectASAP/ASAPCollector#46): the
        // agent's per-zone HLL agg has `grouping_labels = [zone]`, the
        // replay client's `count(unique_users_per_min)` has no `by`
        // modifier (req grouping = `[]`). Post-fix the agg can serve
        // the broader-aggregation query — the engine's merge path
        // collapses the extra label dimension before the result
        // surface. See `labels_compatible` rustdoc.
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["job", "instance"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &["job"], ""),
        );
        assert!(
            result.is_some(),
            "post-fix: a config with `[job, instance]` grouping must serve a `[job]`-only req \
             via the merge path",
        );
    }

    #[test]
    fn label_subset_config_rejects_superset_query() {
        // Config has only `[job]`, query wants `[job, instance]`.
        // The engine cannot invent a partition the agg never
        // materialised, so this remains incompatible.
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["job"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "cpu",
                &[Statistic::Sum],
                Some(300_000),
                &["job", "instance"],
                "",
            ),
        );
        assert!(result.is_none());
    }

    #[test]
    fn label_mismatch_rejected() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &["region"],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &["job"], ""),
        );
        assert!(result.is_none());
    }

    // --- spatial filter compatibility ---

    #[test]
    fn spatial_filter_empty_both() {
        let configs = single_config(make_config(1, "cpu", "Sum", "", 300, "tumbling", &[], ""));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn spatial_filter_query_empty_config_has_filter() {
        // Config scoped to env=prod, query has no filter → reject
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    #[test]
    fn spatial_filter_same() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], "env=prod"),
        );
        assert!(result.is_some());
    }

    #[test]
    fn spatial_filter_different() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "Sum",
            "",
            300,
            "tumbling",
            &[],
            "env=prod",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Sum], Some(300_000), &[], "env=staging"),
        );
        assert!(result.is_none());
    }

    // --- sub-type ---

    #[test]
    fn sub_type_min_matches_min() {
        let configs = single_config(make_config(
            1,
            "cpu",
            "MinMax",
            "min",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Min], Some(300_000), &[], ""),
        );
        assert!(result.is_some());
    }

    #[test]
    fn sub_type_max_rejects_min() {
        // Max statistic requires sub_type == "max", but config has "min"
        let configs = single_config(make_config(
            1,
            "cpu",
            "MinMax",
            "min",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("cpu", &[Statistic::Max], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    // --- multi-population ---

    #[test]
    fn multi_pop_finds_key_agg() {
        let value_cfg =
            make_config(10, "req", "CountMinSketchWithHeap", "", 300, "tumbling", &[], "");
        let key_cfg = make_config(11, "req", "DeltaSetAggregator", "", 300, "tumbling", &[], "");
        let expected_value = value_cfg.aggregation_id();
        let expected_key = key_cfg.aggregation_id();
        let mut configs = HashMap::new();
        configs.insert(value_cfg.aggregation_id(), value_cfg);
        configs.insert(key_cfg.aggregation_id(), key_cfg);
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], Some(300_000), &[], ""),
        );
        let info = result.unwrap();
        assert_eq!(info.aggregation_id_for_value, expected_value);
        assert_eq!(info.aggregation_id_for_key, expected_key);
    }

    #[test]
    fn multi_pop_no_key_agg_returns_none() {
        // CountMinSketchWithHeap present but no SetAggregator/DeltaSetAggregator
        let configs = single_config(make_config(
            10,
            "req",
            "CountMinSketchWithHeap",
            "",
            300,
            "tumbling",
            &[],
            "",
        ));
        let result = find_compatible_aggregation(
            &configs,
            &req("req", &[Statistic::Topk], Some(300_000), &[], ""),
        );
        assert!(result.is_none());
    }

    // --- avg (Vec<Statistic>) ---

    #[test]
    fn avg_finds_sum_and_count() {
        let sum = make_config(1, "cpu", "Sum", "", 300, "tumbling", &["job"], "");
        let cnt = make_config(2, "cpu", "CountMinSketch", "", 300, "tumbling", &["job"], "");
        let mut configs = HashMap::new();
        configs.insert(sum.aggregation_id(), sum);
        configs.insert(cnt.aggregation_id(), cnt);
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "cpu",
                &[Statistic::Sum, Statistic::Count],
                Some(300_000),
                &["job"],
                "",
            ),
        );
        assert!(result.is_some());
    }

    #[test]
    fn avg_different_windows_rejected() {
        let sum = make_config(1, "cpu", "Sum", "", 300, "tumbling", &["job"], "");
        // Count config has different window_size — must be rejected
        let cnt = make_config(2, "cpu", "CountMinSketch", "", 900, "tumbling", &["job"], "");
        let mut configs = HashMap::new();
        configs.insert(sum.aggregation_id(), sum);
        configs.insert(cnt.aggregation_id(), cnt);
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "cpu",
                &[Statistic::Sum, Statistic::Count],
                Some(300_000),
                &["job"],
                "",
            ),
        );
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Source-of-truth agreement check.
    //
    // `compatible_agg_types(Statistic)` (this file) and
    // `promql_utilities::query_logics::logics::map_statistic_to_precompute_operator`
    // are two views onto the same `(Statistic, AggregationType)` capability
    // table. The planner emits configs from the canonical map; capability
    // matching dispatches queries against the compat list. They MUST agree —
    // every canonical map output for a given Statistic must be a member of
    // `compatible_agg_types(Statistic)` — or queries the planner configured
    // will silently fall through capability matching to the cold-tier
    // fallback.
    //
    // This test enumerates every supported `(Statistic, QueryTreatmentType)`
    // pair, calls the canonical map, and asserts membership. Any future edit
    // on either side that breaks the agreement fails the build.
    // -----------------------------------------------------------------------

    #[test]
    fn capability_canonical_map_agreement() {
        use promql_utilities::query_logics::enums::QueryTreatmentType;
        use promql_utilities::query_logics::logics::map_statistic_to_precompute_operator;

        // Listed exhaustively so adding a new `Statistic` variant fails to
        // compile here (forcing the author to decide its compat membership).
        let stats = [
            Statistic::Count,
            Statistic::Sum,
            Statistic::Cardinality,
            Statistic::Increase,
            Statistic::Rate,
            Statistic::Min,
            Statistic::Max,
            Statistic::Quantile,
            Statistic::Topk,
        ];
        let treatments = [QueryTreatmentType::Exact, QueryTreatmentType::Approximate];

        for &stat in &stats {
            let compat = compatible_agg_types(stat);
            for &treat in &treatments {
                match map_statistic_to_precompute_operator(stat, treat) {
                    Ok((agg_type, _sub_type)) => {
                        assert!(
                            compat.contains(&agg_type),
                            "Divergence: map_statistic_to_precompute_operator({stat:?}, {treat:?}) \
                             returns {agg_type:?}, but compatible_agg_types({stat:?}) = {compat:?} \
                             does not list it. Either add {agg_type:?} to compatible_agg_types or \
                             change the canonical map. See the docstring on \
                             compatible_agg_types for the source-of-truth invariant.",
                        );
                    }
                    Err(_) => {
                        // The canonical map declines this pair (e.g.
                        // Quantile-Exact, Cardinality, etc.). That's fine —
                        // capability_matching never sees a planner-emitted
                        // config for that pair, so there's nothing to agree on.
                    }
                }
            }
        }
    }

    /// Pin the canonical-approximator picks driving the ASAP-tier query path
    /// (the "five sketch types" CMS / KLL / HLL / DDSketch / CountSketch
    /// canonical statistic table from PROGRESS.md). HLL / DDSketch /
    /// CountSketch route via the modified-OTLP wire format and are not in the
    /// planner's canonical map; KLL covers Quantile, CMS covers Sum + Count,
    /// CMSWithHeap covers Topk. Each must appear in its `Statistic`'s compat
    /// list — this is the bug fix that motivated this PR.
    #[test]
    fn five_sketch_canonical_statistics_in_compat_list() {
        // KLL → Quantile
        assert!(
            compatible_agg_types(Statistic::Quantile).contains(&AggregationType::DatasketchesKLL),
            "KLL must be a compatible type for Quantile",
        );
        // DDSketch → Quantile (Phase-3.1 fix). Required so the MVP demo's
        // `quantile_over_time(0.99, http_requests_total_latency_ms[1m])`
        // — which routes a DDSketch agg from `mvp-workload.yaml` and may
        // miss the inference-YAML exact-string match — still resolves
        // through capability matching instead of returning a 404.
        assert!(
            compatible_agg_types(Statistic::Quantile).contains(&AggregationType::DDSketch),
            "DDSketch must be a compatible type for Quantile (Phase-3.1 fix)",
        );
        // HLL → Cardinality (Phase-3.1 fix). HLL accumulators answer
        // `Statistic::Cardinality` natively (and `Statistic::Count` as a
        // cardinality alias); enumerating them here lets capability
        // matching pick up an HLL-only deploy.
        assert!(
            compatible_agg_types(Statistic::Cardinality).contains(&AggregationType::HLL),
            "HLL must be a compatible type for Cardinality (Phase-3.1 fix)",
        );
        // CMS → Sum (the headline bug fix that motivated this PR)
        assert!(
            compatible_agg_types(Statistic::Sum).contains(&AggregationType::CountMinSketch),
            "CountMinSketch must be a compatible type for Sum (PR fix)",
        );
        // CMS → Count
        assert!(
            compatible_agg_types(Statistic::Count).contains(&AggregationType::CountMinSketch),
            "CountMinSketch must be a compatible type for Count",
        );
        // CMSWithHeap → Topk
        assert!(
            compatible_agg_types(Statistic::Topk)
                .contains(&AggregationType::CountMinSketchWithHeap),
            "CountMinSketchWithHeap must be a compatible type for Topk",
        );
        // CountSketch → Topk (warm-engine-error-on-replay-queries fix).
        // Required so the MVP demo's `topk(5, top_endpoint_qps)` —
        // which routes through the agent's `countsketchprocessor`
        // and lands as a CountSketch-only config — resolves
        // through capability matching. Without this, the warm
        // engine returned `status=error` for every topk replay row.
        assert!(
            compatible_agg_types(Statistic::Topk).contains(&AggregationType::CountSketch),
            "CountSketch must be a compatible type for Topk (warm-engine-error fix)",
        );
        // HLL → Count (warm-engine-error-on-replay-queries fix). The
        // MVP demo's `count(unique_users_per_min)` is structurally a
        // PromQL `Statistic::Count` (the AggregationOperator::Count
        // → Statistic::Count mapping in
        // `promql_utilities::query_logics::enums`); the
        // `HllSketchAccumulator` answers it as a cardinality alias
        // (`hll_sketch_accumulator.rs:220`). Without HLL listed
        // here, capability matching missed and the warm engine
        // returned `status=error` for every count-of-HLL replay row.
        assert!(
            compatible_agg_types(Statistic::Count).contains(&AggregationType::HLL),
            "HLL must be a compatible type for Count (warm-engine-error fix)",
        );
    }

    /// Phase-3.1 regression test for the canonical MVP-demo failure
    /// described in `docs/spec-mvp-controller-driven-multi-stage-demo.md`:
    /// the controller plans `http_requests_total_latency_ms` as a
    /// `DDSketch` for the `quantile_over_time(0.99,
    /// http_requests_total_latency_ms[1m])` query class. When the
    /// inference YAML doesn't include an exact-string entry for the
    /// query, `find_query_config` misses and the engine falls into
    /// capability matching. Pre-fix, `compatible_agg_types(Quantile)`
    /// listed only KLL types, so the DDSketch agg was filtered out
    /// and the ASAP-tier engine returned a 404 / null; post-fix,
    /// DDSketch is enumerated and capability matching resolves the
    /// agg cleanly.
    #[test]
    fn ddsketch_resolves_quantile_query_post_fix() {
        let cfg = make_config(
            42,
            "http_requests_total_latency_ms",
            "DDSketch",
            "",
            60,
            "tumbling",
            &[],
            "",
        );
        let expected = cfg.aggregation_id();
        let mut configs = HashMap::new();
        configs.insert(cfg.aggregation_id(), cfg);
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "http_requests_total_latency_ms",
                &[Statistic::Quantile],
                Some(60_000),
                &[],
                "",
            ),
        );
        let info = result.expect(
            "post-fix: capability matching must resolve quantile_over_time against a DDSketch-only config",
        );
        assert_eq!(info.aggregation_id_for_value, expected);
        assert_eq!(info.aggregation_type_for_value, AggregationType::DDSketch);
        // DDSketch is single-population (not is_multi_population_value_type),
        // so the matcher pairs it with itself for the key agg.
        assert_eq!(info.aggregation_id_for_key, expected);
    }

    /// Regression test for the pre-fix bug: a query for `Statistic::Sum`
    /// against a CMS-only configuration must now resolve via capability
    /// matching, not fall through to the cold tier. Pre-fix, this returned
    /// `None`; post-fix, it returns the CMS aggregation paired with the
    /// `DeltaSetAggregator` key aggregation.
    #[test]
    fn cms_resolves_sum_query_post_fix() {
        let value_cfg = make_config(
            42,
            "http_requests_total",
            "CountMinSketch",
            "sum",
            300,
            "tumbling",
            &[],
            "",
        );
        // CountMinSketch is a multi-population value type and
        // `find_compatible_aggregation` requires a paired key aggregation.
        let key_cfg = make_config(
            43,
            "http_requests_total",
            "DeltaSetAggregator",
            "",
            300,
            "tumbling",
            &[],
            "",
        );
        let expected_value = value_cfg.aggregation_id();
        let expected_key = key_cfg.aggregation_id();
        let mut configs = HashMap::new();
        configs.insert(value_cfg.aggregation_id(), value_cfg);
        configs.insert(key_cfg.aggregation_id(), key_cfg);
        let result = find_compatible_aggregation(
            &configs,
            &req(
                "http_requests_total",
                &[Statistic::Sum],
                Some(300_000),
                &[],
                "",
            ),
        );
        let info = result.expect(
            "post-fix: capability matching must resolve sum_over_time against a CMS-only config",
        );
        assert_eq!(info.aggregation_id_for_value, expected_value);
        assert_eq!(
            info.aggregation_type_for_value,
            AggregationType::CountMinSketch
        );
        assert_eq!(info.aggregation_id_for_key, expected_key);
    }

    // -----------------------------------------------------------------------
    // Phase-5: storage-backend routing
    //
    // The Phase-5 `EngineRouter` (see `asap-query-engine/src/routing/query_engine_routing.rs`)
    // consults `compatible_storage_backends(stat, accuracy, metric_storage)`
    // to pick a backend. These tests pin the routing matrix so the dispatcher
    // stays in lock-step with the design doc §8.
    // -----------------------------------------------------------------------

    #[test]
    fn gorilla_s3_metric_routes_to_archive() {
        let backends = compatible_storage_backends(
            Statistic::Sum,
            AccuracyTarget::Exact,
            StorageBackend::GorillaObjectStore,
        );
        assert_eq!(backends, vec![StorageBackend::GorillaObjectStore]);
    }

    #[test]
    fn asap_query_metric_routes_to_simple_engine() {
        let backends = compatible_storage_backends(
            Statistic::Quantile,
            AccuracyTarget::Approximate,
            StorageBackend::SketchStore,
        );
        // Step-1 of the JSONL deprecation: ASAP-tier only routes
        // to itself; the previous `ColdJsonlFallback` failover slot
        // has been deleted.
        assert_eq!(backends, vec![StorageBackend::SketchStore]);
    }

    #[test]
    fn double_write_metric_returns_both_options() {
        // Exact: archive head, ASAP-tier failover.
        let exact = compatible_storage_backends(
            Statistic::Sum,
            AccuracyTarget::Exact,
            StorageBackend::DoubleWrite,
        );
        assert_eq!(
            exact,
            vec![
                StorageBackend::GorillaObjectStore,
                StorageBackend::SketchStore,
            ]
        );
        // Approximate: ASAP-tier head (cheaper for ε/δ-bounded
        // answers), archive failover.
        let approx = compatible_storage_backends(
            Statistic::Quantile,
            AccuracyTarget::Approximate,
            StorageBackend::DoubleWrite,
        );
        assert_eq!(
            approx,
            vec![
                StorageBackend::SketchStore,
                StorageBackend::GorillaObjectStore,
            ]
        );
    }

    /// Exact-on-archive subsumes approximate-on-warm: a metric configured
    /// only for Gorilla-S3 still routes to the archive even when the caller
    /// asks for an approximate answer (no ASAP-tier sketch exists to back-
    /// fall to in that deploy shape).
    #[test]
    fn gorilla_s3_with_non_exact_accuracy_still_archives() {
        let backends = compatible_storage_backends(
            Statistic::Quantile,
            AccuracyTarget::Approximate,
            StorageBackend::GorillaObjectStore,
        );
        assert_eq!(backends, vec![StorageBackend::GorillaObjectStore]);
    }

    #[test]
    fn storage_backend_default_is_asap_tier() {
        // `#[serde(default)]` on `StreamingConfig.storage_backend` (and on
        // `StorageBackend::default()`) MUST be `SketchStore` so pre-Phase-5
        // configs decode without bumping deploys onto the archive.
        assert_eq!(StorageBackend::default(), StorageBackend::SketchStore);
    }

    #[test]
    fn storage_backend_data_source_id_is_pinned() {
        // The router registers engines by these strings; dashboards
        // byte-compare them. Pin to catch accidental rename.
        assert_eq!(
            StorageBackend::SketchStore.data_source_id(),
            ENGINE_ID_ASAP_QUERY
        );
        assert_eq!(
            StorageBackend::GorillaObjectStore.data_source_id(),
            ENGINE_ID_THANOS_QUERY,
        );
        assert_eq!(StorageBackend::DoubleWrite.data_source_id(), "double_write",);
        assert_eq!(
            StorageBackend::PrometheusRemote.data_source_id(),
            "prometheus_remote",
        );
    }

    #[test]
    fn storage_backend_engine_id_parser_accepts_only_canonical_query_engines() {
        assert_eq!(
            parse_storage_backend_engine_id(ENGINE_ID_ASAP_QUERY),
            Some(StorageBackend::SketchStore),
        );
        assert_eq!(
            parse_storage_backend_engine_id(ENGINE_ID_THANOS_QUERY),
            Some(StorageBackend::GorillaObjectStore),
        );
        assert_eq!(parse_storage_backend_engine_id("not_an_engine"), None);
    }

    /// Source-of-truth agreement check, mirrors
    /// `capability_canonical_map_agreement` for the storage axis.
    ///
    /// For every `(Statistic, AccuracyTarget, StorageBackend)` triple
    /// the returned backend list must be non-empty and its head must
    /// match the routing matrix in `compatible_storage_backends`'s
    /// docstring.
    #[test]
    fn capability_storage_backend_agreement() {
        let stats = [
            Statistic::Count,
            Statistic::Sum,
            Statistic::Cardinality,
            Statistic::Increase,
            Statistic::Rate,
            Statistic::Min,
            Statistic::Max,
            Statistic::Quantile,
            Statistic::Topk,
        ];
        let accuracies = [AccuracyTarget::Exact, AccuracyTarget::Approximate];
        let configs = [
            StorageBackend::SketchStore,
            StorageBackend::GorillaObjectStore,
            StorageBackend::DoubleWrite,
            StorageBackend::PrometheusRemote,
        ];

        for &stat in &stats {
            for &acc in &accuracies {
                for &cfg in &configs {
                    let backends = compatible_storage_backends(stat, acc, cfg);
                    assert!(
                        !backends.is_empty(),
                        "compatible_storage_backends({stat:?}, {acc:?}, {cfg:?}) returned empty \
                         — every metric configuration must route to at least one backend",
                    );
                    let last = *backends.last().unwrap();
                    assert!(
                        last == StorageBackend::SketchStore
                            || last == StorageBackend::GorillaObjectStore
                            || last == StorageBackend::PrometheusRemote,
                        "backend list for ({stat:?}, {acc:?}, {cfg:?}) must terminate in a \
                         dispatchable failover (SketchStore, GorillaObjectStore, or \
                         PrometheusRemote); got {last:?}",
                    );
                    // The expected head is determined by `(metric_storage_config, accuracy)`:
                    let expected_head = match (cfg, acc) {
                        (StorageBackend::GorillaObjectStore, _) => {
                            StorageBackend::GorillaObjectStore
                        }
                        (StorageBackend::SketchStore, _) => StorageBackend::SketchStore,
                        (StorageBackend::DoubleWrite, AccuracyTarget::Exact) => {
                            StorageBackend::GorillaObjectStore
                        }
                        (StorageBackend::DoubleWrite, AccuracyTarget::Approximate) => {
                            StorageBackend::SketchStore
                        }
                        (StorageBackend::PrometheusRemote, _) => StorageBackend::PrometheusRemote,
                    };
                    assert_eq!(
                        backends[0], expected_head,
                        "head mismatch for ({stat:?}, {acc:?}, {cfg:?}): expected {expected_head:?}, got {:?}",
                        backends[0],
                    );
                }
            }
        }
    }
}
