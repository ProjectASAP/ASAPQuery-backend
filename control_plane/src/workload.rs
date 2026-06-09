//! Declarative workload registration.
//!
//! Loads a YAML file describing workloads and their assignments so the
//! control plane can pre-populate the plan store and assign workloads to
//! agents on connect without requiring an explicit HTTP `POST /api/v1/plan`.

use serde::{Deserialize, Deserializer, Serialize};
use tracing::{info, warn};

use crate::types::SketchType;

/// Aggregation role a single (metric, query-shape) pair plays in the planner.
///
/// **Why this exists** (B2 full restructure): a single metric can carry
/// MULTIPLE aggregation roles when the workload YAML registers more than
/// one PromQL shape for it. The canonical case is
/// `http_requests_total`, which the MVP demo's `mvp-workload.yaml`
/// registers three times (entries 2/3/4 of [`deploy/configs/mvp-workload.yaml`]):
///   * `sum by (zone) (http_requests_total)` → [`AggRole::Sum`]
///   * `sum by (zone) (rate(http_requests_total[5m]))` → [`AggRole::Sum`]
///     (rate binds to ExactAgg(Sum)-shaped capability)
///   * `count(http_requests_total{zone="z0"})` → [`AggRole::Count`]
///
/// Before this enum: the `WorkloadStore` was keyed by metric name alone
/// and `set(metric, …)` overwrote on collision — only the LAST entry
/// survived, so the `sum by (zone)` query (entries 2 + 3) lost its plan
/// and the data-plane refused it with `ExactAgg(Sum) capability not
/// satisfied` (the surviving plan was DDSketch from entry 1's quantile
/// shape).
///
/// After: the store is keyed by `(metric, role)` so each shape gets its
/// own plan, its own `AggregationConfig` on the backend's streaming
/// config, and its own routing-connector pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggRole {
    /// `quantile_over_time`, `quantile(...)`, `histogram_quantile(...)`,
    /// or workload entries with `sketch_family_override: DDSketch | KLL`.
    /// Routes to a quantile-shaped sketch (DDSketch / KLL).
    Quantile,
    /// Bare counter selector, `sum(...)`, `sum_over_time(...)`,
    /// `rate(...)`, `increase(...)`. All bind to ExactAgg(Sum)-shaped
    /// capability on the data plane; the streaming-config emits an
    /// `aggregation_type: Sum` rather than a sketch.
    Sum,
    /// `count(...)`, `count_over_time(...)`, `count_distinct_over_time(...)`,
    /// or workload entries with `sketch_family_override: HLL`. Routes
    /// to HLL when a sketch is appropriate, otherwise to a Sum-as-count
    /// exact-aggregation.
    Count,
    /// `topk(...)`, `topk_over_time(...)`, or workload entries with
    /// `sketch_family_override: CountSketch | CountMinSketch`. Routes
    /// to CountSketch / CMS-with-heap.
    Topk,
    /// Fallback bucket — specialized sketch families that don't fit the
    /// four shapes above (e.g. CountMinSketch frequency without a topk
    /// outer), or PromQL shapes the role-classifier can't recognise
    /// today. Preserves "unique" semantics for the (metric, role) key
    /// so multiple unrecognised entries still don't collide.
    Other,
}

impl AggRole {
    /// Stable lowercase tag for routing-connector pipeline names, log
    /// fields, and HTTP path discriminators.
    pub fn as_str(&self) -> &'static str {
        match self {
            AggRole::Quantile => "quantile",
            AggRole::Sum => "sum",
            AggRole::Count => "count",
            AggRole::Topk => "topk",
            AggRole::Other => "other",
        }
    }
}

impl std::fmt::Display for AggRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Classify a single workload entry into its `AggRole`.
///
/// Resolution order:
/// 1. **`sketch_family_override`** wins when present:
///    * `DDSketch` / `KLL` → [`AggRole::Quantile`]
///    * `HLL` → [`AggRole::Count`]
///    * `CountSketch` → [`AggRole::Topk`]
///    * `CountMinSketch` → [`AggRole::Other`] (frequency — no
///      single canonical shape; we keep it out of `Topk` so the topk
///      variant stays semantically pure for sketch-with-heap families)
/// 2. **PromQL AST classification** by outermost-function name in
///    `query_string`. Recognised function tokens:
///    * `quantile_over_time` / `quantile` / `histogram_quantile` → [`AggRole::Quantile`]
///    * `sum` / `sum_over_time` / `rate` / `increase` → [`AggRole::Sum`]
///    * `count` / `count_over_time` / `count_distinct_over_time` → [`AggRole::Count`]
///    * `topk` / `topk_over_time` → [`AggRole::Topk`]
///    * Bare metric selector (no outer function) → [`AggRole::Sum`]
///      (matches PromQL's instant-vector semantics — a bare counter
///      sums values across time-aligned samples).
/// 3. Fallback: [`AggRole::Other`].
///
/// Ambiguous case decisions (documented for the B2 PR):
///   * `count_over_time(metric)` — Count (cardinality semantics).
///     The Sum-shaped alternative is rare in practice; users who want
///     it write `sum_over_time(count(...))` which classifies as Sum.
///   * `rate` / `increase` — Sum. Both bind to ExactAgg(Sum) on the
///     data plane (see `data_plane/src/precompute_engine/ingest_handler.rs`'s
///     handling of `AggKind::ExactAgg { Sum }`).
pub fn derive_agg_role(entry: &WorkloadEntry) -> AggRole {
    // 1. `sketch_family_override` wins.
    if let Some(family) = entry.sketch_family_override.as_ref() {
        return match family {
            SketchType::DDSketch | SketchType::KLL => AggRole::Quantile,
            SketchType::HLL => AggRole::Count,
            SketchType::CountSketch => AggRole::Topk,
            SketchType::CountMinSketch => AggRole::Other,
        };
    }

    // 2. PromQL AST classification — outermost-token sniff. We don't
    //    need a full AST walk: PromQL function calls always lead with
    //    `<name>(`, so the leading identifier carries the shape. For
    //    nested aggregations the OUTERMOST one drives the role (it's
    //    the one bound to the data-plane capability).
    if let Some(qs) = entry.query_string.as_ref() {
        let trimmed = qs.trim_start();
        // Find the leading identifier — letters / underscores up to
        // the first non-identifier char (`(`, space, `{`, etc.).
        let token_end = trimmed
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .unwrap_or(trimmed.len());
        let leading = &trimmed[..token_end];
        if !leading.is_empty() {
            match leading {
                "quantile_over_time" | "quantile" | "histogram_quantile" => {
                    return AggRole::Quantile
                }
                "sum" | "sum_over_time" | "rate" | "irate" | "increase" => return AggRole::Sum,
                "count" | "count_over_time" | "count_distinct_over_time" => return AggRole::Count,
                "topk" | "topk_over_time" => return AggRole::Topk,
                _ => {}
            }
        }
        // Bare metric selector — no outer function call. The leading
        // token is a metric name (or empty if the query starts with a
        // brace). Treat as Sum (PromQL's default instant-vector
        // interpretation aligns with Sum-shaped capability).
        if !trimmed.is_empty() && !trimmed.starts_with('{') {
            return AggRole::Sum;
        }
    }

    // 3. No query_string and no override — default to Sum (Mode-3
    //    raw-passthrough entries in the workload registry typically
    //    declare a bare metric with `assign_to_role: archive` and no
    //    `query_string`).
    AggRole::Other
}

/// A single workload entry from the workloads YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadEntry {
    /// Metric name this workload targets (e.g. `http_request_duration_seconds`).
    pub metric_name: String,
    /// PromQL / SQL query string for the planner.
    #[serde(default)]
    pub query_string: Option<String>,
    /// Required accuracy SLA (0.0 – 1.0).
    #[serde(default = "default_accuracy_sla")]
    pub accuracy_sla: f64,
    /// Role that should receive this workload (e.g. `"agent"`, `"backend"`).
    #[serde(default = "default_role")]
    pub assign_to_role: String,
    /// Optional explicit sketch family override. When set, the planner pins
    /// this family for the metric (modulo `(sketch, statistic)` validity
    /// per `sketch_algebra::capability_matching::is_valid_pair`). Threaded
    /// into `QueryWorkload::sketch_type_override` by the registry pre-pop
    /// path so the typed L4 binding (`bind_workload_typed`) honours it.
    ///
    /// MVP-§46 contract entries 5–8 in `deploy/configs/mvp-workload.yaml`
    /// rely on this field to pin HLL / CountSketch / CountMinSketch
    /// against metrics whose name-classified statistic class is
    /// `Cardinality` / `TopK` / `Frequency`.
    #[serde(default, deserialize_with = "deserialize_sketch_family")]
    pub sketch_family_override: Option<SketchType>,
    /// Optional storage tier hint (e.g. `"warm"`, `"archive"`). Round-trips
    /// silently for now — kept here so the YAML schema matches the
    /// capability_matching agent's expected shape (no rename step at
    /// integration). Not yet read by the planner.
    #[serde(default)]
    pub target_path: Option<String>,
    /// MVP blocker B3 — declarative grouping labels for the wire-attr
    /// allowlist the agent applies before sketching. Mirrors the
    /// streaming-config's `grouping_labels` contract: the agent's
    /// `transform/keep_for_<metric>` OTTL processor calls
    /// `keep_keys(datapoint.attributes, [...])` on this list, stripping
    /// every other attr BEFORE the sketch processor mints sids.
    ///
    /// Why this is a separate field (not parsed from `query_string`):
    /// the canonical MVP workload `quantile_over_time(0.99,
    /// http_requests_total_latency_ms[30s])` carries no `by (...)`
    /// clause, so the PromQL parser surfaces an EMPTY group_by_labels.
    /// Without a declarative field the analyzer ends up with an empty
    /// `QueryWorkload.group_by_labels` → an empty `keep_keys` list →
    /// the agent strips ALL attrs and mints a single sid per metric
    /// (instead of one per `(metric, zone)`), defeating the streaming-
    /// config contract.
    ///
    /// Threaded into `QueryWorkload::group_by_labels` by the registry
    /// pre-pop loop in `main`, so it merges with any `by (...)` keys
    /// the PromQL parser surfaces. Empty / missing ⇒ same behaviour as
    /// pre-B3 (no allowlist injected).
    #[serde(default)]
    pub grouping_labels: Vec<String>,
    /// Optional per-metric sketch sampling probability `p` in `(0, 1]`.
    ///
    /// Activates the warm-sketch sampling layer (geometric admission for
    /// the frequency families / hash-threshold for HLL) the agent's
    /// sketch processors carry: the encoder admits a `p` fraction of
    /// updates, stores the RAW sampled state, stamps `p` on the
    /// `SketchEnvelope`, and the backend rescales count-like estimates by
    /// `1/p` at query time. `1.0` (the default — also the value `0`/unset
    /// normalises to) disables sampling so the emitted agent config and
    /// wire bytes are byte-identical to today.
    ///
    /// Today this is a static operator-set knob; a dynamic
    /// optimizer-driven `p` (tuned from runtime samples against an
    /// accuracy/bandwidth budget) is a follow-up and is intentionally out
    /// of scope here.
    ///
    /// Threaded into `EdgeStageConfig::metric_to_sample_p` by the registry
    /// pre-pop loop in `main`, which the L5 edge emitter reads in
    /// `build_edge_processor_block` and writes onto the per-metric
    /// sketch-processor block (`sample_p`) only when `< 1.0`.
    #[serde(default = "default_sample_p")]
    pub sample_p: f64,
    /// Optional **known distinct key (item) count per flush window** for the
    /// cardinality / frequency sketch families — the declarative twin of
    /// [`crate::types::WorkloadCharacteristics::distinct_keys_per_window`].
    ///
    /// Today this refines ONLY the HLL sparse-vs-dense base selection at the
    /// edge: a *per-series* HLL is emitted sparse by default (PR #358), but a
    /// per-series HLL whose cardinality is known to exceed the in-memory
    /// sparse→dense promotion point pays only promotion churn from the sparse
    /// base, so when this hint is `Some(n)` with `n` above that crossover the
    /// L5 emitter emits it dense instead (completing the #358 follow-up).
    ///
    /// Threaded into `EdgeStageConfig::metric_to_distinct_keys` by
    /// [`crate::emit::collect_metric_to_distinct_keys`] (registry pre-pop loop
    /// in `main` + the replan companion), which the L5 `asap_edge` emitter
    /// reads in `emit_edge_yaml_asap_edge`'s HLL branch. `None` / missing ⇒
    /// the scope-based default of PR #358 (per-series ⇒ sparse), so the
    /// emitted config stays byte-identical when the hint is absent.
    #[serde(default)]
    pub distinct_keys_per_window: Option<u64>,
    /// Optional **inner high-cardinality dimension** for the item-counting
    /// sketch families (HLL / CountSketch / CountMinSketch): the data-point
    /// attribute whose VALUE is the "item" the sketch counts/ranks, as
    /// opposed to a [`grouping_labels`](Self::grouping_labels) key that
    /// splits the sketch into per-group series.
    ///
    /// Why this is a separate declarative field (not derivable from the
    /// metric name or `query_string`): the inner dimension is a producer
    /// data-point attribute name (`user_id` for the HLL metric
    /// `unique_users_per_min`, `endpoint` for the CountSketch metric
    /// `top_endpoint_qps` and the Count-Min metric `endpoint_request_freq`).
    /// Neither the metric NAME nor the PromQL carries it — `count(...)` /
    /// `topk(...)` / `rate(...)` name no attribute. Without a declarative
    /// field the high-cardinality inner attribute stays in the sketch's
    /// series key (one cardinality-1 HLL per `user_id` instead of one HLL
    /// per zone), so the HLL/CMS warm queries return semantically wrong /
    /// empty results.
    ///
    /// Threaded into `EdgeStageConfig::metric_to_item_label` by the registry
    /// pre-pop loop in `main` (and the replan companion stitch), which the
    /// L5 edge emitter reads in `emit_edge_yaml_asap_edge` and writes onto
    /// the per-metric sketch entry as `item_label`. For CountSketch the
    /// emitter falls back to the metric-name convention
    /// (`countsketch_item_label_for`) when this field is unset, preserving
    /// the prior behaviour. `None` / missing ⇒ no `item_label` is emitted
    /// for HLL/CMS (backward-compatible).
    #[serde(default)]
    pub item_label: Option<String>,
}

fn default_accuracy_sla() -> f64 {
    0.01
}

/// Default per-metric sampling probability — `1.0` (sampling disabled,
/// exact). Keeps the emitted config byte-identical to pre-sampling when a
/// workload entry omits `sample_p`.
fn default_sample_p() -> f64 {
    1.0
}
fn default_role() -> String {
    "agent".into()
}

/// Case-insensitive `SketchType` deserialiser. The wire YAML in
/// `deploy/configs/mvp-workload.yaml` spells the variants in mixed case
/// (`KLL`, `HLL`, `CountSketch`, `CountMinSketch`, `DDSketch`) to match
/// the capability-matching agent's schema, while `SketchType`'s
/// `#[serde(rename_all = "lowercase")]` would otherwise reject those
/// strings. Accepts both spellings.
fn deserialize_sketch_family<'de, D>(deserializer: D) -> Result<Option<SketchType>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    let Some(s) = opt else { return Ok(None) };
    let kind = match s.trim().to_ascii_lowercase().as_str() {
        "ddsketch" => SketchType::DDSketch,
        "kll" => SketchType::KLL,
        "hll" => SketchType::HLL,
        "countsketch" => SketchType::CountSketch,
        "countminsketch" | "countmin" | "cms" => SketchType::CountMinSketch,
        other => {
            return Err(serde::de::Error::custom(format!(
                "unknown sketch_family_override `{other}`; expected one of \
             DDSketch / KLL / HLL / CountSketch / CountMinSketch"
            )))
        }
    };
    Ok(Some(kind))
}

/// Registry of declarative workloads loaded from a YAML file.
#[derive(Debug, Clone)]
pub struct WorkloadRegistry {
    entries: Vec<WorkloadEntry>,
}

impl WorkloadRegistry {
    /// Load from a YAML file. Returns an empty registry on any error.
    pub fn load(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => match serde_yaml::from_str::<Vec<WorkloadEntry>>(&contents) {
                Ok(entries) => {
                    info!(path, count = entries.len(), "loaded workload registry");
                    Self { entries }
                }
                Err(e) => {
                    warn!(path, error = %e, "invalid workloads YAML; using empty registry");
                    Self { entries: vec![] }
                }
            },
            Err(_) => {
                info!(path, "workloads file not found; using empty registry");
                Self { entries: vec![] }
            }
        }
    }

    /// Create an empty registry (no file).
    pub fn empty() -> Self {
        Self { entries: vec![] }
    }

    /// Create a registry from in-memory entries (useful for tests and
    /// programmatic construction).
    pub fn from_entries(entries: Vec<WorkloadEntry>) -> Self {
        Self { entries }
    }

    /// Returns all workload entries.
    pub fn entries(&self) -> &[WorkloadEntry] {
        &self.entries
    }

    /// Returns workload entries assigned to a given role.
    pub fn for_role(&self, role: &str) -> Vec<&WorkloadEntry> {
        self.entries
            .iter()
            .filter(|e| e.assign_to_role.eq_ignore_ascii_case(role))
            .collect()
    }

    /// Returns the first workload entry for a given role, if any.
    pub fn first_for_role(&self, role: &str) -> Option<&WorkloadEntry> {
        self.entries
            .iter()
            .find(|e| e.assign_to_role.eq_ignore_ascii_case(role))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_empty_on_missing_file() {
        let reg = WorkloadRegistry::load("/nonexistent/workloads.yaml");
        assert!(reg.entries().is_empty());
    }

    #[test]
    fn empty_registry() {
        let reg = WorkloadRegistry::empty();
        assert!(reg.entries().is_empty());
        assert!(reg.first_for_role("agent").is_none());
    }

    #[test]
    fn deserialize_entries() {
        let yaml = r#"
- metric_name: latency
  query_string: "histogram_quantile(0.99, rate(http_duration_bucket[5m]))"
  accuracy_sla: 0.01
  assign_to_role: agent
- metric_name: error_count
  accuracy_sla: 0.05
  assign_to_role: backend
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].metric_name, "latency");
        assert_eq!(entries[0].assign_to_role, "agent");
        assert!(entries[0].query_string.is_some());
        assert_eq!(entries[1].metric_name, "error_count");
        assert!(entries[1].query_string.is_none());
    }

    #[test]
    fn for_role_filters_correctly() {
        let reg = WorkloadRegistry {
            entries: vec![
                WorkloadEntry {
                    metric_name: "a".into(),
                    query_string: None,
                    accuracy_sla: 0.01,
                    assign_to_role: "agent".into(),
                    sketch_family_override: None,
                    target_path: None,
                    grouping_labels: vec![],
                    sample_p: 1.0,
                    distinct_keys_per_window: None,
                    item_label: None,
                },
                WorkloadEntry {
                    metric_name: "b".into(),
                    query_string: None,
                    accuracy_sla: 0.05,
                    assign_to_role: "backend".into(),
                    sketch_family_override: None,
                    target_path: None,
                    grouping_labels: vec![],
                    sample_p: 1.0,
                    distinct_keys_per_window: None,
                    item_label: None,
                },
                WorkloadEntry {
                    metric_name: "c".into(),
                    query_string: None,
                    accuracy_sla: 0.02,
                    assign_to_role: "agent".into(),
                    sketch_family_override: None,
                    target_path: None,
                    grouping_labels: vec![],
                    sample_p: 1.0,
                    distinct_keys_per_window: None,
                    item_label: None,
                },
            ],
        };
        assert_eq!(reg.for_role("agent").len(), 2);
        assert_eq!(reg.for_role("backend").len(), 1);
        assert_eq!(reg.first_for_role("agent").unwrap().metric_name, "a");
    }

    #[test]
    fn deserialize_sample_p_default_and_explicit() {
        // sample_p is optional and defaults to 1.0 (sampling disabled);
        // an explicit value round-trips. This is the operator-facing
        // per-metric sampling knob.
        let yaml = r#"
- metric_name: freq_metric
  sketch_family_override: CountMinSketch
  sample_p: 0.1
- metric_name: card_metric
  sketch_family_override: HLL
  sample_p: 0.25
- metric_name: unset_metric
  sketch_family_override: HLL
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 3);
        assert!((entries[0].sample_p - 0.1).abs() < 1e-12);
        assert!((entries[1].sample_p - 0.25).abs() < 1e-12);
        // Unset ⇒ default 1.0 (sampling disabled / byte-identical).
        assert!((entries[2].sample_p - 1.0).abs() < 1e-12);
    }

    #[test]
    fn deserialize_item_label_default_and_explicit() {
        // item_label is optional (None by default) and round-trips when
        // declared. It names the inner high-cardinality data-point
        // attribute the HLL/CountSketch/CMS family counts or ranks
        // (e.g. user_id / endpoint), as opposed to a grouping_labels key.
        let yaml = r#"
- metric_name: unique_users_per_min
  sketch_family_override: HLL
  grouping_labels: [zone]
  item_label: user_id
- metric_name: endpoint_request_freq
  sketch_family_override: CountMinSketch
  grouping_labels: [zone]
  item_label: endpoint
- metric_name: http_requests_total_latency_ms
  sketch_family_override: KLL
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].item_label.as_deref(), Some("user_id"));
        assert_eq!(entries[1].item_label.as_deref(), Some("endpoint"));
        // Unset ⇒ None (no inner item dimension; byte-identical to before).
        assert_eq!(entries[2].item_label, None);
    }

    #[test]
    fn deserialize_sketch_family_override_mixed_case() {
        // The live wire YAML in `deploy/configs/mvp-workload.yaml` spells
        // the override values in mixed case (KLL / HLL / CountSketch /
        // CountMinSketch). Verify deserialization picks them up — without
        // this, MVP §46 entries 5–8 silently drop their family override
        // (the original stitching-gap symptom).
        let yaml = r#"
- metric_name: a
  sketch_family_override: KLL
- metric_name: b
  sketch_family_override: HLL
- metric_name: c
  sketch_family_override: CountSketch
- metric_name: d
  sketch_family_override: CountMinSketch
- metric_name: e
  sketch_family_override: DDSketch
- metric_name: f
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(entries.len(), 6);
        assert_eq!(entries[0].sketch_family_override, Some(SketchType::KLL));
        assert_eq!(entries[1].sketch_family_override, Some(SketchType::HLL));
        assert_eq!(
            entries[2].sketch_family_override,
            Some(SketchType::CountSketch)
        );
        assert_eq!(
            entries[3].sketch_family_override,
            Some(SketchType::CountMinSketch)
        );
        assert_eq!(
            entries[4].sketch_family_override,
            Some(SketchType::DDSketch)
        );
        assert_eq!(entries[5].sketch_family_override, None);
    }

    // ── AggRole tests (B2 full restructure) ──────────────────────────────

    fn entry(metric: &str, q: Option<&str>, override_: Option<SketchType>) -> WorkloadEntry {
        WorkloadEntry {
            metric_name: metric.into(),
            query_string: q.map(|s| s.to_string()),
            accuracy_sla: 0.01,
            assign_to_role: "agent".into(),
            sketch_family_override: override_,
            target_path: None,
            grouping_labels: vec![],
            sample_p: 1.0,
            distinct_keys_per_window: None,
            item_label: None,
        }
    }

    #[test]
    fn agg_role_override_pins_quantile_for_dd_and_kll() {
        assert_eq!(
            derive_agg_role(&entry("m", None, Some(SketchType::DDSketch))),
            AggRole::Quantile
        );
        assert_eq!(
            derive_agg_role(&entry("m", None, Some(SketchType::KLL))),
            AggRole::Quantile
        );
    }

    #[test]
    fn agg_role_override_pins_count_for_hll() {
        assert_eq!(
            derive_agg_role(&entry("m", None, Some(SketchType::HLL))),
            AggRole::Count
        );
    }

    #[test]
    fn agg_role_override_pins_topk_for_countsketch() {
        assert_eq!(
            derive_agg_role(&entry("m", None, Some(SketchType::CountSketch))),
            AggRole::Topk
        );
    }

    #[test]
    fn agg_role_override_pins_other_for_cms() {
        // CountMinSketch is a frequency estimator without an inherent
        // topk shape — keep it in `Other` so the topk variant stays
        // pure for sketch-with-heap families.
        assert_eq!(
            derive_agg_role(&entry("m", None, Some(SketchType::CountMinSketch))),
            AggRole::Other
        );
    }

    #[test]
    fn agg_role_quantile_query_strings() {
        for q in [
            "quantile_over_time(0.99, m[5m])",
            "quantile(0.5, m)",
            "histogram_quantile(0.99, rate(m_bucket[5m]))",
        ] {
            assert_eq!(
                derive_agg_role(&entry("m", Some(q), None)),
                AggRole::Quantile,
                "query `{q}` should classify as Quantile"
            );
        }
    }

    #[test]
    fn agg_role_sum_query_strings() {
        for q in [
            "sum by (zone) (m)",
            "sum_over_time(m[5m])",
            "rate(m[5m])",
            "increase(m[5m])",
            "sum by (zone) (rate(m[5m]))",
        ] {
            assert_eq!(
                derive_agg_role(&entry("m", Some(q), None)),
                AggRole::Sum,
                "query `{q}` should classify as Sum"
            );
        }
    }

    #[test]
    fn agg_role_count_query_strings() {
        for q in [
            "count(m)",
            "count_over_time(m[5m])",
            r#"count(m{zone="z0"})"#,
        ] {
            assert_eq!(
                derive_agg_role(&entry("m", Some(q), None)),
                AggRole::Count,
                "query `{q}` should classify as Count"
            );
        }
    }

    #[test]
    fn agg_role_topk_query_strings() {
        for q in ["topk(5, m)", "topk_over_time(3, m[5m])"] {
            assert_eq!(
                derive_agg_role(&entry("m", Some(q), None)),
                AggRole::Topk,
                "query `{q}` should classify as Topk"
            );
        }
    }

    #[test]
    fn agg_role_bare_metric_selector_is_sum() {
        assert_eq!(
            derive_agg_role(&entry(
                "http_requests_total",
                Some("http_requests_total"),
                None
            )),
            AggRole::Sum
        );
    }

    #[test]
    fn agg_role_no_query_string_no_override_is_other() {
        assert_eq!(derive_agg_role(&entry("m", None, None)), AggRole::Other);
    }

    #[test]
    fn agg_role_override_beats_query_string() {
        // An explicit `sketch_family_override: HLL` paired with a
        // `count(...)` query — both happen to classify as Count, but
        // we exercise the override priority with a deliberately
        // mismatched pair (override KLL on a count query) to pin the
        // override-first rule.
        let e = entry("m", Some("count(m)"), Some(SketchType::KLL));
        assert_eq!(derive_agg_role(&e), AggRole::Quantile);
    }

    #[test]
    fn deserialize_sketch_family_override_lowercase_aliases() {
        // Lowercase / kebab-case spellings also accepted, plus the two
        // CMS aliases (`countmin`, `cms`).
        let yaml = r#"
- metric_name: a
  sketch_family_override: ddsketch
- metric_name: b
  sketch_family_override: countmin
- metric_name: c
  sketch_family_override: cms
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            entries[0].sketch_family_override,
            Some(SketchType::DDSketch)
        );
        assert_eq!(
            entries[1].sketch_family_override,
            Some(SketchType::CountMinSketch)
        );
        assert_eq!(
            entries[2].sketch_family_override,
            Some(SketchType::CountMinSketch)
        );
    }

    #[test]
    fn three_synthetic_http_requests_total_entries_classify_to_two_distinct_roles() {
        // Synthetic mirror of `deploy/configs/mvp-workload.yaml`
        // entries 2/3/4 — proves `derive_agg_role` produces distinct
        // roles for the three http_requests_total shapes. Pre-B2 the
        // workload store collapsed these onto one key and only the
        // last entry's plan survived; the (metric, role) keyed store
        // + per-entry role classification fixes that.
        let entries = vec![
            entry(
                "http_requests_total",
                Some("sum by (zone) (http_requests_total)"),
                None,
            ),
            entry(
                "http_requests_total",
                Some("sum by (zone) (rate(http_requests_total[5m]))"),
                None,
            ),
            entry(
                "http_requests_total",
                Some(r#"count(http_requests_total{zone="z0"})"#),
                None,
            ),
        ];
        let roles: Vec<AggRole> = entries.iter().map(derive_agg_role).collect();
        assert_eq!(roles, vec![AggRole::Sum, AggRole::Sum, AggRole::Count]);
        // The store distinguishes Sum vs Count keys, so two of the
        // three entries (the two Sum-shaped ones) still collide
        // under (metric, role). That's the documented behaviour —
        // two YAML entries with the SAME (metric, role) overwrite,
        // which is the legitimate "operator updated their workload"
        // path. The fix scope is collisions across DIFFERENT shapes,
        // not idempotent re-registers.
        let distinct: std::collections::HashSet<_> = roles.iter().copied().collect();
        assert_eq!(distinct.len(), 2, "Sum + Count = 2 distinct roles");
    }

    #[test]
    fn live_mvp_workload_yaml_assigns_three_roles_to_http_requests_total() {
        // B2 full restructure regression: the live
        // `deploy/configs/mvp-workload.yaml` carries THREE entries for
        // `http_requests_total` (entries 2/3/4 — sum/sum+rate/count).
        // Pre-B2 these collapsed onto one workload-store key and
        // dropped two of the three plans, so the `sum by (zone)`
        // query returned `ExactAgg(Sum) capability not satisfied`.
        //
        // The fix is the `(metric, role)` key + the per-entry role
        // classification via `derive_agg_role`. This test pins that
        // the three entries classify to two distinct roles (`Sum` for
        // entries 2 + 3, `Count` for entry 4) — the multi-row keyed
        // store can persist them all simultaneously.
        use std::path::PathBuf;
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("deploy/configs/mvp-workload.yaml");
        if !path.exists() {
            // Live file not in this checkout; skip silently (matches
            // the sibling override test below).
            return;
        }
        let registry = WorkloadRegistry::load(path.to_str().unwrap());
        let http_requests_entries: Vec<&WorkloadEntry> = registry
            .entries()
            .iter()
            .filter(|e| e.metric_name == "http_requests_total")
            .collect();
        assert!(
            http_requests_entries.len() >= 3,
            "mvp-workload.yaml is expected to carry ≥3 entries for \
             http_requests_total (sum, sum(rate), count); got {}",
            http_requests_entries.len()
        );
        let roles: Vec<AggRole> = http_requests_entries
            .iter()
            .map(|e| derive_agg_role(e))
            .collect();
        // At least one Sum and at least one Count among the entries.
        assert!(
            roles.contains(&AggRole::Sum),
            "expected ≥1 Sum-role entry among http_requests_total in \
             mvp-workload.yaml; got {roles:?}"
        );
        assert!(
            roles.contains(&AggRole::Count),
            "expected ≥1 Count-role entry among http_requests_total in \
             mvp-workload.yaml; got {roles:?}"
        );
    }

    #[test]
    fn live_mvp_workload_yaml_loads_with_overrides() {
        // Smoke-test the live deploy file. Confirms entries 5–8 carry
        // their `sketch_family_override` after deserialization (the
        // original stitching gap was this field being silently ignored
        // by `serde`'s unknown-field default behaviour).
        use std::path::PathBuf;
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.pop();
        path.push("deploy/configs/mvp-workload.yaml");
        if !path.exists() {
            // Live file not in this checkout; skip silently.
            return;
        }
        let registry = WorkloadRegistry::load(path.to_str().unwrap());
        let by_name: std::collections::HashMap<&str, &WorkloadEntry> = registry
            .entries()
            .iter()
            .map(|e| (e.metric_name.as_str(), e))
            .collect();

        assert_eq!(
            by_name
                .get("request_size_bytes")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::KLL),
            "request_size_bytes must carry KLL override",
        );
        assert_eq!(
            by_name
                .get("unique_users_per_min")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::HLL),
            "unique_users_per_min must carry HLL override",
        );
        assert_eq!(
            by_name
                .get("top_endpoint_qps")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::CountSketch),
            "top_endpoint_qps must carry CountSketch override",
        );
        assert_eq!(
            by_name
                .get("endpoint_request_freq")
                .and_then(|e| e.sketch_family_override.clone()),
            Some(SketchType::CountMinSketch),
            "endpoint_request_freq must carry CountMinSketch override",
        );
    }
}
