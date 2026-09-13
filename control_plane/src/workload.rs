//! Declarative workload registration.
//!
//! Loads a YAML file describing workloads and their assignments so the
//! control plane can pre-populate the plan store and assign workloads to
//! agents on connect without requiring an explicit HTTP `POST /api/v1/plan`.

use serde::{Deserialize, Deserializer, Serialize};
use tracing::{info, warn};

use crate::types::SketchType;
use planner_types::pre_asap::AggIntent;

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
    /// `topk(...)`, or workload entries with
    /// `sketch_family_override: CountSketch | CountMinSketch`. Routes
    /// to CountSketch / CMS-with-heap. (`topk_over_time(...)` isn't a
    /// real function this parser recognizes — dropped from this list;
    /// see `agg_role_topk_query_strings`'s test comment.)
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
/// 2. **Real `AggIntent` classification** — `query_string` is parsed
///    through the same canonical pipeline the live serving path uses
///    (`query_parser::parse_query_expr_canonical` →
///    `asap_tier_analysis::collect_agg_intents`), and the OUTERMOST
///    intent (the one bound to the data-plane capability) is matched:
///    * [`AggIntent::Quantile`] → [`AggRole::Quantile`]
///    * [`AggIntent::TopK`] → [`AggRole::Topk`]
///    * [`AggIntent::Cardinality`], [`AggIntent::Count`], or the
///      windowed-Count-as-Frequency extension
///      (`intent_algebra::as_frequency`) → [`AggRole::Count`]
///    * [`AggIntent::Sum`], [`AggIntent::Rate`], [`AggIntent::Increase`]
///      → [`AggRole::Sum`]
///    * Anything else recognised but not one of the four shapes above
///      (`Min`/`Max`/`Avg`/`StdDev`/histogram accessors/…) →
///      [`AggRole::Other`].
///    * Bare metric selector (no `Aggregate` node at all) →
///      [`AggRole::Sum`] (matches PromQL's instant-vector semantics —
///      a bare counter sums values across time-aligned samples).
/// 3. Unparseable / no query string / no override → [`AggRole::Other`].
///
/// This used to be a PromQL-string leading-token sniff — the same
/// duplicate-classifier smell already retired from the live serving path
/// (see the former `analyzer-parity-matrix.md`'s "engine (duplicate)"
/// analyzer). `AggIntent` classification is now the single source of
/// truth for "what shape is this query," here and on the serving path.
///
/// Ambiguous case decisions (documented for the B2 PR, still holds under
/// `AggIntent` classification):
///   * `count_over_time(metric)` — Count (cardinality/frequency
///     semantics, whichever the lowerer picks). The Sum-shaped
///     alternative is rare in practice; users who want it write
///     `sum_over_time(count(...))` which classifies as Sum.
///   * `rate` / `irate` / `increase` — Sum. `irate` folds onto
///     `AggIntent::Rate` at L3 same as `rate`; both bind to
///     ExactAgg(Increase) on the data plane (see
///     `data_plane/src/precompute_engine/ingest_handler.rs`'s handling
///     of `AggKind::ExactAgg { Increase }`).
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

    // 2. Real AggIntent classification via the canonical parse/lower
    //    pipeline — the same one `capability_for`/serving uses. Canonical
    //    structural lowering is owned by ASAPPlanner's frontend.
    let Some(qs) = entry.query_string.as_ref() else {
        return AggRole::Other;
    };
    let accuracy = crate::types::accuracy_target_from_legacy_accuracy_sla(entry.accuracy_sla);
    let Ok(expr) = crate::query_parser::parse_query_expr_canonical(qs, accuracy) else {
        return AggRole::Other;
    };
    let mut intents: Vec<AggIntent> = Vec::new();
    collect_agg_intents(&expr, &mut intents);
    let Some(outer) = intents.first() else {
        // No Aggregate node at all — a bare metric selector (or a
        // window-only shape with no AggType to map onto). Bare
        // selectors default to Sum per PromQL's instant-vector
        // semantics; anything else falls through to Other below.
        let trimmed = qs.trim_start();
        if !trimmed.is_empty() && !trimmed.starts_with('{') {
            return AggRole::Sum;
        }
        return AggRole::Other;
    };
    if crate::planner_selection::as_frequency(outer).is_some() {
        return AggRole::Count;
    }
    match outer {
        AggIntent::Quantile { .. } => AggRole::Quantile,
        AggIntent::TopK { .. } => AggRole::Topk,
        AggIntent::Cardinality { .. } | AggIntent::Count { .. } => AggRole::Count,
        AggIntent::Sum { .. } | AggIntent::Rate | AggIntent::Increase => AggRole::Sum,
        _ => AggRole::Other,
    }
}

fn collect_agg_intents(expr: &planner_types::pre_asap::QueryExpr, out: &mut Vec<AggIntent>) {
    use planner_types::pre_asap::QueryExpr;
    match expr {
        QueryExpr::Aggregate {
            measures, child, ..
        } => {
            out.extend(measures.iter().cloned());
            collect_agg_intents(child, out);
        }
        QueryExpr::Filter { child, .. }
        | QueryExpr::Project { child, .. }
        | QueryExpr::Dedup { child, .. }
        | QueryExpr::Sort { child, .. }
        | QueryExpr::Limit { child, .. }
        | QueryExpr::PromqlSubquery { child, .. }
        | QueryExpr::TimeRange { child, .. }
        | QueryExpr::TimeShift { child, .. }
        | QueryExpr::SQLWindowFunc { child, .. } => collect_agg_intents(child, out),
        QueryExpr::Concat { children, .. } => {
            for child in children {
                collect_agg_intents(child, out);
            }
        }
        QueryExpr::Join { left, right, .. }
        | QueryExpr::SetOp { left, right, .. }
        | QueryExpr::BinaryOp {
            lhs: left,
            rhs: right,
            ..
        } => {
            collect_agg_intents(left, out);
            collect_agg_intents(right, out);
        }
        _ => {}
    }
}

/// A single workload entry from the workloads YAML file.
///
/// `deny_unknown_fields`: a misspelled or unsupported key is a planning input
/// the controller cannot honour. Accepting it silently would let the operator
/// believe a declared cadence / hint reached the planner when it never left the
/// YAML, so the registry rejects the file instead (see
/// [`WorkloadRegistry::try_load`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// by ASAPPlanner's legal candidate enumeration). Threaded
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

    /// Optional continuous-monitoring (CDM) declaration: when set, this metric
    /// becomes a monitored standing query — the controller auto-emits the
    /// backend coordinator's `monitors:` entry (authoritative τ) and, in
    /// future, the edge `threshold:` block. `None` / missing ⇒ no monitor.
    #[serde(default)]
    pub monitor: Option<MonitorDecl>,

    /// Optional evaluation cadence for this declared query class, in the same
    /// `1h5m30s` spelling [`crate::pipeline::parse_duration`] accepts.
    ///
    /// Why this exists: the cadence is a **cost input**, not a scheduler knob.
    /// [`crate::physical::deployment_cost::delta`] uses it as the batch-mode
    /// flush-period proxy, so the same declaration costed through the HTTP
    /// `POST /api/v1/plan` path (whose [`crate::pipeline::QuerySpec`] has
    /// carried `repeat_every` all along) and through this YAML registry used
    /// to reach the planner with *different* flush periods — the startup path
    /// hardcoded `None`. Threaded into `QuerySpec::repeat_every` by the
    /// registry pre-pop loop in `main`, which the analyzer parses into
    /// [`crate::types::QueryWorkload::repeat_every`].
    ///
    /// `None` / missing ⇒ unchanged behaviour (the cost model falls back to
    /// its window-derived flush rate).
    #[serde(default)]
    pub repeat_every: Option<String>,
}

/// User-facing continuous-monitoring declaration on a [`WorkloadEntry`]. τ/ε and
/// the window are authoritative at the coordinator; this is the controller's
/// source for emitting them. See `crate::emit::monitor`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorDecl {
    /// Threshold τ the global aggregate is monitored against.
    pub tau: f64,
    /// Additive functional: "sum" (default), "cms_point", or "linear_buckets".
    #[serde(default)]
    pub functional: String,
    /// CMS point-frequency key x (functional = cms_point).
    #[serde(default)]
    pub key: String,
    /// Relative tolerance ε (the alert fires at (1−ε)τ).
    #[serde(default = "default_monitor_epsilon")]
    pub epsilon: f64,
    /// Tumbling epoch length in seconds; MUST match the metric's edge window.
    pub window_secs: u64,
}

fn default_monitor_epsilon() -> f64 {
    0.05
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
    /// Runtime-injected entries — e.g. from the autonomous `/api/v1/plan/auto`
    /// apply path. Kept SEPARATE from the static YAML `entries` so the static
    /// accessors (`entries()`, `for_role()`) are byte-for-byte unchanged; only
    /// monitor emission ([`monitor_intents`]) overlays these. `Arc<RwLock<…>>`
    /// so cloned registries (AppState + Replanner hold clones) share the same
    /// runtime injections.
    runtime: std::sync::Arc<std::sync::RwLock<Vec<WorkloadEntry>>>,
}

impl WorkloadRegistry {
    /// Collect the continuous-monitoring intents declared across all workload
    /// entries, resolved against the given coordinator endpoint. The controller
    /// feeds these to the backend `monitors:` emitter (and, later, the edge
    /// `threshold:` emitter); each carries the content-addressed agg_id derived
    /// from the metric name so it lines up with the edge's per-window sketch.
    pub fn monitor_intents(
        &self,
        coordinator_url: &str,
    ) -> Vec<crate::emit::monitor::MonitorIntent> {
        use crate::emit::monitor::{Functional, MonitorIntent};
        let runtime = self.runtime.read().expect("runtime registry lock poisoned");
        self.entries
            .iter()
            .chain(runtime.iter())
            .filter_map(|e| {
                let m = e.monitor.as_ref()?;
                Some(MonitorIntent {
                    metric: e.metric_name.clone(),
                    functional: Functional::from_name(&m.functional),
                    key: m.key.clone(),
                    coeffs: Vec::new(),
                    coordinator_url: coordinator_url.to_string(),
                    tau: m.tau,
                    epsilon: m.epsilon,
                    window_ms: m.window_secs.saturating_mul(1000),
                })
            })
            .collect()
    }

    /// Inject (or replace, keyed by `metric_name`) a runtime workload entry.
    /// Used by the autonomous-allocation apply path to register a synthesized
    /// monitor so the next replan/repost emits it into the backend
    /// `StreamingConfig` (the coordinator then derives the ε-floor `p`). Shared
    /// across registry clones via the `Arc<RwLock<…>>` overlay.
    pub fn insert_runtime(&self, entry: WorkloadEntry) {
        let mut rt = self
            .runtime
            .write()
            .expect("runtime registry lock poisoned");
        rt.retain(|e| e.metric_name != entry.metric_name);
        rt.push(entry);
    }

    /// Load from a YAML file. Returns an empty registry on any error.
    ///
    /// Prefer [`Self::try_load`] on the startup path: a registry that parses
    /// into nothing is indistinguishable, at every later step, from an
    /// operator who declared no workloads at all.
    pub fn load(path: &str) -> Self {
        match Self::try_load(path) {
            Ok(registry) => registry,
            Err(error) => {
                warn!(path, error = %error, "invalid workloads YAML; using empty registry");
                Self::empty()
            }
        }
    }

    /// Load from a YAML file, reporting an unusable file instead of degrading
    /// to an empty registry.
    ///
    /// A **missing** file stays non-fatal — the controller is expected to run
    /// without a declarative registry (the process e2e tests boot it with
    /// `CONTROLLER_WORKLOADS` pointing at a path that does not exist). A file
    /// that exists but does not parse is fatal: it carries planning input the
    /// operator wrote down, and silently planning *nothing* from it has the
    /// same observable shape as a controller that planned everything.
    pub fn try_load(path: &str) -> Result<Self, String> {
        let contents = match std::fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                info!(path, "workloads file not found; using empty registry");
                return Ok(Self::empty());
            }
            Err(error) => return Err(format!("cannot read workload registry: {error}")),
        };
        let entries = serde_yaml::from_str::<Vec<WorkloadEntry>>(&contents)
            .map_err(|error| format!("cannot parse workload registry: {error}"))?;
        info!(path, count = entries.len(), "loaded workload registry");
        Ok(Self {
            entries,
            runtime: Default::default(),
        })
    }

    /// Create an empty registry (no file).
    pub fn empty() -> Self {
        Self {
            entries: vec![],
            runtime: Default::default(),
        }
    }

    /// Create a registry from in-memory entries (useful for tests and
    /// programmatic construction).
    pub fn from_entries(entries: Vec<WorkloadEntry>) -> Self {
        Self {
            entries,
            runtime: Default::default(),
        }
    }

    /// Returns all workload entries.
    pub fn entries(&self) -> &[WorkloadEntry] {
        &self.entries
    }

    #[cfg(test)]
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

    /// An unsupported key is planning input the controller cannot honour;
    /// accepting the file would report a cadence / hint that never left the YAML.
    #[test]
    fn unknown_registry_key_is_rejected() {
        let path = std::env::temp_dir().join(format!(
            "asap_workload_unknown_key_{}.yaml",
            std::process::id()
        ));
        std::fs::write(
            &path,
            "- metric_name: http_requests_total
  accuracy_sla: 0.99
  repeat_evry: 30s
",
        )
        .unwrap();
        let error = WorkloadRegistry::try_load(path.to_str().unwrap())
            .expect_err("unknown key must not load");
        assert!(error.contains("repeat_evry"), "unexpected error: {error}");
        // The lenient wrapper still degrades, which is why startup uses `try_load`.
        assert!(WorkloadRegistry::load(path.to_str().unwrap())
            .entries()
            .is_empty());
        std::fs::remove_file(&path).unwrap();
    }

    /// Running without a declarative registry stays supported: the process e2e
    /// tests boot the controller with `CONTROLLER_WORKLOADS` pointing nowhere.
    #[test]
    fn missing_registry_file_is_not_a_startup_failure() {
        let registry = WorkloadRegistry::try_load("/definitely/missing/workloads.yaml")
            .expect("a missing registry is not an error");
        assert!(registry.entries().is_empty());
    }

    #[test]
    fn load_empty_on_missing_file() {
        let reg = WorkloadRegistry::load("/nonexistent/workloads.yaml");
        assert!(reg.entries().is_empty());
    }

    #[test]
    fn monitor_intents_collected_from_workload_decls() {
        // Only the entry carrying a `monitor:` block produces an intent; the
        // intent's agg_id MUST match the Go edge's fnv64 for the same metric.
        let yaml = r#"
- metric_name: bytes_sent
  accuracy_sla: 0.95
  assign_to_role: agent
  monitor:
    tau: 1000.0
    functional: sum
    window_secs: 60
- metric_name: plain_metric
  accuracy_sla: 0.95
  assign_to_role: agent
"#;
        let entries: Vec<WorkloadEntry> = serde_yaml::from_str(yaml).expect("parse workload yaml");
        let reg = WorkloadRegistry::from_entries(entries);
        let intents = reg.monitor_intents("data-plane:4319");
        assert_eq!(intents.len(), 1, "only the entry with a monitor decl");
        let i = &intents[0];
        assert_eq!(i.metric, "bytes_sent");
        assert_eq!(i.tau, 1000.0);
        assert_eq!(i.window_ms, 60_000);
        assert_eq!(i.epsilon, 0.05); // serde default
        assert_eq!(i.coordinator_url, "data-plane:4319");
        assert!(matches!(
            i.functional,
            crate::emit::monitor::Functional::Sum
        ));
        let entry = crate::emit::monitor::streaming_config_monitor_entry(i);
        assert_eq!(
            entry["agg_id"].as_u64().unwrap(),
            crate::emit::monitor::agg_id_for_metric("bytes_sent")
        );
    }

    #[test]
    fn empty_registry() {
        let reg = WorkloadRegistry::empty();
        assert!(reg.entries().is_empty());
        assert!(reg.first_for_role("agent").is_none());
    }

    #[test]
    fn runtime_injected_monitor_is_emitted() {
        // autonomous apply path: insert a runtime entry carrying a monitor and
        // assert monitor_intents() overlays it (the static accessors stay empty).
        let reg = WorkloadRegistry::empty();
        assert!(reg.monitor_intents("dp:4319").is_empty());
        reg.insert_runtime(WorkloadEntry {
            metric_name: "auto_latency".into(),
            query_string: None,
            accuracy_sla: 0.01,
            assign_to_role: "agent".into(),
            sketch_family_override: Some(crate::types::SketchType::DDSketch),
            target_path: None,
            grouping_labels: vec![],
            sample_p: 1.0,
            distinct_keys_per_window: None,
            item_label: None,
            monitor: Some(MonitorDecl {
                tau: 7000.0,
                functional: "sum".into(),
                key: String::new(),
                epsilon: 0.05,
                window_secs: 30,
            }),
            repeat_every: None,
        });
        let intents = reg.monitor_intents("dp:4319");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].metric, "auto_latency");
        assert_eq!(intents[0].tau, 7000.0);
        assert_eq!(intents[0].epsilon, 0.05);
        // static accessors untouched by the runtime overlay
        assert!(reg.entries().is_empty());
        // replace-by-metric: re-inserting the same metric does not duplicate
        reg.insert_runtime(WorkloadEntry {
            metric_name: "auto_latency".into(),
            query_string: None,
            accuracy_sla: 0.01,
            assign_to_role: "agent".into(),
            sketch_family_override: Some(crate::types::SketchType::DDSketch),
            target_path: None,
            grouping_labels: vec![],
            sample_p: 1.0,
            distinct_keys_per_window: None,
            item_label: None,
            monitor: Some(MonitorDecl {
                tau: 9000.0,
                functional: "sum".into(),
                key: String::new(),
                epsilon: 0.05,
                window_secs: 30,
            }),
            repeat_every: None,
        });
        let intents = reg.monitor_intents("dp:4319");
        assert_eq!(intents.len(), 1, "replaced, not duplicated");
        assert_eq!(intents[0].tau, 9000.0);
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
        let reg = WorkloadRegistry::from_entries(vec![
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
                monitor: None,
                repeat_every: None,
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
                monitor: None,
                repeat_every: None,
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
                monitor: None,
                repeat_every: None,
            },
        ]);
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
            monitor: None,
            repeat_every: None,
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
        for q in ["quantile_over_time(0.99, m[5m])", "quantile(0.5, m)"] {
            assert_eq!(
                derive_agg_role(&entry("m", Some(q), None)),
                AggRole::Quantile,
                "query `{q}` should classify as Quantile"
            );
        }
    }

    #[test]
    fn agg_role_classic_bucket_histogram_quantile_is_other() {
        // Classic `_bucket` + `rate(...)` histogram quantiles lower to the
        // exact-only `HistogramQuantile` intent, classified here as `Other`.
        assert_eq!(
            derive_agg_role(&entry(
                "m",
                Some("histogram_quantile(0.99, rate(m_bucket[5m]))"),
                None
            )),
            AggRole::Other
        );
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
    fn agg_role_handles_a_parenthesized_query_the_old_string_sniff_could_not() {
        // A leading-token PromQL-string sniff (the pre-migration
        // implementation) reads the FIRST character to find the outer
        // function name; a redundant wrapping paren isn't an identifier
        // character, so the old code's `token_end` search finds an
        // empty leading token, falls through to "not a bare selector
        // either" and mis-defaults to `Sum`. Real `AggIntent`
        // classification parses the whole expression regardless of
        // surface punctuation and correctly resolves the inner
        // `quantile_over_time` shape.
        assert_eq!(
            derive_agg_role(&entry("m", Some("(quantile_over_time(0.9, m[5m]))"), None)),
            AggRole::Quantile
        );
    }

    #[test]
    fn agg_role_topk_query_strings() {
        // `topk_over_time` was in this list pre-migration, but it isn't a
        // real function this parser (or vanilla PromQL) recognizes at
        // all — confirmed via grep, it appears nowhere in
        // query_parser/promql.rs or intent_algebra/lower.rs. A workload
        // entry with that query_string would fail to parse anywhere else
        // in the real pipeline too (main.rs's handle_plan included), so
        // the old string-sniffing heuristic classifying it as Topk was
        // itself the bug, not something this test should keep pinning.
        // ASAPPlanner distinguishes heavy-hitter TopK from generic PromQL
        // ranking. Only the former is a sketchable TopK intent.
        {
            let q = "topk(5, count_over_time(m[1m]))";
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
        let entries = [
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
}
