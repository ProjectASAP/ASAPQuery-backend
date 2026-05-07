//! Per-metric storage-backend routing table consulted by the HTTP query
//! handler at request time.
//!
//! ## Why this exists
//!
//! Phase-5 (PR #87) wired the `EngineRouter` into the HTTP query handler,
//! but the per-metric `StorageBackend` axis was sourced from
//! `StreamingConfig::storage_backend()` — a single field that applies to
//! the entire streaming config. In production deploys (the
//! `precompute_engine` binary loading `backend-streaming.yaml`) the
//! field decodes via `Self::new(...)` which always defaults to
//! `SketchWarmTier`, so the handler always took the
//! `SimpleEngine`-direct-dispatch branch and the `EngineRouter` was
//! effectively bypassed for every query — the `data_source:
//! gorilla_archive` info-line never landed on cold-archive responses
//! even when the chunks were on disk in MinIO.
//!
//! The fix lives **outside** the streaming pipeline: the streaming
//! engine on the OTLP-ingest path never sees Gorilla data (the
//! `gorillas3processor` writes chunks directly to S3), so there is
//! nothing for `StreamingConfig::from_yaml_data` to learn. What we
//! actually need is a tiny standalone routing table — one entry per
//! metric whose storage backend differs from the default — that the
//! HTTP handler consults to pick the right engine for each query.
//!
//! ## v7: dual-routing per metric
//!
//! v6.1 surfaced an architectural gap: routing one metric to one engine
//! forces an exclusive trade-off between criterion ④ (warm-tier
//! accuracy) and criterion ⑤ (cold-fallback). Every quantile/sum-by
//! query on `http_requests_total` had to go to either the warm tier
//! (so the accuracy reducer could compute relative error) or the
//! archive (so the `data_source: gorilla_archive` info-line landed on
//! the cold-fallback probe). v7 closes this by letting one metric have
//! multiple targets, each with an optional query-shape filter; the
//! HTTP handler inspects the parsed PromQL and picks the matching
//! target. Predictable / planned queries (quantile, sum_over_time)
//! land on the warm tier; ad-hoc / post-hoc queries
//! (count, topk, rate-post-hoc) route to the cold archive.
//!
//! ## Schema
//!
//! Two compatible shapes are accepted. The single-target form is
//! preserved verbatim from v6.1 so existing deploys keep working
//! unchanged:
//!
//! ```yaml
//! # v6.1 form (single-target):
//! default: sketch_warm_tier
//! metrics:
//!   audit_events: gorilla_s3_archive
//! ```
//!
//! ```yaml
//! # v7 form (multi-target with query-shape selection):
//! default: sketch_warm_tier
//! routes:
//!   - metric: http_requests_total
//!     targets:
//!       - backend: sketch_warm_tier
//!         # default — predictable / planned queries land here
//!       - backend: gorilla_s3_archive
//!         applies_to_query_shape: [count, topk, rate_post_hoc]
//!   - metric: http_freshness_probe_warm
//!     targets:
//!       - backend: sketch_warm_tier
//!   - metric: http_freshness_probe_archive
//!     targets:
//!       - backend: gorilla_s3_archive
//! ```
//!
//! The two shapes can be mixed in the same YAML — metrics under
//! `metrics:` keep the old single-target semantics; metrics under
//! `routes:` use the new list-of-targets semantics. A metric listed
//! in BOTH wins from `routes:` (multi-target overrides single-target).
//!
//! Valid `StorageBackend` values mirror the snake-cased serde tags on
//! `asap_types::StorageBackend`: `sketch_warm_tier`,
//! `gorilla_s3_archive`, `double_write`. (Step-1 of the JSONL
//! deprecation refactor removed the `cold_jsonl_fallback` tag.)
//!
//! Loaded once at backend startup (CLI flag `--backend-storage-routing`
//! on `precompute_engine`) and stored in `AppState`. Lookup is
//! O(metric-name-hash); a query that doesn't match any entry falls back
//! to `default` (which itself falls back to `SketchWarmTier`).
//!
//! ## Out of scope
//!
//! * Hot reload — the controller's plan-push is the long-term answer
//!   for per-metric routing; this YAML layer is the bridge that
//!   unblocks issue #46 criteria ④/⑤/⑥ until the plan-push lands.
//! * Per-`(metric, statistic, accuracy)` granularity — `StorageBackend`
//!   already encodes the `DoubleWrite` axis the cost-aware dispatcher
//!   uses to pick warm-vs-archive per query.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use asap_types::StorageBackend;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tracing::{debug, info};

// ---------------------------------------------------------------------------
// Query-shape taxonomy
// ---------------------------------------------------------------------------

/// A coarse-grained classification of an incoming PromQL query. The
/// HTTP handler extracts this from the parsed AST and consults the
/// routing table's `applies_to_query_shape` filters to pick a target.
///
/// The shapes intentionally mirror the v7 spec's
/// `[count, topk, rate_post_hoc]` enumeration — each is a PromQL
/// shape the cold-archive engine answers natively, and which the
/// warm-tier sketch path either can't serve at all (count over an
/// approximate sketch is misleading) or serves with worse precision
/// than the archive (rate post-hoc).
///
/// Phase α (controller-emitted routing tables) adds `HistogramQuantile`
/// / `Delta` / `Deriv` / `Absent` — these are PromQL shapes no warm-tier
/// sketch can serve and the controller's emitter reliably routes them
/// to the archive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryShape {
    /// `count(<metric>{...})` — series count after label predicates.
    /// Cold archive serves exactly via postings index; warm tier has
    /// no compatible aggregation (a CMS doesn't track per-series
    /// existence).
    Count,
    /// `topk(k, ...)` — top-k by value. Cold archive answers by
    /// streaming through chunk samples and tracking k-largest; warm
    /// tier needs a `CountMinSketchWithHeap` to answer at all.
    Topk,
    /// `rate(<metric>[<range>])` — per-second rate over a range.
    /// Marked "post-hoc" because the warm tier's pre-computed
    /// `Increase` aggregation answers `rate` natively for known
    /// queries, so this shape only kicks in for ad-hoc rate queries
    /// the controller didn't pre-plan for.
    RatePostHoc,
    /// `quantile_over_time(φ, <metric>[<range>])` and
    /// `quantile(...)` aggregations. Warm tier serves natively via
    /// DDSketch / KLL accumulators; cold archive can also serve but
    /// at higher latency.
    Quantile,
    /// `sum_over_time(...)` and `sum(...) by (...)`. Warm tier
    /// serves via a sum-typed accumulator.
    Sum,
    /// `last_over_time(<metric>[<range>])`. Used by the v6
    /// freshness probes — warm tier serves via a counter-typed
    /// accumulator (Change B), archive serves by selecting the
    /// most-recent sample in each chunk.
    LastOverTime,
    /// `histogram_quantile(φ, ...)` — Prometheus-native histogram
    /// readout. No warm-tier sketch fits the bucket-vector input
    /// shape; archive serves via post-hoc bucket scan.
    HistogramQuantile,
    /// `delta(<counter>[<range>])` — first-difference over a range.
    /// Archive-only.
    Delta,
    /// `deriv(<gauge>[<range>])` — least-squares slope of a gauge.
    /// Archive-only.
    Deriv,
    /// `absent(<metric>{...})` — 1 if no series match, vacuous
    /// vector otherwise. Archive answers natively from the postings
    /// index; warm-tier sketch has no compatible aggregation.
    Absent,
    /// Anything else — `min/max_over_time`, `count_over_time`,
    /// `avg_over_time`, etc. Lets the routing table register
    /// targets that catch the long tail without enumerating every
    /// PromQL function.
    Other,
}

impl QueryShape {
    /// Stable string tag used in YAML / JSON. Mirrors the controller's
    /// `config::stage_config::emit_backend_storage_routing` shape
    /// vocabulary — the wire form is the canonical PromQL function
    /// name (or a `_` prefixed variant for shapes without a single
    /// canonical name, e.g. `rate_post_hoc`).
    pub fn as_str(self) -> &'static str {
        match self {
            QueryShape::Count => "count",
            QueryShape::Topk => "topk",
            QueryShape::RatePostHoc => "rate_post_hoc",
            QueryShape::Quantile => "quantile",
            QueryShape::Sum => "sum",
            QueryShape::LastOverTime => "last_over_time",
            QueryShape::HistogramQuantile => "histogram_quantile",
            QueryShape::Delta => "delta",
            QueryShape::Deriv => "deriv",
            QueryShape::Absent => "absent",
            QueryShape::Other => "other",
        }
    }
}

/// Classify a parsed PromQL expression into a [`QueryShape`]. Walks
/// the AST and returns the first shape that matches any node — so
/// `topk(5, sum by (zone) (rate(http_requests_total[5m])))` is
/// classified as `Topk` (the outermost shape wins).
///
/// `RatePostHoc` is conservative: any `rate(...)` call surfaces as
/// `RatePostHoc`. The routing-table consumer can decide whether to
/// honour that or fall through to the default target.
pub fn classify_query_shape(expr: &promql_parser::parser::Expr) -> QueryShape {
    use promql_parser::parser::Expr;
    match expr {
        // Aggregations are the outermost shape — `topk(...)` wins
        // over any inner call. We use the operator's Display impl
        // (the canonical PromQL keyword: "sum", "count", "topk",
        // "quantile", ...) — Debug-formatting the underlying
        // `TokenType` returns the numeric token id, not the
        // keyword.
        Expr::Aggregate(agg) => {
            let op = agg.op.to_string().to_lowercase();
            if op == "topk" || op == "bottomk" {
                QueryShape::Topk
            } else if op == "count" || op == "count_values" {
                QueryShape::Count
            } else if op == "quantile" {
                QueryShape::Quantile
            } else if op == "sum" {
                // `sum by (...) (...)` — recurse on the inner
                // expression; if the inner is a `rate(...)` the
                // post-hoc rate path wins.
                let inner = classify_query_shape(&agg.expr);
                if matches!(inner, QueryShape::RatePostHoc) {
                    QueryShape::RatePostHoc
                } else {
                    QueryShape::Sum
                }
            } else {
                // min/max/avg/group/stddev/stdvar/...
                classify_query_shape(&agg.expr)
            }
        }
        Expr::Call(call) => {
            let name = call.func.name.to_lowercase();
            if name == "rate" || name == "irate" {
                QueryShape::RatePostHoc
            } else if name == "quantile_over_time" {
                QueryShape::Quantile
            } else if name == "sum_over_time" {
                QueryShape::Sum
            } else if name == "count_over_time" {
                QueryShape::Count
            } else if name == "last_over_time" {
                QueryShape::LastOverTime
            } else if name == "histogram_quantile" {
                QueryShape::HistogramQuantile
            } else if name == "delta" || name == "increase" {
                QueryShape::Delta
            } else if name == "deriv" {
                QueryShape::Deriv
            } else if name == "absent" || name == "absent_over_time" {
                QueryShape::Absent
            } else {
                QueryShape::Other
            }
        }
        Expr::Paren(p) => classify_query_shape(&p.expr),
        Expr::Unary(u) => classify_query_shape(&u.expr),
        Expr::Binary(bin) => {
            // Pick whichever side has the more-specific shape; a
            // `rate(...)` on either side is enough to mark
            // RatePostHoc.
            let l = classify_query_shape(&bin.lhs);
            if !matches!(l, QueryShape::Other) {
                l
            } else {
                classify_query_shape(&bin.rhs)
            }
        }
        Expr::Subquery(sq) => classify_query_shape(&sq.expr),
        // Bare vector / matrix selectors — no function applied.
        _ => QueryShape::Other,
    }
}

// ---------------------------------------------------------------------------
// On-disk YAML schema
// ---------------------------------------------------------------------------

/// One target in the v7 multi-target form.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RoutingTargetYaml {
    backend: StorageBackend,
    /// Optional list of query shapes this target applies to. When
    /// `None`, the target is the default — it catches any shape the
    /// other targets didn't claim. When `Some(list)`, the target
    /// only fires when the incoming query's shape is in `list`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    applies_to_query_shape: Option<Vec<QueryShape>>,
}

/// One row in the v7 multi-target form.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RouteYaml {
    metric: String,
    targets: Vec<RoutingTargetYaml>,
}

/// On-disk YAML schema. Public only so the loader / tests can build it
/// from literals; runtime callers should go through
/// [`BackendStorageRouting`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct BackendStorageRoutingYaml {
    /// Fallback storage backend for any metric not explicitly listed.
    /// Optional; defaults to `SketchWarmTier`.
    #[serde(default)]
    default: StorageBackend,
    /// v6.1 form — per-metric overrides keyed by the bare metric name
    /// (no labels). Each value is a single `StorageBackend`. A metric
    /// listed here keeps the v6.1 semantics: every query for the
    /// metric routes to the named backend regardless of shape.
    #[serde(default)]
    metrics: HashMap<String, StorageBackend>,
    /// v7 form — per-metric overrides as a list of `(backend,
    /// applies_to_query_shape)` targets. The HTTP handler inspects
    /// the parsed PromQL, classifies it via [`classify_query_shape`],
    /// and picks the first target whose `applies_to_query_shape`
    /// either is `None` (default) or contains the query's shape.
    /// Falls back to the first target on no match.
    #[serde(default)]
    routes: Vec<RouteYaml>,
}

// ---------------------------------------------------------------------------
// In-memory routing table
// ---------------------------------------------------------------------------

/// One target in the in-memory routing table — the runtime form of
/// [`RoutingTargetYaml`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingTarget {
    /// Storage backend the HTTP handler should dispatch to.
    pub backend: StorageBackend,
    /// Optional list of query shapes this target claims. `None` =
    /// default (always fires); `Some(list)` = this target only fires
    /// when the query's classified shape is in `list`.
    pub applies_to_query_shape: Option<Vec<QueryShape>>,
}

impl RoutingTarget {
    /// Build a target that applies to every query shape (the v6.1
    /// single-target form).
    pub fn always(backend: StorageBackend) -> Self {
        Self {
            backend,
            applies_to_query_shape: None,
        }
    }

    /// Build a target that only applies to the listed shapes.
    pub fn for_shapes(backend: StorageBackend, shapes: Vec<QueryShape>) -> Self {
        Self {
            backend,
            applies_to_query_shape: Some(shapes),
        }
    }
}

/// In-memory routing table consulted by the HTTP handler at request
/// time. Build via [`Self::from_yaml_file`] / [`Self::from_yaml_str`]
/// or [`Self::empty`] (everything routes to `SketchWarmTier`).
#[derive(Debug, Clone)]
pub struct BackendStorageRouting {
    default: StorageBackend,
    /// For each metric, the ordered list of targets the HTTP handler
    /// walks to pick a backend. v6.1 single-target rows come in as a
    /// single-element vec with `applies_to_query_shape: None`.
    metrics: HashMap<String, Vec<RoutingTarget>>,
}

impl BackendStorageRouting {
    /// Build an empty router — every metric resolves to
    /// `SketchWarmTier`. Equivalent to "no routing config at all" and
    /// preserves pre-Phase-5 dispatch (`SimpleEngine` direct path).
    pub fn empty() -> Self {
        Self {
            default: StorageBackend::default(),
            metrics: HashMap::new(),
        }
    }

    /// Construct directly from an explicit map. Used by tests; production
    /// callers go through [`Self::from_yaml_file`].
    pub fn new(default: StorageBackend, metrics: HashMap<String, Vec<RoutingTarget>>) -> Self {
        Self { default, metrics }
    }

    /// Construct from the v6.1 single-target map shape. Each entry
    /// maps to a single-target list with no query-shape filter.
    /// Preserved for tests and existing call sites; new code should
    /// use [`Self::new`] with explicit `RoutingTarget`s.
    pub fn new_from_single_targets(
        default: StorageBackend,
        metrics: HashMap<String, StorageBackend>,
    ) -> Self {
        let metrics = metrics
            .into_iter()
            .map(|(k, v)| (k, vec![RoutingTarget::always(v)]))
            .collect();
        Self { default, metrics }
    }

    /// Parse YAML text. See module docs for the schema.
    pub fn from_yaml_str(text: &str) -> Result<Self> {
        let parsed: BackendStorageRoutingYaml =
            serde_yaml::from_str(text).context("failed to parse backend-storage-routing YAML")?;

        // Start from the v6.1 single-target map.
        let mut metrics: HashMap<String, Vec<RoutingTarget>> = parsed
            .metrics
            .into_iter()
            .map(|(k, v)| (k, vec![RoutingTarget::always(v)]))
            .collect();

        // Overlay v7 multi-target rows. A metric in BOTH wins from
        // `routes:` (multi-target overrides single-target).
        for row in parsed.routes {
            if row.targets.is_empty() {
                anyhow::bail!(
                    "backend-storage-routing: metric '{}' has empty targets list",
                    row.metric,
                );
            }
            let targets = row
                .targets
                .into_iter()
                .map(|t| RoutingTarget {
                    backend: t.backend,
                    applies_to_query_shape: t.applies_to_query_shape,
                })
                .collect();
            metrics.insert(row.metric, targets);
        }

        Ok(Self {
            default: parsed.default,
            metrics,
        })
    }

    /// Read + parse a YAML file. Returns the populated router on
    /// success. The caller is expected to log the entry count at
    /// startup so operators can spot-check the deployment.
    pub fn from_yaml_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read backend-storage-routing YAML: {:?}", path))?;
        let routing = Self::from_yaml_str(&text)?;
        info!(
            path = %path.display(),
            default = ?routing.default,
            entries = routing.metrics.len(),
            multi_target_entries = routing.metrics.values().filter(|t| t.len() > 1).count(),
            "Loaded backend-storage-routing YAML",
        );
        Ok(routing)
    }

    /// Phase α (MVP): parse a controller-emitted JSON document into a
    /// fresh routing table. The schema mirrors
    /// `controller/src/config/stage_config.rs::emit_backend_storage_routing`:
    ///
    /// ```json
    /// {
    ///   "default_engine": "sketch_warm_tier",
    ///   "metrics": [
    ///     { "name": "http_requests_total",
    ///       "targets": [
    ///         { "engine": "sketch_warm_tier" },
    ///         { "engine": "thanos_archive",
    ///           "applies_to_query_shape": ["count", "topk", "rate_post_hoc",
    ///                                      "histogram_quantile", "delta", "absent"] }
    ///       ]
    ///     }
    ///   ]
    /// }
    /// ```
    ///
    /// Engine-name compatibility (controller → backend `StorageBackend`):
    ///
    /// * `sketch_warm_tier` → `SketchWarmTier`
    /// * `thanos_archive` → `GorillaS3Archive` (Phase α uses the existing
    ///   archive engine; future phases may register a real Thanos engine).
    /// * `gorilla_s3_archive` → `GorillaS3Archive` (back-compat alias).
    /// * `double_write` → `DoubleWrite`.
    ///
    /// Unknown query-shape strings are mapped to [`QueryShape::Other`]
    /// rather than failing the parse — the controller's vocabulary may
    /// drift forward of the backend's. Empty `targets` arrays are
    /// rejected (same contract as `from_yaml_str`).
    ///
    /// Side fields (e.g. `warm_tier_native_shapes`) the controller emits
    /// for operator inspection are ignored — the JSON parser pulls only
    /// `default_engine` and `metrics:[...]`.
    pub fn from_json_payload(value: &JsonValue) -> Result<Self> {
        let default_engine = value
            .get("default_engine")
            .and_then(|v| v.as_str())
            .unwrap_or("sketch_warm_tier");
        let default = parse_engine_string(default_engine).with_context(|| {
            format!(
                "backend-storage-routing JSON: invalid default_engine '{}'",
                default_engine
            )
        })?;

        let metrics_arr = value
            .get("metrics")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                anyhow::anyhow!("backend-storage-routing JSON: missing 'metrics' array")
            })?;

        let mut metrics: HashMap<String, Vec<RoutingTarget>> = HashMap::new();
        for (i, entry) in metrics_arr.iter().enumerate() {
            let name = entry
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "backend-storage-routing JSON: metrics[{}] missing 'name'",
                        i
                    )
                })?
                .to_string();
            let targets_arr = entry
                .get("targets")
                .and_then(|v| v.as_array())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "backend-storage-routing JSON: metric '{}' missing 'targets' array",
                        name,
                    )
                })?;
            if targets_arr.is_empty() {
                anyhow::bail!(
                    "backend-storage-routing JSON: metric '{}' has empty targets list",
                    name,
                );
            }
            let mut targets: Vec<RoutingTarget> = Vec::with_capacity(targets_arr.len());
            for (j, t) in targets_arr.iter().enumerate() {
                let engine_str = t
                    .get("engine")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "backend-storage-routing JSON: metric '{}' targets[{}] missing 'engine'",
                            name, j,
                        )
                    })?;
                let backend = parse_engine_string(engine_str).with_context(|| {
                    format!(
                        "backend-storage-routing JSON: metric '{}' targets[{}] invalid engine '{}'",
                        name, j, engine_str,
                    )
                })?;
                let applies_to_query_shape =
                    t.get("applies_to_query_shape").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|s| s.as_str())
                            .map(parse_query_shape_string)
                            .collect::<Vec<_>>()
                    });
                targets.push(RoutingTarget {
                    backend,
                    applies_to_query_shape,
                });
            }
            metrics.insert(name, targets);
        }

        Ok(Self { default, metrics })
    }

    /// Atomically replace this routing table's contents with `new_table`.
    /// Used by the `POST /api/v1/storage_routing` swap handler — the
    /// HTTP layer wraps the table in `arc_swap::ArcSwap` so the swap is
    /// observed atomically by all in-flight queries; this method is the
    /// in-place form that callers without an `ArcSwap` wrapper can use
    /// (e.g. tests, single-threaded shadow-mode evaluation). Production
    /// deployments should go through [`HotReloadBackendStorageRouting`]
    /// instead.
    pub fn replace(&mut self, new_table: BackendStorageRouting) {
        let added = new_table.metrics.len();
        let removed = self.metrics.len();
        *self = new_table;
        info!(
            added,
            removed,
            new_default = ?self.default,
            "BackendStorageRouting: replaced (in-place)",
        );
    }

    /// Look up the storage backend for `metric_name`, ignoring query
    /// shape. Walks the metric's target list and returns the first
    /// target's backend (the v6.1 default-target slot). Falls back
    /// to the table's `default` when the metric is not listed.
    ///
    /// Exists for v6.1 callers that haven't been threaded with a
    /// parsed PromQL query. New code on the dual-routing path
    /// should call [`Self::lookup_with_shape`].
    pub fn lookup(&self, metric_name: &str) -> StorageBackend {
        match self.metrics.get(metric_name) {
            Some(targets) => {
                let backend = targets
                    .first()
                    .map(|t| t.backend)
                    .unwrap_or(self.default);
                debug!(
                    metric = metric_name,
                    backend = ?backend,
                    target_count = targets.len(),
                    "backend-storage-routing: lookup (no shape) — picked first target",
                );
                backend
            }
            None => {
                debug!(
                    metric = metric_name,
                    default = ?self.default,
                    "backend-storage-routing: no override, using default",
                );
                self.default
            }
        }
    }

    /// Look up the storage backend for `(metric_name, query_shape)`.
    ///
    /// Walks the metric's target list and returns the backend of the
    /// first target whose `applies_to_query_shape` either is `None`
    /// (default) or contains `shape`. If no target matches (empty
    /// or all filters miss), returns the first target's backend
    /// (matches v6.1's single-target semantics for the default
    /// slot). Falls back to `self.default` when the metric is not
    /// listed.
    ///
    /// Selection ordering: v7 puts the *default* target (no filter)
    /// first in the YAML, then the shape-specific overrides; this
    /// matches v6.1 behaviour for callers that don't supply a
    /// shape, but lets shape-specific lookups skip past the default
    /// slot if any later target's filter matches `shape`. The
    /// implementation walks the list in two passes:
    ///   1. First, prefer a target whose filter explicitly includes
    ///      `shape` — this lets a `[count, topk, rate_post_hoc]`
    ///      target win over the default warm-tier slot for those
    ///      shapes.
    ///   2. If no shape-specific target matches, fall back to the
    ///      first target with `applies_to_query_shape: None`
    ///      (the default slot).
    ///   3. If even that's missing, use the first target.
    pub fn lookup_with_shape(&self, metric_name: &str, shape: QueryShape) -> StorageBackend {
        match self.metrics.get(metric_name) {
            Some(targets) => {
                // Pass 1: explicit shape match.
                for t in targets {
                    if let Some(list) = &t.applies_to_query_shape {
                        if list.contains(&shape) {
                            debug!(
                                metric = metric_name,
                                shape = ?shape,
                                backend = ?t.backend,
                                "backend-storage-routing: shape-specific match",
                            );
                            return t.backend;
                        }
                    }
                }
                // Pass 2: default slot.
                for t in targets {
                    if t.applies_to_query_shape.is_none() {
                        debug!(
                            metric = metric_name,
                            shape = ?shape,
                            backend = ?t.backend,
                            "backend-storage-routing: default-slot fallback",
                        );
                        return t.backend;
                    }
                }
                // Pass 3: the first target, regardless of filter
                // (only reachable when the metric only has
                // shape-specific targets and none matched).
                let backend = targets
                    .first()
                    .map(|t| t.backend)
                    .unwrap_or(self.default);
                debug!(
                    metric = metric_name,
                    shape = ?shape,
                    backend = ?backend,
                    "backend-storage-routing: no match; first-target fallback",
                );
                backend
            }
            None => {
                debug!(
                    metric = metric_name,
                    shape = ?shape,
                    default = ?self.default,
                    "backend-storage-routing: metric unlisted, using default",
                );
                self.default
            }
        }
    }

    /// Read-only view of the configured default. Tests use this; the
    /// HTTP handler goes through `lookup` / `lookup_with_shape`.
    pub fn default_backend(&self) -> StorageBackend {
        self.default
    }

    /// Number of explicit per-metric overrides. Tests / operator
    /// tooling.
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// `true` iff no per-metric overrides are configured. Equivalent to
    /// `Self::empty()` but preserves a custom `default`.
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }

    /// Number of targets registered for `metric_name`. Returns 0 for
    /// unlisted metrics. Tests use this to assert dual-routing is
    /// wired correctly.
    pub fn target_count(&self, metric_name: &str) -> usize {
        self.metrics.get(metric_name).map(|t| t.len()).unwrap_or(0)
    }
}

impl Default for BackendStorageRouting {
    fn default() -> Self {
        Self::empty()
    }
}

/// Map a JSON `engine` string into a backend `StorageBackend` variant.
/// Phase α accepts both the controller's vocabulary (`thanos_archive`)
/// and the existing YAML's vocabulary (`gorilla_s3_archive`) — both
/// resolve to `StorageBackend::GorillaS3Archive` because the cold-archive
/// engine registered today serves both via `GorillaQueryEngine`.
/// `unknown_engine` returns an error so a typo doesn't silently turn
/// into a default-routing footgun.
fn parse_engine_string(s: &str) -> Result<StorageBackend> {
    match s {
        "sketch_warm_tier" | "sketch_warm" => Ok(StorageBackend::SketchWarmTier),
        // `thanos_archive` is the controller-emitted name; the backend
        // currently registers the Gorilla-S3 cold archive under
        // `gorilla_archive` / `gorilla_s3_archive`. They map to the
        // same `StorageBackend` variant for Phase α — when a real
        // Thanos engine lands the parser can split the two.
        "thanos_archive" | "gorilla_s3_archive" | "gorilla_archive" => {
            Ok(StorageBackend::GorillaS3Archive)
        }
        "double_write" => Ok(StorageBackend::DoubleWrite),
        // Phase ε.2: the controller's Mode 3
        // (`RawAtEdgePrometheusArchive`) emits this when a metric's
        // raw data is shipped to Prometheus's native OTLP receiver.
        // The backend's `PrometheusForwardEngine` (in
        // `engines::prometheus::forward`) registers under this id
        // when `ASAP_PROMETHEUS_QUERY_URL` is set.
        "prometheus_remote" => Ok(StorageBackend::PrometheusRemote),
        other => Err(anyhow::anyhow!(
            "unknown engine '{}': expected one of \
             [sketch_warm_tier, thanos_archive, gorilla_s3_archive, double_write, \
             prometheus_remote]",
            other,
        )),
    }
}

/// Map a JSON `applies_to_query_shape` string into a backend
/// `QueryShape`. Unknown shapes are mapped to [`QueryShape::Other`] —
/// the controller's vocabulary may emit shape names a backend revision
/// doesn't yet understand, and `Other` is the safe fall-through (the
/// archive's claim list typically includes `Other` so unknowns still
/// route to the archive).
fn parse_query_shape_string(s: &str) -> QueryShape {
    match s {
        "count" => QueryShape::Count,
        "topk" => QueryShape::Topk,
        "rate_post_hoc" | "rate" | "irate" => QueryShape::RatePostHoc,
        "quantile" | "quantile_over_time" => QueryShape::Quantile,
        "sum" | "sum_over_time" => QueryShape::Sum,
        "last_over_time" => QueryShape::LastOverTime,
        "histogram_quantile" => QueryShape::HistogramQuantile,
        "delta" | "increase" => QueryShape::Delta,
        "deriv" => QueryShape::Deriv,
        "absent" | "absent_over_time" => QueryShape::Absent,
        _ => QueryShape::Other,
    }
}

/// Compute a stable, short hash of a `BackendStorageRouting` table for
/// the swap handler's response. The controller uses this to verify the
/// backend installed exactly the bytes it pushed (cheap drift check on
/// every plan emit).
///
/// The hash is computed over the routing table's data fields — default
/// + sorted metric → sorted target list. We sort to make the hash
/// reproducible across `HashMap` iteration orders.
pub fn routing_table_hash(table: &BackendStorageRouting) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut h = DefaultHasher::new();
    // Default tag.
    table.default.data_source_id().hash(&mut h);
    // Sort metric names for determinism.
    let mut names: Vec<&String> = table.metrics.keys().collect();
    names.sort();
    for name in names {
        name.hash(&mut h);
        let targets = &table.metrics[name];
        for t in targets {
            t.backend.data_source_id().hash(&mut h);
            if let Some(shapes) = &t.applies_to_query_shape {
                for s in shapes {
                    s.as_str().hash(&mut h);
                }
                "}".hash(&mut h);
            } else {
                "*".hash(&mut h);
            }
        }
    }
    format!("{:016x}", h.finish())
}

// ---------------------------------------------------------------------------
// Phase α: HotReloadBackendStorageRouting — atomic-swap wrapper
// ---------------------------------------------------------------------------

/// Atomic-swap wrapper around `BackendStorageRouting`, mirroring
/// [`crate::data_model::HotReloadStreamingConfig`]. Lets the
/// `POST /api/v1/storage_routing` HTTP handler swap the table at
/// runtime without restarting the backend. Cloneable; clones share the
/// underlying `ArcSwap` so all holders see the same swaps.
///
/// ## Read path
///
/// HTTP query handler calls [`Self::snapshot`] once per request to get
/// a stable `Arc<BackendStorageRouting>` it can call
/// `lookup_with_shape` on. The snapshot is cheap (single atomic load)
/// and lock-free; concurrent swaps don't block readers.
///
/// ## Write path
///
/// The swap handler calls [`Self::swap`] with the new table parsed from
/// the controller's JSON. The previous `Arc` is dropped when the last
/// in-flight reader goes out of scope.
///
/// ## Bootstrap
///
/// Built at backend startup from either:
/// * `Self::from_yaml_file(path)` — load the static
///   `deploy/configs/backend-storage-routing.yaml` (legacy
///   bootstrap; preserved for dev / standalone deployments).
/// * `Self::empty()` — start with an empty table; the controller's
///   first push fills it.
#[derive(Clone)]
pub struct HotReloadBackendStorageRouting {
    inner: std::sync::Arc<arc_swap::ArcSwap<BackendStorageRouting>>,
}

impl HotReloadBackendStorageRouting {
    /// Construct with an initial routing table.
    pub fn new(initial: BackendStorageRouting) -> Self {
        Self {
            inner: std::sync::Arc::new(arc_swap::ArcSwap::new(std::sync::Arc::new(initial))),
        }
    }

    /// Construct with an empty table — every metric resolves to
    /// `SketchWarmTier` until the first push lands.
    pub fn empty() -> Self {
        Self::new(BackendStorageRouting::empty())
    }

    /// Construct from a pre-built `Arc<BackendStorageRouting>` —
    /// avoids a redundant clone when the caller already holds one.
    pub fn from_arc(initial: std::sync::Arc<BackendStorageRouting>) -> Self {
        Self {
            inner: std::sync::Arc::new(arc_swap::ArcSwap::new(initial)),
        }
    }

    /// Cheap, cloneable snapshot of the current table. Stable for the
    /// caller's lifetime; concurrent swaps don't invalidate it.
    pub fn snapshot(&self) -> std::sync::Arc<BackendStorageRouting> {
        self.inner.load_full()
    }

    /// Atomically replace the current table. Returns the `Arc` that
    /// was just replaced for callers that want to log the diff.
    pub fn swap(&self, new: BackendStorageRouting) -> std::sync::Arc<BackendStorageRouting> {
        self.inner.swap(std::sync::Arc::new(new))
    }
}

impl std::fmt::Debug for HotReloadBackendStorageRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot();
        f.debug_struct("HotReloadBackendStorageRouting")
            .field("entries", &snap.metrics.len())
            .field("default", &snap.default)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_router_routes_everything_to_warm_tier() {
        let r = BackendStorageRouting::empty();
        assert_eq!(r.lookup("anything"), StorageBackend::SketchWarmTier);
        assert_eq!(r.lookup("http_requests_total"), StorageBackend::SketchWarmTier);
        assert_eq!(
            r.lookup_with_shape("anything", QueryShape::Count),
            StorageBackend::SketchWarmTier
        );
    }

    #[test]
    fn yaml_with_per_metric_override_routes_correctly_v6_1_form() {
        // v6.1 form: `metrics:` map. Each value is a single backend.
        let yaml = r#"
default: sketch_warm_tier
metrics:
  http_requests_total: gorilla_s3_archive
  audit_events: gorilla_s3_archive
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(
            r.lookup("http_requests_total"),
            StorageBackend::GorillaS3Archive
        );
        assert_eq!(
            r.lookup("audit_events"),
            StorageBackend::GorillaS3Archive
        );
        assert_eq!(r.lookup("unlisted"), StorageBackend::SketchWarmTier);
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn yaml_default_only_routes_all_metrics_to_default() {
        let yaml = "default: gorilla_s3_archive\n";
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(r.lookup("anything"), StorageBackend::GorillaS3Archive);
        assert!(r.is_empty());
        assert_eq!(r.default_backend(), StorageBackend::GorillaS3Archive);
    }

    #[test]
    fn yaml_omitted_default_falls_back_to_sketch_warm() {
        let yaml = "metrics:\n  foo: gorilla_s3_archive\n";
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        assert_eq!(r.lookup("foo"), StorageBackend::GorillaS3Archive);
        assert_eq!(r.lookup("bar"), StorageBackend::SketchWarmTier);
    }

    #[test]
    fn empty_yaml_is_valid_and_empty() {
        let r = BackendStorageRouting::from_yaml_str("").expect("empty parse");
        assert_eq!(r.lookup("foo"), StorageBackend::SketchWarmTier);
        assert!(r.is_empty());
    }

    #[test]
    fn invalid_yaml_returns_error() {
        let yaml = "metrics: not-a-map\n";
        assert!(BackendStorageRouting::from_yaml_str(yaml).is_err());
    }

    // ── v7 dual-routing tests ──────────────────────────────────────────

    #[test]
    fn yaml_v7_multi_target_routes_count_to_archive_quantile_to_warm() {
        // v7 form: `routes:` list. http_requests_total has TWO
        // targets — the default warm-tier slot and a cold-archive
        // slot scoped to count/topk/rate_post_hoc.
        let yaml = r#"
default: sketch_warm_tier
routes:
  - metric: http_requests_total
    targets:
      - backend: sketch_warm_tier
      - backend: gorilla_s3_archive
        applies_to_query_shape: [count, topk, rate_post_hoc]
  - metric: http_freshness_probe_warm
    targets:
      - backend: sketch_warm_tier
  - metric: http_freshness_probe_archive
    targets:
      - backend: gorilla_s3_archive
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");

        // Count + topk + rate_post_hoc → archive.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Count),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Topk),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::RatePostHoc),
            StorageBackend::GorillaS3Archive,
        );

        // Quantile + sum_over_time + everything else → warm.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Quantile),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Sum),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Other),
            StorageBackend::SketchWarmTier,
        );

        // Single-target metrics keep v6.1 semantics regardless of shape.
        assert_eq!(
            r.lookup_with_shape("http_freshness_probe_warm", QueryShape::LastOverTime),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_freshness_probe_archive", QueryShape::LastOverTime),
            StorageBackend::GorillaS3Archive,
        );
    }

    #[test]
    fn yaml_v6_1_single_target_keeps_old_semantics_under_lookup_with_shape() {
        // A v6.1 entry with no targets list — every shape resolves
        // to the same backend (no dual-routing).
        let yaml = r#"
metrics:
  audit_events: gorilla_s3_archive
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        for shape in [
            QueryShape::Count,
            QueryShape::Topk,
            QueryShape::Quantile,
            QueryShape::Sum,
            QueryShape::Other,
        ] {
            assert_eq!(
                r.lookup_with_shape("audit_events", shape),
                StorageBackend::GorillaS3Archive,
                "shape={shape:?} must resolve to gorilla_s3_archive (single-target)",
            );
        }
    }

    #[test]
    fn yaml_mixed_metrics_and_routes_routes_wins() {
        // Both `metrics:` and `routes:` populated; a metric in BOTH
        // wins from `routes:` (multi-target overrides single-target).
        let yaml = r#"
default: sketch_warm_tier
metrics:
  http_requests_total: gorilla_s3_archive
routes:
  - metric: http_requests_total
    targets:
      - backend: sketch_warm_tier
      - backend: gorilla_s3_archive
        applies_to_query_shape: [count]
"#;
        let r = BackendStorageRouting::from_yaml_str(yaml).expect("parse");
        // Default slot (warm) wins for non-count shapes.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Quantile),
            StorageBackend::SketchWarmTier,
        );
        // Count → archive.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Count),
            StorageBackend::GorillaS3Archive,
        );
        // The `metrics:` entry was overridden by the multi-target
        // `routes:` entry (the single-target archive vanished).
        assert_eq!(r.target_count("http_requests_total"), 2);
    }

    #[test]
    fn empty_targets_list_is_a_parse_error() {
        let yaml = r#"
routes:
  - metric: http_requests_total
    targets: []
"#;
        let err = BackendStorageRouting::from_yaml_str(yaml).expect_err("must reject empty list");
        assert!(err.to_string().contains("empty targets"));
    }

    #[test]
    fn lookup_falls_back_to_first_target_when_no_shape_matches() {
        // Manually-built entry where every target has a filter and
        // none matches `Quantile`. The lookup must return *some*
        // backend rather than panic.
        let mut metrics = HashMap::new();
        metrics.insert(
            "x".to_string(),
            vec![
                RoutingTarget::for_shapes(StorageBackend::GorillaS3Archive, vec![QueryShape::Count]),
                RoutingTarget::for_shapes(
                    StorageBackend::SketchWarmTier,
                    vec![QueryShape::Topk],
                ),
            ],
        );
        let r = BackendStorageRouting::new(StorageBackend::SketchWarmTier, metrics);
        // No filter matches `Quantile`; must return the first target's
        // backend.
        assert_eq!(
            r.lookup_with_shape("x", QueryShape::Quantile),
            StorageBackend::GorillaS3Archive,
        );
    }

    // ── classify_query_shape unit tests ────────────────────────────────

    fn parse(query: &str) -> promql_parser::parser::Expr {
        promql_parser::parser::parse(query).expect("parse")
    }

    #[test]
    fn classifies_count_aggregate_as_count() {
        let e = parse("count(http_requests_total{service=\"payments\"})");
        assert_eq!(classify_query_shape(&e), QueryShape::Count);
    }

    #[test]
    fn classifies_topk_as_topk() {
        let e = parse("topk(5, sum by (zone) (rate(http_requests_total[5m])))");
        assert_eq!(classify_query_shape(&e), QueryShape::Topk);
    }

    #[test]
    fn classifies_rate_call_as_rate_post_hoc() {
        let e = parse("rate(http_requests_total[5m])");
        assert_eq!(classify_query_shape(&e), QueryShape::RatePostHoc);
    }

    #[test]
    fn classifies_sum_by_rate_as_rate_post_hoc() {
        // `sum by (zone) (rate(...))` — the inner rate makes this
        // post-hoc, the outer sum doesn't change that.
        let e = parse("sum by (zone) (rate(http_requests_total[5m]))");
        assert_eq!(classify_query_shape(&e), QueryShape::RatePostHoc);
    }

    #[test]
    fn classifies_quantile_over_time_as_quantile() {
        let e = parse("quantile_over_time(0.99, http_requests_total_latency_ms[1m])");
        assert_eq!(classify_query_shape(&e), QueryShape::Quantile);
    }

    #[test]
    fn classifies_sum_by_as_sum() {
        let e = parse("sum by (zone) (http_requests_total)");
        assert_eq!(classify_query_shape(&e), QueryShape::Sum);
    }

    #[test]
    fn classifies_sum_over_time_as_sum() {
        let e = parse("sum_over_time(http_requests_total[1m])");
        assert_eq!(classify_query_shape(&e), QueryShape::Sum);
    }

    #[test]
    fn classifies_last_over_time_as_last_over_time() {
        let e = parse("last_over_time(http_freshness_probe_warm[10s])");
        assert_eq!(classify_query_shape(&e), QueryShape::LastOverTime);
    }

    #[test]
    fn classifies_bare_selector_as_other() {
        let e = parse("http_requests_total");
        assert_eq!(classify_query_shape(&e), QueryShape::Other);
    }

    // ── Phase α: from_json_payload + replace + hot-reload tests ────────

    fn fixture_json() -> serde_json::Value {
        serde_json::json!({
            "default_engine": "sketch_warm_tier",
            "metrics": [
                {
                    "name": "http_requests_total",
                    "targets": [
                        { "engine": "sketch_warm_tier" },
                        {
                            "engine": "thanos_archive",
                            "applies_to_query_shape": [
                                "histogram_quantile", "delta", "deriv",
                                "absent", "rate_post_hoc", "count"
                            ]
                        }
                    ],
                    "warm_tier_native_shapes": ["topk", "rate", "sum"]
                },
                {
                    "name": "request_latency_seconds",
                    "targets": [
                        { "engine": "sketch_warm_tier" },
                        {
                            "engine": "thanos_archive",
                            "applies_to_query_shape": [
                                "histogram_quantile", "delta", "absent",
                                "rate_post_hoc", "topk", "count"
                            ]
                        }
                    ]
                }
            ]
        })
    }

    #[test]
    fn json_payload_parses_controller_fixture() {
        let r = BackendStorageRouting::from_json_payload(&fixture_json()).expect("parse");
        assert_eq!(r.default_backend(), StorageBackend::SketchWarmTier);
        assert_eq!(r.len(), 2);

        // http_requests_total: histogram_quantile / delta / etc → archive,
        // quantile / sum / topk → warm.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::HistogramQuantile),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Delta),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Count),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Quantile),
            StorageBackend::SketchWarmTier,
        );
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::Topk),
            StorageBackend::SketchWarmTier,
        );
        // LastOverTime not in the archive's filter list → falls
        // through to the default (warm) slot.
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::LastOverTime),
            StorageBackend::SketchWarmTier,
        );
    }

    #[test]
    fn json_payload_unknown_shape_defaults_to_other() {
        let value = serde_json::json!({
            "default_engine": "sketch_warm_tier",
            "metrics": [
                {
                    "name": "x",
                    "targets": [
                        { "engine": "sketch_warm_tier" },
                        {
                            "engine": "thanos_archive",
                            "applies_to_query_shape": ["some_future_shape", "count"]
                        }
                    ]
                }
            ]
        });
        let r = BackendStorageRouting::from_json_payload(&value).expect("parse");
        // count still routes to archive; the unknown shape was mapped
        // to QueryShape::Other (silently — forward-compat).
        assert_eq!(
            r.lookup_with_shape("x", QueryShape::Count),
            StorageBackend::GorillaS3Archive,
        );
        assert_eq!(
            r.lookup_with_shape("x", QueryShape::Other),
            StorageBackend::GorillaS3Archive,
        );
    }

    #[test]
    fn json_payload_invalid_engine_errors() {
        let value = serde_json::json!({
            "default_engine": "sketch_warm_tier",
            "metrics": [{
                "name": "x",
                "targets": [{ "engine": "not_a_real_engine" }]
            }]
        });
        let err = BackendStorageRouting::from_json_payload(&value).expect_err("must reject");
        // The bad engine name appears somewhere in the error chain
        // (the parser wraps the inner `parse_engine_string` error in
        // a context that mentions the metric/target index).
        let chain_str = format!("{err:#}");
        assert!(
            chain_str.contains("not_a_real_engine"),
            "error chain must mention the bad engine: {chain_str}"
        );
    }

    #[test]
    fn json_payload_empty_targets_errors() {
        let value = serde_json::json!({
            "default_engine": "sketch_warm_tier",
            "metrics": [{ "name": "x", "targets": [] }]
        });
        let err = BackendStorageRouting::from_json_payload(&value).expect_err("must reject");
        assert!(err.to_string().contains("empty targets"));
    }

    #[test]
    fn json_payload_missing_metrics_errors() {
        let value = serde_json::json!({ "default_engine": "sketch_warm_tier" });
        let err = BackendStorageRouting::from_json_payload(&value).expect_err("must reject");
        assert!(err.to_string().contains("metrics"));
    }

    #[test]
    fn json_payload_default_engine_optional_falls_back_to_warm() {
        let value = serde_json::json!({
            "metrics": [
                { "name": "x", "targets": [{ "engine": "sketch_warm_tier" }] }
            ]
        });
        let r = BackendStorageRouting::from_json_payload(&value).expect("parse");
        assert_eq!(r.default_backend(), StorageBackend::SketchWarmTier);
    }

    #[test]
    fn json_payload_back_compat_gorilla_s3_archive_alias() {
        // An older deploy might emit the YAML's vocabulary instead of
        // `thanos_archive`; both must parse and resolve the same.
        let value = serde_json::json!({
            "default_engine": "sketch_warm_tier",
            "metrics": [{
                "name": "audit_events",
                "targets": [
                    { "engine": "gorilla_s3_archive" }
                ]
            }]
        });
        let r = BackendStorageRouting::from_json_payload(&value).expect("parse");
        assert_eq!(r.lookup("audit_events"), StorageBackend::GorillaS3Archive);
    }

    /// Phase ε.2: the controller's Mode 3
    /// (`RawAtEdgePrometheusArchive`) emits `engine: prometheus_remote`
    /// in the routing JSON for metrics whose raw data is shipped to
    /// Prometheus's native OTLP receiver. The backend's parser must
    /// accept this string and resolve to `StorageBackend::PrometheusRemote`
    /// so the dispatcher's `engine_by_id` lookup hits the registered
    /// `PrometheusForwardEngine`.
    #[test]
    fn json_payload_prometheus_remote_parses_to_prometheus_remote_backend() {
        let value = serde_json::json!({
            "default_engine": "sketch_warm_tier",
            "metrics": [{
                "name": "node_cpu_seconds_total",
                "targets": [
                    { "engine": "prometheus_remote" }
                ]
            }]
        });
        let r = BackendStorageRouting::from_json_payload(&value).expect("parse");
        assert_eq!(
            r.lookup("node_cpu_seconds_total"),
            StorageBackend::PrometheusRemote,
        );
        assert_eq!(
            StorageBackend::PrometheusRemote.data_source_id(),
            "prometheus_remote",
            "the routing-table lookup must produce the same id the engine \
             registers under so the dispatcher can find it",
        );
    }

    #[test]
    fn replace_swaps_table_in_place() {
        let mut r = BackendStorageRouting::new_from_single_targets(
            StorageBackend::SketchWarmTier,
            HashMap::from([(
                "old_metric".to_string(),
                StorageBackend::GorillaS3Archive,
            )]),
        );
        let new = BackendStorageRouting::from_json_payload(&fixture_json()).expect("parse");
        r.replace(new);
        // Old metric is gone; new metrics are visible.
        assert_eq!(r.lookup("old_metric"), StorageBackend::SketchWarmTier);
        assert_eq!(
            r.lookup_with_shape("http_requests_total", QueryShape::HistogramQuantile),
            StorageBackend::GorillaS3Archive,
        );
    }

    #[test]
    fn hot_reload_wrapper_swap_is_observed_by_clones() {
        let hr = HotReloadBackendStorageRouting::empty();
        let hr_writer = hr.clone();

        let new = BackendStorageRouting::from_json_payload(&fixture_json()).expect("parse");
        hr_writer.swap(new);

        let snap = hr.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(
            snap.lookup_with_shape("http_requests_total", QueryShape::Delta),
            StorageBackend::GorillaS3Archive,
        );
    }

    #[test]
    fn hot_reload_wrapper_concurrent_readers_see_no_torn_state() {
        use std::thread;
        let hr = HotReloadBackendStorageRouting::empty();
        let writer_hr = hr.clone();
        let writer = thread::spawn(move || {
            for i in 0..50 {
                let mut metrics = HashMap::new();
                metrics.insert(
                    format!("metric_{i}"),
                    vec![RoutingTarget::always(StorageBackend::GorillaS3Archive)],
                );
                let new = BackendStorageRouting::new(StorageBackend::SketchWarmTier, metrics);
                writer_hr.swap(new);
            }
        });
        let reader_hr = hr.clone();
        let reader = thread::spawn(move || {
            for _ in 0..200 {
                let snap = reader_hr.snapshot();
                // Snapshot must always be internally consistent —
                // either empty (initial) or one-entry (post-swap).
                let n = snap.len();
                assert!(n == 0 || n == 1, "torn snapshot: {n} entries");
            }
        });
        writer.join().unwrap();
        reader.join().unwrap();
    }

    #[test]
    fn routing_table_hash_is_stable_across_runs() {
        let r1 = BackendStorageRouting::from_json_payload(&fixture_json()).expect("parse 1");
        let r2 = BackendStorageRouting::from_json_payload(&fixture_json()).expect("parse 2");
        assert_eq!(routing_table_hash(&r1), routing_table_hash(&r2));
    }

    #[test]
    fn routing_table_hash_differs_when_table_differs() {
        let r1 = BackendStorageRouting::from_json_payload(&fixture_json()).expect("parse");
        let r2 = BackendStorageRouting::empty();
        assert_ne!(routing_table_hash(&r1), routing_table_hash(&r2));
    }

    #[test]
    fn classifies_histogram_quantile_correctly() {
        let e = parse("histogram_quantile(0.99, sum by (le) (rate(http_request_duration_bucket[5m])))");
        assert_eq!(classify_query_shape(&e), QueryShape::HistogramQuantile);
    }

    #[test]
    fn classifies_delta_correctly() {
        let e = parse("delta(http_requests_total[5m])");
        assert_eq!(classify_query_shape(&e), QueryShape::Delta);
    }

    #[test]
    fn classifies_absent_correctly() {
        let e = parse("absent(http_requests_total{job=\"x\"})");
        assert_eq!(classify_query_shape(&e), QueryShape::Absent);
    }
}
