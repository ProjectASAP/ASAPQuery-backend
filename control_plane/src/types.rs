use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

// ── Workload characteristics ───────────────────────────────────────────────────

/// Hint about the statistical distribution of keys in the data stream.
/// Affects fill-rate estimation and therefore delta compression projections.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataDistribution {
    /// Zipf-distributed keys (s ≈ 1.1).  A small number of keys dominate,
    /// so only a fraction of sketch cells are touched per window.  This is
    /// the typical production case.
    #[default]
    Zipf,
    /// All keys are equally probable.  Every window fills the sketch more
    /// uniformly; delta compression benefit is lower.
    Uniform,
    /// Traffic arrives in bursts with a concentrated key set.  Effective
    /// fill rate is lower on average but spikes can reach Uniform levels.
    Bursty,
}

/// Observable characteristics of the incoming data stream.
///
/// Callers supply these alongside a [`QueryWorkload`] so the planner can
/// compare raw vs. sketch-full vs. sketch-delta transmission costs and
/// estimate the CPU / memory overhead at the SDK or agent collector.
///
/// All fields have conservative defaults so callers can omit the struct
/// entirely and still get a valid (if pessimistic) decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadCharacteristics {
    /// Number of distinct active time series for this metric.
    pub series_count: u64,
    /// Sample rate per series at the SDK / agent (Hz).
    pub samples_per_sec_per_series: f64,
    /// Wire size of one raw OTLP metric data point after protobuf encoding
    /// (bytes).  Typical range: 50–200 bytes.
    pub bytes_per_raw_sample: u32,
    /// Known distinct key values per flush period for frequency / cardinality
    /// sketches.  `None` → inferred analytically from inserts and distribution.
    pub distinct_keys_per_window: Option<u64>,
    /// Statistical distribution of keys in the stream.
    pub data_distribution: DataDistribution,
    /// Optional memory cap at the SDK / agent collector (bytes).
    /// `None` → no budget constraint applied.
    pub memory_budget_bytes: Option<u64>,
}

impl Default for WorkloadCharacteristics {
    fn default() -> Self {
        Self {
            series_count: 1_000,
            samples_per_sec_per_series: 100.0,
            bytes_per_raw_sample: 100,
            distinct_keys_per_window: None,
            data_distribution: DataDistribution::Zipf,
            memory_budget_bytes: None,
        }
    }
}

// ── Delta transmission decision ────────────────────────────────────────────────

/// Reason the planner chose not to enable delta encoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaSkipReason {
    /// Estimated fill rate is so high that delta compression ratio < 2×,
    /// making snapshot overhead unjustifiable.
    FillRateTooHigh,
    /// Snapshot memory for all series × sketches would exceed the configured
    /// memory budget at the agent.
    MemoryBudgetExceeded,
    /// This sketch type has no delta implementation (e.g. KLL).
    SketchTypeUnsupported,
    /// Compression ratio fell below the minimum acceptable threshold.
    CompressionRatioBelowThreshold,
}

/// Reason the planner chose raw pass-through over sketch transmission.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawDataReason {
    /// Series count × sample rate is so small that sketch CPU / memory
    /// overhead is not justified by the bandwidth savings.
    WorkloadTooSmall,
}

/// The control plane's resolved decision on transmission mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum DeltaDecision {
    /// Enable delta-compressed sketch payloads.
    UseDelta {
        /// Minimum absolute cell change included in a delta payload (T).
        threshold: f64,
        /// Estimated compression ratio (full_bytes / delta_bytes).
        estimated_compression_ratio: f64,
        /// Estimated outbound bandwidth with delta enabled (bytes/sec).
        estimated_delta_bytes_per_sec: f64,
        /// Additional CPU at the agent per sample due to snapshot diff and
        /// sparse encoding (µs/sample), amortised over the flush period.
        delta_cpu_overhead_micros_per_sample: f64,
        /// Additional memory at the agent for storing snapshots (bytes).
        delta_memory_overhead_bytes: f64,
    },
    /// Transmit full (non-delta) sketch payloads each flush.
    UseFullSketch {
        reason: DeltaSkipReason,
        /// Estimated outbound bandwidth with full sketches (bytes/sec).
        estimated_full_bytes_per_sec: f64,
    },
    /// Skip sketch aggregation; pass raw OTLP samples through.
    UseRaw {
        reason: RawDataReason,
        /// Estimated outbound bandwidth with raw samples (bytes/sec).
        estimated_raw_bytes_per_sec: f64,
    },
}

impl Default for DeltaDecision {
    fn default() -> Self {
        DeltaDecision::UseFullSketch {
            reason: DeltaSkipReason::CompressionRatioBelowThreshold,
            estimated_full_bytes_per_sec: 0.0,
        }
    }
}

/// Bandwidth and overhead estimates for all three transmission strategies.
/// Carried on every [`CollectionPlan`] for observability and debugging.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransmissionCostSummary {
    /// Raw OTLP pass-through bandwidth (bytes/sec).
    pub raw_bytes_per_sec: f64,
    /// Full-sketch transmission bandwidth (bytes/sec).
    pub sketch_full_bytes_per_sec: f64,
    /// Delta-sketch transmission bandwidth (bytes/sec); 0 if delta not viable.
    pub sketch_delta_bytes_per_sec: f64,
    /// Extra CPU at the agent per sample in delta mode (µs/sample).
    pub delta_cpu_overhead_micros_per_sample: f64,
    /// Extra memory at the agent for delta snapshots (bytes).
    pub delta_memory_overhead_bytes: f64,
    /// Estimated fill rate (fraction of sketch cells changed per flush).
    pub estimated_fill_rate: f64,
    /// Flush rate derived from window_duration or repeat_every (Hz).
    pub flush_rate_hz: f64,
}

// ── Enumerations ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggType {
    Quantile,
    Cardinality,
    Frequency,
}

impl std::fmt::Display for AggType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggType::Quantile => write!(f, "quantile"),
            AggType::Cardinality => write!(f, "cardinality"),
            AggType::Frequency => write!(f, "frequency"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SketchType {
    DDSketch,
    KLL,
    HLL,
    CountSketch,
    CountMinSketch,
}

impl std::fmt::Display for SketchType {
    /// Returns the OTel Collector component type string (must match the Go
    /// factory's `component.MustNewType(…)` in each processor).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SketchType::DDSketch => write!(f, "ddsketch"),
            SketchType::KLL => write!(f, "KLL"),
            SketchType::HLL => write!(f, "HLL"),
            SketchType::CountSketch => write!(f, "countsketch"),
            SketchType::CountMinSketch => write!(f, "countmin"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OutputMode {
    Raw,
    Sketch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessorMode {
    Batch,
    Window,
}

impl std::fmt::Display for ProcessorMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessorMode::Batch => write!(f, "batch"),
            ProcessorMode::Window => write!(f, "window"),
        }
    }
}

// ── Core types ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct QueryWorkload {
    pub metric_name: String,
    pub label_filters: HashMap<String, String>,
    pub group_by_labels: Vec<String>,
    pub aggregations: Vec<AggType>,
    pub time_window: Duration,
    pub repeat_every: Option<Duration>,
    pub accuracy_sla: f64,
    pub latency_sla: Option<Duration>,
    /// When set, the planner must use this sketch type instead of running
    /// the cost model. Allows pinning for collectors that support a subset.
    pub sketch_type_override: Option<SketchType>,
    /// When true, sketches offer no benefit and the plan must use raw
    /// pass-through (SP-2–SP-4 collapse to raw-preservation).
    /// Set for stateful per-sample queries (RSI, MACD, stochastic, SUM).
    pub exact_required: bool,
    /// Quantile φ targets implied by the query (e.g. [0.5] for TWAP,
    /// [0.0, 1.0] for price range).  Empty for non-quantile workloads.
    pub quantiles: Vec<f64>,
}

// ── Sketch defaults (YAML-configurable) ──────────────────────────────────────

/// Per-sketch-type default parameters.  Loaded from a YAML config file at
/// startup; falls back to compile-time defaults when the file is absent.
///
/// Example `sketch_params_default.yml`:
/// ```yaml
/// quantile_grid: [0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0]
/// ddsketch:
///   relative_accuracy: 0.01
/// kll:
///   min_k: 32
/// hll:
///   precision_coarse: 10
///   precision_fine: 14
///   precision_threshold: 0.02
/// count_sketch:
///   epsilon: 0.022
///   delta: 0.007
/// count_min_sketch:
///   rows: 5
///   cols: 2048
///   metric_name: "countsketch_partition"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SketchDefaults {
    /// Quantile grid used when no query-specific φ values are available.
    pub quantile_grid: Vec<f64>,
    pub ddsketch: DDSketchDefaults,
    pub kll: KLLDefaults,
    pub hll: HLLDefaults,
    pub count_sketch: CountSketchDefaults,
    pub count_min_sketch: CountMinSketchDefaults,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DDSketchDefaults {
    pub relative_accuracy: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct KLLDefaults {
    /// Minimum k value (clamped from 1/accuracy_sla).
    pub min_k: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HLLDefaults {
    /// Precision for coarse SLA (accuracy > threshold).
    pub precision_coarse: u32,
    /// Precision for fine SLA (accuracy ≤ threshold).
    pub precision_fine: u32,
    /// SLA boundary between coarse and fine precision.
    pub precision_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CountSketchDefaults {
    /// Relative error bound (ε ≈ 1/√cols).
    pub epsilon: f64,
    /// Error probability (δ ≈ e^(−rows)).
    pub delta: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CountMinSketchDefaults {
    pub rows: u32,
    pub cols: u32,
    pub metric_name: String,
}

impl Default for SketchDefaults {
    fn default() -> Self {
        Self {
            quantile_grid: vec![0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0],
            ddsketch: DDSketchDefaults::default(),
            kll: KLLDefaults::default(),
            hll: HLLDefaults::default(),
            count_sketch: CountSketchDefaults::default(),
            count_min_sketch: CountMinSketchDefaults::default(),
        }
    }
}

impl Default for DDSketchDefaults {
    fn default() -> Self {
        Self {
            relative_accuracy: 0.01,
        }
    }
}

impl Default for KLLDefaults {
    fn default() -> Self {
        Self { min_k: 32 }
    }
}

impl Default for HLLDefaults {
    fn default() -> Self {
        Self {
            precision_coarse: 10,
            precision_fine: 14,
            precision_threshold: 0.02,
        }
    }
}

impl Default for CountSketchDefaults {
    fn default() -> Self {
        Self {
            epsilon: 0.022,
            delta: 0.007,
        }
    }
}

impl Default for CountMinSketchDefaults {
    fn default() -> Self {
        Self {
            rows: 5,
            cols: 2048,
            metric_name: "countsketch_partition".into(),
        }
    }
}

impl SketchDefaults {
    /// Load from a YAML file, falling back to compiled defaults on any error.
    pub fn load(path: &str) -> Self {
        match std::fs::read_to_string(path) {
            Ok(contents) => serde_yaml::from_str(&contents).unwrap_or_else(|e| {
                tracing::warn!(path, error = %e, "invalid sketch_defaults YAML; using built-in defaults");
                Self::default()
            }),
            Err(_) => Self::default(),
        }
    }
}

/// Per-sketch-type parameters.  Each variant carries only the fields relevant
/// to that sketch family, avoiding the "bag of unrelated fields" problem.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SketchParams {
    DDSketch {
        relative_accuracy: f64,
        quantiles: Vec<f64>,
    },
    KLL {
        k: u32,
        quantiles: Vec<f64>,
    },
    HLL {
        precision: u32,
    },
    CountSketch {
        /// Relative error bound (ε).
        epsilon: f64,
        /// Error probability (δ).
        delta: f64,
    },
    CountMinSketch {
        rows: u32,
        cols: u32,
        /// Metric name required by the CMS processor.
        metric_name: String,
    },
}

impl Default for SketchParams {
    fn default() -> Self {
        let d = SketchDefaults::default();
        SketchParams::DDSketch {
            relative_accuracy: d.ddsketch.relative_accuracy,
            quantiles: d.quantile_grid,
        }
    }
}

impl SketchParams {
    /// Extract quantiles if this sketch type supports them.
    pub fn quantiles(&self) -> &[f64] {
        match self {
            SketchParams::DDSketch { quantiles, .. } | SketchParams::KLL { quantiles, .. } => {
                quantiles
            }
            _ => &[],
        }
    }
}

/// GOS delta-gating knobs pushed to the edge (Count-Sketch families).
/// `epsilon` = the staleness share ε_st of the metric's accuracy budget;
/// `sites` = k (fleet size for the merged-error bound); `anisotropic` selects
/// the per-cell {T_j} water-filling (O(d·w) edge memory) over the isotropic
/// scalar (O(1)).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GosKnobs {
    pub epsilon: f64,
    pub sites: u32,
    pub anisotropic: bool,
}

impl GosKnobs {
    /// Derive the edge delta-gate knobs from the metric's ε budget and the
    /// edge-CPU-vs-communication cost weights: `epsilon` is the **staleness
    /// share** ε_st from [`crate::epsilon_alloc::split_budget`] (the rest, ε_sa,
    /// would fund sampling). `sites` = fleet size k (≥1); `anisotropic` selects
    /// per-cell {T_j} over the isotropic scalar at the edge.
    pub fn derive(epsilon: f64, sites: u32, w_edge: f64, w_comm: f64, anisotropic: bool) -> Self {
        let (_eps_sa, eps_st) = crate::epsilon_alloc::split_budget(epsilon, w_edge, w_comm);
        Self {
            epsilon: eps_st,
            sites: sites.max(1),
            anisotropic,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AgentCollectorConfig {
    pub output_mode: OutputMode,
    pub sketch_type: SketchType,
    pub sketch_params: SketchParams,
    pub aggregate_by: Vec<String>,
    pub label_matchers: Vec<String>,
    pub window_duration: Option<Duration>,
    pub mode: ProcessorMode,
    pub enable_self_monitoring: bool,
    pub transmit_sketch: bool,
    pub drop_original: bool,
    /// Whether to enable the series ID (UID) registry on the OTLP receiver.
    /// When true, the receiver caches metric name + attributes per series and
    /// assigns a compact `series_id`. Subsequent exports can omit attributes
    /// and send only the ID, saving ~120 bytes per sample.
    pub enable_series_id: bool,
    /// TTL for series ID cache entries (seconds). 0 = use receiver default.
    pub series_id_ttl_secs: u64,
    /// Whether the agent processor should enable delta encoding.
    /// Set by the delta cost model after sketch type selection.
    pub delta_transmission: bool,
    /// Minimum absolute cell change included in a delta payload (T).
    /// Ignored when `delta_transmission` is false.
    pub delta_threshold: f64,
    /// GOS relative delta gating (design-gos-unified-edge-telemetry.md §7).
    /// When set, the edge replaces the fixed `delta_threshold` with the
    /// norm-adaptive GOS threshold; `None` = fixed threshold (unchanged).
    /// Produced by the ε-budget split (`epsilon_alloc::split_budget`).
    pub gos: Option<GosKnobs>,
    /// Data sink the planner wants the agent to emit to. Decoupled
    /// from the planner output (which sketch / window / projection)
    /// because where the data goes is a deployment-scope concern,
    /// not a planning concern. The previous hardcoded
    /// "prometheus exporter on :8889" approach broke the moment we
    /// tried to ship sketch types — stock Prometheus exporter
    /// silently drops `DDSketchDataPoint` / `HLLSketchDataPoint`
    /// etc. — so emit OTLP-to-backend for sketch deployments and
    /// keep the prometheus path only for legacy raw-scalar pipelines.
    pub data_sink: AgentDataSink,
}

/// What the agent's collector exports to.
///
/// `Otlp` — the agent's pipeline ends with an OTLP exporter
/// pointed at the configured endpoint. Required for sketch
/// transport: the modified-OTLP `Data::Ddsketch` / `KLLSketch` /
/// etc. variants are carried natively over OTLP and decoded by
/// the backend's `OtlpReceiver` + the per-sketch
/// `from_sketchlib_proto_bytes` / `from_msgpack_bytes` decoders.
///
/// `PrometheusScrape` — agent exposes `/metrics` on the listed
/// host:port for an external scraper. Loses sketch types at the
/// translation step; only useful for raw-scalar pipelines.
#[derive(Debug, Clone)]
pub enum AgentDataSink {
    /// `endpoint` is an OTLP gRPC endpoint, e.g. `data-plane:4317`.
    /// `compression` is the transport-level codec; the canonical
    /// path uses `none` because the backend's tonic gRPC server
    /// rejects gzip-compressed bodies (returns Unimplemented).
    Otlp {
        endpoint: String,
        compression: String,
    },
    /// Pre-existing path: prometheus exporter at `endpoint`. Kept
    /// for back-compat with the legacy raw-scalar deployment.
    PrometheusScrape { endpoint: String },
}

impl Default for AgentDataSink {
    /// Default is OTLP-to-backend at the canonical compose
    /// hostname. Override per-deployment via the planner's
    /// `--agent-data-sink` flag (or future config push).
    fn default() -> Self {
        AgentDataSink::Otlp {
            endpoint: "data-plane:4317".to_string(),
            compression: "none".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct GatewayCollectorConfig {
    pub passthrough: bool,
}

#[derive(Debug, Clone)]
pub struct PrecomputeJob {
    pub query_expr: String,
    pub granularity: Duration,
    pub sketch_source: String,
    pub store_path: String,
}

// ── Per-stage resource budgets ────────────────────────────────────────────────

/// Per-stage resource caps. The agent cap feeds the physical planner's
/// placement decision (sketch build deferred off the agent when it would
/// exceed the cap).
///
/// `None` means unbounded (no cap enforced).  Typically sourced from
/// [`WorkloadCharacteristics::memory_budget_bytes`] for the agent stage and
/// from a runtime config file for backend / precompute stages.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StageResourceBudgets {
    /// Max sketch memory at the agent OTel Collector (bytes).
    pub agent_memory_bytes: Option<u64>,
    /// Max sketch-insertion CPU budget at the agent (µs/sample).
    pub agent_cpu_micros_per_sample: Option<f64>,
    /// Max sketch memory at the backend OTel Collector (bytes).
    pub backend_memory_bytes: Option<u64>,
    /// Max memory at the ASAPQuery Precompute Engine (bytes).
    pub precompute_memory_bytes: Option<u64>,
}

impl StageResourceBudgets {
    /// Derive budgets from [`WorkloadCharacteristics`]: propagates the agent
    /// memory cap; other stages default to unbounded.
    pub fn from_workload_chars(wc: &WorkloadCharacteristics) -> Self {
        Self {
            agent_memory_bytes: wc.memory_budget_bytes,
            ..Default::default()
        }
    }
}

// ── Collection plan ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CollectionPlan {
    pub agent_config: AgentCollectorConfig,
    pub gateway_config: GatewayCollectorConfig,
    pub precompute: Vec<PrecomputeJob>,
    pub valid_until: DateTime<Utc>,
    /// Resolved delta transmission decision and rationale.
    pub delta_decision: DeltaDecision,
    /// Bandwidth and overhead estimates for all three transmission strategies.
    pub transmission_cost_summary: TransmissionCostSummary,
}
