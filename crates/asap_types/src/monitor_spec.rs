use serde::{Deserialize, Serialize};

/// Continuous-monitoring readout shared by the control and data planes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MonitorFunctional {
    #[default]
    Sum,
    CmsPoint,
    LinearBuckets,
    F2,
}

impl MonitorFunctional {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::CmsPoint => "cms_point",
            Self::LinearBuckets => "linear_buckets",
            Self::F2 => "f2",
        }
    }

    pub fn from_name(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "cms_point" | "cms" => Self::CmsPoint,
            "linear_buckets" | "linear" => Self::LinearBuckets,
            "f2" | "l2" => Self::F2,
            _ => Self::Sum,
        }
    }
}

/// One continuous-monitoring (CDM) threshold spec. The data-plane monitor
/// coordinator owns the AUTHORITATIVE `tau`/`epsilon`/`window_ms` (the edge
/// copy is advisory), keyed by the same content-addressed `agg_id` the edge and
/// coordinator share. `key` is the CMS point-frequency key for point monitors
/// (empty for Sum / whole-stream). See
/// `ASAPCollector/docs/continuous-monitoring-tumbling-cost-analysis.md`.
///
/// Stays here (unlike `data_plane::storage_engines::types::StreamingConfig`,
/// which holds a `Vec<MonitorSpec>` field) because `control_plane` genuinely
/// needs it: `emit/monitor.rs` builds the `StreamingConfig.monitors[]` JSON
/// entry by hand and has a regression test asserting that JSON deserializes
/// into this exact type. `control_plane` cannot depend on `data_plane` (the
/// dependency runs the other way), so this type has to live somewhere both
/// sides can reach — same reasoning as `AggregationConfig`/`PolicyFingerprint`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorSpec {
    pub agg_id: u64,
    /// Additive readout the edge reports: "sum" (default), "cms_point", "f2".
    /// Pass-through metadata so the edge can auto-learn its reporting mode from
    /// the pushed config; the coordinator allocation is value-driven and does not
    /// branch on it (p_i ∝ √(value/rate) is the F2 allocation when value=‖f‖²).
    #[serde(default)]
    pub functional: String,
    /// CMS point-frequency key x; empty (default) for Sum / whole-stream / F2.
    #[serde(default)]
    pub key: String,
    /// Threshold τ (authoritative here, not at the edge).
    pub tau: f64,
    /// Relative tolerance ε; the alert fires when the estimate reaches (1−ε)τ.
    #[serde(default = "default_monitor_epsilon")]
    pub epsilon: f64,
    /// Tumbling epoch length in ms; MUST match the edge window for this agg_id.
    pub window_ms: u64,
    /// Count-Sketch depth (rows) for whole-sketch `functional="f2"` monitors.
    /// 0 (default) for scalar monitors; MUST match the edge's Count-Sketch for
    /// this agg when F2 (both sides square/merge the same cell matrix).
    #[serde(default)]
    pub d: usize,
    /// Count-Sketch width (buckets/row) for F2 monitors; 0 for scalar.
    #[serde(default)]
    pub w: usize,
    /// F2 monitoring variant: "distributed" (default, ship every window) or
    /// "geometric" (Sharfman–Schuster–Keren safe-zone, ship on local violation).
    /// Ignored by scalar monitors.
    #[serde(default)]
    pub mode: String,
}

fn default_monitor_epsilon() -> f64 {
    0.05
}
