use std::fmt;
use std::str::FromStr;

pub use asap_physical_operators::Statistic;

#[derive(
    clap::ValueEnum,
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    #[default]
    #[value(alias = "PROMQL", alias = "promql")]
    PromQl,
    MetricsQl,
    ClickHouseSql,
}

/// Policy for cleaning up old aggregates from the store.
/// Must be explicitly specified in inference_config.yaml.
#[derive(Clone, Debug, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupPolicy {
    /// Keep only the N most recent aggregates (circular buffer behavior)
    CircularBuffer,
    /// Never clean up aggregates
    NoCleanup,
}

impl fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CleanupPolicy::CircularBuffer => write!(f, "circular_buffer"),
            CleanupPolicy::NoCleanup => write!(f, "no_cleanup"),
        }
    }
}

impl FromStr for CleanupPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "circular_buffer" => Ok(CleanupPolicy::CircularBuffer),
            "no_cleanup" => Ok(CleanupPolicy::NoCleanup),
            _ => Err(format!("Unknown cleanup policy: '{s}'")),
        }
    }
}

/// Window lifecycle and flush semantics owned by streaming configuration.
/// Tumbling is the default; wire names are lowercase.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WindowKind {
    /// Fixed, non-overlapping windows (e.g. a new 30s bucket every 30s).
    #[default]
    Tumbling,
    /// Overlapping windows that advance by less than their width.
    Sliding,
    /// Gap-based windows that close after a period of inactivity.
    Session,
}

impl fmt::Display for WindowKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowKind::Tumbling => write!(f, "tumbling"),
            WindowKind::Sliding => write!(f, "sliding"),
            WindowKind::Session => write!(f, "session"),
        }
    }
}

impl FromStr for WindowKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tumbling" => Ok(WindowKind::Tumbling),
            "sliding" => Ok(WindowKind::Sliding),
            "session" => Ok(WindowKind::Session),
            _ => Err(format!("Unknown window kind: '{s}'")),
        }
    }
}
