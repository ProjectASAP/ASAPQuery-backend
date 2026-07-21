use std::fmt;
use std::str::FromStr;
use tracing::debug;

/// The scalar value a serving-time query wants out of an already-built
/// accumulator: "given a live `AggregateCore` implementation, which
/// number do you want?" Every accumulator's `AggregateCore::query_statistic`
/// dispatches on this. Distinct from L3's `AggIntent` (a planning-time
/// IR node carrying accuracy targets, column refs, φ, k) — nothing at
/// L3/L4 reaches down to a live Rust struct's fields, so `Statistic` has
/// no upstream ASAPController equivalent; it's this workspace's own
/// serving-time vocabulary.
///
/// Formerly `promql_utilities::query_logics::enums::Statistic` — moved
/// here because its real center of gravity (`compatible_agg_types`,
/// `QueryRequirements`, capability matching) already lived in this
/// crate, and `asap_types` — not `data_plane` — is the shared foundation
/// both `control_plane`'s ecosystem and `data_plane` can depend on
/// without a cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Statistic {
    Count,
    Sum,
    Cardinality,
    Increase,
    Rate,
    Min,
    Max,
    Quantile,
    Topk,
}

impl fmt::Display for Statistic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        debug!("Formatting Statistic: {:?}", self);
        match self {
            Statistic::Count => write!(f, "count"),
            Statistic::Sum => write!(f, "sum"),
            Statistic::Cardinality => write!(f, "cardinality"),
            Statistic::Increase => write!(f, "increase"),
            Statistic::Rate => write!(f, "rate"),
            Statistic::Min => write!(f, "min"),
            Statistic::Max => write!(f, "max"),
            Statistic::Quantile => write!(f, "quantile"),
            Statistic::Topk => write!(f, "topk"),
        }
    }
}

#[allow(clippy::should_implement_trait)]
impl Statistic {
    pub fn from_str(s: &str) -> Option<Self> {
        debug!("Parsing Statistic from string: {}", s);
        match s.to_lowercase().as_str() {
            "count" => Some(Statistic::Count),
            "sum" => Some(Statistic::Sum),
            "cardinality" => Some(Statistic::Cardinality),
            "increase" => Some(Statistic::Increase),
            "rate" => Some(Statistic::Rate),
            "min" => Some(Statistic::Min),
            "max" => Some(Statistic::Max),
            "quantile" => Some(Statistic::Quantile),
            "topk" => Some(Statistic::Topk),
            _ => None,
        }
    }
}

impl FromStr for Statistic {
    type Err = ();

    /// Parse a statistic from a string (case-insensitive).
    /// Use `s.parse::<Statistic>()` or `Statistic::from_str(s)`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        debug!("FromStr trait parsing Statistic: {}", s);
        Statistic::from_str(s).ok_or(())
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq)]
#[allow(non_camel_case_types)]
pub enum QueryLanguage {
    #[value(alias = "PROMQL")]
    promql,
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

/// Window type for streaming aggregations.
#[derive(
    Clone, Debug, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum WindowType {
    #[default]
    Tumbling,
    Sliding,
}

impl fmt::Display for WindowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WindowType::Tumbling => write!(f, "tumbling"),
            WindowType::Sliding => write!(f, "sliding"),
        }
    }
}

impl FromStr for WindowType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tumbling" => Ok(WindowType::Tumbling),
            "sliding" => Ok(WindowType::Sliding),
            _ => Err(format!("Unknown window type: '{s}'")),
        }
    }
}
