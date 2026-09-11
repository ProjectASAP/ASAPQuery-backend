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
    FrequencyL2,
    FrequencyEntropy,
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
            Statistic::FrequencyL2 => write!(f, "frequency_l2"),
            Statistic::FrequencyEntropy => write!(f, "frequency_entropy"),
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
            "frequency_l2" => Some(Statistic::FrequencyL2),
            "frequency_entropy" => Some(Statistic::FrequencyEntropy),
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

/// Window lifecycle/flush semantics for streaming aggregations.
///
/// Formerly a local `WindowType` (`Tumbling`/`Sliding`) enum, retired in
/// favor of a re-export of `asap_ir::intent_algebra::query_expr::WindowKind`
/// (ASAPController PR #143 added `Copy`/`Default`/`Hash`/`Display`/
/// `FromStr` and `#[serde(rename_all = "snake_case")]` upstream
/// specifically so that re-export could replace the old local type
/// without touching any call site's behavior).
///
/// **Vendored back as a local type** (ASAPPlanner pin migration, see
/// `control_plane/docs/design-asapplanner-pin-migration.md`): ASAPPlanner
/// deleted `QueryExpr::Window` outright ("no producer exists" — issue
/// #181/#192) and with it every trace of a window-lifecycle `WindowKind`
/// concept; ASAPPlanner's own scope (a batch query-workload planner, not
/// a streaming execution engine) has no use for tumbling/sliding/session
/// flush semantics. This workspace's streaming aggregation config still
/// does, so the type moves back to being owned here — same shape as the
/// old re-export (`Tumbling` default, lowercase `Display`/`FromStr`
/// round-trip, same wire format), so none of this repo's ~145 call sites
/// needed to change.
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
