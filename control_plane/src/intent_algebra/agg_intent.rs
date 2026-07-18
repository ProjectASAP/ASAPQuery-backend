//! Layer 3 aggregation-intent vocabulary.
//!
//! Per `control_plane/docs/design.md` §6 "`AggIntent` — what to compute, not
//! how" (around line ~468). L3 carries intent ("compute a quantile to
//! ε=0.01 accuracy"). The choice between `HashAgg` / `SortAgg` /
//! `SketchAgg(KLL{k=200})` is made by L4 cost-aware rules, not encoded
//! here.
//!
//! Intent vs operator distinction. `AggIntent::TopK` is an *intent* (a
//! dedicated heavy-hitter sketch primitive — SpaceSaving, CMS-with-heap
//! — computes it in a single pass). The generic `Sort + Limit` operator
//! pair survives in `QueryExpr` for non-heavy-hitter cases (`ORDER BY
//! name LIMIT 10`). L1→L2→L3 lowering picks one or the other
//! deterministically.
//!
//! No `QuantileOverTime` intent. The window is fully captured by the
//! surrounding `QueryExpr::Window` node; the quantile *operation* is the
//! same regardless. PromQL `quantile_over_time(0.99, m[5m])` lowers to
//! `Window{size=5m} → Aggregate{aggs:[Quantile{q=0.99}]}`.
//!
//! ## Phase 1 IR merge (ASAPController base)
//!
//! Per `control_plane/docs/migration-plan-backend-plan.md` Phase 1: this
//! file adopts ASAPController's current `AggIntent` vocabulary as the base
//! (its ~40-variant set is now richer than this file's pre-merge 25
//! variants — see `control_plane/docs/design-backend-plan-wire-format.md`
//! §4). Two deliberate deviations from a byte-for-byte port, both
//! recorded in the migration plan's decision log:
//!
//! 1. **`window: Duration` stays on the time-series-derivative variants**
//!    (`Rate` / `Increase` / `Changes` / `Delta` / `IDelta` / `Deriv` /
//!    `Resets` / `PredictLinear` / `DoubleExpSmoothing`), instead of
//!    ASAPController's design of threading the window only through the
//!    enclosing `QueryExpr::TimeRange` node. Moving the window off the
//!    intent requires verifying `query_expr.rs`/`lower.rs` thread it
//!    correctly everywhere `AggIntent` is currently constructed or
//!    consulted — that's Phase 2 scope (those are Phase 2 files), not
//!    this one. Reconcile in Phase 2 once `query_expr.rs` lands.
//! 2. **`AggIntent::Frequency { accuracy }` is kept**, not folded into
//!    ASAPController's `RankingMeasure::Frequency`. These are different
//!    concepts that happen to share a name: control_plane's `Frequency`
//!    is a standalone, independently-bindable point-frequency query
//!    (`count(*) WHERE key = k`, bound to CMS in
//!    `sketch_algebra/rules/bind_cms_count.rs`); ASAPController's
//!    `RankingMeasure::Frequency` classifies what a `TopK` ranks by, and
//!    has no relation to point-frequency queries at all. This is exactly
//!    the "genuinely control_plane-only, no ASAPController equivalent"
//!    exception to the merge's tie-break rule. `RankingMeasure` is still
//!    adopted below, additively — it's a real capability control_plane
//!    lacked (nothing today classifies whether a `TopK` is a sketchable
//!    heavy-hitter), it just isn't a replacement for `Frequency`.
//!
//! `irate`/`rate` fold: per the decided tie-break, `AggIntent::Irate` is
//! removed; `irate(...)` and `rate(...)` both lower to `AggIntent::Rate`
//! (unchanged from today — the PromQL L1→L2 walk already maps `"rate" |
//! "irate" => AggFunc::Rate`, and no `AggFunc::Irate` variant exists, so
//! `AggIntent::Irate` was already unreachable from real query parsing;
//! only unit tests constructed it directly. The estimation-method
//! distinction (windowed-average vs last-two-samples) is deferred to L4,
//! per ASAPController's design — no L4 rule makes that distinction today,
//! which is fine, since nothing reachable exercised it before this change
//! either).

#![allow(dead_code)]

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::intent_algebra::schema::{Column, ColumnId, DataType};
use crate::types_v2::AccuracyTarget;

/// "What to compute" at L3 — the vocabulary the planner pivots on.
///
/// The single-column reducers (`Sum` / `Min` / `Max` / `Avg` / `StdDev` /
/// `Variance` / `Quantile` / `Cardinality`) carry `col: Option<ColumnId>` —
/// the positional input column they reduce. `None` is the PromQL
/// convention "the time-series sample value"; SQL `SUM(bytes),
/// AVG(latency)` sets distinct `Some(id)`s. `TopK`'s grouping rides on the
/// enclosing `QueryExpr::Aggregate.by`, like every other aggregate; the
/// intent itself carries only `k` + the accuracy target, no `col` (it
/// ranks by the aggregate output, not a base column — see
/// [`RankingMeasure`] below).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AggIntent {
    // ── Data-model-agnostic ──────────────────────────────────────────────
    /// COUNT(*) / `count` — number of rows / samples per group.
    /// `accuracy: Exact` selects an exact counter; `Epsilon` / `EpsilonDelta`
    /// unlock CMS / linear-counting sketch families.
    Count { accuracy: AccuracyTarget },
    /// SUM(col). Always exact at L3 — no approximation intent for `Sum`
    /// in the catalog (`design.md` §6 line ~485).
    Sum {
        #[serde(default)]
        col: Option<ColumnId>,
    },
    /// Per-group minimum — exact at L3.
    Min {
        #[serde(default)]
        col: Option<ColumnId>,
    },
    /// Per-group maximum — exact at L3.
    Max {
        #[serde(default)]
        col: Option<ColumnId>,
    },
    /// Arithmetic mean. Exact at L3; sketch backends fold this onto a
    /// `Quantile{q=0.5}` only when the cost model allows the relaxation.
    Avg {
        #[serde(default)]
        col: Option<ColumnId>,
    },
    /// Sample standard deviation when `population == false`; population
    /// stddev otherwise. PromQL `stddev` / `stddev_over_time`; SQL
    /// `STDDEV(col)`.
    StdDev {
        #[serde(default)]
        col: Option<ColumnId>,
        population: bool,
    },
    /// Variance — PromQL `stdvar` / `stdvar_over_time`; SQL
    /// `VARIANCE(col)`.
    Variance {
        #[serde(default)]
        col: Option<ColumnId>,
        population: bool,
    },
    /// Compute the φ-th quantile (0 ≤ q ≤ 1) to the given accuracy.
    /// Sketch families: KLL, DDSketch, t-digest.
    Quantile {
        #[serde(default)]
        col: Option<ColumnId>,
        q: f64,
        accuracy: AccuracyTarget,
    },
    /// Heavy-hitter top-k. Distinct from generic `Sort + Limit` because
    /// a dedicated sketch primitive (SpaceSaving, CMS-with-heap,
    /// Misra-Gries) computes it as a single operation. L1→L2→L3 lowering
    /// produces this when it recognises `topk(k, …)` (PromQL) or
    /// `ORDER BY count DESC LIMIT k` (SQL).
    TopK { k: usize, accuracy: AccuracyTarget },
    /// COUNT DISTINCT — number of distinct values in the input column,
    /// to the given accuracy. Sketch families: HLL, theta-sketch.
    Cardinality {
        #[serde(default)]
        col: Option<ColumnId>,
        accuracy: AccuracyTarget,
    },
    /// control_plane-only (no ASAPController equivalent — see module docs).
    /// Point-frequency of a key in the input — `count(*) WHERE key = k`
    /// modeled as a sketch query. Sketch families: CMS, count-min-log.
    /// **Not** the same concept as [`RankingMeasure::Frequency`] below,
    /// which classifies what a `TopK` ranks by.
    Frequency { accuracy: AccuracyTarget },

    // ── Time-series streaming derivatives ────────────────────────────────
    /// Per-second average derivative with PromQL's counter-reset
    /// adjustment. Also serves `irate(...)` post the rate/irate fold (see
    /// module docs) — the windowed-average vs last-two-samples estimation
    /// choice is an L4 concern, not encoded here.
    Rate { window: Duration },
    /// Cumulative increase over the given window with counter-reset
    /// adjustment. Specialized — see `Rate` above.
    Increase { window: Duration },

    // ── Counter-derivative / range-vector functions ──────────────────────
    // All per-series, label-preserving reductions of a single series'
    // range window to one value. Each has distinct semantics and is
    // deliberately NOT aliased to `Rate`/`Increase`/`Count`.
    /// `changes(m[range])` — number of times the value changed in the
    /// window.
    Changes { window: Duration },
    /// `delta(m[range])` — difference between the first and last sample
    /// (gauge semantics; not counter-reset-adjusted). Distinct from
    /// [`AggIntent::Increase`].
    Delta { window: Duration },
    /// `idelta(m[range])` — difference between the last two samples.
    IDelta { window: Duration },
    /// `deriv(m[range])` — per-second derivative via simple linear
    /// regression over the window (gauges).
    Deriv { window: Duration },
    /// `resets(m[range])` — count of counter resets over the window.
    Resets { window: Duration },
    /// `predict_linear(m[range], t)` — linear-regression extrapolation of
    /// the value `t` seconds into the future.
    PredictLinear {
        window: Duration,
        /// The prediction horizon in seconds (the 2nd, scalar argument).
        seconds: f64,
    },
    /// `double_exponential_smoothing(v[w], sf, tf)` (a.k.a. the legacy
    /// `holt_winters`) — Holt-Winters double-exponential smoothing.
    DoubleExpSmoothing {
        window: Duration,
        /// Data (level) smoothing factor `sf` ∈ (0, 1).
        smoothing: f64,
        /// Trend smoothing factor `tf` ∈ (0, 1).
        trend: f64,
    },

    // ── Native-histogram accessors ────────────────────────────────────────
    // Per-series extractions from a native-histogram instant vector — one
    // float per series, label-preserving. (Classic `le`-bucket
    // `histogram_quantile` stays a `Quantile` over the bucketed vector —
    // per Step γ5, the PromQL parser substitutes it directly into a plain
    // `Aggregate { Quantile(φ) }`; bucket-aware handling is a
    // physical-planner concern, not this L3 intent.)
    /// `histogram_count(v)` — observation count of each native histogram.
    HistogramCount,
    /// `histogram_sum(v)` — sum of observations.
    HistogramSum,
    /// `histogram_avg(v)` — mean (`sum/count`).
    HistogramAvg,
    /// `histogram_stddev(v)` — standard deviation of observations.
    HistogramStdDev,
    /// `histogram_stdvar(v)` — variance of observations.
    HistogramStdVar,
    /// `histogram_fraction(lower, upper, v)` — fraction of observations in
    /// `[lower, upper]`.
    HistogramFraction { lower: f64, upper: f64 },
    /// `histogram_quantile(φ, <le-bucketed vector>)` over a *native*
    /// histogram — exact bucket interpolation, not a sketch-able
    /// quantile; distinct from [`AggIntent::Quantile`].
    HistogramQuantile { q: f64 },

    /// A per-sample element-wise math / trig transform — `abs`, `ceil`,
    /// `sqrt`, `ln`, `clamp_max`, the trig family, … Label-preserving.
    Math(MathFunc),

    // ── Presence functions ────────────────────────────────────────────────
    /// `absent(v)` — a 1-sample vector when the instant vector `v` has no
    /// matching series, else empty.
    Absent,
    /// `absent_over_time(v[w])` — `absent` over a range vector.
    AbsentOverTime,
    /// `present_over_time(v[w])` — value 1 per series that has any sample
    /// in the range (per-series).
    PresentOverTime,

    /// A time / calendar accessor — `timestamp`, `minute`, `hour`,
    /// `day_of_week`, … over each sample's timestamp. Label-preserving.
    TimeFn(TimeFunc),

    // ── Extended aggregation operators ────────────────────────────────────
    /// `group(v)` — a constant `1` per group ("group presence"). The
    /// grouping keys ride on the enclosing `Aggregate.by`. Deliberately
    /// NOT aliased to `Sum`/`Count`: the output value is always 1,
    /// independent of the input values.
    Group,
    /// `count_values("l", v)` — group the input series by their sample
    /// *value* and count each distinct value, emitting that value as a
    /// new label `l`. Unlike every other reducer this adds a synthesized
    /// `Utf8` label column, so `Aggregate` schema derivation special-cases
    /// it (two output columns, not one).
    CountValues { label: String },

    // ── Additional range-vector reducers ──────────────────────────────────
    // All per-series, label-preserving reductions of a single series'
    // range window to one value.
    /// `last_over_time(v[w])` — the most recent sample in the window.
    LastOverTime,
    /// `first_over_time(v[w])` — the oldest sample in the window.
    FirstOverTime,
    /// `mad_over_time(v[w])` — median absolute deviation over the window.
    MadOverTime,
    /// `ts_of_min_over_time(v[w])` — timestamp of the minimum sample.
    TsOfMinOverTime,
    /// `ts_of_max_over_time(v[w])` — timestamp of the maximum sample.
    TsOfMaxOverTime,
    /// `ts_of_first_over_time(v[w])` — timestamp of the first sample.
    TsOfFirstOverTime,
    /// `ts_of_last_over_time(v[w])` — timestamp of the last sample.
    TsOfLastOverTime,
}

/// Time / calendar accessor functions, evaluated over a timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fn", rename_all = "snake_case")]
pub enum TimeFunc {
    /// `timestamp(v)` — the sample's own timestamp as a value.
    Timestamp,
    Minute,
    Hour,
    DayOfWeek,
    DayOfMonth,
    DayOfYear,
    Month,
    Year,
    DaysInMonth,
}

/// The element-wise math / trig functions. Unary over the sample value
/// unless a variant carries scalar params (`clamp*`, `round`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "fn", rename_all = "snake_case")]
pub enum MathFunc {
    Abs,
    Ceil,
    Floor,
    Exp,
    Ln,
    Log2,
    Log10,
    Sqrt,
    Sgn,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Sinh,
    Cosh,
    Tanh,
    Asinh,
    Acosh,
    Atanh,
    /// `deg(v)` — radians → degrees.
    Deg,
    /// `rad(v)` — degrees → radians.
    Rad,
    /// `round(v, to_nearest)` — nearest multiple of `to_nearest` (default 1).
    Round {
        to_nearest: f64,
    },
    /// `clamp(v, min, max)`.
    Clamp {
        min: f64,
        max: f64,
    },
    /// `clamp_min(v, min)`.
    ClampMin {
        min: f64,
    },
    /// `clamp_max(v, max)`.
    ClampMax {
        max: f64,
    },
}

impl AggIntent {
    /// True iff this intent has no ASAP-tier (streaming sketch) binding
    /// today. `false` means a `Bind*` rule may match. `true` means the
    /// L5 emitter routes the intent to the cold-store / archive tier.
    ///
    /// Every intent newly added by the Phase 1 IR merge (histogram
    /// accessors, math/trig, time accessors, presence functions, `Group`
    /// / `CountValues`, the extended range-vector reducers) is
    /// archive-only today — none has a `Bind*` rule yet. Adding a sketch
    /// family for any of them is a future PR; flipping the flag here is
    /// the single point of change (same policy as before the merge).
    pub fn archive_only(&self) -> bool {
        matches!(
            self,
            AggIntent::Absent
                | AggIntent::AbsentOverTime
                | AggIntent::PresentOverTime
                | AggIntent::Delta { .. }
                | AggIntent::Deriv { .. }
                | AggIntent::PredictLinear { .. }
                | AggIntent::DoubleExpSmoothing { .. }
                | AggIntent::IDelta { .. }
                | AggIntent::Resets { .. }
                | AggIntent::Changes { .. }
                | AggIntent::HistogramCount
                | AggIntent::HistogramSum
                | AggIntent::HistogramAvg
                | AggIntent::HistogramStdDev
                | AggIntent::HistogramStdVar
                | AggIntent::HistogramFraction { .. }
                | AggIntent::HistogramQuantile { .. }
                | AggIntent::Math(_)
                | AggIntent::TimeFn(_)
                | AggIntent::Group
                | AggIntent::CountValues { .. }
                | AggIntent::LastOverTime
                | AggIntent::FirstOverTime
                | AggIntent::MadOverTime
                | AggIntent::TsOfMinOverTime
                | AggIntent::TsOfMaxOverTime
                | AggIntent::TsOfFirstOverTime
                | AggIntent::TsOfLastOverTime
        )
    }

    /// Whether this is a *per-series* reduction — it reduces a single
    /// series' samples over its range window (one value out per series),
    /// so it does **not** collapse across series and every label column
    /// is preserved. (Cross-series reductions like `sum`/`avg` over a
    /// series set, and control_plane's point-query `Frequency`, return
    /// `false`.)
    pub fn is_per_series(&self) -> bool {
        matches!(
            self,
            Self::Rate { .. }
                | Self::Increase { .. }
                | Self::Changes { .. }
                | Self::Delta { .. }
                | Self::IDelta { .. }
                | Self::Deriv { .. }
                | Self::Resets { .. }
                | Self::PredictLinear { .. }
                | Self::DoubleExpSmoothing { .. }
                | Self::HistogramCount
                | Self::HistogramSum
                | Self::HistogramAvg
                | Self::HistogramStdDev
                | Self::HistogramStdVar
                | Self::HistogramFraction { .. }
                | Self::Math(_)
                | Self::Absent
                | Self::AbsentOverTime
                | Self::PresentOverTime
                | Self::TimeFn(_)
                | Self::LastOverTime
                | Self::FirstOverTime
                | Self::MadOverTime
                | Self::TsOfMinOverTime
                | Self::TsOfMaxOverTime
                | Self::TsOfFirstOverTime
                | Self::TsOfLastOverTime
        )
    }

    /// The positional input column this intent reduces, if it carries
    /// one. `None` = the synthetic time-series sample value (PromQL), an
    /// argument-less aggregate (`Count` / `TopK`), or control_plane's
    /// point-query `Frequency` (keyed, not column-reducing).
    pub fn input_col(&self) -> Option<ColumnId> {
        match self {
            AggIntent::Sum { col }
            | AggIntent::Min { col }
            | AggIntent::Max { col }
            | AggIntent::Avg { col }
            | AggIntent::Quantile { col, .. }
            | AggIntent::Cardinality { col, .. }
            | AggIntent::StdDev { col, .. }
            | AggIntent::Variance { col, .. } => *col,
            _ => None,
        }
    }

    /// Output column name + type produced by this intent when applied to
    /// `input`. Used by `QueryExpr::Aggregate`'s schema-derivation rule.
    ///
    /// PromQL convention: aggregate column name = intent kind (`count`,
    /// `quantile_0_99`, …) so consumers can locate it without an alias
    /// lookup.
    pub fn output_column(&self, input: &Column) -> Column {
        match self {
            AggIntent::Count { .. } => col("count", DataType::Int64, false),
            AggIntent::Sum { .. } => col("sum", input.dtype.clone(), false),
            AggIntent::Min { .. } => col("min", input.dtype.clone(), input.nullable),
            AggIntent::Max { .. } => col("max", input.dtype.clone(), input.nullable),
            AggIntent::Avg { .. } => col("avg", DataType::Float64, false),
            AggIntent::StdDev { .. } => col("stddev", DataType::Float64, false),
            AggIntent::Variance { .. } => col("variance", DataType::Float64, false),
            AggIntent::Quantile { q, .. } => col(
                &format!("quantile_{}", quantile_suffix(*q)),
                DataType::Float64,
                false,
            ),
            // TopK output is a struct/list per row; modeled as Utf8 for
            // L3 (the L4 sketch-bound IR upgrades the dtype).
            AggIntent::TopK { k, .. } => col(&format!("topk_{k}"), DataType::Utf8, false),
            AggIntent::Cardinality { .. } => col("cardinality", DataType::Int64, false),
            AggIntent::Frequency { .. } => col("frequency", DataType::Int64, false),
            AggIntent::Rate { .. } => col("rate", DataType::Float64, false),
            AggIntent::Increase { .. } => col("increase", DataType::Float64, false),
            AggIntent::Changes { .. } => col("changes", DataType::Int64, false),
            AggIntent::Delta { .. } => col("delta", DataType::Float64, false),
            AggIntent::IDelta { .. } => col("idelta", DataType::Float64, false),
            AggIntent::Deriv { .. } => col("deriv", DataType::Float64, false),
            AggIntent::Resets { .. } => col("resets", DataType::Int64, false),
            AggIntent::PredictLinear { .. } => col("predict_linear", DataType::Float64, false),
            AggIntent::DoubleExpSmoothing { .. } => {
                col("double_exponential_smoothing", DataType::Float64, false)
            }
            AggIntent::HistogramCount => col("histogram_count", DataType::Float64, false),
            AggIntent::HistogramSum => col("histogram_sum", DataType::Float64, false),
            AggIntent::HistogramAvg => col("histogram_avg", DataType::Float64, false),
            AggIntent::HistogramStdDev => col("histogram_stddev", DataType::Float64, false),
            AggIntent::HistogramStdVar => col("histogram_stdvar", DataType::Float64, false),
            AggIntent::HistogramFraction { .. } => {
                col("histogram_fraction", DataType::Float64, false)
            }
            AggIntent::HistogramQuantile { .. } => {
                col("histogram_quantile", DataType::Float64, false)
            }
            AggIntent::Math(_) => col("value", DataType::Float64, false),
            AggIntent::Absent => col("absent", DataType::Int64, false),
            AggIntent::AbsentOverTime => col("absent", DataType::Int64, false),
            AggIntent::PresentOverTime => col("present", DataType::Int64, false),
            AggIntent::TimeFn(_) => col("value", DataType::Float64, false),
            AggIntent::Group => col("group", DataType::Float64, false),
            AggIntent::CountValues { .. } => col("count", DataType::Int64, false),
            AggIntent::LastOverTime => col("last_over_time", DataType::Float64, false),
            AggIntent::FirstOverTime => col("first_over_time", DataType::Float64, false),
            AggIntent::MadOverTime => col("mad_over_time", DataType::Float64, false),
            AggIntent::TsOfMinOverTime => col("ts_of_min_over_time", DataType::Float64, false),
            AggIntent::TsOfMaxOverTime => col("ts_of_max_over_time", DataType::Float64, false),
            AggIntent::TsOfFirstOverTime => col("ts_of_first_over_time", DataType::Float64, false),
            AggIntent::TsOfLastOverTime => col("ts_of_last_over_time", DataType::Float64, false),
        }
    }
}

fn col(name: &str, dtype: DataType, nullable: bool) -> Column {
    Column {
        name: name.into(),
        dtype,
        nullable,
    }
}

/// `0.99` → `"0_99"`, `0.5` → `"0_5"`. Used by `Quantile` output naming
/// so `quantile_0_99` is a valid identifier downstream.
fn quantile_suffix(q: f64) -> String {
    let mut s = format!("{q}");
    if let Some(stripped) = s.strip_prefix('-') {
        s = format!("neg_{stripped}");
    }
    s.replace('.', "_")
}

// ── AggIntent helpers ────────────────────────────────────────────────────────
//
// Step γ7: relocated from `relational.rs` (where they were free fns
// operating on the canonical re-exported `AggIntent`). `relational`
// re-exports them during the legacy-IR retirement; consumers migrate to
// `intent_algebra::*` paths and the re-exports drop with `relational`.

/// What a top-k ranks its groups by — the axis that decides whether the
/// ranking is a sketchable **heavy-hitter** or a generic order-by-value
/// `Sort + Limit`. Adopted from ASAPController (Phase 1 IR merge) — this
/// is a real capability control_plane lacked before this merge, not a
/// replacement for [`AggIntent::Frequency`] (see module docs).
///
/// Sketchability follows the *additivity* of the ranking measure, not
/// "count" per se: an additive per-key aggregate admits a single-pass
/// heavy-hitter sketch (CMS-with-heap / SpaceSaving), a non-additive one
/// does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RankingMeasure {
    /// Unweighted frequency — `count` of rows/samples per key. Additive,
    /// and the one heavy-hitter measure **realised today** (→
    /// `AggIntent::TopK`).
    Frequency,
    /// Weighted frequency — an additive `sum` of a per-row weight per
    /// key. Sketchable in principle (weighted SpaceSaving), but no
    /// weighted heavy-hitter sketch is realised yet, so a `sum`-ranked
    /// top-k currently stays a generic `Sort + Limit`.
    WeightedSum,
    /// A non-additive measure (`avg` / `quantile` / `min` / `max`) or a
    /// raw, un-aggregated value. Never a heavy-hitter — always generic.
    NonAdditive,
}

impl RankingMeasure {
    /// Whether a top-k ranked by this measure can be a heavy-hitter
    /// **with a sketch that exists today**. Only [`Frequency`](Self::Frequency)
    /// is realised; [`WeightedSum`](Self::WeightedSum) is additive
    /// (sketchable in principle) but has no implemented sketch yet, so
    /// it stays generic until one lands.
    pub fn is_realised_heavy_hitter(self) -> bool {
        matches!(self, RankingMeasure::Frequency)
    }
}

/// Classify the aggregate a top-k ranks by into its [`RankingMeasure`].
pub fn ranking_measure(agg: &AggIntent) -> RankingMeasure {
    match agg {
        AggIntent::Count { .. } => RankingMeasure::Frequency,
        AggIntent::Sum { .. } => RankingMeasure::WeightedSum,
        _ => RankingMeasure::NonAdditive,
    }
}

/// The single rule that decides whether a top-k ranking is the frequency
/// **heavy-hitter** that [`AggIntent::TopK`] represents, as opposed to a
/// generic order-by-value `Sort + Limit`. A ranking qualifies iff it
/// takes the **top** k (`descending`) **and** ranks by a measure with a
/// realised heavy-hitter sketch.
pub fn is_frequency_heavy_hitter(descending: bool, measure: RankingMeasure) -> bool {
    descending && measure.is_realised_heavy_hitter()
}

/// Two instances of this aggregation can be merged
/// (`agg(A ∪ B) = combine(agg(A), agg(B))`). `Avg` / `StdDev` /
/// `Variance` need richer partial state than a single value, so they are
/// not mergeable.
pub fn agg_is_mergeable(op: &AggIntent) -> bool {
    !matches!(
        op,
        AggIntent::Avg { .. } | AggIntent::StdDev { .. } | AggIntent::Variance { .. }
    )
}

/// Whether this op implies `exact_required` — no sketch benefit. The
/// exact intents are `Sum / Count / Avg / Min / Max / Group /
/// CountValues`.
pub fn agg_is_exact(op: &AggIntent) -> bool {
    matches!(
        op,
        AggIntent::Sum { .. }
            | AggIntent::Count { .. }
            | AggIntent::Avg { .. }
            | AggIntent::Min { .. }
            | AggIntent::Max { .. }
            | AggIntent::Group
            | AggIntent::CountValues { .. }
    )
}

/// Accuracy parameter as a fractional ε (`0.0` for exact ops), unpacked
/// from the typed `AccuracyTarget` on Quantile / Cardinality / Frequency
/// / Count / TopK.
pub fn agg_accuracy(op: &AggIntent) -> f64 {
    match op {
        AggIntent::Quantile { accuracy, .. }
        | AggIntent::Cardinality { accuracy, .. }
        | AggIntent::Frequency { accuracy }
        | AggIntent::Count { accuracy }
        | AggIntent::TopK { accuracy, .. } => accuracy_target_to_f64(accuracy),
        _ => 0.0,
    }
}

fn accuracy_target_to_f64(t: &AccuracyTarget) -> f64 {
    match t {
        AccuracyTarget::Exact => 0.0,
        AccuracyTarget::Epsilon(eps) | AccuracyTarget::EpsilonDelta { eps, .. } => *eps,
    }
}

/// Default `Frequency` intent — `accuracy = e / 2000`. control_plane-only
/// (see module docs); not touched by the ASAPController merge.
pub fn default_frequency() -> AggIntent {
    AggIntent::Frequency {
        accuracy: AccuracyTarget::Epsilon(std::f64::consts::E / 2000.0),
    }
}

/// Default `Cardinality` intent over the sample value — `accuracy =
/// hll_accuracy(14)`.
pub fn default_cardinality() -> AggIntent {
    AggIntent::Cardinality {
        col: None,
        accuracy: AccuracyTarget::Epsilon(crate::sketch_algebra::capability::hll_accuracy(14)),
    }
}

/// Default `Quantile` intent over the sample value at φ = `q`, `accuracy
/// = ε 0.01`. Canonical `Quantile` is single-φ; multi-φ callers invoke
/// this once per φ.
pub fn default_quantile(q: f64) -> AggIntent {
    AggIntent::Quantile {
        col: None,
        q,
        accuracy: AccuracyTarget::Epsilon(0.01),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};

    fn c(name: &str, dtype: DataType) -> Column {
        Column {
            name: name.into(),
            dtype,
            nullable: false,
        }
    }

    #[test]
    fn agg_intent_serde_roundtrip() {
        let cases = vec![
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            },
            AggIntent::Sum { col: None },
            AggIntent::Sum { col: Some(3) },
            AggIntent::Min { col: None },
            AggIntent::Max { col: None },
            AggIntent::Avg { col: None },
            AggIntent::StdDev {
                col: None,
                population: false,
            },
            AggIntent::Variance {
                col: Some(1),
                population: true,
            },
            AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            },
            AggIntent::Cardinality {
                col: None,
                accuracy: AccuracyTarget::EpsilonDelta {
                    eps: 0.01,
                    delta: 0.001,
                },
            },
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggIntent::Increase {
                window: Duration::from_secs(300),
            },
            AggIntent::HistogramCount,
            AggIntent::HistogramFraction {
                lower: 0.0,
                upper: 1.0,
            },
            AggIntent::Math(MathFunc::Abs),
            AggIntent::Math(MathFunc::Clamp { min: 0.0, max: 1.0 }),
            AggIntent::Absent,
            AggIntent::AbsentOverTime,
            AggIntent::PresentOverTime,
            AggIntent::TimeFn(TimeFunc::DayOfWeek),
            AggIntent::Group,
            AggIntent::CountValues {
                label: "value".into(),
            },
            AggIntent::LastOverTime,
            AggIntent::TsOfMaxOverTime,
        ];
        for variant in cases {
            let json = serde_json::to_string(&variant).unwrap();
            let back: AggIntent = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "round-trip failed for {variant:?}");
        }
    }

    #[test]
    fn output_column_names_are_intent_keyed() {
        let v = c("value", DataType::Float64);
        assert_eq!(
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            }
            .output_column(&v)
            .name,
            "count"
        );
        assert_eq!(AggIntent::Sum { col: None }.output_column(&v).name, "sum");
        assert_eq!(
            AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }
            .output_column(&v)
            .name,
            "quantile_0_99"
        );
        assert_eq!(
            AggIntent::TopK {
                k: 5,
                accuracy: AccuracyTarget::Exact,
            }
            .output_column(&v)
            .name,
            "topk_5"
        );
    }

    #[test]
    fn quantile_output_is_float64() {
        let v = c("value", DataType::Int64);
        let out = AggIntent::Quantile {
            col: None,
            q: 0.5,
            accuracy: AccuracyTarget::Epsilon(0.01),
        }
        .output_column(&v);
        assert!(matches!(out.dtype, DataType::Float64));
    }

    #[test]
    fn sum_preserves_input_dtype() {
        let int_col = c("c", DataType::Int64);
        let float_col = c("c", DataType::Float64);
        assert!(matches!(
            AggIntent::Sum { col: None }.output_column(&int_col).dtype,
            DataType::Int64
        ));
        assert!(matches!(
            AggIntent::Sum { col: None }.output_column(&float_col).dtype,
            DataType::Float64
        ));
    }

    // ── Archive-only intent tests ──────────────────────────────────────────

    /// Every intent with no ASAP-tier sketch binding today — the Phase β
    /// migration targets plus everything newly added by the Phase 1 IR
    /// merge — must return `archive_only() == true`. The negative cases
    /// are the ASAP-tier-bound intents — they must continue to return
    /// false, otherwise the L4 binder would short-circuit them to the
    /// cold tier.
    #[test]
    fn archive_only_flag_partitions_intents() {
        let archive: Vec<AggIntent> = vec![
            AggIntent::Absent,
            AggIntent::AbsentOverTime,
            AggIntent::PresentOverTime,
            AggIntent::Delta {
                window: Duration::from_secs(60),
            },
            AggIntent::Deriv {
                window: Duration::from_secs(60),
            },
            AggIntent::PredictLinear {
                window: Duration::from_secs(300),
                seconds: 60.0,
            },
            AggIntent::DoubleExpSmoothing {
                window: Duration::from_secs(300),
                smoothing: 0.3,
                trend: 0.3,
            },
            AggIntent::IDelta {
                window: Duration::from_secs(60),
            },
            AggIntent::Resets {
                window: Duration::from_secs(300),
            },
            AggIntent::Changes {
                window: Duration::from_secs(300),
            },
            AggIntent::HistogramCount,
            AggIntent::Math(MathFunc::Abs),
            AggIntent::TimeFn(TimeFunc::Hour),
            AggIntent::Group,
            AggIntent::CountValues { label: "v".into() },
            AggIntent::LastOverTime,
        ];
        for v in archive {
            assert!(
                v.archive_only(),
                "{v:?} should be archive-only (no ASAP-tier binding yet)"
            );
        }

        // Warm-tier — must NOT be flagged archive-only or the L4 binder
        // breaks. `Irate` is intentionally absent from this list (removed
        // by the rate/irate fold — see module docs).
        let warm: Vec<AggIntent> = vec![
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact,
            },
            AggIntent::Sum { col: None },
            AggIntent::Min { col: None },
            AggIntent::Max { col: None },
            AggIntent::Avg { col: None },
            AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::TopK {
                k: 10,
                accuracy: AccuracyTarget::Epsilon(0.05),
            },
            AggIntent::Cardinality {
                col: None,
                accuracy: AccuracyTarget::EpsilonDelta {
                    eps: 0.01,
                    delta: 0.001,
                },
            },
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Epsilon(0.01),
            },
            AggIntent::Rate {
                window: Duration::from_secs(60),
            },
            AggIntent::Increase {
                window: Duration::from_secs(300),
            },
        ];
        for v in warm {
            assert!(
                !v.archive_only(),
                "{v:?} is ASAP-tier and must not be archive-only"
            );
        }
    }

    #[test]
    fn archive_only_intent_serde_roundtrip() {
        let cases = vec![
            AggIntent::Absent,
            AggIntent::AbsentOverTime,
            AggIntent::PresentOverTime,
            AggIntent::Delta {
                window: Duration::from_secs(60),
            },
            AggIntent::Deriv {
                window: Duration::from_secs(60),
            },
            AggIntent::PredictLinear {
                window: Duration::from_secs(300),
                seconds: 60.0,
            },
            AggIntent::DoubleExpSmoothing {
                window: Duration::from_secs(300),
                smoothing: 0.3,
                trend: 0.3,
            },
            AggIntent::IDelta {
                window: Duration::from_secs(60),
            },
            AggIntent::Resets {
                window: Duration::from_secs(300),
            },
            AggIntent::Changes {
                window: Duration::from_secs(300),
            },
        ];
        for v in cases {
            let json = serde_json::to_string(&v).unwrap();
            let back: AggIntent = serde_json::from_str(&json).unwrap();
            assert_eq!(v, back, "round-trip failed for {v:?}");
        }
    }

    #[test]
    fn archive_only_output_column_names() {
        let v = c("value", DataType::Float64);
        assert_eq!(AggIntent::Absent.output_column(&v).name, "absent");
        assert_eq!(AggIntent::AbsentOverTime.output_column(&v).name, "absent");
        assert_eq!(AggIntent::PresentOverTime.output_column(&v).name, "present");
        assert_eq!(
            AggIntent::Delta {
                window: Duration::from_secs(60)
            }
            .output_column(&v)
            .name,
            "delta"
        );
        assert_eq!(
            AggIntent::Resets {
                window: Duration::from_secs(60)
            }
            .output_column(&v)
            .name,
            "resets"
        );
    }

    // ── RankingMeasure tests (Phase 1 IR merge, adopted from ASAPController) ──

    #[test]
    fn frequency_heavy_hitter_rule() {
        use RankingMeasure::*;
        assert!(is_frequency_heavy_hitter(true, Frequency));
        assert!(!is_frequency_heavy_hitter(false, Frequency));
        assert!(!is_frequency_heavy_hitter(true, NonAdditive));
        assert!(!is_frequency_heavy_hitter(true, WeightedSum));
    }

    #[test]
    fn ranking_measure_classifies_by_additivity() {
        use RankingMeasure::*;
        assert_eq!(
            ranking_measure(&AggIntent::Count {
                accuracy: AccuracyTarget::Exact
            }),
            Frequency
        );
        assert_eq!(ranking_measure(&AggIntent::Sum { col: None }), WeightedSum);
        assert_eq!(ranking_measure(&AggIntent::Avg { col: None }), NonAdditive);
        assert_eq!(ranking_measure(&AggIntent::Max { col: None }), NonAdditive);
        assert!(Frequency.is_realised_heavy_hitter());
        assert!(!WeightedSum.is_realised_heavy_hitter());
        assert!(!NonAdditive.is_realised_heavy_hitter());
    }

    #[test]
    fn input_col_tracks_only_reducers() {
        assert_eq!(AggIntent::Sum { col: Some(3) }.input_col(), Some(3));
        assert_eq!(
            AggIntent::Avg { col: None }.input_col(),
            None,
            "None = PromQL sample value"
        );
        assert_eq!(
            AggIntent::Count {
                accuracy: AccuracyTarget::Exact
            }
            .input_col(),
            None
        );
        assert_eq!(
            AggIntent::Frequency {
                accuracy: AccuracyTarget::Exact
            }
            .input_col(),
            None,
            "Frequency is a keyed point-query, not a column reducer"
        );
    }
}
