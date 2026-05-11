//! General query algebra — the full IR for SQL and PromQL queries.
//!
//! This module defines two mutually recursive expression types:
//!
//! * [`QueryExpr`] — *relational* operators.  Each node takes zero or more
//!   relations as input and produces a relation.  Maps to SQL's FROM / GROUP BY
//!   / JOIN / UNION layer and PromQL's binary / sub-query layer.
//!
//! * [`ScalarExpr`] — *scalar* operators.  Each node computes a single value
//!   from a row.  Used for WHERE predicates, SELECT projections, HAVING
//!   conditions, and JOIN conditions.
//!
//! # Stage vocabulary
//!
//! Once the [`crate::physical::allocator::SketchAllocator`] annotates the tree,
//! every node carries a [`PipelineStage`](crate::physical::plan::PipelineStage)
//! tag that says where the work executes:
//!
//! | Stage     | Component                  |
//! |-----------|----------------------------|
//! | Agent     | OTel Collector at the SDK  |
//! | Backend   | Central merge collector    |
//! | Precompute| ASAPQuery engine           |
//! | Db        | Backend OLAP / exact store |

use std::time::Duration;

use crate::types::AggType;
use crate::types_v2::AccuracyTarget;

// ── AggIntent harmonization (Step α of legacy_expr migration) ────────────────
//
// The legacy `AggIntent` / `ExactAgg` enums that historically lived here have
// been deleted in favor of the canonical `intent_algebra::agg_intent::AggIntent`
// vocabulary. The translation table is documented in the migration spec; in
// short:
//
//   Legacy Quantile { quantiles, accuracy }  → fan-out into multiple
//                                              canonical Quantile { q, accuracy }
//                                              siblings (callers wrap them
//                                              in a Merge node).
//   Legacy Cardinality { accuracy: f64 }    → canonical Cardinality
//                                              { accuracy: AccuracyTarget }
//   Legacy Frequency  { accuracy: f64 }     → canonical Frequency
//                                              { accuracy: AccuracyTarget }
//   Legacy Extrema { min: true, max: false } → canonical Min
//   Legacy Extrema { min: false, max: true } → canonical Max
//   Legacy Extrema { min: true, max: true }  → fan-out into Min + Max siblings.
//   Legacy Extrema { min: false, max: false } → translation error.
//   Legacy Exact(Sum)   → canonical Sum
//   Legacy Exact(Count) → canonical Count { accuracy: AccuracyTarget::Exact }
//   Legacy Exact(Avg)   → canonical Avg
//   Legacy Exact(Min)   → canonical Min
//   Legacy Exact(Max)   → canonical Max
//   Legacy PerPartition { inner, keys } → recurse on inner, then wrap in the
//                                          `PerPartitionWrap` shape below.
//                                          (PerPartition structural collapse
//                                          to `Aggregate { by, aggs }` is
//                                          Step γ's job.)
//
// `SketchAgg.op` / `WindowedAgg.agg` carry canonical `AggIntent` directly;
// PerPartition semantics ride on `PerPartitionWrap` (a thin legacy-only
// wrapper consumed by the four sites that still build it). Free helpers
// (`agg_to_legacy_agg_type`, `agg_is_mergeable`, etc.) mirror what the old
// `AggIntent::method()` API used to provide so the migration is a typed
// search-and-replace rather than a semantic rewrite.

/// Canonical L3 aggregation intent. Re-exported here so existing
/// `legacy_expr::AggIntent` references keep working — the type is now the
/// single canonical [`crate::intent_algebra::agg_intent::AggIntent`].
pub use crate::intent_algebra::agg_intent::AggIntent;

/// Per-partition wrapper that historically lived on the legacy `AggIntent`
/// enum as a `PerPartition { inner, keys }` variant. Canonical L3 represents
/// this shape via `QueryExpr::Aggregate { by: keys, aggs: [inner] }`, but
/// the legacy carriers (`SketchAgg` / `WindowedAgg`) still need an inline
/// place for `keys` until Step γ collapses them. This wrapper sits exactly
/// where the variant used to.
#[derive(Debug, Clone, PartialEq)]
pub struct PerPartitionWrap {
    pub inner: AggIntent,
    pub keys:  Vec<String>,
}

// ── Shared sketch / predicate types ───────────────────────────────────────────

/// Base relation / metric stream source.
#[derive(Debug, Clone)]
pub struct SourceSpec {
    /// Table name (SQL) or metric name (PromQL).
    pub name: String,
}

/// How the stream is partitioned.
#[derive(Debug, Clone)]
pub enum PartitionKeys {
    /// `by (k1, k2, ...)` — explicit key list.
    By(Vec<String>),
    /// `without (k1, k2, ...)` — complement; resolved against schema at plan time.
    Without(Vec<String>),
}

impl PartitionKeys {
    pub fn keys(&self) -> &[String] {
        match self {
            PartitionKeys::By(k) | PartitionKeys::Without(k) => k,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.keys().is_empty()
    }

    pub fn into_by_keys(self) -> Vec<String> {
        match self {
            PartitionKeys::By(k) => k,
            // For Without, return empty — caller resolves complement.
            PartitionKeys::Without(k) => k,
        }
    }
}

/// Which column / field the sketch aggregation targets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnRef {
    /// Explicit column name (SQL: `AVG(price)` → `Named("price")`).
    Named(String),
    /// The implicit metric sample value (PromQL — always the series value).
    SampleValue,
    /// All rows / COUNT(*).
    Wildcard,
}

// ── AggIntent helpers ────────────────────────────────────────────────────────
//
// These free functions replace the legacy `AggIntent::method()` API. They
// operate on the canonical re-exported `AggIntent` and preserve the old
// semantics one-to-one. After Step γ moves consumers onto the canonical
// surface they can switch to canonical `impl AggIntent` methods or new
// L4-bound accessors; for now this is the minimal-churn shim.

/// Map a canonical [`AggIntent`] to the coarse [`AggType`] used by the
/// legacy planner. Preserves the old `AggIntent::to_agg_type()` semantics:
/// quantile / extrema / exact all collapse onto `AggType::Quantile`,
/// `Cardinality` → `AggType::Cardinality`, `Frequency` → `AggType::Frequency`.
pub fn agg_to_legacy_agg_type(op: &AggIntent) -> AggType {
    match op {
        AggIntent::Cardinality { .. } => AggType::Cardinality,
        AggIntent::Frequency { .. } => AggType::Frequency,
        // Quantile / Min / Max / Sum / Count / Avg / TopK / Rate / Increase
        // all rode the legacy "Quantile" bucket in the AggType taxonomy.
        _ => AggType::Quantile,
    }
}

/// Two instances of this sketch can be merged
/// (`sketch(A ∪ B) = merge(sketch(A), sketch(B))`). Preserves the old
/// `AggIntent::is_mergeable()` rule: Avg is the only non-mergeable case.
pub fn agg_is_mergeable(op: &AggIntent) -> bool {
    !matches!(op, AggIntent::Avg)
}

/// Quantile φ values carried by a `Quantile` intent (empty for non-quantile).
/// Canonical `AggIntent::Quantile` is single-φ post Step α (fan-out happens
/// at construction time); this returns a single-element vec.
pub fn agg_quantiles(op: &AggIntent) -> Vec<f64> {
    match op {
        AggIntent::Quantile { q, .. } => vec![*q],
        _ => vec![],
    }
}

/// Whether this op implies `exact_required` (no sketch benefit). Preserves
/// the legacy `AggIntent::is_exact()` rule: the legacy `Exact(_)` and
/// `Extrema { .. }` cases now map to canonical `Sum / Count / Avg / Min /
/// Max` — those are the cases that flip this flag.
pub fn agg_is_exact(op: &AggIntent) -> bool {
    matches!(
        op,
        AggIntent::Sum
            | AggIntent::Count { .. }
            | AggIntent::Avg
            | AggIntent::Min
            | AggIntent::Max
    )
}

/// Accuracy parameter as a fractional ε (0.0 for exact ops). Preserves the
/// legacy `AggIntent::accuracy() -> f64` accessor by unpacking the typed
/// `AccuracyTarget` carried on canonical Quantile / Cardinality / Frequency
/// / Count / TopK.
pub fn agg_accuracy(op: &AggIntent) -> f64 {
    match op {
        AggIntent::Quantile { accuracy, .. }
        | AggIntent::Cardinality { accuracy }
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

/// Translate a legacy `accuracy: f64` field into the typed
/// `AccuracyTarget`. `0.0` round-trips to `Exact` (matching the old "0.0
/// → exact" sentinel); anything else becomes `Epsilon(eps)`.
pub fn accuracy_target_from_legacy(accuracy: f64) -> AccuracyTarget {
    if accuracy == 0.0 {
        AccuracyTarget::Exact
    } else {
        AccuracyTarget::Epsilon(accuracy)
    }
}

// ── Default constructors (backward compat) ───────────────────────────────────
//
// These mirror the old `AggIntent::default_*` constructors. After Step γ
// the call sites that still need defaults migrate to canonical L4-aware
// builders (sketch_algebra::params + AccuracyTarget on the L3 intent).

/// Default Frequency intent — `accuracy = e / 2000`, matching the legacy
/// `AggIntent::default_frequency` constant.
pub fn default_frequency() -> AggIntent {
    AggIntent::Frequency {
        accuracy: AccuracyTarget::Epsilon(std::f64::consts::E / 2000.0),
    }
}

/// Default Cardinality intent — `accuracy = hll_accuracy(14)`, matching the
/// legacy `AggIntent::default_cardinality` constant.
pub fn default_cardinality() -> AggIntent {
    AggIntent::Cardinality {
        accuracy: AccuracyTarget::Epsilon(hll_accuracy(14)),
    }
}

/// Default Quantile intent. Canonical Quantile is single-φ; callers that
/// historically passed `vec![0.5, 0.99]` to `AggIntent::default_quantile`
/// now invoke this helper once per φ and wrap the results in a
/// `QueryExpr::Merge` of `SketchAgg` siblings (F1 fan-out per the Step α
/// translation spec).
pub fn default_quantile(q: f64) -> AggIntent {
    AggIntent::Quantile {
        q,
        accuracy: AccuracyTarget::Epsilon(0.01),
    }
}

// ── Accuracy helpers ─────────────────────────────────────────────────────────
//
// The 2026-05 layered-cleanup refactor moved these helpers to
// `sketch_algebra::capability` (their structural home — sketch-family
// error bounds). The thin re-exports below keep `legacy_expr::hll_accuracy` /
// `legacy_expr::countmin_accuracy` available so the in-file
// `default_cardinality` call site (and any external `algebra::expr::hll_accuracy`
// reference resolved via the back-compat `algebra` alias in `lib.rs`) keep
// compiling.

pub use crate::sketch_algebra::capability::{countmin_accuracy, hll_accuracy};

/// Unified window specification — captures all language-level window semantics.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub kind: WindowKind,
    pub time_col: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowKind {
    /// Fixed-size, non-overlapping.
    Tumbling { size: Duration },
    /// Fixed-size, overlapping (each sample belongs to ceil(size/slide) windows).
    Sliding { size: Duration, slide: Duration },
    /// Aggregate all samples (no time dimension).
    Unbounded,
    /// From epoch to current time.
    Landmark,
    /// Gap-based: window closes after inactivity.
    Session { gap: Duration },
}

/// A single filter predicate pushed down to the collector.
#[derive(Debug, Clone)]
pub struct Predicate {
    pub col: String,
    pub op:  FilterOp,
    pub val: FilterVal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Like,
    NotLike,
    IsNull,
    IsNotNull,
    /// PromQL `=~` label matcher (RE2 syntax).
    Regex(String),
    /// PromQL `!~` label matcher.
    NotRegex(String),
}

#[derive(Debug, Clone)]
pub enum FilterVal {
    Str(String),
    Num(f64),
    Int(i64),
    Null,
}

// ── Relational algebra ────────────────────────────────────────────────────────

/// Full relational + sketch algebra — the sole query IR.
///
/// Every variant is a *node* in the logical query plan tree.  Leaves are
/// [`QueryExpr::Source`] or [`QueryExpr::Ref`].  Interior nodes combine their
/// `input` child(ren) through the operator they implement.
///
/// # Migration status (legacy_expr migration, Batch 2)
///
/// The ten "A-classified" variants — `Filter`, `Project`, `Partition`,
/// `Distinct`, `Merge`, `Join`, `SetOp`, `Sort`, `Limit`, `BinaryOp` —
/// have canonical structural twins in
/// [`crate::intent_algebra::query_expr::QueryExpr`]. The canonical spelling
/// uses `child:` where these legacy variants use `input:`; the typed
/// [`crate::intent_algebra::Predicate`] replaces [`ScalarExpr`] in `Filter`
/// / `Join` / `Aggregate::having` (translation via
/// [`crate::intent_algebra::from_legacy_scalar`] for the four supported
/// scalar shapes).
///
/// These legacy variants stay here for now because:
///   1. [`legacy_lower`] (Batch 13's target) reshape paths still construct
///      them internally.
///   2. The legacy [`ScalarExpr`] retains four E-deferred variants
///      (`FunctionCall`, `ScalarSubquery`, `InList`, `Between`) that the
///      canonical `Predicate` doesn't cover yet — deleting the legacy
///      `Filter` would lose `ScalarExpr` expressiveness from consumers
///      that haven't migrated.
///
/// Consumer-side redirect (legacy → canonical, with `input:` → `child:` and
/// `ScalarExpr` → `Predicate`) lands in subsequent batches once Batch 2's
/// additive lift is in place.
#[derive(Debug, Clone)]
pub enum QueryExpr {
    // ── Base relations ────────────────────────────────────────────────────

    /// A named metric stream or table.  The outermost leaf.
    Source(SourceSpec),

    /// Reference to a CTE / let-binding by name.  Resolved at plan time.
    Ref(String),

    // ── Filtering & projection ────────────────────────────────────────────

    /// σ — row-level filter (WHERE / PromQL label matchers).
    Filter {
        pred:  ScalarExpr,
        input: Box<QueryExpr>,
    },

    /// π — column projection (SELECT list).
    Project {
        cols:  Vec<ProjectItem>,
        input: Box<QueryExpr>,
    },

    // ── Aggregation ───────────────────────────────────────────────────────

    /// γ + α — GROUP BY followed by aggregate functions.
    ///
    /// `keys` is the GROUP BY column list (empty → global aggregate).
    /// `aggs` is the list of aggregate expressions to compute.
    /// `having` is an optional post-aggregation predicate.
    Aggregate {
        keys:   Vec<String>,
        aggs:   Vec<AggItem>,
        having: Option<ScalarExpr>,
        input:  Box<QueryExpr>,
    },

    // ── Time / streaming operators ────────────────────────────────────────

    /// ψ — time window (PromQL `[5m]`; SQL tumbling/sliding window).
    Window {
        duration: Duration,
        slide:    Option<Duration>,
        input:    Box<QueryExpr>,
    },

    /// γ+α specialisation for sketch aggregations (single sketch per node).
    ///
    /// Kept separate from [`Self::Aggregate`] so the allocator can reason
    /// about which sketch type to use without parsing `AggFunc` variants.
    SketchAgg {
        op:    AggIntent,
        col:   ColumnRef,
        input: Box<QueryExpr>,
    },

    /// Core sketch algebra operator: windowed aggregation intent.
    /// Bundles the window and the aggregation because in sketch systems
    /// the window defines the sketch lifecycle (when to flush/reset).
    WindowedAgg {
        agg:    AggIntent,
        window: WindowSpec,
        col:    ColumnRef,
        input:  Box<QueryExpr>,
    },

    // ── Distributed / multi-stage operators ──────────────────────────────

    /// Partition the stream by key-tuple (GROUP BY / `by (dims)`).
    Partition {
        keys:  PartitionKeys,
        input: Box<QueryExpr>,
    },

    /// δ — deduplicate on `cols` (a tuple of columns) before sketch ingestion.
    /// SQL `SELECT DISTINCT` lowers to this; the column set may be empty
    /// (full-row distinct) or multi-column. PromQL has no direct analog.
    Distinct {
        cols:  Vec<ColumnRef>,
        input: Box<QueryExpr>,
    },

    /// τ — retain only the top-K entries (heavy hitters).
    TopK {
        k:     u64,
        by:    Vec<String>,
        input: Box<QueryExpr>,
    },

    /// ⊕ — merge sketches from independent branches (distributed union).
    Merge {
        inputs: Vec<QueryExpr>,
    },

    // ── Join operators ────────────────────────────────────────────────────

    /// Relational join.
    Join {
        kind:  JoinKind,
        pred:  Option<ScalarExpr>,
        left:  Box<QueryExpr>,
        right: Box<QueryExpr>,
    },

    // ── Set operators ─────────────────────────────────────────────────────

    /// UNION / INTERSECT / EXCEPT (with or without ALL).
    SetOp {
        kind:  SetOpKind,
        all:   bool,
        left:  Box<QueryExpr>,
        right: Box<QueryExpr>,
    },

    // ── Ordering & limiting ───────────────────────────────────────────────

    /// ORDER BY.
    Sort {
        keys:  Vec<SortKey>,
        input: Box<QueryExpr>,
    },

    /// LIMIT [OFFSET].
    Limit {
        n:      u64,
        offset: u64,
        input:  Box<QueryExpr>,
    },

    // ── Subquery / CTE ────────────────────────────────────────────────────

    /// SQL `WITH name AS (expr) IN body` or PromQL recording rule binding.
    LetBinding {
        name: String,
        expr: Box<QueryExpr>,
        body: Box<QueryExpr>,
    },

    // ── PromQL-specific operators ─────────────────────────────────────────

    /// `histogram_quantile(φ, <buckets>)` — converts an HLL / histogram
    /// sketch into a quantile estimate.
    HistogramQuantile {
        phi:   f64,
        input: Box<QueryExpr>,
    },

    /// PromQL sub-query syntax: `<expr>[range:resolution]`.
    PromQLSubquery {
        range:      Duration,
        resolution: Option<Duration>,
        input:      Box<QueryExpr>,
    },

    /// Binary operation between two instant-vector expressions (PromQL `+`, `/`, …).
    /// Also used for SQL arithmetic between sub-relations.
    BinaryOp {
        op:           BinaryOpKind,
        lhs:          Box<QueryExpr>,
        rhs:          Box<QueryExpr>,
        vector_match: Option<VectorMatch>,
    },
}

// ── Scalar algebra ────────────────────────────────────────────────────────────

/// Scalar expression — computes a single value from a row.
///
/// Used in [`QueryExpr::Filter`] predicates, [`ProjectItem`] expressions,
/// [`QueryExpr::Aggregate`] HAVING clauses, and JOIN conditions.
#[derive(Debug, Clone)]
pub enum ScalarExpr {
    /// Column reference: `t.col` or just `col`.
    Column(String),

    /// Literal value.
    Literal(LiteralValue),

    /// Arithmetic / comparison / logical / regex binary operator.
    BinaryOp {
        op:  BinaryOpKind,
        lhs: Box<ScalarExpr>,
        rhs: Box<ScalarExpr>,
    },

    /// Named function call (e.g. `ABS(x)`, `DATE_TRUNC('hour', ts)`).
    FunctionCall {
        name: String,
        args: Vec<ScalarExpr>,
    },

    /// Scalar sub-query (`SELECT MAX(price) FROM orders`).
    ScalarSubquery(Box<QueryExpr>),

    /// `expr IN (v1, v2, …)` or `NOT IN (…)`.
    InList {
        expr:    Box<ScalarExpr>,
        list:    Vec<ScalarExpr>,
        negated: bool,
    },

    /// `expr BETWEEN low AND high` or `NOT BETWEEN …`.
    Between {
        expr:    Box<ScalarExpr>,
        low:     Box<ScalarExpr>,
        high:    Box<ScalarExpr>,
        negated: bool,
    },

    /// `expr IS NULL` / `IS NOT NULL`.
    IsNull {
        expr:    Box<ScalarExpr>,
        negated: bool,
    },
}

// ── Supporting enumerations ───────────────────────────────────────────────────

/// A single item in a SELECT projection list.
#[derive(Debug, Clone)]
pub struct ProjectItem {
    /// Output column name (SQL `AS alias`; None → use expression name).
    pub alias: Option<String>,
    pub expr:  ScalarExpr,
}

/// One aggregate function in a GROUP BY / AGGREGATE node.
#[derive(Debug, Clone)]
pub struct AggItem {
    /// Output column name.
    pub alias:    String,
    /// The aggregate function.
    pub func:     AggFunc,
    /// Column(s) the function operates on.
    pub col:      ColumnRef,
    /// Whether DISTINCT is applied before aggregation.
    pub distinct: bool,
}

/// All aggregate functions that the algebra supports.
///
/// "Sketchable" variants (Quantile, CountDistinct, HeavyHitters) can be
/// approximated by a sketch in early pipeline stages; the rest require
/// exact computation.
#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// Sample / population standard deviation.
    StdDev { population: bool },
    /// Sample / population variance.
    Variance { population: bool },
    /// Approximate quantile at φ ∈ (0, 1].  Maps to DDSketch.
    Quantile(f64),
    /// COUNT DISTINCT — maps to HLL.
    CountDistinct,
    /// Top-K heavy hitters — maps to CountSketch.
    HeavyHitters { k: u64 },
    /// PromQL `rate()` — per-second increase over a window.
    Rate,
    /// PromQL `increase()` — total increase over a window.
    Increase,
    /// PromQL `delta()` — change over a window (may be negative).
    Delta,
    /// Arbitrary named aggregate (UDA or extension).
    Custom(String),
}

impl AggFunc {
    /// Returns true when this function can be computed from merged partial
    /// results:  `f(A ∪ B) = combine(f(A), f(B))`.
    pub fn is_mergeable(&self) -> bool {
        match self {
            AggFunc::Avg | AggFunc::StdDev { .. } | AggFunc::Variance { .. } => false,
            _ => true,
        }
    }

    /// Returns true when this function requires sketch approximation to be
    /// bandwidth-efficient (i.e. the raw data would be too large to ship).
    pub fn is_sketchable(&self) -> bool {
        matches!(
            self,
            AggFunc::Quantile(_) | AggFunc::CountDistinct | AggFunc::HeavyHitters { .. }
        )
    }

    /// Suggest the appropriate [`AggIntent`] for this function, if any.
    ///
    /// Canonical Quantile is single-φ post Step α; this helper returns one
    /// canonical intent. Callers that need multi-φ behaviour build the
    /// merge fan-out themselves (cf. the construction sites in
    /// `legacy_lower::agg_func_to_intent`).
    pub fn to_sketch_op(&self) -> Option<AggIntent> {
        match self {
            AggFunc::Quantile(phi) => Some(default_quantile(*phi)),
            AggFunc::CountDistinct => Some(default_cardinality()),
            AggFunc::HeavyHitters { .. } => Some(default_frequency()),
            AggFunc::Count   => Some(AggIntent::Count { accuracy: AccuracyTarget::Exact }),
            AggFunc::Sum     => Some(AggIntent::Sum),
            AggFunc::Avg     => Some(AggIntent::Avg),
            AggFunc::Min     => Some(AggIntent::Min),
            AggFunc::Max     => Some(AggIntent::Max),
            _                => None,
        }
    }
}

/// Binary operator kinds — used in both [`ScalarExpr::BinaryOp`] and
/// [`QueryExpr::BinaryOp`] (PromQL instant-vector arithmetic).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOpKind {
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    // Comparison
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    // Logical
    And,
    Or,
    // Bitwise
    BitAnd,
    BitOr,
    BitXor,
    // String / pattern
    Concat,
    Like,
    NotLike,
    Regex,
    NotRegex,
    // PromQL-specific
    Unless,
    Atan2,
}

/// JOIN variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    Cross,
    /// Semi-join: return only left rows that have a match (WHERE EXISTS).
    Semi,
    /// Anti-join: return only left rows that have no match (WHERE NOT EXISTS).
    AntiSemi,
}

/// Set-operation variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// PromQL vector matching semantics (`on (…)` / `ignoring (…)` plus
/// `group_left` / `group_right`).
#[derive(Debug, Clone)]
pub struct VectorMatch {
    pub kind:     VectorMatchKind,
    pub labels:   Vec<String>,
    pub grouping: Option<VectorGrouping>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VectorMatchKind {
    On,
    Ignoring,
}

#[derive(Debug, Clone)]
pub struct VectorGrouping {
    pub side:   GroupSide,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupSide {
    Left,
    Right,
}

/// ORDER BY sort key.
#[derive(Debug, Clone)]
pub struct SortKey {
    pub col:  String,
    pub desc: bool,
    /// NULLS FIRST / NULLS LAST (None → database default).
    pub nulls_first: Option<bool>,
}

/// Scalar literal.
#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Duration(Duration),
}

impl QueryExpr {
    /// Walk the expression tree depth-first and call `f` on every node.
    pub fn walk<F: FnMut(&QueryExpr)>(&self, f: &mut F) {
        f(self);
        match self {
            QueryExpr::Source(_) | QueryExpr::Ref(_) => {}
            QueryExpr::Filter { input, .. }
            | QueryExpr::Project { input, .. }
            | QueryExpr::Window { input, .. }
            | QueryExpr::SketchAgg { input, .. }
            | QueryExpr::WindowedAgg { input, .. }
            | QueryExpr::Partition { input, .. }
            | QueryExpr::Distinct { input, .. }
            | QueryExpr::TopK { input, .. }
            | QueryExpr::Sort { input, .. }
            | QueryExpr::Limit { input, .. }
            | QueryExpr::HistogramQuantile { input, .. }
            | QueryExpr::PromQLSubquery { input, .. } => input.walk(f),

            QueryExpr::Aggregate { input, .. } => input.walk(f),

            QueryExpr::Merge { inputs } => {
                for i in inputs { i.walk(f); }
            }
            QueryExpr::Join { left, right, .. }
            | QueryExpr::SetOp { left, right, .. }
            | QueryExpr::BinaryOp { lhs: left, rhs: right, .. } => {
                left.walk(f);
                right.walk(f);
            }
            QueryExpr::LetBinding { expr, body, .. } => {
                expr.walk(f);
                body.walk(f);
            }
        }
    }

    /// Returns `true` when the sub-tree contains at least one [`QueryExpr::SketchAgg`]
    /// or [`QueryExpr::TopK`] node (i.e. sketch work is present).
    pub fn has_sketch_work(&self) -> bool {
        let mut found = false;
        self.walk(&mut |n| {
            if matches!(n, QueryExpr::SketchAgg { .. } | QueryExpr::WindowedAgg { .. } | QueryExpr::TopK { .. }) {
                found = true;
            }
        });
        found
    }

    /// Returns the outermost metric/table name from the first `Source` leaf.
    pub fn source_name(&self) -> Option<&str> {
        match self {
            QueryExpr::Source(s) => Some(&s.name),
            QueryExpr::Filter { input, .. }
            | QueryExpr::Project { input, .. }
            | QueryExpr::Window { input, .. }
            | QueryExpr::SketchAgg { input, .. }
            | QueryExpr::WindowedAgg { input, .. }
            | QueryExpr::Partition { input, .. }
            | QueryExpr::Distinct { input, .. }
            | QueryExpr::TopK { input, .. }
            | QueryExpr::Sort { input, .. }
            | QueryExpr::Limit { input, .. }
            | QueryExpr::Aggregate { input, .. }
            | QueryExpr::HistogramQuantile { input, .. }
            | QueryExpr::PromQLSubquery { input, .. } => input.source_name(),
            QueryExpr::Merge { inputs } => inputs.first()?.source_name(),
            QueryExpr::Join { left, .. }
            | QueryExpr::SetOp { left, .. }
            | QueryExpr::BinaryOp { lhs: left, .. } => left.source_name(),
            QueryExpr::LetBinding { body, .. } => body.source_name(),
            QueryExpr::Ref(_) => None,
        }
    }
}

// ── Predicate → ScalarExpr conversion ────────────────────────────────────────

/// Convert a slice of legacy [`Predicate`]s (AND-list) into a single
/// [`ScalarExpr`] tree.  An empty slice becomes `Literal(true)`.
fn scalar_from_predicates(preds: &[Predicate]) -> ScalarExpr {
    if preds.is_empty() {
        return ScalarExpr::Literal(LiteralValue::Bool(true));
    }
    let mut iter = preds.iter().map(scalar_from_predicate);
    let first = iter.next().unwrap();
    iter.fold(first, |acc, p| ScalarExpr::BinaryOp {
        op:  BinaryOpKind::And,
        lhs: Box::new(acc),
        rhs: Box::new(p),
    })
}

fn scalar_from_predicate(p: &Predicate) -> ScalarExpr {
    let col = ScalarExpr::Column(p.col.clone());
    let val = match &p.val {
        FilterVal::Str(s)  => ScalarExpr::Literal(LiteralValue::Str(s.clone())),
        FilterVal::Num(n)  => ScalarExpr::Literal(LiteralValue::Float(*n)),
        FilterVal::Int(i)  => ScalarExpr::Literal(LiteralValue::Int(*i)),
        FilterVal::Null    => ScalarExpr::Literal(LiteralValue::Null),
    };
    match &p.op {
        FilterOp::Eq  => bin(BinaryOpKind::Eq,  col, val),
        FilterOp::Ne  => bin(BinaryOpKind::Ne,  col, val),
        FilterOp::Lt  => bin(BinaryOpKind::Lt,  col, val),
        FilterOp::Le  => bin(BinaryOpKind::Le,  col, val),
        FilterOp::Gt  => bin(BinaryOpKind::Gt,  col, val),
        FilterOp::Ge  => bin(BinaryOpKind::Ge,  col, val),
        FilterOp::Like    => bin(BinaryOpKind::Like,    col, val),
        FilterOp::NotLike => bin(BinaryOpKind::NotLike, col, val),
        FilterOp::IsNull     => ScalarExpr::IsNull { expr: Box::new(col), negated: false },
        FilterOp::IsNotNull  => ScalarExpr::IsNull { expr: Box::new(col), negated: true  },
        FilterOp::Regex(r)    => bin(
            BinaryOpKind::Regex,
            col,
            ScalarExpr::Literal(LiteralValue::Str(r.clone())),
        ),
        FilterOp::NotRegex(r) => bin(
            BinaryOpKind::NotRegex,
            col,
            ScalarExpr::Literal(LiteralValue::Str(r.clone())),
        ),
    }
}

fn bin(op: BinaryOpKind, lhs: ScalarExpr, rhs: ScalarExpr) -> ScalarExpr {
    ScalarExpr::BinaryOp { op, lhs: Box::new(lhs), rhs: Box::new(rhs) }
}

// ── Display helpers ───────────────────────────────────────────────────────────

impl std::fmt::Display for BinaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOpKind::Add => "+", BinaryOpKind::Sub => "-",
            BinaryOpKind::Mul => "*", BinaryOpKind::Div => "/",
            BinaryOpKind::Mod => "%", BinaryOpKind::Pow => "^",
            BinaryOpKind::Eq  => "=", BinaryOpKind::Ne  => "!=",
            BinaryOpKind::Lt  => "<", BinaryOpKind::Le  => "<=",
            BinaryOpKind::Gt  => ">", BinaryOpKind::Ge  => ">=",
            BinaryOpKind::And => "AND", BinaryOpKind::Or => "OR",
            BinaryOpKind::BitAnd => "&", BinaryOpKind::BitOr => "|",
            BinaryOpKind::BitXor => "XOR",
            BinaryOpKind::Concat => "||",
            BinaryOpKind::Like    => "LIKE",    BinaryOpKind::NotLike => "NOT LIKE",
            BinaryOpKind::Regex   => "=~",      BinaryOpKind::NotRegex => "!~",
            BinaryOpKind::Unless  => "unless",  BinaryOpKind::Atan2 => "atan2",
        };
        write!(f, "{s}")
    }
}

impl std::fmt::Display for AggFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggFunc::Count           => write!(f, "COUNT"),
            AggFunc::Sum             => write!(f, "SUM"),
            AggFunc::Avg             => write!(f, "AVG"),
            AggFunc::Min             => write!(f, "MIN"),
            AggFunc::Max             => write!(f, "MAX"),
            AggFunc::StdDev { .. }   => write!(f, "STDDEV"),
            AggFunc::Variance { .. } => write!(f, "VARIANCE"),
            AggFunc::Quantile(p)     => write!(f, "QUANTILE({p})"),
            AggFunc::CountDistinct   => write!(f, "COUNT_DISTINCT"),
            AggFunc::HeavyHitters { k } => write!(f, "HEAVY_HITTERS({k})"),
            AggFunc::Rate            => write!(f, "rate"),
            AggFunc::Increase        => write!(f, "increase"),
            AggFunc::Delta           => write!(f, "delta"),
            AggFunc::Custom(s)       => write!(f, "{s}"),
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn src(name: &str) -> QueryExpr {
        QueryExpr::Source(SourceSpec { name: name.into() })
    }

    // ── has_sketch_work ───────────────────────────────────────────────────────

    #[test]
    fn has_sketch_work_true_when_ddsketch_present() {
        let qe = QueryExpr::SketchAgg {
            op:    default_quantile(0.5),
            col:   ColumnRef::SampleValue,
            input: Box::new(src("m")),
        };
        assert!(qe.has_sketch_work());
    }

    #[test]
    fn has_sketch_work_false_for_plain_source() {
        let qe = QueryExpr::Source(SourceSpec { name: "x".into() });
        assert!(!qe.has_sketch_work());
    }

    // ── source_name ───────────────────────────────────────────────────────────

    #[test]
    fn source_name_extracted_through_chain() {
        let qe = QueryExpr::Window {
            duration: Duration::from_secs(60),
            slide:    None,
            input: Box::new(QueryExpr::Filter {
                pred:  ScalarExpr::Literal(LiteralValue::Bool(true)),
                input: Box::new(src("my_metric")),
            }),
        };
        assert_eq!(qe.source_name(), Some("my_metric"));
    }

    // ── AggFunc helpers ───────────────────────────────────────────────────────

    #[test]
    fn agg_func_mergeability() {
        assert!(AggFunc::Sum.is_mergeable());
        assert!(AggFunc::Count.is_mergeable());
        assert!(AggFunc::Min.is_mergeable());
        assert!(AggFunc::Max.is_mergeable());
        assert!(!AggFunc::Avg.is_mergeable());
        assert!(!AggFunc::StdDev { population: false }.is_mergeable());
        assert!(!AggFunc::Variance { population: true }.is_mergeable());
    }

    #[test]
    fn agg_func_sketchability() {
        assert!(AggFunc::Quantile(0.99).is_sketchable());
        assert!(AggFunc::CountDistinct.is_sketchable());
        assert!(AggFunc::HeavyHitters { k: 10 }.is_sketchable());
        assert!(!AggFunc::Avg.is_sketchable());
        assert!(!AggFunc::Sum.is_sketchable());
    }

    #[test]
    fn agg_func_to_sketch_op_quantile() {
        let op = AggFunc::Quantile(0.99).to_sketch_op();
        assert!(matches!(op, Some(AggIntent::Quantile { .. })));
    }

    #[test]
    fn agg_func_to_sketch_op_count_distinct() {
        let op = AggFunc::CountDistinct.to_sketch_op();
        assert!(matches!(op, Some(AggIntent::Cardinality { .. })));
    }

    #[test]
    fn agg_func_to_sketch_op_heavy_hitters() {
        let op = AggFunc::HeavyHitters { k: 50 }.to_sketch_op();
        assert!(matches!(op, Some(AggIntent::Frequency { .. })));
    }

    // ── ScalarExpr predicate list conversion ──────────────────────────────────

    #[test]
    fn empty_pred_list_becomes_literal_true() {
        let s = scalar_from_predicates(&[]);
        assert!(matches!(s, ScalarExpr::Literal(LiteralValue::Bool(true))));
    }

    #[test]
    fn two_preds_become_and_tree() {
        let preds = vec![
            Predicate { col: "a".into(), op: FilterOp::Eq, val: FilterVal::Int(1) },
            Predicate { col: "b".into(), op: FilterOp::Gt, val: FilterVal::Num(2.0) },
        ];
        let s = scalar_from_predicates(&preds);
        assert!(matches!(s, ScalarExpr::BinaryOp { op: BinaryOpKind::And, .. }));
    }

    // ── BinaryOpKind display ──────────────────────────────────────────────────

    #[test]
    fn binary_op_kind_display() {
        assert_eq!(BinaryOpKind::Add.to_string(),       "+");
        assert_eq!(BinaryOpKind::And.to_string(),       "AND");
        assert_eq!(BinaryOpKind::Regex.to_string(),     "=~");
        assert_eq!(BinaryOpKind::NotRegex.to_string(),  "!~");
        assert_eq!(BinaryOpKind::Unless.to_string(),    "unless");
    }

    // ── Complex nested tree ───────────────────────────────────────────────────

    #[test]
    fn complex_nested_tree() {
        // TopK(10, Partition(symbol, Window(5m, SketchAgg(CountSketch, Source(price)))))
        let qe = QueryExpr::TopK {
            k: 10,
            by: vec![],
            input: Box::new(QueryExpr::Partition {
                keys:  PartitionKeys::By(vec!["symbol".into()]),
                input: Box::new(QueryExpr::Window {
                    duration: Duration::from_secs(300),
                    slide:    None,
                    input:    Box::new(QueryExpr::SketchAgg {
                        op:    default_frequency(),
                        col:   ColumnRef::Wildcard,
                        input: Box::new(src("price")),
                    }),
                }),
            }),
        };
        assert!(qe.has_sketch_work());
        assert_eq!(qe.source_name(), Some("price"));
    }

    // ── LetBinding and Subquery ───────────────────────────────────────────────

    #[test]
    fn let_binding_construction() {
        let expr = QueryExpr::LetBinding {
            name: "base".into(),
            expr: Box::new(QueryExpr::Source(SourceSpec { name: "cpu".into() })),
            body: Box::new(QueryExpr::Ref("base".into())),
        };
        match expr {
            QueryExpr::LetBinding { name, .. } => assert_eq!(name, "base"),
            _ => panic!(),
        }
    }

    #[test]
    fn histogram_quantile_node() {
        let expr = QueryExpr::HistogramQuantile {
            phi:   0.95,
            input: Box::new(QueryExpr::Source(SourceSpec { name: "hist".into() })),
        };
        match expr {
            QueryExpr::HistogramQuantile { phi, .. } => {
                assert!((phi - 0.95).abs() < 1e-9);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn promql_subquery_node() {
        let expr = QueryExpr::PromQLSubquery {
            range:      Duration::from_secs(3600),
            resolution: Some(Duration::from_secs(60)),
            input:      Box::new(QueryExpr::Source(SourceSpec { name: "m".into() })),
        };
        match expr {
            QueryExpr::PromQLSubquery { range, resolution, .. } => {
                assert_eq!(range, Duration::from_secs(3600));
                assert_eq!(resolution, Some(Duration::from_secs(60)));
            }
            _ => panic!(),
        }
    }

    // ── AggIntent and related types ──────────────────────────────────────────

    #[test]
    fn agg_intent_cardinality_is_mergeable() {
        assert!(agg_is_mergeable(&default_cardinality()));
    }

    #[test]
    fn agg_intent_avg_not_mergeable() {
        assert!(!agg_is_mergeable(&AggIntent::Avg));
    }

    #[test]
    fn per_partition_wrap_carries_inner_and_keys() {
        let wrap = PerPartitionWrap {
            inner: default_cardinality(),
            keys:  vec!["region".into()],
        };
        assert_eq!(wrap.keys, vec!["region".to_string()]);
        assert!(agg_is_mergeable(&wrap.inner));
    }

    #[test]
    fn partition_keys_without_variant() {
        let keys = PartitionKeys::Without(vec!["instance".into()]);
        assert_eq!(keys.keys(), &["instance".to_string()]);
        assert!(!keys.is_empty());
    }

    #[test]
    fn agg_intent_to_legacy_agg_type() {
        use crate::types::AggType;
        assert_eq!(agg_to_legacy_agg_type(&default_cardinality()), AggType::Cardinality);
        assert_eq!(agg_to_legacy_agg_type(&default_frequency()),   AggType::Frequency);
        assert_eq!(agg_to_legacy_agg_type(&default_quantile(0.5)), AggType::Quantile);
    }

    #[test]
    fn agg_intent_is_exact() {
        assert!(agg_is_exact(&AggIntent::Sum));
        assert!(agg_is_exact(&AggIntent::Min));
        assert!(agg_is_exact(&AggIntent::Max));
        assert!(!agg_is_exact(&default_cardinality()));
    }

}
