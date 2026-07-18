//! Layer 3 IR — `QueryExpr` DAG (intent-only, language-orthogonal,
//! deployment-independent).
//!
//! Per `control_plane/docs/design.md` §6 "`core::intent_algebra` — Layer 3"
//! (around line ~269). Pure intent at this layer: no language-specific
//! operators (no `HistogramQuantile`, no `PromQLSubquery`), no sketch
//! types, no sketch parameters, no physical operator choice.
//!
//! Single-root tree per query. Multi-root DAGs (cross-query CSE fan-in)
//! live one level above in `WorkloadPlan` (`types_v2::WorkloadPlan`).
//! Within-query CTE / let-binding fan-in *is* expressible here via
//! [`QueryExpr::LetBinding`] + [`QueryExpr::Ref`].
//!
//! Variant set. Phase B shipped `Scan`, `Window`, `Aggregate`,
//! `LetBinding`, `Ref`. Batch 2 of the relational migration adds the ten
//! "A-classified" structurally-canonical variants from `design.md` §6:
//! `Filter`, `Project`, `Partition`, `Distinct`, `Merge`, `Join`,
//! `SetOp`, `Sort`, `Limit`, `BinaryOp`. Step γ7 adds `Subquery` (the
//! canonical counterpart of `relational::PromQLSubquery`). `WindowFunc`
//! remains deferred until the planner grows a consumer for it. The shape
//! defined here is forward-compatible — adding more variants is purely
//! additive.
//!
//! Single-input variants here use `child:` (matching the existing
//! `Window`, `Aggregate`, `LetBinding` shape). Legacy `input:` survives in
//! `relational::QueryExpr` until its consumers redirect through here.

#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::intent_algebra::agg_intent::AggIntent;
use crate::intent_algebra::schema::{Column, ColumnId, DataType, Schema};
use crate::types_v2::BindingName;

/// Errors produced when the L3 IR is constructed or its output schema is
/// derived. Surfaced by the lowering function and any caller that walks
/// the DAG.
#[derive(Debug, Error)]
pub enum QueryExprError {
    /// `Ref(name)` did not resolve against any in-scope `LetBinding`.
    #[error("unresolved ref: {0}")]
    UnresolvedRef(String),
    /// `Aggregate { by, .. }` referenced a column position that is not
    /// in the input schema. Caught at schema-derivation time per the
    /// design's locally-checkable invariant.
    #[error("by-column id {0} out of range (input has {1} columns)")]
    InvalidGroupByColumn(ColumnId, usize),
    /// `Window` requires a `time_index` field on its input schema —
    /// `design.md` §6 schema-flow table.
    #[error("Window requires a time_index on input schema")]
    WindowMissingTimeIndex,
    /// `Merge` requires at least one child to derive its output schema.
    #[error("Merge requires at least one child")]
    EmptyMerge,
    /// A legacy `ScalarExpr` variant has no canonical `Predicate` counterpart
    /// yet. Surfaces from [`from_legacy_scalar`] for `ScalarSubquery` only —
    /// it carries a legacy `QueryExpr` sub-tree that needs the tree converter.
    #[error("legacy ScalarExpr variant `{0}` is not yet representable in canonical Predicate")]
    UnsupportedLegacyScalar(&'static str),
}

/// Streaming / time-window kind. PromQL `[5m]` is `Sliding`; SQL `TUMBLE`
/// is `Tumbling`; PromQL has no native `Session` window so it stays
/// unused for the DC + PromQL scope of this PR.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowKind {
    Tumbling,
    Sliding,
    Session,
}

/// Source of a `Scan`. Phase B ships `TimeSeries` (the only shape DC +
/// PromQL needs); `Table` is sketched out so future deployment models
/// (asap-fusion / OLAP) plug in without an enum-shape rev. Recursive
/// `Source::Join` is design.md §6 line ~378 territory and stays out of
/// scope for now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Source {
    /// PromQL / DC lifecycle leaf — a metric stream identified by name +
    /// optional label filters; produces `(timestamp, value, *labels)`
    /// columns.
    TimeSeries { metric: String },
    /// Tabular leaf — reserved for asap-fusion. Carries the table name;
    /// columns ride on the supplied `Schema`.
    Table { table_ref: String },
}

/// Equality label filter on a `Scan`. PromQL `{service="api"}` → one of
/// these; richer match operators (`!=`, `=~`, `!~`) live in `Filter`'s
/// generic predicate per design.md §6 line ~296 and are deferred to the
/// follow-up phase that adds the `Filter` variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelFilter {
    pub label: String,
    pub equals: String,
}

/// Optional HAVING-style predicate on `Aggregate`. Modeled as an opaque
/// expression string at L3 — Phase B doesn't have a typed predicate IR
/// yet; adding one is a separate PR (would also introduce the `Filter`
/// variant per design.md §6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HavingPredicate(pub String);

// ── Supporting types lifted from relational ─────────────────────────────────
//
// Per Batch 2 of the relational migration: these are structural copies of
// the legacy supporting enums so the canonical [`QueryExpr`] variants below
// can reference them without rooting the canonical IR in `relational`.
// Field shapes mirror `design.md` §6.

/// Which column / field a sketch / projection / DISTINCT operation targets.
/// Survives at L3 as a name-keyed alias (design.md §6.1) even though
/// canonical schema uses positional [`ColumnId`] — intents like
/// `AggIntent::TopK { by: Vec<ColumnRef> }` consume it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnRef {
    /// Explicit column name (SQL: `AVG(price)` → `Named("price")`).
    Named(String),
    /// The implicit metric sample value (PromQL — always the series value).
    SampleValue,
    /// All rows / COUNT(*).
    Wildcard,
}

/// Partition-key spec — `by (k1, k2, …)` or `without (k1, k2, …)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
}

/// Binary operator kinds — used in both [`Predicate::BinaryOp`] (scalar
/// composition) and [`QueryExpr::BinaryOp`] (PromQL instant-vector
/// arithmetic between two relational sub-expressions).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

impl std::fmt::Display for BinaryOpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            BinaryOpKind::Add => "+",
            BinaryOpKind::Sub => "-",
            BinaryOpKind::Mul => "*",
            BinaryOpKind::Div => "/",
            BinaryOpKind::Mod => "%",
            BinaryOpKind::Pow => "^",
            BinaryOpKind::Eq => "=",
            BinaryOpKind::Ne => "!=",
            BinaryOpKind::Lt => "<",
            BinaryOpKind::Le => "<=",
            BinaryOpKind::Gt => ">",
            BinaryOpKind::Ge => ">=",
            BinaryOpKind::And => "AND",
            BinaryOpKind::Or => "OR",
            BinaryOpKind::BitAnd => "&",
            BinaryOpKind::BitOr => "|",
            BinaryOpKind::BitXor => "XOR",
            BinaryOpKind::Concat => "||",
            BinaryOpKind::Like => "LIKE",
            BinaryOpKind::NotLike => "NOT LIKE",
            BinaryOpKind::Regex => "=~",
            BinaryOpKind::NotRegex => "!~",
            BinaryOpKind::Unless => "unless",
            BinaryOpKind::Atan2 => "atan2",
        };
        write!(f, "{s}")
    }
}

/// JOIN variant. design.md §6 lists Inner / LeftOuter / RightOuter /
/// FullOuter / Cross / Semi / AntiSemi.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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

/// Set-operation variant — UNION / INTERSECT / EXCEPT.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// ORDER BY sort key.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SortKey {
    pub col: String,
    pub desc: bool,
    /// NULLS FIRST / NULLS LAST (None → database default).
    #[serde(default)]
    pub nulls_first: Option<bool>,
}

/// PromQL vector matching semantics (`on (…)` / `ignoring (…)` plus
/// `group_left` / `group_right`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorMatch {
    pub kind: VectorMatchKind,
    pub labels: Vec<String>,
    #[serde(default)]
    pub grouping: Option<VectorGrouping>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorMatchKind {
    On,
    Ignoring,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorGrouping {
    pub side: GroupSide,
    pub labels: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupSide {
    Left,
    Right,
}

/// Scalar literal value. Subset of values used by the canonical
/// [`Predicate`] — extended literal kinds (durations, intervals) stay in
/// `relational::LiteralValue` until the E-variants migrate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiteralValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
}

/// One item in a SELECT projection list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectItem {
    /// Output column name (SQL `AS alias`; None → use expression name).
    #[serde(default)]
    pub alias: Option<String>,
    /// Projected expression. Modeled as a [`Predicate`] for the four
    /// covered scalar shapes (column / literal / binary op / is-null);
    /// the legacy E-variants (`FunctionCall`, `ScalarSubquery`, `InList`,
    /// `Between`) stay in `relational::ScalarExpr` and live in
    /// [`ProjectItem::raw_expr`] until they migrate.
    pub expr: Predicate,
}

// ── Typed Predicate ──────────────────────────────────────────────────────────

/// Typed scalar predicate — the canonical counterpart of
/// `relational::ScalarExpr`. Covers all eight legacy scalar shapes:
/// column / literal / binary-op / is-null (the structurally clean four)
/// plus `FunctionCall` / `InList` / `Between` / `ScalarSubquery` (the
/// E-variants). `ScalarSubquery` carries a canonical [`QueryExpr`] —
/// translating a legacy `ScalarSubquery` requires the legacy→canonical
/// tree converter, so [`from_legacy_scalar`] still defers that one arm.
///
/// See [`from_legacy_scalar`] for the migration helper.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Predicate {
    /// Column reference (by name).
    Column(ColumnRef),
    /// Constant literal.
    Literal(LiteralValue),
    /// Binary operator (=, !=, <, <=, >, >=, AND, OR, …).
    BinaryOp {
        op: BinaryOpKind,
        lhs: Box<Predicate>,
        rhs: Box<Predicate>,
    },
    /// IS NULL / IS NOT NULL.
    IsNull { expr: Box<Predicate>, negated: bool },
    /// Named scalar function call (`ABS(x)`, `DATE_TRUNC('hour', ts)`).
    FunctionCall { name: String, args: Vec<Predicate> },
    /// Scalar sub-query (`SELECT MAX(price) FROM orders`).
    ScalarSubquery(Box<QueryExpr>),
    /// `expr IN (v1, v2, …)` / `NOT IN (…)`.
    InList {
        expr: Box<Predicate>,
        list: Vec<Predicate>,
        negated: bool,
    },
    /// `expr BETWEEN low AND high` / `NOT BETWEEN …`.
    Between {
        expr: Box<Predicate>,
        low: Box<Predicate>,
        high: Box<Predicate>,
        negated: bool,
    },
}

/// L3 algebra node. See module doc for the variant subset rationale.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "node", rename_all = "snake_case")]
pub enum QueryExpr {
    /// Outermost leaf — a metric stream / table read. `schema` is the
    /// authoritative output of this scan, supplied by the lowering pass
    /// (which consults the source / DB schema catalog at L1→L2 time).
    Scan {
        source: Source,
        #[serde(default)]
        label_filters: Vec<LabelFilter>,
        schema: Schema,
    },
    /// Streaming / time-window. Defines the lifecycle (flush / reset
    /// bounds) of any aggregate in its sub-tree.
    Window {
        kind: WindowKind,
        size: Duration,
        #[serde(default)]
        slide: Option<Duration>,
        child: Box<QueryExpr>,
    },
    /// γ + α — GROUP BY + aggregate intents. `by` are positional
    /// references into `child.output_schema().columns`; `aggs` carry
    /// `AggIntent` (no sketch types — that's L4).
    Aggregate {
        by: Vec<ColumnId>,
        aggs: Vec<AggIntent>,
        #[serde(default)]
        having: Option<HavingPredicate>,
        child: Box<QueryExpr>,
    },
    /// SQL `WITH name AS (expr) SELECT ... FROM name` / PromQL recording-
    /// rule binding. Names a sub-expression; references via `Ref(name)`.
    /// Output schema = `child`'s output schema.
    LetBinding {
        name: BindingName,
        expr: Box<QueryExpr>,
        child: Box<QueryExpr>,
    },
    /// Reference a `LetBinding` by name. Resolved at plan time; output
    /// schema = the named binding's expression's output schema.
    Ref { name: BindingName },

    // ── A-classified variants lifted in Batch 2 of the relational migration ──
    //
    // Each is a structural copy of the legacy variant of the same name in
    // `relational::QueryExpr`. Single-input variants here use `child:` to
    // match the existing canonical `Window`/`Aggregate`/`LetBinding` shape,
    // whereas legacy spells them `input:`. Consumers that haven't migrated
    // yet keep using `relational::QueryExpr::*` — the legacy variants stay
    // in place until the consumer-side redirect lands in subsequent batches.
    /// σ — row-level filter (WHERE / PromQL label matchers). Uses the new
    /// typed [`Predicate`] (only Column / Literal / BinaryOp / IsNull at L3
    /// for now; `FunctionCall` / `ScalarSubquery` / `InList` / `Between`
    /// stay in `relational::ScalarExpr` until their own batch).
    Filter {
        pred: Predicate,
        child: Box<QueryExpr>,
    },

    /// π — column projection (SELECT list).
    Project {
        cols: Vec<ProjectItem>,
        child: Box<QueryExpr>,
    },

    /// Partition the stream by key tuple (`GROUP BY` / PromQL `by (dims)`).
    /// Logical-only marker — carries a sharding hint for L5's stage allocator.
    Partition {
        keys: PartitionKeys,
        child: Box<QueryExpr>,
    },

    /// δ — SQL `DISTINCT` / row deduplication on `cols`.
    Distinct {
        cols: Vec<ColumnRef>,
        child: Box<QueryExpr>,
    },

    /// ⊕ — union of sub-results from independent stages or shards (the
    /// exact-merge case). Sketch unions live in `PhysicalExpr`, not here.
    Merge { children: Vec<QueryExpr> },

    /// Logical join. L4 picks the physical alternative
    /// (`HashJoin` / `SortMergeJoin` / `SketchJoin`).
    Join {
        kind: JoinKind,
        pred: Predicate,
        left: Box<QueryExpr>,
        right: Box<QueryExpr>,
    },

    /// UNION / INTERSECT / EXCEPT, with or without ALL.
    SetOp {
        kind: SetOpKind,
        all: bool,
        left: Box<QueryExpr>,
        right: Box<QueryExpr>,
    },

    /// Generic ORDER BY — survives L3 for non-heavy-hitter cases
    /// (`ORDER BY name LIMIT 10`, `ORDER BY ts DESC LIMIT 1`). The heavy-
    /// hitter shape (`ORDER BY count DESC LIMIT k`, PromQL `topk(k, …)`)
    /// produces [`AggIntent::TopK`] rather than `Sort + Limit`.
    Sort {
        keys: Vec<SortKey>,
        child: Box<QueryExpr>,
    },

    /// `LIMIT n OFFSET k`. Generic case only — see `Sort`'s doc-comment.
    Limit {
        n: usize,
        offset: usize,
        child: Box<QueryExpr>,
    },

    /// Arithmetic / comparison / boolean composition between two relational
    /// sub-expressions (PromQL `+`, `/`, `and`, `or`, `unless`; SQL boolean
    /// composition between sub-relations).
    BinaryOp {
        op: BinaryOpKind,
        lhs: Box<QueryExpr>,
        rhs: Box<QueryExpr>,
        #[serde(default)]
        vector_match: Option<VectorMatch>,
    },

    /// PromQL sub-query (`<expr>[range:resolution]`) — re-evaluates `child`
    /// at `resolution`-spaced steps across the trailing `range` window,
    /// producing a range-vector the enclosing function consumes. The
    /// canonical counterpart of `relational::QueryExpr::PromQLSubquery`.
    /// Logical pass-through for schema flow — the range/resolution are a
    /// sampling hint the L5 precompute stage reads, not a schema transform.
    Subquery {
        range: Duration,
        #[serde(default)]
        resolution: Option<Duration>,
        child: Box<QueryExpr>,
    },
}

impl QueryExpr {
    /// Compute the output schema of this node. Walks the tree, resolving
    /// `Ref` against `LetBinding`s in scope. Errors propagate per
    /// [`QueryExprError`].
    ///
    /// Callers that want the schema of the *root* of a single query call
    /// `expr.output_schema(&BindingScope::default())`.
    pub fn output_schema(&self) -> Result<Schema, QueryExprError> {
        self.output_schema_in(&BindingScope::default())
    }

    /// Variant of [`Self::output_schema`] that takes an explicit binding
    /// scope. Used internally during DAG walks; exposed for callers that
    /// pre-populate bindings from a workload-level container.
    pub fn output_schema_in(&self, scope: &BindingScope) -> Result<Schema, QueryExprError> {
        match self {
            QueryExpr::Scan { schema, .. } => Ok(schema.clone()),
            QueryExpr::Window { child, .. } => {
                let in_schema = child.output_schema_in(scope)?;
                if in_schema.time_index.is_none() {
                    return Err(QueryExprError::WindowMissingTimeIndex);
                }
                // Window propagates row identity → carries unique_keys
                // verbatim. Synthetic `window_id` / `window_start/end`
                // columns (design.md §6 schema-flow row 6) are deferred
                // until the planner consumes them.
                Ok(in_schema)
            }
            QueryExpr::Aggregate {
                by, aggs, child, ..
            } => {
                let in_schema = child.output_schema_in(scope)?;
                // by-column ids must be in range.
                let mut out_cols: Vec<Column> = Vec::with_capacity(by.len() + aggs.len());
                for &id in by {
                    let c =
                        in_schema
                            .columns
                            .get(id)
                            .ok_or(QueryExprError::InvalidGroupByColumn(
                                id,
                                in_schema.columns.len(),
                            ))?;
                    out_cols.push(c.clone());
                }
                // One new column per intent. PromQL convention:
                // intent applied to the synthetic `value` column when
                // present; otherwise to the first non-grouped column.
                let value_col_idx = in_schema
                    .column_id("value")
                    .or_else(|| (0..in_schema.columns.len()).find(|i| !by.contains(i)));
                let probe = value_col_idx
                    .and_then(|i| in_schema.columns.get(i))
                    .cloned()
                    .unwrap_or(Column {
                        name: "value".into(),
                        dtype: DataType::Float64,
                        nullable: false,
                    });
                for intent in aggs {
                    out_cols.push(intent.output_column(&probe));
                }
                // Output unique_keys = [by]. The group-by column tuple
                // is unique in the output by construction (design.md §6
                // schema-flow table).
                let unique_keys = if by.is_empty() {
                    Vec::new()
                } else {
                    vec![(0..by.len()).collect()]
                };
                // Aggregate strips the time axis — output is one row per
                // group, not one row per timestamp.
                Ok(Schema {
                    columns: out_cols,
                    time_index: None,
                    unique_keys,
                })
            }
            QueryExpr::LetBinding { name, expr, child } => {
                // Bind `name` to `expr`'s output schema, then evaluate
                // `child` in the extended scope.
                let bound = expr.output_schema_in(scope)?;
                let extended = scope.with(name.clone(), bound);
                child.output_schema_in(&extended)
            }
            QueryExpr::Ref { name } => scope
                .lookup(name)
                .cloned()
                .ok_or_else(|| QueryExprError::UnresolvedRef(name.as_str().into())),

            // ── A-variants — schema-flow per design.md §6 ────────────────
            // Filter / Partition / Sort / Limit / Subquery pass the child's
            // schema through unchanged.
            QueryExpr::Filter { child, .. }
            | QueryExpr::Partition { child, .. }
            | QueryExpr::Sort { child, .. }
            | QueryExpr::Limit { child, .. }
            | QueryExpr::Subquery { child, .. } => child.output_schema_in(scope),

            // Project: schema-flow says "the input schema projected to
            // `cols`". Phase-B placeholder — return the child schema until
            // the planner consumes Project's column-mapping output.
            QueryExpr::Project { child, .. } => child.output_schema_in(scope),

            // Distinct: tighten unique_keys with the named cols. Schema is
            // otherwise pass-through (design.md §6 schema-flow table).
            QueryExpr::Distinct { cols, child } => {
                let in_schema = child.output_schema_in(scope)?;
                let mut out = in_schema.clone();
                let mut key_ids: Vec<ColumnId> = Vec::with_capacity(cols.len());
                for c in cols {
                    if let ColumnRef::Named(name) = c {
                        if let Some(id) = in_schema.column_id(name) {
                            key_ids.push(id);
                        }
                    }
                }
                if !key_ids.is_empty() {
                    out.add_unique_key(key_ids);
                }
                Ok(out)
            }

            // Merge / SetOp / Join / BinaryOp: take the left/first child's
            // schema as the representative (design.md §6 schema-flow).
            // Full union-compatibility checks land when the type-checker
            // consumes them; structural lift only here.
            QueryExpr::Merge { children } => children
                .first()
                .ok_or(QueryExprError::EmptyMerge)
                .and_then(|c| c.output_schema_in(scope)),
            QueryExpr::SetOp { left, .. } | QueryExpr::Join { left, .. } => {
                left.output_schema_in(scope)
            }
            QueryExpr::BinaryOp { lhs, .. } => lhs.output_schema_in(scope),
        }
    }
}

/// Lexical scope for `LetBinding` / `Ref` resolution. A persistent map
/// from binding name to the bound expression's output schema. Built
/// during the schema-derivation walk; the caller usually starts with
/// [`BindingScope::default()`].
#[derive(Debug, Default, Clone)]
pub struct BindingScope {
    bindings: HashMap<String, Schema>,
}

impl BindingScope {
    /// Empty scope — no in-scope bindings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a new scope with `name` bound to `schema`. The original
    /// scope is left unchanged (functional style — keeps recursion
    /// shadow semantics correct).
    pub fn with(&self, name: BindingName, schema: Schema) -> Self {
        let mut bindings = self.bindings.clone();
        bindings.insert(name.as_str().into(), schema);
        Self { bindings }
    }

    /// Look up `name` in the current scope. `None` if unbound.
    pub fn lookup(&self, name: &BindingName) -> Option<&Schema> {
        self.bindings.get(name.as_str())
    }
}

// ── relational::ScalarExpr → canonical Predicate translation ────────────────

/// Translate a [`relational::ScalarExpr`](crate::intent_algebra::relational::ScalarExpr)
/// into the canonical typed [`Predicate`]. All scalar shapes translate
/// except `ScalarSubquery`, which carries a legacy `QueryExpr` sub-tree:
/// that arm still returns [`QueryExprError::UnsupportedLegacyScalar`] until
/// the legacy→canonical tree converter lands and can recurse into it.
///
/// `LiteralValue::Duration` is folded into a `Predicate::Literal(Int)`
/// carrying the nanosecond count, because the canonical [`LiteralValue`]
/// is deliberately narrower than the legacy spelling (no `Duration` literal
/// at this layer — see design.md §6 schema-flow `DataType` list).
pub fn from_legacy_scalar(
    se: &crate::intent_algebra::relational::ScalarExpr,
) -> Result<Predicate, QueryExprError> {
    use crate::intent_algebra::relational as l;
    match se {
        l::ScalarExpr::Column(name) => Ok(Predicate::Column(ColumnRef::Named(name.clone()))),
        l::ScalarExpr::Literal(lit) => Ok(Predicate::Literal(literal_from_legacy(lit))),
        l::ScalarExpr::BinaryOp { op, lhs, rhs } => Ok(Predicate::BinaryOp {
            op: binary_op_from_legacy(op),
            lhs: Box::new(from_legacy_scalar(lhs)?),
            rhs: Box::new(from_legacy_scalar(rhs)?),
        }),
        l::ScalarExpr::IsNull { expr, negated } => Ok(Predicate::IsNull {
            expr: Box::new(from_legacy_scalar(expr)?),
            negated: *negated,
        }),
        l::ScalarExpr::FunctionCall { name, args } => Ok(Predicate::FunctionCall {
            name: name.clone(),
            args: args
                .iter()
                .map(from_legacy_scalar)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        l::ScalarExpr::InList {
            expr,
            list,
            negated,
        } => Ok(Predicate::InList {
            expr: Box::new(from_legacy_scalar(expr)?),
            list: list
                .iter()
                .map(from_legacy_scalar)
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        }),
        l::ScalarExpr::Between {
            expr,
            low,
            high,
            negated,
        } => Ok(Predicate::Between {
            expr: Box::new(from_legacy_scalar(expr)?),
            low: Box::new(from_legacy_scalar(low)?),
            high: Box::new(from_legacy_scalar(high)?),
            negated: *negated,
        }),
        // `ScalarSubquery` carries a legacy `QueryExpr` sub-tree — needs the
        // legacy→canonical tree converter to recurse. Deferred to that PR.
        l::ScalarExpr::ScalarSubquery(_) => {
            Err(QueryExprError::UnsupportedLegacyScalar("ScalarSubquery"))
        }
    }
}

fn literal_from_legacy(lit: &crate::intent_algebra::relational::LiteralValue) -> LiteralValue {
    use crate::intent_algebra::relational as l;
    match lit {
        l::LiteralValue::Null => LiteralValue::Null,
        l::LiteralValue::Bool(b) => LiteralValue::Bool(*b),
        l::LiteralValue::Int(i) => LiteralValue::Int(*i),
        l::LiteralValue::Float(f) => LiteralValue::Float(*f),
        l::LiteralValue::Str(s) => LiteralValue::Str(s.clone()),
        // Durations fold to nanoseconds-as-Int — canonical LiteralValue
        // has no Duration variant (narrower by design).
        l::LiteralValue::Duration(d) => LiteralValue::Int(d.as_nanos() as i64),
    }
}

fn binary_op_from_legacy(op: &crate::intent_algebra::relational::BinaryOpKind) -> BinaryOpKind {
    use crate::intent_algebra::relational as l;
    match op {
        l::BinaryOpKind::Add => BinaryOpKind::Add,
        l::BinaryOpKind::Sub => BinaryOpKind::Sub,
        l::BinaryOpKind::Mul => BinaryOpKind::Mul,
        l::BinaryOpKind::Div => BinaryOpKind::Div,
        l::BinaryOpKind::Mod => BinaryOpKind::Mod,
        l::BinaryOpKind::Pow => BinaryOpKind::Pow,
        l::BinaryOpKind::Eq => BinaryOpKind::Eq,
        l::BinaryOpKind::Ne => BinaryOpKind::Ne,
        l::BinaryOpKind::Lt => BinaryOpKind::Lt,
        l::BinaryOpKind::Le => BinaryOpKind::Le,
        l::BinaryOpKind::Gt => BinaryOpKind::Gt,
        l::BinaryOpKind::Ge => BinaryOpKind::Ge,
        l::BinaryOpKind::And => BinaryOpKind::And,
        l::BinaryOpKind::Or => BinaryOpKind::Or,
        l::BinaryOpKind::BitAnd => BinaryOpKind::BitAnd,
        l::BinaryOpKind::BitOr => BinaryOpKind::BitOr,
        l::BinaryOpKind::BitXor => BinaryOpKind::BitXor,
        l::BinaryOpKind::Concat => BinaryOpKind::Concat,
        l::BinaryOpKind::Like => BinaryOpKind::Like,
        l::BinaryOpKind::NotLike => BinaryOpKind::NotLike,
        l::BinaryOpKind::Regex => BinaryOpKind::Regex,
        l::BinaryOpKind::NotRegex => BinaryOpKind::NotRegex,
        l::BinaryOpKind::Unless => BinaryOpKind::Unless,
        l::BinaryOpKind::Atan2 => BinaryOpKind::Atan2,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent_algebra::schema::{Column, DataType};
    use crate::types_v2::AccuracyTarget;

    fn col(name: &str, dtype: DataType) -> Column {
        Column {
            name: name.into(),
            dtype,
            nullable: false,
        }
    }

    fn ts_scan() -> QueryExpr {
        QueryExpr::Scan {
            source: Source::TimeSeries {
                metric: "http_request_duration_seconds".into(),
            },
            label_filters: vec![LabelFilter {
                label: "service".into(),
                equals: "api".into(),
            }],
            schema: Schema::with_time_index(
                vec![
                    col("ts", DataType::Timestamp),
                    col("service", DataType::Utf8),
                    col("value", DataType::Float64),
                ],
                0,
                vec![vec![0, 1]],
            ),
        }
    }

    #[test]
    fn query_expr_simple_aggregate() {
        let expr = QueryExpr::Aggregate {
            by: vec![1], // service
            aggs: vec![AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(ts_scan()),
            }),
        };
        let schema = expr.output_schema().unwrap();
        // Output: [service, quantile_0_99]
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "service");
        assert_eq!(schema.columns[1].name, "quantile_0_99");
        // unique_keys = [by] — the by columns project to positions [0..by.len())
        // in the output schema.
        assert_eq!(schema.unique_keys, vec![vec![0]]);
        // Aggregate strips the time axis.
        assert!(schema.time_index.is_none());
    }

    #[test]
    fn query_expr_let_binding_ref() {
        // LetBinding{name="w", expr=Window over Scan,
        //            child=Aggregate{ child=Ref{"w"} }}
        let expr = QueryExpr::LetBinding {
            name: BindingName::new("w"),
            expr: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(ts_scan()),
            }),
            child: Box::new(QueryExpr::Aggregate {
                by: vec![1],
                aggs: vec![AggIntent::Max { col: None }],
                having: None,
                child: Box::new(QueryExpr::Ref {
                    name: BindingName::new("w"),
                }),
            }),
        };
        let schema = expr.output_schema().unwrap();
        assert_eq!(schema.columns[0].name, "service");
        assert_eq!(schema.columns[1].name, "max");
        assert_eq!(schema.unique_keys, vec![vec![0]]);
    }

    #[test]
    fn query_expr_unresolved_ref_errors() {
        let expr = QueryExpr::Ref {
            name: BindingName::new("nope"),
        };
        let err = expr.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::UnresolvedRef(s) if s == "nope"));
    }

    #[test]
    fn query_expr_window_requires_time_index() {
        let bad_scan = QueryExpr::Scan {
            source: Source::Table {
                table_ref: "t".into(),
            },
            label_filters: vec![],
            // Tabular scan with no time index.
            schema: Schema::new(vec![col("a", DataType::Int64)]),
        };
        let expr = QueryExpr::Window {
            kind: WindowKind::Tumbling,
            size: Duration::from_secs(60),
            slide: None,
            child: Box::new(bad_scan),
        };
        let err = expr.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::WindowMissingTimeIndex));
    }

    #[test]
    fn query_expr_aggregate_invalid_by_column() {
        let expr = QueryExpr::Aggregate {
            by: vec![99],
            aggs: vec![AggIntent::Sum { col: None }],
            having: None,
            child: Box::new(ts_scan()),
        };
        let err = expr.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::InvalidGroupByColumn(99, _)));
    }

    #[test]
    fn query_expr_serde_roundtrip() {
        let expr = QueryExpr::Aggregate {
            by: vec![1],
            aggs: vec![AggIntent::Quantile {
                col: None,
                q: 0.99,
                accuracy: AccuracyTarget::Epsilon(0.01),
            }],
            having: None,
            child: Box::new(ts_scan()),
        };
        let json = serde_json::to_string(&expr).unwrap();
        let back: QueryExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(expr, back);
    }

    /// `unique_keys` is the load-bearing CSE-legality hook (design.md §6
    /// line ~1284). Two `Ref` consumers can share a producer iff the
    /// producer's output schema has at least one provable unique-key set;
    /// without it, the deduper has to be conservative and reuse drops on
    /// the floor.
    #[test]
    fn cse_substitution_legal_only_with_unique_keys() {
        // Producer 1: Scan → Window → Aggregate. Aggregate produces
        // `unique_keys = [by]` (a provable unique key). Two `Ref`
        // consumers can legally share this.
        let producer_with_uk = QueryExpr::Aggregate {
            by: vec![1],
            aggs: vec![AggIntent::Sum { col: None }],
            having: None,
            child: Box::new(QueryExpr::Window {
                kind: WindowKind::Sliding,
                size: Duration::from_secs(300),
                slide: None,
                child: Box::new(ts_scan()),
            }),
        };
        let s1 = producer_with_uk.output_schema().unwrap();
        assert!(
            s1.has_unique_key(),
            "Aggregate must emit unique_keys = [by] per design.md §6 schema-flow"
        );

        // Producer 2: bare Scan with NO unique key declared. CSE deduper
        // would have to refuse to share this without further proof.
        let producer_without_uk = QueryExpr::Scan {
            source: Source::Table {
                table_ref: "t".into(),
            },
            label_filters: vec![],
            schema: Schema::new(vec![col("a", DataType::Int64)]),
        };
        let s2 = producer_without_uk.output_schema().unwrap();
        assert!(
            !s2.has_unique_key(),
            "no unique_keys → CSE deduper must conservatively refuse to share"
        );

        // The asymmetry is the design's claim: unique_keys is what makes
        // CSE substitution legal. Encoded here as a unit invariant so
        // downstream rewrites of the schema-flow rules can't silently
        // break it.
        assert_ne!(s1.has_unique_key(), s2.has_unique_key());
    }

    // ── A-variant lift (Batch 2) ─────────────────────────────────────────

    #[test]
    fn filter_passes_child_schema_through() {
        let expr = QueryExpr::Filter {
            pred: Predicate::Literal(LiteralValue::Bool(true)),
            child: Box::new(ts_scan()),
        };
        let s = expr.output_schema().unwrap();
        // Filter is row-level — schema unchanged.
        assert_eq!(s.columns.len(), 3);
        assert_eq!(s.columns[2].name, "value");
        assert!(s.time_index.is_some());
    }

    #[test]
    fn distinct_tightens_unique_keys() {
        // Scan over a tabular source with no unique keys, then DISTINCT on
        // `a`. Output schema should now have unique_keys=[[0]].
        let scan = QueryExpr::Scan {
            source: Source::Table {
                table_ref: "t".into(),
            },
            label_filters: vec![],
            schema: Schema::new(vec![col("a", DataType::Int64), col("b", DataType::Utf8)]),
        };
        let expr = QueryExpr::Distinct {
            cols: vec![ColumnRef::Named("a".into())],
            child: Box::new(scan),
        };
        let s = expr.output_schema().unwrap();
        assert!(s.has_unique_key());
        assert_eq!(s.unique_keys, vec![vec![0]]);
    }

    #[test]
    fn merge_uses_first_child_schema_or_errors_empty() {
        let scan = ts_scan();
        let expr = QueryExpr::Merge {
            children: vec![scan.clone(), scan.clone()],
        };
        let s = expr.output_schema().unwrap();
        assert_eq!(s.columns.len(), 3);

        let empty = QueryExpr::Merge { children: vec![] };
        let err = empty.output_schema().unwrap_err();
        assert!(matches!(err, QueryExprError::EmptyMerge));
    }

    #[test]
    fn limit_and_sort_pass_schema_through() {
        let expr = QueryExpr::Limit {
            n: 10,
            offset: 0,
            child: Box::new(QueryExpr::Sort {
                keys: vec![SortKey {
                    col: "ts".into(),
                    desc: false,
                    nulls_first: None,
                }],
                child: Box::new(ts_scan()),
            }),
        };
        let s = expr.output_schema().unwrap();
        assert_eq!(s.columns.len(), 3);
    }

    #[test]
    fn binary_op_uses_lhs_schema() {
        let expr = QueryExpr::BinaryOp {
            op: BinaryOpKind::Add,
            lhs: Box::new(ts_scan()),
            rhs: Box::new(ts_scan()),
            vector_match: None,
        };
        let s = expr.output_schema().unwrap();
        assert_eq!(s.columns.len(), 3);
    }

    #[test]
    fn join_uses_left_child_schema() {
        let expr = QueryExpr::Join {
            kind: JoinKind::Inner,
            pred: Predicate::Literal(LiteralValue::Bool(true)),
            left: Box::new(ts_scan()),
            right: Box::new(ts_scan()),
        };
        let s = expr.output_schema().unwrap();
        assert_eq!(s.columns.len(), 3);
    }

    #[test]
    fn a_variant_serde_roundtrip_filter() {
        let expr = QueryExpr::Filter {
            pred: Predicate::BinaryOp {
                op: BinaryOpKind::Eq,
                lhs: Box::new(Predicate::Column(ColumnRef::Named("service".into()))),
                rhs: Box::new(Predicate::Literal(LiteralValue::Str("api".into()))),
            },
            child: Box::new(ts_scan()),
        };
        let json = serde_json::to_string(&expr).unwrap();
        let back: QueryExpr = serde_json::from_str(&json).unwrap();
        assert_eq!(expr, back);
    }

    // ── from_legacy_scalar → Predicate translation ───────────────────────

    #[test]
    fn from_legacy_scalar_column() {
        use crate::intent_algebra::relational as l;
        let s = l::ScalarExpr::Column("foo".into());
        let p = from_legacy_scalar(&s).unwrap();
        assert!(matches!(
            p,
            Predicate::Column(ColumnRef::Named(ref n)) if n == "foo"
        ));
    }

    #[test]
    fn from_legacy_scalar_literal_bool() {
        use crate::intent_algebra::relational as l;
        let s = l::ScalarExpr::Literal(l::LiteralValue::Bool(true));
        let p = from_legacy_scalar(&s).unwrap();
        assert!(matches!(p, Predicate::Literal(LiteralValue::Bool(true))));
    }

    #[test]
    fn from_legacy_scalar_binary_op_and_is_null() {
        use crate::intent_algebra::relational as l;
        let s = l::ScalarExpr::BinaryOp {
            op: l::BinaryOpKind::Eq,
            lhs: Box::new(l::ScalarExpr::Column("a".into())),
            rhs: Box::new(l::ScalarExpr::Literal(l::LiteralValue::Int(1))),
        };
        let p = from_legacy_scalar(&s).unwrap();
        assert!(matches!(
            p,
            Predicate::BinaryOp {
                op: BinaryOpKind::Eq,
                ..
            }
        ));

        let s2 = l::ScalarExpr::IsNull {
            expr: Box::new(l::ScalarExpr::Column("c".into())),
            negated: true,
        };
        let p2 = from_legacy_scalar(&s2).unwrap();
        assert!(matches!(p2, Predicate::IsNull { negated: true, .. }));
    }

    #[test]
    fn from_legacy_scalar_e_variants_translate() {
        use crate::intent_algebra::relational as l;

        let f = l::ScalarExpr::FunctionCall {
            name: "abs".into(),
            args: vec![l::ScalarExpr::Column("x".into())],
        };
        match from_legacy_scalar(&f).unwrap() {
            Predicate::FunctionCall { name, args } => {
                assert_eq!(name, "abs");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Predicate::Column(ColumnRef::Named(n)) if n == "x"));
            }
            other => panic!("expected FunctionCall, got {other:?}"),
        }

        let il = l::ScalarExpr::InList {
            expr: Box::new(l::ScalarExpr::Column("x".into())),
            list: vec![l::ScalarExpr::Literal(l::LiteralValue::Int(1))],
            negated: false,
        };
        match from_legacy_scalar(&il).unwrap() {
            Predicate::InList { list, negated, .. } => {
                assert_eq!(list.len(), 1);
                assert!(!negated);
            }
            other => panic!("expected InList, got {other:?}"),
        }

        let bt = l::ScalarExpr::Between {
            expr: Box::new(l::ScalarExpr::Column("x".into())),
            low: Box::new(l::ScalarExpr::Literal(l::LiteralValue::Int(0))),
            high: Box::new(l::ScalarExpr::Literal(l::LiteralValue::Int(10))),
            negated: true,
        };
        assert!(matches!(
            from_legacy_scalar(&bt).unwrap(),
            Predicate::Between { negated: true, .. }
        ));
    }

    #[test]
    fn from_legacy_scalar_subquery_still_deferred() {
        use crate::intent_algebra::relational as l;
        // `ScalarSubquery` carries a legacy `QueryExpr` sub-tree — needs the
        // legacy→canonical tree converter, so it still errors for now.
        let sq = l::ScalarExpr::ScalarSubquery(Box::new(l::QueryExpr::Ref("cte".into())));
        assert!(matches!(
            from_legacy_scalar(&sq).unwrap_err(),
            QueryExprError::UnsupportedLegacyScalar("ScalarSubquery")
        ));
    }
}
