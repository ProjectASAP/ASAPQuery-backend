//! Layer 3 aggregation-intent vocabulary.
//!
//! ## Phase 1b (docs/migration-plan-backend-plan.md)
//!
//! `AggIntent` is no longer defined in this repo. This file re-exports the
//! canonical type from ASAPController's `asap-ir` crate (a git dependency
//! in `control_plane/Cargo.toml`, currently pinned to a commit SHA —
//! ASAPController has no tagged releases yet) and holds only what's
//! genuinely control_plane-specific:
//!
//! - **`frequency()` / `as_frequency()`** — control_plane's standalone
//!   point-frequency-via-CMS query (`count(*) WHERE key = k`), carried
//!   through the shared `AggIntent` as an `Extension` rather than a
//!   first-class shared variant. This is **not** the same capability as
//!   `RankingMeasure::Frequency` (which classifies what a `TopK` ranks
//!   by) — see `ASAPController#137` for why `Extension` exists and why
//!   this isn't folded into that. `Extension` is a generic escape hatch
//!   (issue `ASAPController#131`): a deployment-model-specific intent
//!   core doesn't know the shape of, tagged by an `ext_kind` string, with
//!   `payload` opaque to core.
//! - **`archive_only()` / `output_column()`** — free functions, not
//!   methods. Rust's orphan rules don't allow an inherent `impl AggIntent`
//!   block from a crate that doesn't own the type, so these can't be
//!   `.archive_only()`/`.output_column()` call syntax anymore (that
//!   syntax survives for `asap_ir`'s own inherent methods, e.g.
//!   `intent.input_col()`, `intent.is_per_series()` — those aren't
//!   control_plane-specific and need no wrapper).
//!
//! ## What moved, what didn't
//!
//! Per Phase 0's tie-break rule, `asap_ir::AggIntent`'s shape wins
//! wherever it differs from this repo's pre-merge version. One
//! consequence: `Rate` / `Increase` / `Changes` / `Delta` / `IDelta` /
//! `Deriv` / `Resets` no longer carry `window: Duration` — the window
//! lives on the enclosing `QueryExpr::Window` node instead (ASAPController's
//! design). Phase 1 (the copy-based first attempt, `#391`) deferred this;
//! depending on the real external type instead of a local copy makes it
//! unavoidable — you can't add a field to a variant you don't own. Grep
//! for `.window` reads (not constructions) before assuming a call site
//! needs updating: as of this migration there was exactly one real
//! consumer (`sketch_algebra/rules/bind_exact_agg.rs`), which now reads
//! the window off the enclosing `QueryExpr::Window` node it already has
//! in scope, not off the intent.

use asap_ir::intent_algebra::agg_accuracy as asap_agg_accuracy;
use asap_ir::intent_algebra::schema::{Column as AsapColumn, DataType as AsapDataType};
pub use asap_ir::intent_algebra::{
    agg_is_exact, agg_is_mergeable, default_cardinality, default_quantile,
    is_frequency_heavy_hitter, ranking_measure, AggIntent, MathFunc, RankingMeasure, TimeFunc,
};

use crate::intent_algebra::schema::{Column, DataType};
use crate::types_v2::AccuracyTarget;

const FREQUENCY_EXT_KIND: &str = "frequency";

/// `control_plane::Column`/`DataType` and `asap_ir::Column`/`DataType`
/// are structurally identical but not the same type — merging `schema.rs`
/// itself is Phase 2 scope (it cascades into `Schema`/`QueryExpr`, used
/// pervasively; ~38 `Column{}` literals across this repo). Convert at
/// this boundary instead of widening this change.
fn to_asap_dtype(dt: &DataType) -> AsapDataType {
    match dt {
        DataType::Int64 => AsapDataType::Int64,
        DataType::Float64 => AsapDataType::Float64,
        DataType::Utf8 => AsapDataType::Utf8,
        DataType::Bool => AsapDataType::Bool,
        DataType::Timestamp => AsapDataType::Timestamp,
    }
}

fn from_asap_dtype(dt: &AsapDataType) -> DataType {
    match dt {
        AsapDataType::Int64 => DataType::Int64,
        AsapDataType::Float64 => DataType::Float64,
        AsapDataType::Utf8 => DataType::Utf8,
        AsapDataType::Bool => DataType::Bool,
        AsapDataType::Timestamp => DataType::Timestamp,
    }
}

fn to_asap_column(c: &Column) -> AsapColumn {
    AsapColumn::new(c.name.clone(), to_asap_dtype(&c.dtype), c.nullable)
}

fn from_asap_column(c: AsapColumn) -> Column {
    Column {
        name: c.name,
        dtype: from_asap_dtype(&c.dtype),
        nullable: c.nullable,
    }
}

/// Construct control_plane's point-frequency-via-CMS intent. See module
/// docs for why this is an `Extension`, not a shared first-class variant.
pub fn frequency(accuracy: AccuracyTarget) -> AggIntent {
    AggIntent::Extension {
        ext_kind: FREQUENCY_EXT_KIND.to_string(),
        payload: serde_json::json!({ "accuracy": accuracy }),
    }
}

/// Default `Frequency` intent — `accuracy = e / 2000`. Unchanged default
/// from before the merge.
pub fn default_frequency() -> AggIntent {
    frequency(AccuracyTarget::Epsilon(std::f64::consts::E / 2000.0))
}

/// If `intent` is control_plane's `Frequency` extension, extract its
/// accuracy target. `None` for every other intent, including other
/// (currently hypothetical) `Extension` kinds.
pub fn as_frequency(intent: &AggIntent) -> Option<AccuracyTarget> {
    match intent {
        AggIntent::Extension { ext_kind, payload } if ext_kind == FREQUENCY_EXT_KIND => {
            serde_json::from_value(payload.get("accuracy")?.clone()).ok()
        }
        _ => None,
    }
}

/// Accuracy parameter as a fractional ε (`0.0` for exact ops). Wraps
/// `asap_ir::agg_accuracy`, which returns `0.0` for `Frequency` (it's
/// opaque `Extension` payload to core) — special-cased here so callers
/// don't need to know `Frequency` isn't a first-class shared variant.
pub fn agg_accuracy(intent: &AggIntent) -> f64 {
    if let Some(acc) = as_frequency(intent) {
        return match acc {
            AccuracyTarget::Exact => 0.0,
            AccuracyTarget::Epsilon(eps) | AccuracyTarget::EpsilonDelta { epsilon: eps, .. } => eps,
        };
    }
    asap_agg_accuracy(intent)
}

/// True iff this intent has no ASAP-tier (streaming sketch) binding
/// today. `false` means a `Bind*` rule may match. `true` means the L5
/// emitter routes the intent to the cold-store / archive tier.
///
/// Every intent `asap_ir::AggIntent` carries that this repo didn't have
/// before the merge (histogram accessors, math/trig, time/calendar
/// accessors, presence functions, `Group`/`CountValues`, the extended
/// range-vector reducers) is archive-only — none has a `Bind*` rule yet.
/// An unrecognized `Extension` (not control_plane's `Frequency`) is also
/// archive-only by default — no binding exists for a shape core can't
/// even see into.
pub fn archive_only(intent: &AggIntent) -> bool {
    if as_frequency(intent).is_some() {
        return false; // real CMS binding — sketch_algebra/rules/bind_cms_count.rs
    }
    matches!(
        intent,
        AggIntent::Absent
            | AggIntent::AbsentOverTime
            | AggIntent::PresentOverTime
            | AggIntent::Delta
            | AggIntent::Deriv
            | AggIntent::PredictLinear { .. }
            | AggIntent::DoubleExpSmoothing { .. }
            | AggIntent::IDelta
            | AggIntent::Resets
            | AggIntent::Changes
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
            | AggIntent::Extension { .. } // unrecognized extensions only reach here
    )
}

/// Output column name + type produced by this intent when applied to
/// `input`. Delegates to `asap_ir::AggIntent::output_column` (the
/// inherent method on the shared type) for everything except
/// control_plane's `Frequency` extension, which core's generic
/// `Extension` handling can't name/type correctly (core has no idea
/// `ext_kind == "frequency"` means Int64, non-nullable, named
/// `"frequency"` — that's control_plane-only knowledge).
pub fn output_column(intent: &AggIntent, input: &Column) -> Column {
    if as_frequency(intent).is_some() {
        return Column {
            name: "frequency".into(),
            dtype: DataType::Int64,
            nullable: false,
        };
    }
    from_asap_column(intent.output_column(&to_asap_column(input)))
}
