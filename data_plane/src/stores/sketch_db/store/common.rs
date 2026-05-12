//! Legacy `SketchStore` columnar types — now thin type aliases over
//! the generic `index::epoch_columnar` implementation.
//!
//! Before the 2026-05 dedup: this file had its own non-generic
//! `MutableEpoch`, `SealedEpoch`, and `InternTable` (~290 LOC) which
//! duplicated `index/epoch_columnar.rs` with the payload type fixed
//! to `Arc<dyn AggregateCore>`. The duplication carried the same six
//! storage optimizations and the same overlap semantics; the only
//! real difference was the payload type bound.
//!
//! The legacy implementation is gone. The aliases below preserve
//! every call-site identifier (`MutableEpoch`, `SealedEpoch`,
//! `MetricID`, `MetricBucketMap`, `InternTable`) so consumers in
//! `store/global.rs` and `store/per_key.rs` are unchanged at the
//! identifier level.
//!
//! The few API-shape mismatches between the legacy and generic
//! versions (legacy's grouped `range_query_into`, legacy's owned
//! `exact_query`, legacy's `remove_windows`) are now first-class
//! methods on the generic side, gated on `P: Clone` where the
//! legacy semantics needed owning copies — see
//! `index/epoch_columnar.rs` for those impls.
//!
//! See `docs/phase5-unification-plan.md` Phase E for the further
//! step that retires `Arc<dyn AggregateCore>` payloads entirely in
//! favor of typed `SketchSampleState`. After Phase E, this file
//! itself goes away.

use crate::stores::sketch_db::index::epoch_columnar;
use crate::stores::types::{AggregateCore, KeyByLabelValues};
use std::collections::HashMap;
use std::sync::Arc;

/// Compact metric identifier — 4 bytes. Renamed in the generic to
/// `LabelValuesId`; the alias preserves legacy naming.
pub type MetricID = epoch_columnar::LabelValuesId;

/// Monotonically increasing epoch counter.
pub type EpochID = epoch_columnar::EpochId;

/// `(start_unix_ms, end_unix_ms)`.
pub type TimestampRange = epoch_columnar::TimestampRange;

/// Legacy intern table keyed by `Option<KeyByLabelValues>` (the
/// `None` slot represents a missing group-by; that semantic predates
/// the SketchIndex's `BTreeMap<String,String>` key shape).
pub type InternTable = epoch_columnar::InternTable<Option<KeyByLabelValues>>;

/// Active (mutable) epoch holding `Arc<dyn AggregateCore>` payloads.
pub type MutableEpoch = epoch_columnar::MutableEpoch<Arc<dyn AggregateCore>>;

/// Sealed (immutable, sorted) epoch holding `Arc<dyn AggregateCore>`
/// payloads.
pub type SealedEpoch = epoch_columnar::SealedEpoch<Arc<dyn AggregateCore>>;

/// Range-query output shape used by `SketchStore` callers: per-metric
/// list of `(window, aggregate)` pairs. Matches the legacy
/// `MetricBucketMap`.
pub type MetricBucketMap = HashMap<MetricID, Vec<(TimestampRange, Arc<dyn AggregateCore>)>>;
