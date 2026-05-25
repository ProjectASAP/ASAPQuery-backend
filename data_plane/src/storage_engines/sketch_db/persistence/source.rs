//! The trait the flusher uses to enumerate, snapshot, and evict sealed
//! epochs. Decouples `flusher.rs` from `SketchStorePerKey` so the
//! flusher can be unit-tested against a fake source.

use crate::storage_engines::types::KeyByLabelValues;

use super::PersistResult;

/// A compact reference to a sealed epoch held in memory. Returned by
/// [`EpochSource::list_sealed_epochs`] in oldest-first global order (by
/// `end_ts`), interleaved round-robin across agg-ids when the flusher
/// walks them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedEpochRef {
    pub agg_id: u64,
    pub epoch_id: u64,
    /// `max_end` of the epoch's timestamp range. Used for oldest-first
    /// ordering and for the `hot_window_ms` trigger.
    pub end_ts: u64,
    /// Approximate memory footprint of the epoch's sketches. Drives the
    /// memory-pressure trigger; the flusher subtracts this when evicting.
    pub approx_bytes: usize,
}

/// A fully-resolved, self-contained copy of one sealed epoch. Produced by
/// [`EpochSource::snapshot_sealed_epoch`] under a brief per-agg read lock,
/// then serialized on the flusher's thread with no store locks held.
///
/// The `entries` are ready to write to disk: labels are already resolved
/// to `Option<KeyByLabelValues>` (no intern-table lookup needed) and the
/// sketch bytes are opaque to the persistence layer — the writer carries
/// whatever format the SketchStore put in. (The legacy Arroyo/MessagePack
/// path that consumed these bytes lives in deleted modules; the
/// production read-back path will land with the SketchStore-backed
/// refactor — `snapshot_sealed_epoch` returns `Ok(None)` until then.)
#[derive(Debug, Clone)]
pub struct EpochSnapshot {
    pub agg_id: u64,
    pub epoch_id: u64,
    pub min_ts: u64,
    pub max_ts: u64,
    pub entries: Vec<EpochSnapshotEntry>,
    /// Sum of `approx_memory_bytes` across entries (v1: a coarse
    /// per-type heuristic; see `per_key.rs::estimate_epoch_bytes`).
    pub approx_bytes: usize,
}

impl EpochSnapshot {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One resolved entry inside an [`EpochSnapshot`]. Matches the shape
/// the on-disk part writer expects.
#[derive(Debug, Clone)]
pub struct EpochSnapshotEntry {
    pub start_ts: u64,
    pub end_ts: u64,
    /// Optional label set, already resolved from the per-agg intern table.
    pub label: Option<KeyByLabelValues>,
    /// `AggregateCore::type_name()` of the underlying sketch — recorded
    /// so a future read-back path can dispatch to the right
    /// deserializer once the SketchStore-backed snapshot lands.
    pub sketch_type_name: String,
    /// Wire-encoding tag for sketch payloads — see
    /// [`encoding_tag`](crate::storage_engines::sketch_db::index::persistence::part::encoding_tag).
    /// Carries the `SketchEncoding` (Full vs Delta) of the payload so
    /// the disk read-back path can preserve the delta-stitching
    /// carry-in semantics across the in-mem/on-disk boundary. `0`
    /// (the default) means "unknown / treat as Full" — exact-agg
    /// payloads use `0`.
    pub encoding_tag: u8,
    /// Serialized sketch payload (opaque to the persistence layer).
    pub sketch_bytes: Vec<u8>,
}

/// Trait the flusher uses to discover, snapshot, and evict sealed
/// epochs. Implemented by `SketchStorePerKey`; a test fake lives in
/// `flusher.rs`'s unit tests.
///
/// Implementors guarantee that:
///
/// * [`list_sealed_epochs`] returns an approximate global oldest-first
///   view. Approximate is fine: the flusher re-checks each epoch's
///   existence when calling [`snapshot_sealed_epoch`].
/// * [`snapshot_sealed_epoch`] is safe to call concurrently with
///   inserts. It clones the epoch's `Arc` out under a read lock and
///   drops the lock before returning, so no lock is held across
///   downstream serialization / I/O.
/// * [`evict_sealed_epoch`] is idempotent — calling it on an
///   already-evicted (agg_id, epoch_id) is a no-op.
/// * [`approx_memory_bytes`] is cheap (atomic load) and is kept in sync
///   with what the flusher has evicted.
pub trait EpochSource: Send + Sync {
    fn list_sealed_epochs(&self) -> Vec<SealedEpochRef>;

    /// Time-driven seal: roll every un-sealed `current_epoch` window
    /// whose END is at or before `cutoff_end` into a sealed epoch so the
    /// flusher can make it durable. Called by the flusher each tick with
    /// `cutoff_end = now - hot_window` BEFORE [`list_sealed_epochs`].
    ///
    /// The count-driven seal cadence (`seal_window_count`) only fires once
    /// `current_epoch` reaches N distinct windows; a slow/stalled series
    /// never reaches it, leaving aged windows un-sealed — and the flusher
    /// only flushes SEALED epochs, so those windows are never made durable
    /// (the live `parts/`-stays-empty bug). This hook closes that gap.
    ///
    /// Default impl is a no-op so existing/test sources need not implement
    /// it. Returns the number of distinct windows newly sealed.
    fn seal_aged_epochs(&self, _cutoff_end: u64) -> usize {
        0
    }

    fn snapshot_sealed_epoch(
        &self,
        agg_id: u64,
        epoch_id: u64,
    ) -> PersistResult<Option<EpochSnapshot>>;

    fn evict_sealed_epoch(&self, agg_id: u64, epoch_id: u64);

    fn approx_memory_bytes(&self) -> usize;
}
