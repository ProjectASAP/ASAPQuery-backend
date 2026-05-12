//! Per-`aggregation_id` schema metadata and lifecycle.
//!
//! Implements §5 / §6 of the sketch DB design
//! ([`design-sketch-db.md`](../../../../../docs/design-sketch-db.md)).
//!
//! ## Why this exists
//!
//! Today's `SketchStore` is keyed by `aggregation_id` but does not know
//! anything about the schema (sketch type, parameters, grouping labels,
//! window) attached to that id beyond what's in `StreamingConfig`. The
//! sketch DB design needs:
//!
//! 1. **An explicit lifecycle** per `agg_id` — `Active` (writes accepted) /
//!    `Retired` (writes rejected, reads allowed within retention) /
//!    `Expired` (scheduled for deletion).
//! 2. **A write-side barrier** so the store rejects writes targeted at
//!    a retired or expired `agg_id`, even if a slow in-flight ingest
//!    batch routes stale data to it. This is the §6.3 "no writes after
//!    retirement" guarantee.
//! 3. **A place to attach derived metadata** — most importantly the
//!    `AccuracyProfile` (§6.4) so the query path can return error bounds
//!    without recomputation.
//!
//! ## What this file covers
//!
//! Phase 2a added the lifecycle + registry:
//!
//! * `AggSchema` struct with the lifecycle fields populated from
//!   `AggregationConfig` + a `created_at` timestamp.
//! * `AggStatus` enum: `Active` / `Retired` / `Expired`, derived from
//!   `retired_at` and `expires_at` plus the wall clock.
//! * `SchemaRegistry` — an in-memory map keyed by `agg_id` that the
//!   ingest path consults. Built from the current `StreamingConfig`
//!   snapshot at construction; reconciled event-driven by the
//!   `POST /api/v1/streaming-config` swap handler.
//!
//! **§7 schema timeline read API:**
//!
//! * `TimelineSegment` + `TimelineCoverage` types.
//! * `SchemaRegistry::timeline_for_metric(metric, t1_ms, t2_ms)`
//!   returning the non-overlapping, time-ordered segments that cover
//!   `[t1, t2]` for a given metric. Derived on-demand from registry
//!   state — no separate index to keep consistent.
//!
//! The query engine stitches per-segment scalars into a single
//! result via the combiner in `crate::query_engines::timeline_dispatch`,
//! wired through `ASAPQueryEngine::try_handle_query_promql_via_timeline`.
//!
//! **On-disk schema persistence:**
//!
//! * `SchemaRegistry::load_or_new_from_config(path, &StreamingConfig)`
//!   reads a JSON snapshot if present (preserving `created_at_ms` /
//!   `retired_at_ms` / `expires_at_ms`) and reconciles against the
//!   live config.
//! * After every `reconcile` call the registry rewrites the snapshot
//!   atomically (`path.tmp` + rename). Best-effort: I/O errors are
//!   logged but never block a reconcile.
//! * `PrecomputeEngineConfig::schema_persist_path` surfaces this as a
//!   CLI-flag-able option; `--schema-persist-path` on `main.rs`.
//!
//! ## Out of scope here
//! * `AccuracyProfile` derivation — §6.4 in the design doc; the hook
//!   is here as `accuracy_profile()` that returns a stub today and
//!   will be filled in once the sketch types' theoretical bounds are
//!   vendored.
//! * `combine_statistic()` and `PartialResult` for cross-segment
//!   result stitching — see `crate::query_engines::timeline_dispatch`.
//! * Compaction policy that reads `AggStatus` to throttle as expiry
//!   approaches — §9.2 of the design.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asap_types::aggregation_config::AggregationConfig;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::stores::schema::StreamingConfig;

/// Lifecycle state of an `aggregation_id`. Derived from the
/// `AggSchema`'s timestamps and the current wall clock — never stored
/// directly on the schema, because retirement is time-driven.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggStatus {
    /// Listed in the current `StreamingConfig`. Writes accepted.
    Active,
    /// Removed from `StreamingConfig` but still within retention. Writes
    /// rejected by the §6.3 barrier; reads allowed for queries that
    /// reference the historical data.
    Retired,
    /// Past retention. Scheduled for deletion by the time-TTL sweep.
    Expired,
}

/// Per-`aggregation_id` schema metadata. One entry per agg in the
/// registry; lifecycle transitions update `retired_at` and `expires_at`
/// rather than mutating any other field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggSchema {
    pub agg_id: u64,
    pub metric_name: String,
    /// The `AggregationConfig` that defined this schema. Pinned at
    /// schema creation; never mutated. A reconfigure that changes any
    /// field (sketch type, parameters, grouping labels, window size)
    /// must be done via a new `agg_id` per the monotonic-id contract
    /// (`HotReloadStreamingConfig` doc + design doc §6).
    pub config: AggregationConfig,

    /// Wall-clock millis when this schema was first observed (i.e.
    /// when the `agg_id` first appeared in a `StreamingConfig`).
    pub created_at_ms: u64,
    /// Wall-clock millis when this schema was retired (removed from
    /// `StreamingConfig`). `None` while `Active`.
    pub retired_at_ms: Option<u64>,
    /// Wall-clock millis after which the schema's data may be deleted.
    /// `None` while `Active`. Set on retirement to
    /// `retired_at_ms + retention_ms`.
    pub expires_at_ms: Option<u64>,
}

impl AggSchema {
    /// Construct an `Active` schema from an `AggregationConfig` snapshot.
    /// The `created_at_ms` is captured from the wall clock.
    pub fn new_active(config: AggregationConfig) -> Self {
        Self {
            agg_id: config.aggregation_id,
            metric_name: config.metric.clone(),
            config,
            created_at_ms: now_ms(),
            retired_at_ms: None,
            expires_at_ms: None,
        }
    }

    /// Compute the current `AggStatus` against the wall clock. The
    /// status is purely a function of timestamps; never mutate
    /// `AggStatus` directly.
    pub fn status(&self) -> AggStatus {
        let now = now_ms();
        match (self.retired_at_ms, self.expires_at_ms) {
            (None, _) => AggStatus::Active,
            (Some(_), Some(exp)) if now >= exp => AggStatus::Expired,
            (Some(_), _) => AggStatus::Retired,
        }
    }

    /// Mark the schema retired, scheduling expiry `retention` from now.
    /// No-op if the schema is already retired (idempotent — re-applying
    /// the same retirement does not push the expiry out).
    pub fn retire(&mut self, retention: Duration) {
        if self.retired_at_ms.is_some() {
            return;
        }
        let now = now_ms();
        self.retired_at_ms = Some(now);
        self.expires_at_ms = Some(now + retention.as_millis() as u64);
    }

    /// Whether this schema accepts writes. Equivalent to
    /// `status() == AggStatus::Active`. Exposed as a method because
    /// it's the single check the ingest path makes — `is_writable` is
    /// the named contract from §6.3.
    pub fn is_writable(&self) -> bool {
        matches!(self.status(), AggStatus::Active)
    }

    /// §6.4 accuracy profile: theoretical error / confidence bound
    /// of any query answer computed from this schema's sketch,
    /// derived from `config.aggregation_type` + `config.parameters`.
    /// Exposed so HTTP endpoints and future `QueryResult`
    /// enrichment can return "±ε with probability 1 - δ" as a
    /// first-class answer attribute, instead of the user having to
    /// rederive the bound from the sketch literature.
    pub fn accuracy_profile(&self) -> super::accuracy::AccuracyProfile {
        super::accuracy::AccuracyProfile::derive(&self.config)
    }
}

/// Default retention for a retired schema before eviction.
/// 24 hours — covers dashboards / ad-hoc queries that may still
/// reference the old agg_id mid-reconfigure. Shorter than the
/// typical SketchStore data retention (7d+) so schema eviction
/// runs first, freeing space cleanly without fighting per-record
/// retention. Override via `SchemaRegistry::set_retention_for_testing`
/// or the CLI flag plumbed through `SchemaEvictionService`.
pub const DEFAULT_RETIREMENT_RETENTION: Duration = Duration::from_secs(24 * 3600);

/// In-memory registry of `AggSchema` keyed by `aggregation_id`.
///
/// Constructed from a `StreamingConfig` snapshot. The ingest path
/// consults `is_writable(agg_id)` on every write attempt; query path
/// reads `get(agg_id)` to attach schema metadata to results.
///
/// **Concurrency**: a single `RwLock` around the inner map. Writes are
/// rare (only on a `StreamingConfig` swap, which itself happens at most
/// every few seconds in production). Reads are cheap (a single lock
/// acquire + HashMap lookup, nanoseconds). For higher write rates we'd
/// switch to an ArcSwap of an immutable map; the API is designed to
/// allow that migration without callers changing.
pub struct SchemaRegistry {
    schemas: RwLock<HashMap<u64, AggSchema>>,
    /// Retention applied when a schema transitions from Active to
    /// Retired. Tunable per-deployment; Phase 2b will let the
    /// controller override it per-agg via `AggregationConfig`.
    retirement_retention: Duration,
    /// Optional on-disk path where the registry snapshots itself
    /// after every `reconcile` call (Phase 2c). Set via
    /// `with_persistence`. When `None`, the registry lives only in
    /// memory and the timeline loses pre-restart history — which is
    /// the pre-Phase-2c behaviour.
    ///
    /// Persistence is best-effort: I/O errors are logged but never
    /// block a reconcile. The worst case is a stale snapshot on
    /// disk, which will itself be rewritten by the next successful
    /// reconcile.
    persist_path: Option<PathBuf>,
}

impl SchemaRegistry {
    /// Build a fresh registry from a `StreamingConfig` snapshot. Every
    /// agg_id present in the config gets an `Active` schema with
    /// `created_at_ms = now`.
    pub fn from_streaming_config(config: &StreamingConfig) -> Self {
        let mut schemas = HashMap::new();
        for (&agg_id, cfg) in config.get_all_aggregation_configs() {
            schemas.insert(agg_id, AggSchema::new_active(cfg.clone()));
            // Sanity: the agg_id is consistent with the config's own
            // recorded id. If not, prefer the StreamingConfig key
            // (defensive against malformed YAML).
            debug_assert_eq!(agg_id, cfg.aggregation_id);
        }
        Self {
            schemas: RwLock::new(schemas),
            retirement_retention: DEFAULT_RETIREMENT_RETENTION,
            persist_path: None,
        }
    }

    /// Enable on-disk persistence at `path`. After this call every
    /// `reconcile` snapshots the registry atomically (tmp + rename)
    /// to `path`. If the file already exists, prefer
    /// [`Self::load_or_new_from_config`] over this builder so
    /// previously-persisted lifecycle timestamps are recovered
    /// instead of silently overwritten.
    ///
    /// I/O errors on save are logged as warnings but never fail a
    /// reconcile — the registry is always authoritative in memory.
    pub fn with_persistence(mut self, path: impl Into<PathBuf>) -> Self {
        self.persist_path = Some(path.into());
        self
    }

    /// Build a registry that recovers prior lifecycle history from
    /// `path` (if it exists) and then reconciles against the current
    /// `StreamingConfig`. The right way to construct a registry in
    /// production code.
    ///
    /// Load semantics:
    /// * If `path` does not exist: behaves like
    ///   `from_streaming_config(config).with_persistence(path)`
    ///   followed by an explicit save, so the next restart has
    ///   something to read.
    /// * If `path` exists and parses: load every persisted schema
    ///   verbatim (preserving `created_at_ms` / `retired_at_ms` /
    ///   `expires_at_ms`), then reconcile against `config` — new ids
    ///   in the config become Active, ids present only on disk get
    ///   retired if they weren't already.
    /// * If `path` exists but can't be parsed: log a warning and
    ///   fall back to the non-persisted path, preserving forward
    ///   progress over historical accuracy.
    pub fn load_or_new_from_config(path: impl Into<PathBuf>, config: &StreamingConfig) -> Self {
        let path = path.into();
        let mut registry = match Self::load_from_disk(&path) {
            Ok(Some(registry)) => {
                debug!(
                    "Loaded {} persisted schema(s) from {}",
                    registry.schemas.read().map(|m| m.len()).unwrap_or(0),
                    path.display()
                );
                registry
            }
            Ok(None) => Self::from_streaming_config(config),
            Err(e) => {
                warn!(
                    "Failed to load schema registry from {}: {e}. Starting fresh.",
                    path.display()
                );
                Self::from_streaming_config(config)
            }
        };
        registry.persist_path = Some(path);
        // Reconcile so the loaded state is refreshed against the
        // currently-authoritative config (new ids added, removed ids
        // retired). Also triggers an initial save so the snapshot on
        // disk reflects post-reconcile state.
        let _ = registry.reconcile(config);
        registry
    }

    /// Read a registry snapshot from disk. Returns `Ok(None)` if the
    /// file doesn't exist (first-run case), `Ok(Some(_))` if it
    /// parsed, and `Err` on I/O or parse failure.
    fn load_from_disk(path: &Path) -> Result<Option<Self>, std::io::Error> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let snap: PersistedSnapshot = serde_json::from_slice(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        if snap.version != PERSIST_FORMAT_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unsupported schema-registry persist format version {} (expected {})",
                    snap.version, PERSIST_FORMAT_VERSION
                ),
            ));
        }
        let mut map = HashMap::with_capacity(snap.schemas.len());
        for s in snap.schemas {
            map.insert(s.agg_id, s);
        }
        Ok(Some(Self {
            schemas: RwLock::new(map),
            retirement_retention: DEFAULT_RETIREMENT_RETENTION,
            persist_path: None,
        }))
    }

    /// Snapshot all current schemas to `persist_path` atomically
    /// (write `path.tmp`, then rename over `path`). Called at the
    /// tail of `reconcile`. No-op when no `persist_path` is set.
    fn save_to_disk_if_persistent(&self) {
        let Some(path) = self.persist_path.as_ref() else {
            return;
        };
        let schemas: Vec<AggSchema> = match self.schemas.read() {
            Ok(m) => m.values().cloned().collect(),
            Err(e) => {
                warn!("Schema registry lock poisoned; skipping persist: {e}");
                return;
            }
        };
        let snap = PersistedSnapshot {
            version: PERSIST_FORMAT_VERSION,
            schemas,
        };
        let bytes = match serde_json::to_vec_pretty(&snap) {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to serialise schema registry: {e}");
                return;
            }
        };
        let tmp = path.with_extension("tmp");
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = std::fs::create_dir_all(parent);
            }
        }
        if let Err(e) = std::fs::write(&tmp, &bytes) {
            warn!(
                "Failed to write schema registry tmp file {}: {e}",
                tmp.display()
            );
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            warn!(
                "Failed to rename schema registry {} → {}: {e}",
                tmp.display(),
                path.display()
            );
        }
    }

    /// Construct empty (used in tests and as the starting point before
    /// the first `StreamingConfig` arrives).
    pub fn empty() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            retirement_retention: DEFAULT_RETIREMENT_RETENTION,
            persist_path: None,
        }
    }

    /// The §6.3 write-side barrier. Returns `true` only if `agg_id`
    /// is registered AND its `AggStatus` is `Active`. Both unknown
    /// ids and retired/expired ids return `false`.
    ///
    /// Cost: one `RwLock` read acquire + `HashMap::get`. Nanoseconds.
    pub fn is_writable(&self, agg_id: u64) -> bool {
        self.schemas
            .read()
            .ok()
            .and_then(|m| m.get(&agg_id).map(|s| s.is_writable()))
            .unwrap_or(false)
    }

    /// Read-only access to a schema, returned by clone. Used by the
    /// query path to attach `AccuracyProfile` (§6.4) and
    /// `Provenance` (§15) to results.
    pub fn get(&self, agg_id: u64) -> Option<AggSchema> {
        self.schemas.read().ok()?.get(&agg_id).cloned()
    }

    /// Remove a schema record from the registry. Used by
    /// `SchemaEvictionService` after it's dropped the agg's data
    /// from the store. Returns the removed schema if it existed,
    /// `None` if the agg_id was already absent (idempotent).
    ///
    /// Triggers a `save_to_disk_if_persistent` so restart behaviour
    /// stays consistent (an evicted agg won't reappear on restart
    /// from a stale persisted snapshot).
    pub fn remove_schema(&self, agg_id: u64) -> Option<AggSchema> {
        let removed = if let Ok(mut map) = self.schemas.write() {
            map.remove(&agg_id)
        } else {
            None
        };
        if removed.is_some() {
            self.save_to_disk_if_persistent();
        }
        removed
    }

    /// Force a specific `agg_id` into `Retired` status, starting the
    /// configured retirement retention clock. Idempotent — re-retiring
    /// a Retired or Expired schema is a no-op and returns `Some(schema)`
    /// reflecting the current (unchanged) state. Returns `None` if
    /// the `agg_id` is unknown.
    ///
    /// Intended for operator / debug-endpoint use so the eviction path
    /// can be driven without waiting for a `StreamingConfig` swap to
    /// drop the agg.
    pub fn force_retire(&self, agg_id: u64) -> Option<AggSchema> {
        let retention = self.retirement_retention;
        let updated = {
            let mut map = self.schemas.write().ok()?;
            let schema = map.get_mut(&agg_id)?;
            if matches!(schema.status(), AggStatus::Active) {
                schema.retire(retention);
            }
            schema.clone()
        };
        self.save_to_disk_if_persistent();
        Some(updated)
    }

    /// Force a specific `agg_id` into `Expired` status immediately by
    /// setting both `retired_at_ms` and `expires_at_ms` to now. The
    /// next `SchemaEvictionService` tick will drop its data and
    /// remove the schema. Returns the new state, or `None` if the
    /// `agg_id` is unknown.
    ///
    /// Intended for operator / debug-endpoint use so eviction can be
    /// observed in e2e tests without waiting out retirement retention.
    pub fn force_expire(&self, agg_id: u64) -> Option<AggSchema> {
        let updated = {
            let mut map = self.schemas.write().ok()?;
            let schema = map.get_mut(&agg_id)?;
            let now = now_ms();
            schema.retired_at_ms = Some(now);
            schema.expires_at_ms = Some(now);
            schema.clone()
        };
        self.save_to_disk_if_persistent();
        Some(updated)
    }

    /// Iterate (clones) all schemas matching a status filter. Used by
    /// the controller-facing `/api/v1/db/schemas?status=…` endpoint
    /// (§15.2 of the design).
    pub fn list_by_status(&self, status: AggStatus) -> Vec<AggSchema> {
        let map = match self.schemas.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };
        map.values()
            .filter(|s| s.status() == status)
            .cloned()
            .collect()
    }

    /// Reconcile against a fresh `StreamingConfig` snapshot:
    ///   - new agg_ids in `config` but not in registry → create as Active
    ///   - agg_ids in registry but not in `config` → mark Retired
    ///     (no-op if already retired)
    ///   - agg_ids in both → leave alone (schemas are immutable; a
    ///     parameter change must come via a new agg_id per the
    ///     monotonic-id contract)
    ///
    /// Returns a `(added, retired)` summary so the HTTP swap handler
    /// can log the diff.
    ///
    /// Phase 2b will wire this directly into the
    /// `POST /api/v1/streaming-config` swap handler. Phase 2a exposes
    /// it for test coverage and for the registry's own startup
    /// initialization.
    pub fn reconcile(&self, config: &StreamingConfig) -> ReconcileSummary {
        let mut map = match self.schemas.write() {
            Ok(m) => m,
            Err(_) => {
                return ReconcileSummary {
                    added: Vec::new(),
                    retired: Vec::new(),
                }
            }
        };

        let new_ids: std::collections::HashSet<u64> = config
            .get_all_aggregation_configs()
            .keys()
            .copied()
            .collect();

        let mut added = Vec::new();
        for (&agg_id, cfg) in config.get_all_aggregation_configs() {
            if let std::collections::hash_map::Entry::Vacant(e) = map.entry(agg_id) {
                e.insert(AggSchema::new_active(cfg.clone()));
                added.push(agg_id);
            }
        }

        let mut retired = Vec::new();
        let known_ids: Vec<u64> = map.keys().copied().collect();
        for agg_id in known_ids {
            if !new_ids.contains(&agg_id) {
                if let Some(schema) = map.get_mut(&agg_id) {
                    if matches!(schema.status(), AggStatus::Active) {
                        schema.retire(self.retirement_retention);
                        retired.push(agg_id);
                    }
                }
            }
        }

        // Drop the write lock BEFORE persisting — save_to_disk_if_persistent
        // takes a read lock, so holding the write one would deadlock on
        // a single-threaded runtime.
        drop(map);
        let summary = ReconcileSummary { added, retired };
        self.save_to_disk_if_persistent();
        summary
    }

    /// §7 schema timeline — the key to query continuity across
    /// reconfigure boundaries. Returns the ordered list of
    /// `(agg_id, clipped_range)` segments that cover `[t1_ms, t2_ms]`
    /// for the given metric.
    ///
    /// ## Semantics
    ///
    /// For each metric, the registry holds zero or more `AggSchema`
    /// entries. Each one owns the metric starting at its
    /// `created_at_ms` until the next schema for the same metric
    /// appears (or forever if it's the current active one). A retired
    /// schema's ownership ends at its `retired_at_ms` if no successor
    /// exists; otherwise at the successor's `created_at_ms`. An
    /// expired schema still appears in the timeline for reads
    /// targeting the pre-expiry window — the caller decides whether
    /// to read through `TimelineCoverage` below.
    ///
    /// By construction the resulting segments are **non-overlapping**
    /// and ordered by `start_ms`. Gaps in time (e.g. the metric had
    /// no schema at that moment) do **not** produce segments — the
    /// caller sees a coverage hole and can fall back to the exact
    /// DB per §7.3.
    ///
    /// ## Current limitations
    ///
    /// * `created_at_ms` is currently the wall-clock at which the
    ///   backend first observed the schema, not necessarily when the
    ///   first datapoint was written. Without on-disk schema
    ///   persistence, the timeline reflects only the *post-restart*
    ///   history — matching the right-edge-of-time behaviour the
    ///   precompute engine had before the timeline read API existed.
    ///   On-disk schema persistence closes that gap.
    /// * All segments are returned, including those whose schema is
    ///   `Expired`. The caller inspects `TimelineSegment::status` to
    ///   decide whether data is still readable.
    ///
    /// ## Cost
    ///
    /// Linear in the number of schemas for the given metric (one
    /// pass to collect + sort). For the metric counts typical of
    /// sketch DB deployments (dozens of metrics × a handful of
    /// schemas each) this is well under a microsecond. The query
    /// path calls this once per query, so the cost is amortised
    /// across the query.
    pub fn timeline_for_metric(
        &self,
        metric: &str,
        t1_ms: u64,
        t2_ms: u64,
    ) -> Vec<TimelineSegment> {
        if t1_ms > t2_ms {
            return Vec::new();
        }
        let map = match self.schemas.read() {
            Ok(m) => m,
            Err(_) => return Vec::new(),
        };

        // Collect schemas for this metric, sorted by their start
        // (== created_at_ms). This is the authoritative ordering;
        // agg_id alone isn't monotonic across metrics.
        let mut entries: Vec<&AggSchema> =
            map.values().filter(|s| s.metric_name == metric).collect();
        entries.sort_by_key(|s| (s.created_at_ms, s.agg_id));

        // Each schema owns `[created_at_ms, own_end)` where `own_end`
        // is the earlier of (a) the next schema's `created_at_ms`
        // and (b) this schema's own `retired_at_ms`. If neither
        // bounds the schema it owns up to `u64::MAX` (open-ended
        // — the currently Active one). When a gap exists between
        // one schema's `retired_at_ms` and the next's
        // `created_at_ms`, this leaves the gap *unowned* — callers
        // see zero segments there and fall back to the exact DB
        // per §7.3.
        let mut segments = Vec::with_capacity(entries.len());
        for (i, schema) in entries.iter().enumerate() {
            let own_start = schema.created_at_ms;
            let successor_start = entries.get(i + 1).map(|n| n.created_at_ms);
            let own_end = match (successor_start, schema.retired_at_ms) {
                (Some(s), Some(r)) => s.min(r),
                (Some(s), None) => s,
                (None, Some(r)) => r,
                (None, None) => u64::MAX,
            };

            // Clip to the query range.
            let clipped_start = own_start.max(t1_ms);
            // Treat t2 as inclusive (caller passes a closed range).
            let clipped_end = own_end.min(t2_ms.saturating_add(1));
            if clipped_start >= clipped_end {
                continue;
            }

            segments.push(TimelineSegment {
                agg_id: schema.agg_id,
                start_ms: clipped_start,
                end_ms: clipped_end,
                status: schema.status(),
                coverage: coverage_for(schema, own_start, own_end),
            });
        }

        segments
    }

    /// Override the retirement retention. Plumbed through from
    /// `SchemaEvictionService` at startup so deployments can pick
    /// a retention that's ≤ their SketchStore
    /// `persistence_delete_older_than` (see module-level doc on
    /// retention ordering).
    ///
    /// Takes `&mut self` because registry construction patterns
    /// already produce a mutable local before wrapping in `Arc`.
    /// Once wrapped, retention is immutable.
    pub fn set_retention(&mut self, retention: Duration) {
        self.retirement_retention = retention;
    }

    /// Expose the current retirement retention so the eviction
    /// service can log it + diff it against the data-retention
    /// config at startup.
    pub fn retirement_retention(&self) -> Duration {
        self.retirement_retention
    }

    /// Test-only alias for `set_retention`; kept as a separate
    /// name so pre-Phase-5h test call sites don't need renaming.
    #[cfg(test)]
    pub fn set_retention_for_testing(&mut self, retention: Duration) {
        self.retirement_retention = retention;
    }

    /// Insert a schema with caller-supplied timestamps, replacing any
    /// existing entry for the same `agg_id`. Used by the timeline
    /// tests so they can assert against deterministic time ranges
    /// instead of wall-clock-derived ones.
    #[cfg(test)]
    pub fn insert_raw_for_testing(&self, schema: AggSchema) {
        if let Ok(mut map) = self.schemas.write() {
            map.insert(schema.agg_id, schema);
        }
    }
}

/// On-disk JSON format version for the persisted schema registry.
/// Bumped whenever the shape of [`PersistedSnapshot`] or [`AggSchema`]
/// changes incompatibly; loads with a different version are rejected
/// rather than silently coerced, so partial upgrades don't corrupt
/// the timeline's pre-restart history.
pub const PERSIST_FORMAT_VERSION: u32 = 1;

/// Top-level structure written to disk by [`SchemaRegistry`] when
/// `persist_path` is set. Tagged with [`PERSIST_FORMAT_VERSION`] so
/// future schema evolution can refuse to load incompatible files
/// instead of silently losing data.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    version: u32,
    schemas: Vec<AggSchema>,
}

/// Summary of a single `reconcile` call. Surfaced so the HTTP swap
/// handler (Phase 2b) can log structured diff events.
#[derive(Debug, Default)]
pub struct ReconcileSummary {
    pub added: Vec<u64>,
    pub retired: Vec<u64>,
}

/// A single `(agg_id, clipped_range)` segment returned by
/// [`SchemaRegistry::timeline_for_metric`]. Ranges are half-open:
/// inclusive `start_ms`, exclusive `end_ms`. Segments are guaranteed
/// non-overlapping and ordered by `start_ms` by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineSegment {
    pub agg_id: u64,
    pub start_ms: u64,
    pub end_ms: u64,
    /// Lifecycle state of the owning schema at the moment the
    /// timeline was computed. The query path uses this to decide
    /// whether to read from the sketch (`Active` / `Retired`) or
    /// fall back to the exact DB (`Expired`).
    pub status: AggStatus,
    /// Whether data is expected to be present for this segment.
    /// Distinct from `status` — a `Retired` schema still has its
    /// data but a segment that falls entirely past the schema's
    /// expiry is `Purged` even if `status` is still `Retired` at the
    /// moment of the call.
    pub coverage: TimelineCoverage,
}

/// Coarse classification of whether a [`TimelineSegment`]'s data is
/// expected to be readable from the sketch store. §7.3 of the design
/// doc lays out the full state machine; this enum exposes the two
/// states we can determine purely from schema metadata. Follow-up
/// work (cross-segment stitching, backfill coverage) will extend
/// this with `BackfillInProgress` and finer-grained per-window
/// coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimelineCoverage {
    /// Data is (or was) written by the live ingest path and the
    /// schema has not been expired. The query engine should read
    /// from the sketch store.
    Sketch,
    /// The schema has been expired and its data purged (or is
    /// eligible for purge). The query engine should fall back to
    /// the exact DB for this segment per §7.3.
    Purged,
}

fn coverage_for(schema: &AggSchema, _own_start: u64, _own_end: u64) -> TimelineCoverage {
    match schema.status() {
        AggStatus::Expired => TimelineCoverage::Purged,
        AggStatus::Active | AggStatus::Retired => TimelineCoverage::Sketch,
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_types::enums::{AggregationType, WindowType};
    use promql_utilities::data_model::key_by_label_names::KeyByLabelNames;

    fn make_config(agg_id: u64) -> AggregationConfig {
        AggregationConfig::new(
            agg_id,
            AggregationType::CountMinSketch,
            String::new(),
            HashMap::new(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            KeyByLabelNames::empty(),
            String::new(),
            60,
            60,
            WindowType::Tumbling,
            String::new(),
            format!("metric_{agg_id}"),
            None,
            None,
            None,
            None,
        )
    }

    fn make_streaming_config(ids: &[u64]) -> StreamingConfig {
        let map: HashMap<u64, AggregationConfig> =
            ids.iter().map(|&id| (id, make_config(id))).collect();
        StreamingConfig::new(map)
    }

    #[test]
    fn new_active_schema_is_writable() {
        let s = AggSchema::new_active(make_config(1));
        assert_eq!(s.status(), AggStatus::Active);
        assert!(s.is_writable());
        assert_eq!(s.agg_id, 1);
        assert_eq!(s.metric_name, "metric_1");
    }

    #[test]
    fn retire_transitions_to_retired_with_expiry() {
        let mut s = AggSchema::new_active(make_config(1));
        s.retire(Duration::from_secs(60));
        assert_eq!(s.status(), AggStatus::Retired);
        assert!(!s.is_writable());
        assert!(s.retired_at_ms.is_some());
        let exp = s.expires_at_ms.expect("expires set");
        let ret = s.retired_at_ms.unwrap();
        assert!(exp >= ret + 60_000 && exp < ret + 60_500);
    }

    #[test]
    fn retire_is_idempotent() {
        let mut s = AggSchema::new_active(make_config(1));
        s.retire(Duration::from_secs(60));
        let first_exp = s.expires_at_ms;
        std::thread::sleep(std::time::Duration::from_millis(5));
        s.retire(Duration::from_secs(99999)); // would push expiry far if not idempotent
        assert_eq!(s.expires_at_ms, first_exp);
    }

    #[test]
    fn registry_from_streaming_config_marks_all_active() {
        let cfg = make_streaming_config(&[1, 2, 3]);
        let r = SchemaRegistry::from_streaming_config(&cfg);
        for id in [1, 2, 3] {
            assert!(r.is_writable(id));
            assert_eq!(r.get(id).unwrap().status(), AggStatus::Active);
        }
        assert!(!r.is_writable(99));
        assert!(r.get(99).is_none());
    }

    #[test]
    fn empty_registry_rejects_all_writes() {
        let r = SchemaRegistry::empty();
        assert!(!r.is_writable(1));
        assert!(!r.is_writable(0));
    }

    #[test]
    fn reconcile_adds_new_ids() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        let summary = r.reconcile(&make_streaming_config(&[1, 2, 3]));
        // HashMap iteration order is non-deterministic; sort before comparing.
        let mut got = summary.added;
        got.sort();
        assert_eq!(got, vec![2, 3]);
        assert!(summary.retired.is_empty());
        assert!(r.is_writable(2));
        assert!(r.is_writable(3));
    }

    #[test]
    fn reconcile_retires_removed_ids() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1, 2]));
        let summary = r.reconcile(&make_streaming_config(&[2]));
        assert_eq!(summary.added, Vec::<u64>::new());
        assert_eq!(summary.retired, vec![1]);
        assert!(!r.is_writable(1));
        assert!(r.is_writable(2));
        // Schema for 1 is still readable for query continuity.
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Retired);
    }

    #[test]
    fn reconcile_is_noop_for_unchanged_ids() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1, 2]));
        let summary = r.reconcile(&make_streaming_config(&[1, 2]));
        assert!(summary.added.is_empty());
        assert!(summary.retired.is_empty());
    }

    #[test]
    fn reconcile_does_not_re_retire_already_retired_id() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1, 2]));
        let _ = r.reconcile(&make_streaming_config(&[2]));
        // Second reconcile with the same removed id: no new "retired" entry.
        let summary2 = r.reconcile(&make_streaming_config(&[2]));
        assert!(summary2.retired.is_empty());
    }

    #[test]
    fn list_by_status_filters_correctly() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1, 2, 3]));
        let _ = r.reconcile(&make_streaming_config(&[2]));
        let active = r.list_by_status(AggStatus::Active);
        let retired = r.list_by_status(AggStatus::Retired);
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].agg_id, 2);
        assert_eq!(retired.len(), 2);
    }

    #[test]
    fn schema_expires_after_retention_elapses() {
        let mut r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        r.set_retention_for_testing(Duration::from_millis(50));
        let _ = r.reconcile(&make_streaming_config(&[]));
        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Expired);
    }

    // --- §7 timeline_for_metric tests ---

    /// Build a schema with explicit timestamps, bypassing the
    /// wall-clock path. `metric_override` defaults to `metric_{id}`
    /// to keep parity with `make_config` but lets us pin multiple
    /// agg_ids to the same metric for timeline scenarios.
    fn fixed_schema(
        agg_id: u64,
        metric: &str,
        created_at_ms: u64,
        retired_at_ms: Option<u64>,
        expires_at_ms: Option<u64>,
    ) -> AggSchema {
        let mut cfg = make_config(agg_id);
        cfg.metric = metric.to_string();
        AggSchema {
            agg_id,
            metric_name: metric.to_string(),
            config: cfg,
            created_at_ms,
            retired_at_ms,
            expires_at_ms,
        }
    }

    #[test]
    fn timeline_empty_for_unknown_metric() {
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 1_000, None, None));
        let segs = r.timeline_for_metric("qps", 0, 10_000);
        assert!(segs.is_empty());
    }

    #[test]
    fn timeline_single_active_spans_query_range() {
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 1_000, None, None));
        let segs = r.timeline_for_metric("latency", 2_000, 5_000);
        assert_eq!(segs.len(), 1);
        let s = &segs[0];
        assert_eq!(s.agg_id, 1);
        // Active → open-ended → clipped to [t1, t2+1).
        assert_eq!(s.start_ms, 2_000);
        assert_eq!(s.end_ms, 5_001);
        assert_eq!(s.status, AggStatus::Active);
        assert_eq!(s.coverage, TimelineCoverage::Sketch);
    }

    #[test]
    fn timeline_two_segments_reconfigure_mid_range() {
        // agg 1 owns [1_000, 10_000); agg 2 takes over at 10_000.
        // Use a far-future expiry so agg 1 stays Retired (not Expired)
        // against the wall clock at test time.
        let far_future = 32_503_680_000_000_u64; // ~year 3000 in ms.
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(
            1,
            "latency",
            1_000,
            Some(10_000),
            Some(far_future),
        ));
        r.insert_raw_for_testing(fixed_schema(2, "latency", 10_000, None, None));

        let segs = r.timeline_for_metric("latency", 5_000, 15_000);
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].agg_id, 1);
        assert_eq!(segs[0].start_ms, 5_000);
        assert_eq!(segs[0].end_ms, 10_000);
        assert_eq!(segs[0].status, AggStatus::Retired);
        assert_eq!(segs[0].coverage, TimelineCoverage::Sketch);

        assert_eq!(segs[1].agg_id, 2);
        assert_eq!(segs[1].start_ms, 10_000);
        assert_eq!(segs[1].end_ms, 15_001);
        assert_eq!(segs[1].status, AggStatus::Active);
    }

    #[test]
    fn timeline_excludes_segments_outside_query_range() {
        let far_future = 32_503_680_000_000_u64;
        let r = SchemaRegistry::empty();
        // agg 1: [1_000, 10_000) — before query range.
        r.insert_raw_for_testing(fixed_schema(
            1,
            "latency",
            1_000,
            Some(10_000),
            Some(far_future),
        ));
        // agg 2: [10_000, ∞) — active.
        r.insert_raw_for_testing(fixed_schema(2, "latency", 10_000, None, None));

        let segs = r.timeline_for_metric("latency", 20_000, 30_000);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].agg_id, 2);
        assert_eq!(segs[0].start_ms, 20_000);
        assert_eq!(segs[0].end_ms, 30_001);
    }

    #[test]
    fn timeline_expired_segment_marked_purged() {
        // agg 1 retired + already past expiry (expires_at in the past).
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 1_000, Some(2_000), Some(3_000)));
        let segs = r.timeline_for_metric("latency", 500, 2_500);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].agg_id, 1);
        assert_eq!(segs[0].status, AggStatus::Expired);
        assert_eq!(segs[0].coverage, TimelineCoverage::Purged);
    }

    #[test]
    fn timeline_retired_without_successor_ends_at_retirement() {
        // agg 1 retired at 10_000; retention not yet elapsed.
        // Expiry well in the future (year 3000 in ms).
        let far_future = 32_503_680_000_000_u64;
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(
            1,
            "latency",
            1_000,
            Some(10_000),
            Some(far_future),
        ));
        // Query range extends past retirement; retired-without-successor
        // means ownership ends at retired_at, not open-ended.
        let segs = r.timeline_for_metric("latency", 5_000, 20_000);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].end_ms, 10_000);
        assert_eq!(segs[0].status, AggStatus::Retired);
        assert_eq!(segs[0].coverage, TimelineCoverage::Sketch);
    }

    #[test]
    fn timeline_segments_are_non_overlapping_even_with_many_schemas() {
        // Three schemas in sequence for the same metric.
        let far_future = 32_503_680_000_000_u64;
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 0, Some(100), Some(far_future)));
        r.insert_raw_for_testing(fixed_schema(2, "latency", 100, Some(200), Some(far_future)));
        r.insert_raw_for_testing(fixed_schema(3, "latency", 200, None, None));

        let segs = r.timeline_for_metric("latency", 0, 300);
        assert_eq!(segs.len(), 3);
        // Verify ordering + non-overlap.
        let mut last_end = 0;
        for s in &segs {
            assert!(s.start_ms >= last_end, "segments overlap: {:?}", segs);
            assert!(s.end_ms > s.start_ms);
            last_end = s.end_ms;
        }
        assert_eq!(
            segs.iter().map(|s| s.agg_id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[test]
    fn timeline_ignores_other_metrics() {
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 1_000, None, None));
        r.insert_raw_for_testing(fixed_schema(2, "qps", 1_000, None, None));
        let segs = r.timeline_for_metric("latency", 2_000, 5_000);
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].agg_id, 1);
    }

    #[test]
    fn timeline_inverted_range_returns_empty() {
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 0, None, None));
        let segs = r.timeline_for_metric("latency", 5_000, 1_000);
        assert!(segs.is_empty());
    }

    #[test]
    fn timeline_gap_between_schemas_produces_no_segment_in_gap() {
        // A metric whose coverage has a gap: agg 1 retired at 100,
        // agg 2 doesn't appear until 200. Queries hitting the gap
        // [100, 200) see zero segments — caller's cue to fall back
        // to the exact DB per §7.3 coverage hole handling.
        let far_future = 32_503_680_000_000_u64;
        let r = SchemaRegistry::empty();
        r.insert_raw_for_testing(fixed_schema(1, "latency", 0, Some(100), Some(far_future)));
        r.insert_raw_for_testing(fixed_schema(2, "latency", 200, None, None));

        let gap_segs = r.timeline_for_metric("latency", 120, 180);
        assert!(
            gap_segs.is_empty(),
            "query fully inside the gap should see no segments: got {gap_segs:?}"
        );

        // Range straddling the gap: should return both surrounding
        // segments, each clipped, with no third segment for the gap.
        let straddle = r.timeline_for_metric("latency", 50, 250);
        assert_eq!(straddle.len(), 2);
        assert_eq!(straddle[0].agg_id, 1);
        assert_eq!(straddle[0].end_ms, 100);
        assert_eq!(straddle[1].agg_id, 2);
        assert_eq!(straddle[1].start_ms, 200);
    }

    // --- Phase 2c: on-disk persistence tests ---

    #[test]
    fn persistence_roundtrip_preserves_timestamps() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");

        let cfg = make_streaming_config(&[1, 2]);
        let registry = SchemaRegistry::load_or_new_from_config(&path, &cfg);
        assert!(registry.is_writable(1));
        assert!(registry.is_writable(2));

        // Retire agg 2 and persist.
        let cfg_only_1 = make_streaming_config(&[1]);
        let _ = registry.reconcile(&cfg_only_1);
        let retired_at_before = registry.get(2).unwrap().retired_at_ms;
        assert!(retired_at_before.is_some());

        // Drop registry, simulate a restart by loading from the same
        // file. The retirement timestamp for agg 2 must survive.
        drop(registry);
        let reloaded = SchemaRegistry::load_or_new_from_config(&path, &cfg_only_1);
        let reloaded_2 = reloaded.get(2).expect("agg 2 reloaded");
        assert_eq!(reloaded_2.status(), AggStatus::Retired);
        assert_eq!(reloaded_2.retired_at_ms, retired_at_before);
        // agg 1 is still active after reconcile.
        assert!(reloaded.is_writable(1));
    }

    #[test]
    fn load_or_new_from_config_creates_file_on_first_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        assert!(!path.exists());

        let cfg = make_streaming_config(&[7]);
        let _ = SchemaRegistry::load_or_new_from_config(&path, &cfg);
        assert!(path.exists(), "persist file should be written on first run");

        // File should be valid JSON containing agg_id=7.
        let bytes = std::fs::read(&path).unwrap();
        let snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snap["version"], 1);
        let schemas = snap["schemas"].as_array().unwrap();
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0]["agg_id"], 7);
    }

    #[test]
    fn load_or_new_from_config_reconciles_against_fresh_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");

        // First run: persist two ids.
        let cfg_two = make_streaming_config(&[1, 2]);
        drop(SchemaRegistry::load_or_new_from_config(&path, &cfg_two));

        // Second run: config now only has id 3 (a restart with a
        // new streaming-config). The loaded 1 and 2 should be
        // retired; 3 should be active.
        let cfg_three_only = make_streaming_config(&[3]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg_three_only);
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Retired);
        assert_eq!(r.get(2).unwrap().status(), AggStatus::Retired);
        assert!(r.is_writable(3));
    }

    #[test]
    fn corrupt_persist_file_falls_back_to_fresh_registry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        std::fs::write(&path, b"this is not json").unwrap();

        let cfg = make_streaming_config(&[42]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg);
        assert!(r.is_writable(42));
        // Previous known ids on disk were bogus; reloaded file must
        // now be valid (reconcile writes a fresh snapshot over the
        // corrupt one).
        let bytes = std::fs::read(&path).unwrap();
        let snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snap["version"], 1);
    }

    #[test]
    fn unsupported_persist_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schemas.json");
        let snap = serde_json::json!({"version": 999, "schemas": []});
        std::fs::write(&path, serde_json::to_vec(&snap).unwrap()).unwrap();

        // Falls back to from_streaming_config then reconciles and
        // overwrites with the current version.
        let cfg = make_streaming_config(&[1]);
        let r = SchemaRegistry::load_or_new_from_config(&path, &cfg);
        assert!(r.is_writable(1));
        let bytes = std::fs::read(&path).unwrap();
        let got: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got["version"], 1);
    }

    #[test]
    fn persist_without_path_does_not_write_anywhere() {
        // Regression: ensure the non-persistent path is unaffected —
        // no file created under the test's tempdir on reconcile.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("should_not_exist.json");
        let registry = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        let _ = registry.reconcile(&make_streaming_config(&[2]));
        assert!(!path.exists());
    }

    // --- manual retire / expire endpoints (debug/operator surface) ---

    #[test]
    fn force_retire_active_transitions_to_retired() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Active);
        let out = r.force_retire(1).expect("should return new state");
        assert_eq!(out.status(), AggStatus::Retired);
        assert!(out.retired_at_ms.is_some());
        assert!(out.expires_at_ms.is_some());
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Retired);
    }

    #[test]
    fn force_retire_is_idempotent_on_retired() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        let first = r.force_retire(1).unwrap();
        let first_exp = first.expires_at_ms;
        std::thread::sleep(std::time::Duration::from_millis(5));
        let second = r.force_retire(1).unwrap();
        assert_eq!(second.expires_at_ms, first_exp);
    }

    #[test]
    fn force_retire_unknown_returns_none() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        assert!(r.force_retire(999).is_none());
    }

    #[test]
    fn force_expire_active_transitions_to_expired() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Active);
        let out = r.force_expire(1).expect("should return new state");
        assert_eq!(out.status(), AggStatus::Expired);
        assert_eq!(r.get(1).unwrap().status(), AggStatus::Expired);
    }

    #[test]
    fn force_expire_unknown_returns_none() {
        let r = SchemaRegistry::from_streaming_config(&make_streaming_config(&[1]));
        assert!(r.force_expire(999).is_none());
    }
}
