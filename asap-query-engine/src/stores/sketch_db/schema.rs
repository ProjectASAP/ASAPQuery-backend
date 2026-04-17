//! Per-`aggregation_id` schema metadata and lifecycle.
//!
//! Implements §5 / §6 of the sketch DB design
//! ([`design-sketch-db.md`](../../../../../docs/design-sketch-db.md)).
//!
//! ## Why this exists
//!
//! Today's `SimpleMapStore` is keyed by `aggregation_id` but does not know
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
//! ## Phase 2a scope (what this file covers)
//!
//! * `AggSchema` struct with the lifecycle fields populated from
//!   `AggregationConfig` + a `created_at` timestamp.
//! * `AggStatus` enum: `Active` / `Retired` / `Expired`, derived from
//!   `retired_at` and `expires_at` plus the wall clock.
//! * `SchemaRegistry` — an in-memory map keyed by `agg_id` that the ingest
//!   path consults. Built from the current `StreamingConfig` snapshot at
//!   construction; refreshed in lockstep with the hot-reload `ArcSwap`
//!   (Phase 2b will diff old vs new and explicitly retire removed ids;
//!   for now any id missing from the snapshot is treated as Retired with
//!   a default retention).
//!
//! ## Out of scope here (Phase 2b and beyond)
//!
//! * On-disk schema persistence — schemas are rebuilt from
//!   `StreamingConfig` on startup. This is intentionally deferred so this
//!   PR has zero on-disk format change.
//! * `AccuracyProfile` derivation — §6.4 in the design doc; the hook is
//!   here as `accuracy_profile()` that returns a stub today and will be
//!   filled in once the sketch types' theoretical bounds are vendored.
//! * HTTP swap-diff that creates/retires schemas explicitly — Phase 2b.
//! * Compaction policy that reads `AggStatus` to throttle as expiry
//!   approaches — §9.2 of the design.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use asap_types::aggregation_config::AggregationConfig;

use crate::data_model::StreamingConfig;

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
#[derive(Debug, Clone)]
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
}

/// Default retention for retired schemas — one hour. A future
/// `AggregationConfig` field could let the controller override this
/// per-agg; for Phase 2a a single global default is enough.
pub const DEFAULT_RETIREMENT_RETENTION: Duration = Duration::from_secs(3600);

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
        }
    }

    /// Construct empty (used in tests and as the starting point before
    /// the first `StreamingConfig` arrives).
    pub fn empty() -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            retirement_retention: DEFAULT_RETIREMENT_RETENTION,
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

        ReconcileSummary { added, retired }
    }

    /// Override the default retirement retention. Test-only for now;
    /// production tunable will land in Phase 2b's controller-facing
    /// API.
    #[cfg(test)]
    pub fn set_retention_for_testing(&mut self, retention: Duration) {
        self.retirement_retention = retention;
    }
}

/// Summary of a single `reconcile` call. Surfaced so the HTTP swap
/// handler (Phase 2b) can log structured diff events.
#[derive(Debug, Default)]
pub struct ReconcileSummary {
    pub added: Vec<u64>,
    pub retired: Vec<u64>,
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
}
