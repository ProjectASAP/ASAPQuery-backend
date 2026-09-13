//! `BackfillJob` lifecycle types + in-memory `BackfillRegistry`.
//!
//! Implements §10 (refreshable view maintenance / backfill path) of the
//! future storage scope ([`future-storage-and-compression.md`](../../../../../docs/design_docs/future-storage-and-compression.md)).
//!
//! ## Why this exists
//!
//! The sketch tier's §8 incremental maintenance can't fill data from
//! BEFORE an `agg_id` was created. When a reconfigure introduces a new
//! agg_id — say the operator widens a CMS from 256 to 2048, or swaps
//! in KLL200 on top of a metric that previously had only CMS — the new
//! agg has zero history. Queries spanning the reconfigure boundary
//! either see a data cliff (which the §7 schema timeline surfaces
//! honestly via `Partial` results + `warnings`) or have to fall
//! back to the exact DB.
//!
//! Backfill closes that gap: a `BackfillJob` reads raw samples from
//! the exact DB for a `(agg_id, time_range)` window, rebuilds the
//! sketch deterministically (§10.5), and writes it into the store
//! tagged as `origin = Backfilled { job_id }`. After the job
//! completes, the new agg_id's timeline segment covers the historical
//! range too.
//!
//! ## Phase 5a scope (what this file covers)
//!
//! Pure data types + a thread-safe registry:
//!
//! * `BackfillJob` struct with lifecycle fields (`status`,
//!   `windows_done` / `windows_total`, `started_at_ms`,
//!   `completed_at_ms`, `error_message`).
//! * `BackfillSource` enum (the four variants from §10.2) plus the
//!   `OtherSketch { source_agg_id }` intra-tier case.
//! * `BackfillStatus` enum with the full state machine (Queued →
//!   Running → Complete / Failed / Cancelled).
//! * `Coverage` enum (Complete / BackfillInProgress / Missing).
//! * `BackfillRegistry` — monotonic `job_id` allocation + thread-safe
//!   create / get / list / update_status / cancel, entirely
//!   in-memory.
//!
//! ## Out of scope for 5a (future phases)
//!
//! * `RawSampleReader` trait and concrete readers (§10.2) — Phase 5b.
//! * `BackfillWorkerPool` draining the registry — Phase 5c.
//! * HTTP `POST /api/v1/db/backfill` + `GET /api/v1/db/backfill/jobs`
//!   endpoints — Phase 5d.
//! * Deterministic sketch rebuild (§10.5) — Phase 5e.
//! * Coverage lookup by the query path (§7.3) — Phase 5f.
//! * On-disk persistence of job records across restart — Phase 5g
//!   (analogous to schema persistence in Phase 2c).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use asap_types::aggregation_config::AggregationConfig;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Source of raw samples a backfill job reads from. The DB picks a
/// concrete reader at job-dispatch time based on deployment config
/// (§10.2 of the design). All variants feed the same sketch builder
/// downstream; the reader abstraction lives in Phase 5b.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BackfillSource {
    /// Gorilla-compressed files in S3 / MinIO / GCS, produced by
    /// DataCollector's gorillacol + S3 Files exporter.
    S3Gorilla { bucket: String, prefix: String },
    /// Prometheus (or VictoriaMetrics / Thanos / Cortex) via the
    /// HTTP range-query API.
    Prometheus { url: String },
    /// ClickHouse table read through the deployment's configured connection.
    /// The source identity belongs to the job; credentials remain local.
    ClickHouse { database: String, table: String },
    /// Rebuild from a different sketch already in the store. Used for
    /// lossless schema widenings (e.g. CMS(256) → CMS(2048)) where
    /// the source sketch is a strict subset of the target's
    /// representable state. The reader converts `source_agg_id`'s
    /// precomputes into the target's shape without touching the exact
    /// DB — cheaper than a full S3 / Prometheus reread when
    /// applicable.
    OtherSketch { source_agg_id: u64 },
}

/// Lifecycle state of a [`BackfillJob`]. Transitions:
///
/// ```text
///            ┌──────────┐
///            │  Queued  │────────────────┐
///            └────┬─────┘                │ cancel() before run
///                 │ worker picks up      ▼
///                 ▼                 ┌───────────┐
///            ┌──────────┐           │ Cancelled │
///            │  Running │─── cancel ▶│           │
///            └────┬─────┘           └───────────┘
///                 │
///        ┌────────┼────────┐
///        │                 │
///        ▼                 ▼
///  ┌──────────┐      ┌──────────┐
///  │ Complete │      │  Failed  │
///  └──────────┘      └──────────┘
/// ```
///
/// Complete / Failed / Cancelled are terminal. The registry keeps
/// terminal jobs visible for a tunable retention so operators and
/// the HTTP list endpoint (Phase 5d) can see recent history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackfillStatus {
    /// Accepted by the registry, waiting for a worker. Registry's
    /// default state right after `create`.
    Queued,
    /// A worker has picked the job up and is replaying samples. The
    /// `windows_done` field ticks during this state.
    Running,
    /// All windows in `[time_range.0, time_range.1)` have been
    /// rebuilt and written. Terminal.
    Complete,
    /// A permanent error stopped the rebuild (exact-DB read error,
    /// sketch builder mismatch, write rejected by §6.3 schema
    /// barrier, etc.). Terminal. The human-readable reason is in
    /// [`BackfillJob::error_message`].
    Failed,
    /// The operator (or the admission-control loop) aborted the
    /// job. Terminal. Any windows already written are kept — the
    /// job merely stops rebuilding further ones.
    Cancelled,
}

impl BackfillStatus {
    /// Whether the status is terminal. Terminal jobs cannot be
    /// re-transitioned; `start`, `mark_complete`, `mark_failed`, and
    /// `cancel` are no-ops once reached.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            BackfillStatus::Complete | BackfillStatus::Failed | BackfillStatus::Cancelled
        )
    }
}

/// A single REFRESH MATERIALIZED VIEW unit: rebuild `agg_id` over
/// `time_range` from the sample stream described by `source`. See
/// §10.2 of the design.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackfillJob {
    /// Monotonically-increasing id assigned by the registry on
    /// `create`. Unique per-process.
    pub job_id: u64,
    /// Target aggregation. The registry does not itself verify that
    /// the agg_id is `Active` in any sid lifecycle table — Phase 5c's
    /// worker consults the sid-level write barrier before writing.
    pub agg_id: u64,
    /// Inclusive-exclusive `[start_ms, end_ms)` window to rebuild.
    pub time_range: (u64, u64),
    pub source: BackfillSource,
    pub status: BackfillStatus,
    /// Worker-set counter that ticks as windows are rebuilt. Lets
    /// `coverage()` report a `BackfillInProgress { pct }` without
    /// polling the source. `0` while the job is `Queued` or the
    /// worker hasn't emitted its first window yet.
    pub windows_done: u64,
    /// Planner-estimated total number of windows this job will
    /// rebuild. Set at `create` time from `(time_range, window_size)`.
    /// Exposed so the progress percentage is a simple ratio.
    pub windows_total: u64,
    /// Wall-clock millis at `create` time.
    pub created_at_ms: u64,
    /// Wall-clock millis when the status first transitioned to
    /// `Running`. `None` while `Queued`.
    pub started_at_ms: Option<u64>,
    /// Wall-clock millis at the terminal transition. `None` while
    /// non-terminal.
    pub completed_at_ms: Option<u64>,
    /// Human-readable reason present only for `Failed` jobs.
    pub error_message: Option<String>,
}

impl BackfillJob {
    /// Progress as a ratio in `[0, 1]`. Returns `0.0` for jobs where
    /// `windows_total == 0` (empty range — rarely useful, but a job
    /// can be created with a tight `[t, t)` range and will be
    /// trivially complete).
    pub fn progress(&self) -> f64 {
        if self.windows_total == 0 {
            return match self.status {
                BackfillStatus::Complete => 1.0,
                _ => 0.0,
            };
        }
        (self.windows_done as f64 / self.windows_total as f64).clamp(0.0, 1.0)
    }
}

/// §10.4 coverage classification for a `(agg_id, range)` pair. Used
/// by the query engine in Phase 5f to decide whether to read from
/// the sketch (Complete), wait briefly / fall back to the exact DB
/// (BackfillInProgress), or fall back immediately (Missing).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Coverage {
    /// Every window in the requested range has been written by
    /// either native ingest or a completed backfill. Query proceeds
    /// against the sketch.
    Complete,
    /// A backfill job is currently rebuilding this range. The `pct`
    /// is the job's `progress()` so the caller can decide whether to
    /// wait (e.g. if >90%) or fall back.
    BackfillInProgress { job_id: u64, pct: f64 },
    /// Neither native nor backfill covers this range. Caller must
    /// fall back to the exact DB.
    Missing,
}

/// Thread-safe registry of backfill jobs, all in memory.
///
/// ## Guarantees
///
/// * `job_id` allocation is monotonically increasing and unique per
///   registry instance. `AtomicU64::fetch_add` guarantees no two
///   concurrent `create` calls observe the same id.
/// * Read paths (`get`, `list`, `by_status`) take only a `read()`
///   lock on the inner map; writes take `write()`. A single `RwLock`
///   fits the workload — backfill creations are human / control plane
///   triggered, much rarer than reads.
/// * All state transitions go through `update_status` (or the sugar
///   methods `start` / `mark_complete` / `mark_failed` / `cancel`)
///   which enforce terminal-state immutability: once a job is
///   Complete / Failed / Cancelled, further transitions are
///   rejected with `false` return.
///
/// ## Persistence (Phase 5g)
///
/// Opt-in via [`Self::with_persistence`] or the
/// [`Self::load_or_new`] constructor. When set, the registry
/// atomically rewrites the snapshot to `persist_path` (write tmp +
/// rename) after every status transition — `create`, `start`,
/// `mark_complete`, `mark_failed`, `cancel`, `evict_old_terminal`.
/// `tick_progress` is deliberately **not** persisted: workers can
/// tick many times per second, and losing the last few ticks
/// across a restart is harmless (Phase 5e's worker will re-derive
/// `windows_done` from whatever the store already has when it
/// picks a Running job back up).
///
/// I/O errors on save are logged and dropped — the registry is
/// always authoritative in memory. Corrupt files on load are
/// logged and the registry falls back to empty, matching Phase
/// 2c's semantics.
pub struct BackfillRegistry {
    jobs: RwLock<HashMap<u64, BackfillJob>>,
    next_job_id: AtomicU64,
    /// Optional on-disk snapshot path. Set via `with_persistence`.
    persist_path: Option<PathBuf>,
    /// Per-`job_id` list of [`WrittenWindow`] entries that
    /// the corresponding backfill actually wrote to the store.
    /// Phase 5e populates this via `record_window_written`;
    /// Phase 5f's coverage tracker reads it to distinguish
    /// `Backfilled` from `Missing` coverage for a given range.
    ///
    /// Kept separately from `jobs` so writes don't have to pay the
    /// cost of cloning the whole job on every window — only the
    /// provenance list grows.
    ///
    /// Not persisted to disk in Phase 5e. Phase 5g persistence
    /// covered job lifecycle but not written-windows; if restart
    /// recovery of the provenance list becomes necessary, it'll be
    /// a Phase 5g-2 extension.
    written_windows: RwLock<HashMap<u64, Vec<WrittenWindow>>>,
}

/// A single `(agg_id, window_range)` record of a window written by
/// a backfill job. Phase 5f's coverage tracker reads these lists
/// to distinguish `Backfilled { job_id }` coverage from `Missing`.
pub type WrittenWindow = (u64, (u64, u64));

/// On-disk format version for the persisted backfill registry.
/// Bumped on any incompatible change to [`BackfillJob`] or
/// [`PersistedSnapshot`]; version mismatch on load is treated as
/// a corrupt file (registry starts empty, next save rewrites the
/// current version).
pub const PERSIST_FORMAT_VERSION: u32 = 1;

/// Errors returned by [`BackfillRegistry::create_checked`]. Exists
/// so the control-plane-facing HTTP endpoint can render distinct
/// 400 vs 404 vs 409 depending on which invariant was violated,
/// rather than swallowing the detail in a string.
#[derive(Debug, PartialEq, Eq)]
pub enum CreateError {
    /// The `agg_id` isn't known to the schema registry. Caller
    /// probably has a stale config or a typo.
    UnknownAgg { agg_id: u64 },
    /// The requested `end_ms` extends past the agg's
    /// `created_at_ms`, which would put the backfill in conflict
    /// with live ingest. §10.5 time-disjoint invariant.
    Overlap {
        agg_id: u64,
        requested_end_ms: u64,
        created_at_ms: u64,
    },
    /// The requested `start_ms` is older than the `SketchStore`
    /// data-retention horizon — any windows the backfill writes
    /// at that range would immediately be evicted by the
    /// retention sweep. Method B from the design discussion: fail
    /// fast at job creation rather than silently letting the
    /// backfill produce windows that get wiped.
    ///
    /// `earliest_retained_ms` is `now - persistence_delete_older_than_ms`,
    /// i.e. the smallest timestamp that would still survive
    /// retention at creation time.
    OutOfRetention {
        agg_id: u64,
        requested_start_ms: u64,
        earliest_retained_ms: u64,
    },
}

impl std::fmt::Display for CreateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownAgg { agg_id } => {
                write!(f, "unknown agg_id {agg_id} (not in schema registry)")
            }
            Self::Overlap {
                agg_id,
                requested_end_ms,
                created_at_ms,
            } => write!(
                f,
                "backfill end_ms {requested_end_ms} > agg {agg_id} created_at_ms {created_at_ms}; \
                 live ingest already owns [{created_at_ms}, ∞), refuse to race"
            ),
            Self::OutOfRetention {
                agg_id,
                requested_start_ms,
                earliest_retained_ms,
            } => write!(
                f,
                "backfill start_ms {requested_start_ms} for agg {agg_id} is older than the store's \
                 earliest_retained_ms {earliest_retained_ms}; any written windows would be evicted \
                 by retention — extend persistence_delete_older_than before creating this job"
            ),
        }
    }
}

impl std::error::Error for CreateError {}

/// Top-level structure written by `persist_path`. Captures the
/// current `next_job_id` alongside the jobs so a restart doesn't
/// accidentally reuse a previously-allocated id.
#[derive(Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    version: u32,
    next_job_id: u64,
    jobs: Vec<BackfillJob>,
}

impl Default for BackfillRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl BackfillRegistry {
    pub fn new() -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            next_job_id: AtomicU64::new(1),
            persist_path: None,
            written_windows: RwLock::new(HashMap::new()),
        }
    }

    /// Enable on-disk persistence at `path`. Every status transition
    /// from here on atomically rewrites the snapshot. Prefer
    /// [`Self::load_or_new`] over this builder if the file already
    /// exists and you want to recover its contents.
    pub fn with_persistence(mut self, path: impl Into<PathBuf>) -> Self {
        self.persist_path = Some(path.into());
        self
    }

    /// Build a registry that recovers prior job records from `path`
    /// (if it exists) and then enables persistence at the same path.
    /// The right way to construct a registry in production code.
    ///
    /// Load semantics:
    /// * File does not exist → fresh empty registry; first transition
    ///   writes the snapshot so the next restart has something to
    ///   read.
    /// * File exists and parses → load all jobs verbatim, restore
    ///   `next_job_id` so IDs don't collide with persisted ones.
    /// * File exists but is corrupt / wrong version → log warning,
    ///   fall back to empty registry. Forward progress over
    ///   historical accuracy, matching Phase 2c's schema-registry
    ///   behaviour.
    pub fn load_or_new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        let mut registry = match Self::load_from_disk(&path) {
            Ok(Some(r)) => {
                debug!(
                    "Loaded {} persisted backfill job(s) from {}",
                    r.jobs.read().map(|m| m.len()).unwrap_or(0),
                    path.display()
                );
                r
            }
            Ok(None) => Self::new(),
            Err(e) => {
                warn!(
                    "Failed to load backfill registry from {}: {e}. Starting fresh.",
                    path.display()
                );
                Self::new()
            }
        };
        registry.persist_path = Some(path);
        // Write an initial snapshot so a first-run file exists even
        // before any job transitions happen.
        registry.save_to_disk_if_persistent();
        registry
    }

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
                    "unsupported backfill persist format version {} (expected {})",
                    snap.version, PERSIST_FORMAT_VERSION
                ),
            ));
        }
        let mut map = HashMap::with_capacity(snap.jobs.len());
        for job in snap.jobs {
            map.insert(job.job_id, job);
        }
        Ok(Some(Self {
            jobs: RwLock::new(map),
            next_job_id: AtomicU64::new(snap.next_job_id.max(1)),
            persist_path: None,
            written_windows: RwLock::new(HashMap::new()),
        }))
    }

    fn save_to_disk_if_persistent(&self) {
        let Some(path) = self.persist_path.as_ref() else {
            return;
        };
        let jobs: Vec<BackfillJob> = match self.jobs.read() {
            Ok(m) => m.values().cloned().collect(),
            Err(e) => {
                warn!("Backfill registry lock poisoned; skipping persist: {e}");
                return;
            }
        };
        let snap = PersistedSnapshot {
            version: PERSIST_FORMAT_VERSION,
            next_job_id: self.next_job_id.load(Ordering::Relaxed),
            jobs,
        };
        let bytes = match serde_json::to_vec_pretty(&snap) {
            Ok(b) => b,
            Err(e) => {
                warn!("Failed to serialise backfill registry: {e}");
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
                "Failed to write backfill registry tmp file {}: {e}",
                tmp.display()
            );
            return;
        }
        if let Err(e) = std::fs::rename(&tmp, path) {
            warn!(
                "Failed to rename backfill registry {} → {}: {e}",
                tmp.display(),
                path.display()
            );
        }
    }

    /// Allocate a fresh `job_id`, stamp `created_at_ms`, and insert
    /// the job as `Queued`. Returns the allocated id.
    pub fn create(
        &self,
        agg_id: u64,
        time_range: (u64, u64),
        source: BackfillSource,
        windows_total: u64,
    ) -> u64 {
        let job_id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let job = BackfillJob {
            job_id,
            agg_id,
            time_range,
            source,
            status: BackfillStatus::Queued,
            windows_done: 0,
            windows_total,
            created_at_ms: now_ms(),
            started_at_ms: None,
            completed_at_ms: None,
            error_message: None,
        };
        if let Ok(mut map) = self.jobs.write() {
            map.insert(job_id, job);
        }
        self.save_to_disk_if_persistent();
        job_id
    }

    /// Create a job with all §10.5 invariants enforced:
    ///
    /// * **Time-disjoint**: `time_range.1 <= created_at_ms`
    ///   so backfill writes don't race live writes on the same
    ///   `(agg_id, window)` pair. The caller passes the agg's
    ///   `created_at_ms` directly — in the post-schema-retirement
    ///   world there is no `SchemaRegistry::get(agg_id)` to look
    ///   it up from, and the caller (typically the HTTP handler
    ///   or control plane) already has the wall-clock snapshot in
    ///   scope from its `StreamingConfig` reconcile event.
    /// * **Within data retention** (if `data_retention_ms` is
    ///   provided): `time_range.0 >= now - data_retention_ms`.
    ///   Method B from the design discussion — fail fast instead
    ///   of letting the backfill produce windows that the
    ///   SketchStore retention sweep would immediately evict.
    ///   Pass `None` to skip the check (tests, or deployments
    ///   where retention is disabled).
    ///
    /// The `agg_id` is taken from `config.aggregation_id`; the
    /// caller no longer threads it separately.
    ///
    /// Errors map to distinct [`CreateError`] variants so the
    /// control-plane-facing HTTP endpoint can return specific 404 /
    /// 409 / 400 statuses. `CreateError::UnknownAgg` is no longer
    /// returned from this method — the caller proves the agg
    /// exists by holding the `AggregationConfig` — but the variant
    /// is kept on the enum for HTTP error-mapping compatibility
    /// (the handler still produces it when its own lookup misses).
    pub fn create_checked(
        &self,
        config: &AggregationConfig,
        created_at_ms: u64,
        time_range: (u64, u64),
        source: BackfillSource,
        windows_total: u64,
        data_retention_ms: Option<u64>,
    ) -> Result<u64, CreateError> {
        let agg_id = config.policy_fp_u64();
        // Time-disjoint invariant: live ingest writes `[created_at, ∞)`
        // so backfill must stay strictly inside `[0, created_at)` or
        // touch the boundary exactly.
        if time_range.1 > created_at_ms {
            return Err(CreateError::Overlap {
                agg_id,
                requested_end_ms: time_range.1,
                created_at_ms,
            });
        }
        // Data-retention check (Method B): if the store would
        // immediately evict the windows this job would write,
        // reject up-front with a clear message rather than
        // silently wasting CPU + I/O.
        if let Some(retention_ms) = data_retention_ms {
            let earliest_retained_ms = now_ms().saturating_sub(retention_ms);
            if time_range.0 < earliest_retained_ms {
                return Err(CreateError::OutOfRetention {
                    agg_id,
                    requested_start_ms: time_range.0,
                    earliest_retained_ms,
                });
            }
        }
        Ok(self.create(agg_id, time_range, source, windows_total))
    }

    pub fn get(&self, job_id: u64) -> Option<BackfillJob> {
        self.jobs.read().ok()?.get(&job_id).cloned()
    }

    /// Iterate (clones) all jobs in the registry. Order is
    /// HashMap-iteration — non-deterministic. Callers that need a
    /// stable order should sort by `job_id` themselves.
    pub fn list(&self) -> Vec<BackfillJob> {
        self.jobs
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    /// All jobs in a given status. Same ordering caveat as `list`.
    pub fn list_by_status(&self, status: &BackfillStatus) -> Vec<BackfillJob> {
        self.jobs
            .read()
            .map(|m| {
                m.values()
                    .filter(|j| &j.status == status)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Transition `job_id` to `Running`, stamping `started_at_ms`.
    /// Returns `false` if the job doesn't exist or is already past
    /// `Queued` (terminal or already Running). The worker calls this
    /// right before replaying its first window.
    pub fn start(&self, job_id: u64) -> bool {
        let mut map = match self.jobs.write() {
            Ok(m) => m,
            Err(_) => return false,
        };
        let Some(job) = map.get_mut(&job_id) else {
            return false;
        };
        if !matches!(job.status, BackfillStatus::Queued) {
            return false;
        }
        job.status = BackfillStatus::Running;
        job.started_at_ms = Some(now_ms());
        drop(map);
        self.save_to_disk_if_persistent();
        true
    }

    /// Bump `windows_done` by 1. No-op if the job doesn't exist or
    /// is not `Running`. Does NOT cap at `windows_total` — a worker
    /// that overestimates has already shipped, and clamping would
    /// hide the bug.
    pub fn tick_progress(&self, job_id: u64) -> bool {
        let mut map = match self.jobs.write() {
            Ok(m) => m,
            Err(_) => return false,
        };
        let Some(job) = map.get_mut(&job_id) else {
            return false;
        };
        if !matches!(job.status, BackfillStatus::Running) {
            return false;
        }
        job.windows_done = job.windows_done.saturating_add(1);
        true
    }

    pub fn mark_complete(&self, job_id: u64) -> bool {
        self.finish(job_id, BackfillStatus::Complete, None)
    }

    pub fn mark_failed(&self, job_id: u64, error: impl Into<String>) -> bool {
        self.finish(job_id, BackfillStatus::Failed, Some(error.into()))
    }

    /// Cancel the job. Works from `Queued` OR `Running` (see the
    /// state diagram on [`BackfillStatus`]); rejects terminal
    /// states.
    pub fn cancel(&self, job_id: u64) -> bool {
        let mut map = match self.jobs.write() {
            Ok(m) => m,
            Err(_) => return false,
        };
        let Some(job) = map.get_mut(&job_id) else {
            return false;
        };
        if job.status.is_terminal() {
            return false;
        }
        job.status = BackfillStatus::Cancelled;
        job.completed_at_ms = Some(now_ms());
        drop(map);
        self.save_to_disk_if_persistent();
        true
    }

    fn finish(&self, job_id: u64, new_status: BackfillStatus, error: Option<String>) -> bool {
        let mut map = match self.jobs.write() {
            Ok(m) => m,
            Err(_) => return false,
        };
        let Some(job) = map.get_mut(&job_id) else {
            return false;
        };
        if job.status.is_terminal() {
            return false;
        }
        job.status = new_status;
        job.completed_at_ms = Some(now_ms());
        if error.is_some() {
            job.error_message = error;
        }
        drop(map);
        self.save_to_disk_if_persistent();
        true
    }

    /// Record that job `job_id` wrote a backfilled window at
    /// `(agg_id, window_range)`. Called by Phase 5e's
    /// `BackfillWindowProcessor` after a successful per-window
    /// write to the store — this is how the registry knows which
    /// `(agg_id, range)` pairs have been backfilled, which Phase
    /// 5f's coverage tracker uses to distinguish `Backfilled` from
    /// `Missing` coverage.
    ///
    /// Idempotent: recording the same `(job_id, agg_id, range)`
    /// twice appends duplicate entries. Callers shouldn't do that,
    /// but the registry doesn't police it — de-duplication is a
    /// Phase 5f concern.
    pub fn record_window_written(&self, job_id: u64, agg_id: u64, window_range: (u64, u64)) {
        if let Ok(mut map) = self.written_windows.write() {
            map.entry(job_id).or_default().push((agg_id, window_range));
        }
    }

    /// All `(agg_id, window_range)` pairs written by `job_id`.
    /// Empty (or missing) list means either the job hasn't started
    /// writing yet or was cancelled before any window completed.
    pub fn windows_written_by(&self, job_id: u64) -> Vec<WrittenWindow> {
        self.written_windows
            .read()
            .ok()
            .and_then(|m| m.get(&job_id).cloned())
            .unwrap_or_default()
    }

    /// §10.4 coverage classification for `(agg_id, range)`.
    ///
    /// Walks the registry's jobs for `agg_id` and classifies
    /// `range` (half-open `[start_ms, end_ms)`) as:
    ///
    /// * [`Coverage::Complete`] — every ms in the range is covered
    ///   by at least one `Complete` job's written-window list.
    /// * [`Coverage::BackfillInProgress`] — at least one `Running`
    ///   job overlaps the range, and we can't prove Complete
    ///   coverage. Returns the job's current progress so the
    ///   caller can decide whether to wait or fall back.
    /// * [`Coverage::Missing`] — no Complete coverage, no Running
    ///   coverage. Caller should fall back to the exact DB per
    ///   §7.3.
    ///
    /// ## Scope
    ///
    /// This method looks ONLY at backfill state. It does **not**
    /// know about live ingest — callers that want the full
    /// `[Coverage of `range`] = backfill + live` picture should
    /// split the query range at the target agg's `created_at_ms`
    /// first (§10.5 time-disjoint) and ask this method only about
    /// the `[start, created_at)` historical portion. The live
    /// `[created_at, end)` portion is always `Complete` by
    /// definition of the live ingest path.
    ///
    /// ## Algorithm
    ///
    /// 1. Collect every window from every `Complete` job for
    ///    `agg_id` into a flat list.
    /// 2. Sort + merge overlapping intervals into a coverage set.
    /// 3. Check if `range` is fully inside the coverage set —
    ///    if so, return `Complete`.
    /// 4. Otherwise, check `Running` jobs: if one exists whose
    ///    `time_range` overlaps `range`, return
    ///    `BackfillInProgress { job_id, pct }` with that job's
    ///    current progress.
    /// 5. Otherwise return `Missing`.
    ///
    /// Cost: linear in the total windows written by jobs for
    /// `agg_id`. For a 24h backfill with 1-minute windows that's
    /// 1440 windows — a trivial walk. If a single agg accumulates
    /// thousands of jobs with millions of windows each, this will
    /// want caching — deferred until the profile says so.
    pub fn coverage(&self, agg_id: u64, range: (u64, u64)) -> Coverage {
        if range.0 >= range.1 {
            // Empty / inverted range: treat as trivially complete.
            // Saves the caller an `is_empty` branch at every site.
            return Coverage::Complete;
        }

        // Snapshot jobs + written windows. Clone both so we don't
        // hold locks across the interval-merge logic.
        let jobs: Vec<BackfillJob> = self
            .jobs
            .read()
            .map(|m| m.values().filter(|j| j.agg_id == agg_id).cloned().collect())
            .unwrap_or_default();
        let written: HashMap<u64, Vec<WrittenWindow>> = self
            .written_windows
            .read()
            .map(|m| m.clone())
            .unwrap_or_default();

        // Step 1+2: collect and merge Complete-job windows.
        let mut complete_ranges: Vec<(u64, u64)> = Vec::new();
        for job in &jobs {
            if !matches!(job.status, BackfillStatus::Complete) {
                continue;
            }
            if let Some(windows) = written.get(&job.job_id) {
                for &(wagg, wrange) in windows {
                    if wagg == agg_id {
                        complete_ranges.push(wrange);
                    }
                }
            }
        }
        if range_covered_by(range, &complete_ranges) {
            return Coverage::Complete;
        }

        // Step 4: any Running job overlapping range?
        for job in &jobs {
            if !matches!(job.status, BackfillStatus::Running) {
                continue;
            }
            let (s, e) = job.time_range;
            let overlaps = s < range.1 && e > range.0;
            if overlaps {
                return Coverage::BackfillInProgress {
                    job_id: job.job_id,
                    pct: job.progress(),
                };
            }
        }

        Coverage::Missing
    }

    /// Remove any terminal job older than `older_than_ms`. Used by
    /// the eventual retention sweep; returns the number of jobs
    /// evicted. Non-terminal jobs are never evicted.
    pub fn evict_old_terminal(&self, older_than_ms: u64) -> usize {
        let cutoff = now_ms().saturating_sub(older_than_ms);
        let mut map = match self.jobs.write() {
            Ok(m) => m,
            Err(_) => return 0,
        };
        let before = map.len();
        // Use `<=` so `older_than_ms = 0` evicts everything terminal
        // *now* (intuitive meaning of "anything older than 0ms ago"),
        // including jobs that completed in the same millisecond as
        // this call — which is the common case in tests and in
        // control-plane-driven eviction loops where both timestamps
        // come from the same wall clock.
        map.retain(|_, job| {
            !(job.status.is_terminal() && job.completed_at_ms.map(|t| t <= cutoff).unwrap_or(false))
        });
        let evicted = before - map.len();
        drop(map);
        if evicted > 0 {
            self.save_to_disk_if_persistent();
        }
        evicted
    }
}

/// Merge `ranges` into a sorted non-overlapping list and check
/// whether the half-open `[target.0, target.1)` is fully covered.
/// Does not mutate `ranges`. Empty `ranges` → `false` for any
/// non-empty `target`. Exposed at module scope so the unit tests
/// can exercise the interval math independently of the registry.
fn range_covered_by(target: (u64, u64), ranges: &[(u64, u64)]) -> bool {
    if target.0 >= target.1 {
        return true;
    }
    let mut sorted: Vec<(u64, u64)> = ranges.iter().filter(|(a, b)| a < b).copied().collect();
    if sorted.is_empty() {
        return false;
    }
    sorted.sort_unstable();
    // Walk left-to-right, merging overlapping/adjacent spans and
    // checking that the merged span covers `target` monotonically.
    let mut cursor = target.0;
    for (start, end) in sorted {
        if start > cursor {
            // Gap between where we'd need coverage and the next
            // span's start — short-circuit.
            return false;
        }
        if end > cursor {
            cursor = end;
        }
        if cursor >= target.1 {
            return true;
        }
    }
    cursor >= target.1
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// 2026-05 reorg: backfill-* and the two BackfillSource impls moved
// into this folder as submodules.
pub mod clickhouse_reader;
pub mod processor;
pub mod prometheus_reader;
pub mod raw_sample_reader;
pub mod service;
pub mod window_builder;
pub mod worker;

pub use clickhouse_reader::{clickhouse_reader_factory, ClickHouseReader, ClickHouseReaderConfig};
pub use processor::BackfillWindowProcessor;
pub use prometheus_reader::PrometheusReader;
pub use raw_sample_reader::{
    LabelFilter, MockRawSampleReader, RawSample, RawSampleReader, RawSampleReaderError,
};
pub use service::{
    default_reader_factory, noop_reader_factory, BackfillService, BackfillServiceConfig,
    BackfillServiceHandle, ReaderFactory,
};
pub use window_builder::build_backfilled_accumulator;
pub use worker::{BackfillWorker, BackfillWorkerError, WindowProcessor};

#[cfg(test)]
mod tests {
    use super::*;

    fn prom_source() -> BackfillSource {
        BackfillSource::Prometheus {
            url: "http://prom.local:9090".to_string(),
        }
    }

    #[test]
    fn create_allocates_monotonic_ids_and_defaults_to_queued() {
        let r = BackfillRegistry::new();
        let a = r.create(42, (100, 200), prom_source(), 10);
        let b = r.create(42, (200, 300), prom_source(), 10);
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        let job_a = r.get(a).unwrap();
        assert_eq!(job_a.status, BackfillStatus::Queued);
        assert!(job_a.started_at_ms.is_none());
        assert!(job_a.completed_at_ms.is_none());
        assert_eq!(job_a.windows_done, 0);
        assert_eq!(job_a.windows_total, 10);
    }

    #[test]
    fn get_returns_none_for_unknown_job() {
        let r = BackfillRegistry::new();
        assert!(r.get(9999).is_none());
    }

    #[test]
    fn start_transitions_queued_to_running_and_stamps_started_at() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 5);
        assert!(r.start(id));
        let job = r.get(id).unwrap();
        assert_eq!(job.status, BackfillStatus::Running);
        assert!(job.started_at_ms.is_some());
    }

    #[test]
    fn start_is_idempotent_against_already_running_or_terminal() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 5);
        assert!(r.start(id));
        // Second start is a no-op — returns false, state unchanged.
        assert!(!r.start(id));
        assert!(r.mark_complete(id));
        assert!(!r.start(id));
    }

    #[test]
    fn tick_progress_only_during_running() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 3);
        // Queued: ticks rejected.
        assert!(!r.tick_progress(id));
        assert_eq!(r.get(id).unwrap().windows_done, 0);

        r.start(id);
        assert!(r.tick_progress(id));
        assert!(r.tick_progress(id));
        assert_eq!(r.get(id).unwrap().windows_done, 2);

        r.mark_complete(id);
        assert!(!r.tick_progress(id));
        assert_eq!(r.get(id).unwrap().windows_done, 2);
    }

    #[test]
    fn mark_complete_closes_job_and_stamps_completed_at() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 1);
        r.start(id);
        assert!(r.mark_complete(id));
        let job = r.get(id).unwrap();
        assert_eq!(job.status, BackfillStatus::Complete);
        assert!(job.completed_at_ms.is_some());
        assert!(job.error_message.is_none());
    }

    #[test]
    fn mark_failed_stores_error_message() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 1);
        r.start(id);
        assert!(r.mark_failed(id, "exact DB unreachable"));
        let job = r.get(id).unwrap();
        assert_eq!(job.status, BackfillStatus::Failed);
        assert_eq!(job.error_message.as_deref(), Some("exact DB unreachable"));
        assert!(job.completed_at_ms.is_some());
    }

    #[test]
    fn terminal_states_reject_further_transitions() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 1);
        r.start(id);
        r.mark_complete(id);
        assert!(!r.mark_failed(id, "ignored"));
        assert!(!r.mark_complete(id));
        assert!(!r.cancel(id));
        let job = r.get(id).unwrap();
        assert_eq!(job.status, BackfillStatus::Complete);
        assert!(job.error_message.is_none());
    }

    #[test]
    fn cancel_works_from_queued_and_running() {
        let r = BackfillRegistry::new();
        let queued = r.create(1, (0, 100), prom_source(), 1);
        assert!(r.cancel(queued));
        assert_eq!(r.get(queued).unwrap().status, BackfillStatus::Cancelled);

        let running = r.create(2, (0, 100), prom_source(), 1);
        r.start(running);
        assert!(r.cancel(running));
        assert_eq!(r.get(running).unwrap().status, BackfillStatus::Cancelled);
    }

    #[test]
    fn cancel_rejects_terminal_jobs() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 1);
        r.start(id);
        r.mark_complete(id);
        assert!(!r.cancel(id));
    }

    #[test]
    fn list_and_list_by_status_return_expected_sets() {
        let r = BackfillRegistry::new();
        let a = r.create(1, (0, 100), prom_source(), 1);
        let b = r.create(1, (100, 200), prom_source(), 1);
        let c = r.create(1, (200, 300), prom_source(), 1);
        r.start(b);
        r.start(c);
        r.mark_complete(c);

        assert_eq!(r.list().len(), 3);

        let mut queued = r
            .list_by_status(&BackfillStatus::Queued)
            .into_iter()
            .map(|j| j.job_id)
            .collect::<Vec<_>>();
        queued.sort();
        assert_eq!(queued, vec![a]);

        let running = r.list_by_status(&BackfillStatus::Running);
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].job_id, b);

        let done = r.list_by_status(&BackfillStatus::Complete);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].job_id, c);
    }

    #[test]
    fn progress_is_zero_when_total_is_zero_and_not_complete() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 0), prom_source(), 0);
        let job = r.get(id).unwrap();
        assert_eq!(job.progress(), 0.0);
    }

    #[test]
    fn progress_is_one_when_total_is_zero_and_complete() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 0), prom_source(), 0);
        r.start(id);
        r.mark_complete(id);
        let job = r.get(id).unwrap();
        assert_eq!(job.progress(), 1.0);
    }

    #[test]
    fn progress_clamped_between_zero_and_one() {
        let r = BackfillRegistry::new();
        let id = r.create(1, (0, 100), prom_source(), 10);
        r.start(id);
        for _ in 0..25 {
            r.tick_progress(id);
        }
        // windows_done is 25 > windows_total 10; progress clamps to 1.0.
        assert_eq!(r.get(id).unwrap().progress(), 1.0);
    }

    #[test]
    fn unknown_job_transitions_return_false() {
        let r = BackfillRegistry::new();
        assert!(!r.start(42));
        assert!(!r.tick_progress(42));
        assert!(!r.mark_complete(42));
        assert!(!r.mark_failed(42, "nope"));
        assert!(!r.cancel(42));
    }

    #[test]
    fn evict_old_terminal_removes_only_terminal_past_cutoff() {
        let r = BackfillRegistry::new();
        let live = r.create(1, (0, 100), prom_source(), 1);
        let done = r.create(1, (100, 200), prom_source(), 1);
        r.start(done);
        r.mark_complete(done);

        // Cutoff in the future (max u64 interpretation): everything
        // terminal is "older than" so should be evicted; live job stays.
        let evicted = r.evict_old_terminal(0);
        assert_eq!(evicted, 1);
        assert!(r.get(live).is_some());
        assert!(r.get(done).is_none());
    }

    #[test]
    fn backfill_source_roundtrips_through_serde() {
        for src in [
            BackfillSource::ClickHouse {
                database: "telemetry".into(),
                table: "samples".into(),
            },
            BackfillSource::Prometheus {
                url: "u".to_string(),
            },
            BackfillSource::S3Gorilla {
                bucket: "b".to_string(),
                prefix: "p".to_string(),
            },
            BackfillSource::OtherSketch { source_agg_id: 17 },
        ] {
            let json = serde_json::to_string(&src).unwrap();
            let back: BackfillSource = serde_json::from_str(&json).unwrap();
            assert_eq!(src, back);
        }
    }

    #[test]
    fn status_is_terminal_exhaustive_check() {
        assert!(!BackfillStatus::Queued.is_terminal());
        assert!(!BackfillStatus::Running.is_terminal());
        assert!(BackfillStatus::Complete.is_terminal());
        assert!(BackfillStatus::Failed.is_terminal());
        assert!(BackfillStatus::Cancelled.is_terminal());
    }

    // ─── Phase 5g: persistence tests ───────────────────────────────

    #[test]
    fn persistence_roundtrip_preserves_job_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");

        let r = BackfillRegistry::load_or_new(&path);
        let id = r.create(42, (100, 200), prom_source(), 4);
        r.start(id);
        r.mark_complete(id);

        let before = r.get(id).unwrap();
        drop(r);

        // Simulate a restart.
        let reloaded = BackfillRegistry::load_or_new(&path);
        let after = reloaded.get(id).expect("job survived restart");
        assert_eq!(after.status, BackfillStatus::Complete);
        assert_eq!(after.agg_id, 42);
        assert_eq!(after.time_range, (100, 200));
        assert_eq!(after.windows_total, 4);
        assert_eq!(after.started_at_ms, before.started_at_ms);
        assert_eq!(after.completed_at_ms, before.completed_at_ms);
    }

    #[test]
    fn persistence_preserves_next_job_id_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");

        let r = BackfillRegistry::load_or_new(&path);
        let first = r.create(1, (0, 10), prom_source(), 1);
        let second = r.create(1, (10, 20), prom_source(), 1);
        assert_eq!(first, 1);
        assert_eq!(second, 2);
        drop(r);

        // Restart: next id should be 3, not 1, so we don't collide
        // with the persisted job_id=2.
        let reloaded = BackfillRegistry::load_or_new(&path);
        let third = reloaded.create(1, (20, 30), prom_source(), 1);
        assert_eq!(third, 3);
    }

    #[test]
    fn persistence_writes_file_on_create_even_when_empty_before() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");
        assert!(!path.exists());

        let r = BackfillRegistry::load_or_new(&path);
        // load_or_new writes an initial empty snapshot so the file
        // always exists after construction.
        assert!(path.exists());
        let _ = r.create(1, (0, 10), prom_source(), 1);
        let bytes = std::fs::read(&path).unwrap();
        let snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snap["version"], 1);
        assert_eq!(snap["jobs"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn persistence_survives_cancel_and_fail_transitions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");

        let r = BackfillRegistry::load_or_new(&path);
        let cancelled = r.create(1, (0, 10), prom_source(), 1);
        r.cancel(cancelled);
        let failed = r.create(1, (10, 20), prom_source(), 1);
        r.start(failed);
        r.mark_failed(failed, "boom");
        drop(r);

        let reloaded = BackfillRegistry::load_or_new(&path);
        assert_eq!(
            reloaded.get(cancelled).unwrap().status,
            BackfillStatus::Cancelled
        );
        let j = reloaded.get(failed).unwrap();
        assert_eq!(j.status, BackfillStatus::Failed);
        assert_eq!(j.error_message.as_deref(), Some("boom"));
    }

    #[test]
    fn persistence_evict_rewrites_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");

        let r = BackfillRegistry::load_or_new(&path);
        let done = r.create(1, (0, 10), prom_source(), 1);
        r.start(done);
        r.mark_complete(done);
        let live = r.create(1, (10, 20), prom_source(), 1);
        // Evict everything terminal right now.
        let evicted = r.evict_old_terminal(0);
        assert_eq!(evicted, 1);

        let reloaded = BackfillRegistry::load_or_new(&path);
        assert!(reloaded.get(done).is_none());
        assert!(reloaded.get(live).is_some());
    }

    #[test]
    fn corrupt_persist_file_falls_back_to_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");
        std::fs::write(&path, b"this is not json").unwrap();

        let r = BackfillRegistry::load_or_new(&path);
        assert!(r.list().is_empty());
        // Next create should succeed and write a fresh valid snapshot.
        let id = r.create(1, (0, 10), prom_source(), 1);
        let bytes = std::fs::read(&path).unwrap();
        let snap: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(snap["version"], 1);
        assert_eq!(snap["jobs"][0]["job_id"], id);
    }

    #[test]
    fn unsupported_persist_version_is_rejected_and_rewritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backfill.json");
        let snap = serde_json::json!({"version": 999, "next_job_id": 1, "jobs": []});
        std::fs::write(&path, serde_json::to_vec(&snap).unwrap()).unwrap();

        let r = BackfillRegistry::load_or_new(&path);
        let _ = r.create(1, (0, 10), prom_source(), 1);
        // File has been rewritten with the current version.
        let bytes = std::fs::read(&path).unwrap();
        let got: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got["version"], 1);
    }

    #[test]
    fn non_persistent_registry_writes_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("should_not_exist.json");
        let r = BackfillRegistry::new();
        let _ = r.create(1, (0, 10), prom_source(), 1);
        assert!(!path.exists());
    }

    // ─── Phase 5f: coverage() tests ─────────────────────────────

    #[test]
    fn range_covered_by_empty_ranges_is_false() {
        assert!(!range_covered_by((0, 100), &[]));
    }

    #[test]
    fn range_covered_by_single_spanning_range() {
        assert!(range_covered_by((10, 50), &[(0, 100)]));
    }

    #[test]
    fn range_covered_by_merges_adjacent_and_overlapping() {
        assert!(range_covered_by((0, 30), &[(0, 10), (10, 20), (15, 30)]));
        assert!(range_covered_by((5, 25), &[(0, 10), (10, 20), (20, 30)]));
    }

    #[test]
    fn range_covered_by_gap_is_false() {
        assert!(!range_covered_by((0, 30), &[(0, 10), (20, 30)]));
    }

    #[test]
    fn range_covered_by_empty_target_is_trivially_true() {
        assert!(range_covered_by((10, 10), &[]));
    }

    #[test]
    fn coverage_missing_when_no_jobs() {
        let r = BackfillRegistry::new();
        assert_eq!(r.coverage(1, (0, 100)), Coverage::Missing);
    }

    #[test]
    fn coverage_missing_when_only_other_agg_has_jobs() {
        let r = BackfillRegistry::new();
        let job_id = r.create(2, (0, 100), prom_source(), 1);
        r.start(job_id);
        r.record_window_written(job_id, 2, (0, 100));
        r.mark_complete(job_id);
        assert_eq!(r.coverage(1, (0, 100)), Coverage::Missing);
    }

    #[test]
    fn coverage_complete_when_one_job_covers_full_range() {
        let r = BackfillRegistry::new();
        let job_id = r.create(1, (0, 100), prom_source(), 1);
        r.start(job_id);
        r.record_window_written(job_id, 1, (0, 100));
        r.mark_complete(job_id);
        assert_eq!(r.coverage(1, (0, 100)), Coverage::Complete);
    }

    #[test]
    fn coverage_complete_when_merged_windows_cover_range() {
        let r = BackfillRegistry::new();
        let job_id = r.create(1, (0, 100), prom_source(), 10);
        r.start(job_id);
        // Write 10 adjacent windows, each 10ms.
        for i in 0..10 {
            r.record_window_written(job_id, 1, (i * 10, (i + 1) * 10));
        }
        r.mark_complete(job_id);
        assert_eq!(r.coverage(1, (0, 100)), Coverage::Complete);
        // Subrange query is also Complete.
        assert_eq!(r.coverage(1, (25, 75)), Coverage::Complete);
    }

    #[test]
    fn coverage_partial_from_completed_job_is_not_complete() {
        let r = BackfillRegistry::new();
        let job_id = r.create(1, (0, 100), prom_source(), 1);
        r.start(job_id);
        // Only part of the range was written before the job somehow
        // got marked Complete (shouldn't happen, but defensive).
        r.record_window_written(job_id, 1, (0, 50));
        r.mark_complete(job_id);
        // Full range isn't covered → Missing (no Running job).
        assert_eq!(r.coverage(1, (0, 100)), Coverage::Missing);
        // But the covered sub-range IS complete.
        assert_eq!(r.coverage(1, (0, 50)), Coverage::Complete);
    }

    #[test]
    fn coverage_running_overlap_reports_in_progress_with_pct() {
        let r = BackfillRegistry::new();
        let job_id = r.create(1, (0, 100), prom_source(), 10);
        r.start(job_id);
        r.tick_progress(job_id);
        r.tick_progress(job_id);
        // 2 of 10 ticked → 20% progress.
        match r.coverage(1, (0, 100)) {
            Coverage::BackfillInProgress { job_id: id, pct } => {
                assert_eq!(id, job_id);
                assert!((pct - 0.2).abs() < 1e-9, "pct = {pct}");
            }
            other => panic!("expected BackfillInProgress, got {other:?}"),
        }
    }

    #[test]
    fn coverage_complete_takes_precedence_over_running() {
        // A Complete job covers the range fully; a later Running
        // job (say for a retry / adjacent range) exists too. The
        // coverage should still be Complete — Complete is the
        // best classification we can give.
        let r = BackfillRegistry::new();
        let complete = r.create(1, (0, 100), prom_source(), 1);
        r.start(complete);
        r.record_window_written(complete, 1, (0, 100));
        r.mark_complete(complete);

        let running = r.create(1, (50, 150), prom_source(), 5);
        r.start(running);

        // Query inside the Complete range → Complete wins.
        assert_eq!(r.coverage(1, (0, 100)), Coverage::Complete);
        // Query extending into the Running job's range → not fully
        // complete, Running overlaps → InProgress.
        match r.coverage(1, (50, 150)) {
            Coverage::BackfillInProgress { .. } => {}
            other => panic!("expected InProgress for (50, 150), got {other:?}"),
        }
    }

    #[test]
    fn coverage_cancelled_and_failed_jobs_do_not_contribute() {
        let r = BackfillRegistry::new();
        // Cancelled job: even though some windows were written
        // before cancellation, they are not counted as coverage
        // because the job didn't complete cleanly.
        let cancelled = r.create(1, (0, 50), prom_source(), 5);
        r.start(cancelled);
        r.record_window_written(cancelled, 1, (0, 50));
        r.cancel(cancelled);
        // Failed job: same treatment.
        let failed = r.create(1, (50, 100), prom_source(), 5);
        r.start(failed);
        r.record_window_written(failed, 1, (50, 100));
        r.mark_failed(failed, "simulated");

        assert_eq!(r.coverage(1, (0, 100)), Coverage::Missing);
    }

    #[test]
    fn coverage_empty_range_is_trivially_complete() {
        let r = BackfillRegistry::new();
        assert_eq!(r.coverage(1, (42, 42)), Coverage::Complete);
        assert_eq!(r.coverage(1, (50, 10)), Coverage::Complete);
    }
}
