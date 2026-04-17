//! `BackfillJob` lifecycle types + in-memory `BackfillRegistry`.
//!
//! Implements §10 (refreshable view maintenance / backfill path) of the
//! sketch DB design ([`design-sketch-db.md`](../../../../../docs/design-sketch-db.md)).
//!
//! ## Why this exists
//!
//! The sketch tier's §8 incremental maintenance can't fill data from
//! BEFORE an `agg_id` was created. When a reconfigure introduces a new
//! agg_id — say the operator widens a CMS from 256 to 2048, or swaps
//! in KLL200 on top of a metric that previously had only CMS — the new
//! agg has zero history. Queries spanning the reconfigure boundary
//! either see a data cliff (which Phase 3's schema timeline at least
//! surfaces honestly) or have to fall back to the exact DB.
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

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
    /// ClickHouse via native HTTP / SQL.
    ClickHouse { url: String, table: String },
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
    /// the agg_id is `Active` in the [`super::SchemaRegistry`] —
    /// Phase 5c's worker consults the schema barrier before writing.
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
///   fits the workload — backfill creations are human / controller
///   triggered, much rarer than reads.
/// * All state transitions go through `update_status` (or the sugar
///   methods `start` / `mark_complete` / `mark_failed` / `cancel`)
///   which enforce terminal-state immutability: once a job is
///   Complete / Failed / Cancelled, further transitions are
///   rejected with `false` return.
///
/// ## Phase 5a scope
///
/// Registry is purely in-memory. Phase 5g will add persistence
/// mirroring Phase 2c's pattern so restart doesn't lose
/// in-flight-job records. For now, a backend restart during an
/// active backfill means the worker pool loses the job — in Phase
/// 5c the worker will be written so losing an in-flight job at
/// restart is safe (nothing is half-written because writes are
/// per-window atomic).
pub struct BackfillRegistry {
    jobs: RwLock<HashMap<u64, BackfillJob>>,
    next_job_id: AtomicU64,
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
        job_id
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
        true
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
        // controller-driven eviction loops where both timestamps
        // come from the same wall clock.
        map.retain(|_, job| {
            !(job.status.is_terminal() && job.completed_at_ms.map(|t| t <= cutoff).unwrap_or(false))
        });
        before - map.len()
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
            BackfillSource::Prometheus {
                url: "u".to_string(),
            },
            BackfillSource::S3Gorilla {
                bucket: "b".to_string(),
                prefix: "p".to_string(),
            },
            BackfillSource::ClickHouse {
                url: "ch://x".to_string(),
                table: "t".to_string(),
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
}
