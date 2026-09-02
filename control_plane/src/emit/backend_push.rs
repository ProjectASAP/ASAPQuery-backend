//! Typed cumulative push of `BackendStageConfig` to the ASAPQuery-backend.
//!
//! Single entrypoint — [`post_typed_backend_for_role`] — invoked from
//! every plan-emit cycle (HTTP `POST /api/v1/plan`, the replanner's
//! plan-expiry / SLA-violation triggers, startup pre-pop tick, OpAMP
//! on-connect tick). It:
//!
//!   1. Updates the per-`(metric, role)` cache with the new
//!      `BackendStageConfig`.
//!   2. Builds a **cumulative** `BackendStageConfig` whose
//!      `aggregations` + `readouts` concatenate every cache entry's,
//!      ordered deterministically (`(metric, role.as_str())` ascending)
//!      so the emitted JSON body is reproducible across runs and tests.
//!   3. POSTs the cumulative streaming-config JSON to
//!      `/api/v1/streaming-config` — the data plane's atomic
//!      `handle.swap(new_config)` then installs every role's
//!      aggregations simultaneously.
//!   4. Groups cache entries by metric, merges each metric's
//!      `BackendStageConfig`s, and POSTs the per-metric merged routing
//!      table to `/api/v1/storage_routing`.
//!
//! **Why one helper, not two paths**: prior to Option B the control
//! plane had two emit paths into the backend:
//!
//!   * the typed cumulative path from `handle_plan` (post PR #287) —
//!     correct under the data plane's swap semantics;
//!   * the legacy single-aggregation path from `Replanner`
//!     (`generate_streaming_config_yaml`) — emits ONE aggregation per
//!     POST. Under the swap, this WIPES the cumulative state on the
//!     backend the moment plan-expiry or accuracy-violation fires it.
//!
//! Option B unifies both call sites through this helper so the swap
//! semantics are honoured at every emit cycle, and the legacy YAML
//! emitter is retired.
//!
//! Fire-and-forget contract: every error (emit failure, HTTP transport
//! error, non-2xx response) logs at WARN and returns — never panics,
//! never propagates. The next replan cycle retries with the latest
//! plan.

use std::collections::{BTreeMap, HashMap};
// `Future` is only referenced by the now-test-only `retry_transient`
// retry primitive (the production path is `push_documents_coupled`), so the
// import is gated to keep the non-test build warning-free.
#[cfg(test)]
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::backend_client::{BackendClient, BackendPostError};
use crate::emit::{emit_backend_storage_routing, emit_backend_streaming_config_json};
use crate::physical::colored_dag::emitter::BackendStageConfig;
use crate::workload::AggRole;

/// Retry policy for transient POST failures. Tuned to bridge the
/// startup race window in the multinode harness (asap arm from
/// ASAPCollector PR #394), where the controller may issue its first
/// `POST /api/v1/streaming-config` before the backend's HTTP server
/// has finished registering its routes:
///
///   * backend log: `HTTP server listening on port 9091` at T+0
///   * route bind for `/api/v1/streaming-config` lands T+~hundreds-of-ms later
///   * controller's startup `Replanner::replan_all()` POSTs at T+~few-seconds
///
/// Five attempts spanning ~5-8 s cover both the route-bind delay and
/// any TCP-accept race when the backend's compose container is still
/// initialising. Each delay is exponential (3x) with full jitter to
/// avoid synchronised retries from a fleet of controllers.
const RETRY_MAX_ATTEMPTS: u32 = 5;
const RETRY_BASE_DELAY: Duration = Duration::from_millis(100);
const RETRY_DELAY_CAP: Duration = Duration::from_millis(2700);

/// Monotonic counter for `BackendPlan.plan_id` — observability only, not
/// identity (see `BackendPlan`'s own doc). One process-wide sequence is
/// enough; there's no existing streaming-config version counter to
/// reuse for parity.
static PLAN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Cheap process-wide jitter source. We don't have `rand` in the
/// control plane's dependency set and don't want to add it for one
/// call site — `Instant::elapsed` reads the monotonic clock which is
/// already needed for the backoff itself. Returns a value in `0..=cap_ms`.
fn jitter_ms(start: Instant, cap_ms: u64) -> u64 {
    if cap_ms == 0 {
        return 0;
    }
    // Nanos since program start, folded into the jitter range. Good
    // enough to break up synchronous retry storms; not a CSPRNG.
    let nanos = start.elapsed().as_nanos() as u64;
    nanos % (cap_ms + 1)
}

/// Compute the delay before attempt `n` (1-indexed). Returns a value
/// `<= RETRY_DELAY_CAP` so the total span is bounded.
fn backoff_delay(attempt: u32, start: Instant) -> Duration {
    // Exponential base: 100ms, 300ms, 900ms, 2.7s, 2.7s (capped).
    let exp = 3u64.saturating_pow(attempt.saturating_sub(1));
    let base_ms = RETRY_BASE_DELAY.as_millis().saturating_mul(exp as u128) as u64;
    let base_ms = base_ms.min(RETRY_DELAY_CAP.as_millis() as u64);
    // Full jitter: pick a value in [0, base_ms].
    let with_jitter = jitter_ms(start, base_ms);
    Duration::from_millis(with_jitter)
}

/// Retry the given POST closure on [`BackendPostError::Transient`]
/// outcomes with exponential backoff + jitter, capped at
/// `RETRY_MAX_ATTEMPTS` attempts. Permanent failures short-circuit on
/// the first attempt. Returns the final attempt count and outcome —
/// the caller is expected to log appropriately and never propagate
/// (preserve the outer fire-and-forget contract).
///
/// Type parameters allow the closure to capture per-attempt context
/// (clones of the JSON body, the endpoint label) without forcing the
/// caller to box the future.
///
/// Now test-only: the production push path is [`push_documents_coupled`]
/// (P2-3), which couples the two document POSTs into one retried unit so
/// they can't land out of sync. `retry_transient` is retained as the
/// single-operation retry primitive whose backoff schedule
/// ([`backoff_delay`]) `push_documents_coupled` reuses, and its tests pin
/// the transient/permanent/exhaustion contract that the coupled push
/// relies on.
#[cfg(test)]
async fn retry_transient<F, Fut>(
    label: &str,
    mut op: F,
) -> (u32, std::result::Result<(), BackendPostError>)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<(), BackendPostError>>,
{
    let start = Instant::now();
    let mut last_err: Option<BackendPostError> = None;

    for attempt in 1..=RETRY_MAX_ATTEMPTS {
        match op().await {
            Ok(()) => return (attempt, Ok(())),
            Err(BackendPostError::Permanent(e)) => {
                // 4xx other than 404 — won't get better with retry.
                return (attempt, Err(BackendPostError::Permanent(e)));
            }
            Err(BackendPostError::Transient(e)) => {
                last_err = Some(BackendPostError::Transient(e));
                if attempt < RETRY_MAX_ATTEMPTS {
                    let delay = backoff_delay(attempt, start);
                    warn!(
                        op = %label,
                        attempt,
                        max_attempts = RETRY_MAX_ATTEMPTS,
                        retry_in_ms = delay.as_millis() as u64,
                        error = %last_err.as_ref().unwrap(),
                        "transient backend POST failure; will retry after backoff"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    (
        RETRY_MAX_ATTEMPTS,
        Err(last_err.unwrap_or_else(|| {
            BackendPostError::Transient(anyhow::anyhow!(
                "retry loop exhausted without recording a final error"
            ))
        })),
    )
}

/// Per-`(metric, role)` `BackendStageConfig` cache type alias. The
/// cache is owned by the controller's `AppState` and shared with the
/// `Replanner` via `Arc<Mutex<…>>` so both call sites read/write the
/// same cumulative state.
pub type BackendRoutingCache = Mutex<HashMap<(String, AggRole), BackendStageConfig>>;

/// Combined outcome of one cumulative publication cycle.
///
/// Streaming config, storage routing and BackendPlan are independent HTTP
/// POSTs. The outcome records all three so callers never treat a generation
/// with a missing authoritative plan as successfully published. It surfaces
/// partial failure so the next replan can republish the complete generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    /// No backend client configured — nothing was POSTed. Cache was still
    /// updated.
    Skipped,
    /// A document failed to even serialise; nothing was POSTed.
    EmitFailed,
    /// All three documents were accepted by the backend.
    AllApplied,
    /// At least one document failed to land. The documents may now disagree
    /// on the backend; the next replan cycle re-POSTs them to restore
    /// consistency. The carried flags say which succeeded so logs / tests
    /// can tell which side is stale.
    Desynced {
        streaming_ok: bool,
        routing_ok: bool,
        plan_ok: bool,
    },
}

/// POST the streaming-config and storage-routing documents as a COUPLED
/// unit (P2-3): the pair is retried together so a transient failure on
/// EITHER document re-attempts BOTH within the same backoff schedule,
/// rather than letting one land while the other is dropped for a whole
/// replan interval.
///
/// The backend applies each document via an idempotent `handle.swap`, so
/// re-POSTing a document that already succeeded on a prior attempt is
/// harmless — we therefore skip re-POSTing whichever side already returned
/// 2xx and only retry the side(s) still outstanding. The cycle is
/// considered successful only when BOTH sides are confirmed applied; a
/// permanent failure on either side stops the retry of that side
/// immediately (a malformed body won't get better with retries).
///
/// Returns the per-side success flags and the total attempts spent.
async fn push_documents_coupled(
    client: &Arc<BackendClient>,
    streaming_body: String,
    routing_body: String,
) -> (bool, bool, u32) {
    let start = Instant::now();
    let mut streaming_ok = false;
    let mut routing_ok = false;
    // A permanent failure on a side disables further attempts on that side
    // (retrying a 400 just floods the logs).
    let mut streaming_permanent = false;
    let mut routing_permanent = false;

    for attempt in 1..=RETRY_MAX_ATTEMPTS {
        // POST whichever side is still outstanding (not yet ok, not
        // permanently failed). Re-POSTing an already-applied side is safe
        // (idempotent swap) but wasteful, so we skip it.
        if !streaming_ok && !streaming_permanent {
            match client
                .post_streaming_config_json_typed(streaming_body.clone())
                .await
            {
                Ok(()) => streaming_ok = true,
                Err(BackendPostError::Permanent(e)) => {
                    streaming_permanent = true;
                    warn!(op = "streaming-config", error = %e, "permanent backend POST failure; will not retry this side");
                }
                Err(BackendPostError::Transient(e)) => {
                    warn!(op = "streaming-config", attempt, error = %e, "transient backend POST failure (coupled)");
                }
            }
        }
        if !routing_ok && !routing_permanent {
            match client
                .post_storage_routing_json_typed(routing_body.clone())
                .await
            {
                Ok(()) => routing_ok = true,
                Err(BackendPostError::Permanent(e)) => {
                    routing_permanent = true;
                    warn!(op = "storage-routing", error = %e, "permanent backend POST failure; will not retry this side");
                }
                Err(BackendPostError::Transient(e)) => {
                    warn!(op = "storage-routing", attempt, error = %e, "transient backend POST failure (coupled)");
                }
            }
        }

        // Both confirmed → done. Both terminal (ok or permanent) → no point
        // sleeping. Otherwise back off and retry the outstanding side(s).
        let streaming_done = streaming_ok || streaming_permanent;
        let routing_done = routing_ok || routing_permanent;
        if streaming_done && routing_done {
            return (streaming_ok, routing_ok, attempt);
        }
        if attempt < RETRY_MAX_ATTEMPTS {
            let delay = backoff_delay(attempt, start);
            tokio::time::sleep(delay).await;
        }
    }

    (streaming_ok, routing_ok, RETRY_MAX_ATTEMPTS)
}

/// Required push of the encoded `BackendPlan` — no
/// in-function retry loop, unlike [`push_documents_coupled`]. A dropped
/// push just leaves `data_plane`'s serving-time lookup falling back to
/// `SketchStore` reconstruction until the next replan cycle re-pushes,
/// so the next cycle is itself the retry backstop — same contract
/// [`push_or_log`] already establishes for the legacy YAML path. Logs at
/// WARN on failure and return it to the coupled publication outcome.
async fn push_backend_plan_required(client: &Arc<BackendClient>, bytes: Vec<u8>) -> bool {
    match client.post_backend_plan_typed(bytes).await {
        Ok(()) => {
            debug!(stage = "backend", endpoint = %client.endpoint(), "BackendPlan push succeeded");
            true
        }
        Err(e) => {
            warn!(
                stage = "backend",
                endpoint = %client.endpoint(),
                error = %e,
                "BackendPlan push failed; publication generation is incomplete"
            );
            false
        }
    }
}

/// Update the cumulative cache with `be` for `(metric, role)` and
/// POST the cumulative streaming-config + storage-routing JSON
/// documents to the backend.
///
/// `backend_client`: `None` is the explicit "no backend configured"
/// signal — the function still logs the would-have-emitted shape and
/// returns. This preserves the fire-and-forget contract from PR #287.
///
/// Errors at any step are logged at WARN and surfaced via the returned
/// [`PushOutcome`] — never propagated (the fire-and-forget contract is
/// preserved; existing call sites simply ignore the return value).
///
/// P2-3: the streaming-config and storage-routing documents are POSTed as
/// a COUPLED pair (see [`push_documents_coupled`]) so a transient failure
/// on one re-attempts both, rather than letting them land out of sync for
/// a whole replan interval.
pub async fn post_typed_backend_for_role(
    backend_client: Option<&Arc<BackendClient>>,
    cache: &BackendRoutingCache,
    metric: &str,
    role: AggRole,
    be: BackendStageConfig,
    // CDM monitor specs to embed in the cumulative streaming-config (global, so
    // included on every coupled push). Empty for non-monitored deployments.
    monitors: &[crate::emit::monitor::MonitorIntent],
) -> PushOutcome {
    // ── 1. Update cache and collect cumulative entries ───────────────────
    //
    // Snapshot the cache under a single lock so concurrent calls don't
    // interleave half-applied state. The clone is cheap (the per-(metric,
    // role) BackendStageConfig payloads are O(aggregations + readouts)
    // and one POST cycle).
    let cumulative_entries: Vec<((String, AggRole), BackendStageConfig)> = {
        let mut cache = cache.lock().await;
        cache.insert((metric.to_string(), role), be);
        let mut v: Vec<((String, AggRole), BackendStageConfig)> =
            cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        // Deterministic ordering so the emitted JSON body is
        // reproducible across runs (HashMap iteration would otherwise
        // make captured-body regression assertions flaky).
        v.sort_by(|(a_k, _), (b_k, _)| {
            a_k.0
                .cmp(&b_k.0)
                .then_with(|| a_k.1.as_str().cmp(b_k.1.as_str()))
        });
        v
    };

    push_cumulative_entries(
        backend_client,
        &cumulative_entries,
        metric,
        Some(role),
        monitors,
    )
    .await
}

/// Re-POST the FULL cumulative streaming-config + storage-routing derived
/// from the CURRENT cache, WITHOUT re-planning or mutating the cache (P0-1).
///
/// This exists because the data_plane backend is a plain HTTP service that
/// receives POSTs — it is NOT an OpAMP agent — so its restart fires none of
/// the controller's re-push triggers (startup `replan_all`, OpAMP
/// on-connect). After a backend restart its in-memory streaming-config is
/// gone, and the expiry ticker only re-POSTs `(metric, role)` pairs whose
/// plan `valid_until` elapsed; a query needing a non-default aggregation
/// (Sum / ExactAgg) then capability-misses to archive until something
/// expires.
///
/// The controller calls this on a bounded low-frequency cadence (see
/// `Replanner::run_backend_repost_ticker`). Each call is idempotent: the
/// data plane installs the cumulative config via an idempotent
/// `handle.swap`, so re-POSTing the SAME shape is a no-op on a backend that
/// already has it, and a full refresh on one that lost it. The push is
/// coupled (P2-3) so streaming-config + storage-routing never land split.
///
/// Returns [`PushOutcome::Skipped`] when no backend client is configured or
/// the cache is empty (nothing to refresh).
pub async fn repost_cumulative_backend_config(
    backend_client: Option<&Arc<BackendClient>>,
    cache: &BackendRoutingCache,
    monitors: &[crate::emit::monitor::MonitorIntent],
) -> PushOutcome {
    let cumulative_entries: Vec<((String, AggRole), BackendStageConfig)> = {
        let cache = cache.lock().await;
        if cache.is_empty() {
            // Nothing planned yet — a re-POST would emit an empty config.
            // Skip so a fresh controller that hasn't planned anything doesn't
            // wipe a backend that an out-of-band path populated.
            return PushOutcome::Skipped;
        }
        let mut v: Vec<((String, AggRole), BackendStageConfig)> =
            cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        v.sort_by(|(a_k, _), (b_k, _)| {
            a_k.0
                .cmp(&b_k.0)
                .then_with(|| a_k.1.as_str().cmp(b_k.1.as_str()))
        });
        v
    };
    push_cumulative_entries(
        backend_client,
        &cumulative_entries,
        "<periodic-refresh>",
        None,
        monitors,
    )
    .await
}

/// Shared push body for [`post_typed_backend_for_role`] and
/// [`repost_cumulative_backend_config`]: build BOTH cumulative documents
/// from the already-snapshotted `cumulative_entries`, then coupled-push
/// them (P2-3).
///
/// `role` is `Some` for a single-role replan emit and `None` for a
/// periodic full refresh; it only flavours the log line.
async fn push_cumulative_entries(
    backend_client: Option<&Arc<BackendClient>>,
    cumulative_entries: &[((String, AggRole), BackendStageConfig)],
    metric: &str,
    role: Option<AggRole>,
    monitors: &[crate::emit::monitor::MonitorIntent],
) -> PushOutcome {
    // ── Build BOTH cumulative documents up front (P2-3) ───────────────────
    //
    // Serialise the streaming-config AND the storage-routing JSON before
    // POSTing either one, so a serialise failure on the routing side never
    // leaves a streaming-config already POSTed (and vice versa). Both
    // documents derive from the SAME `cumulative_entries` snapshot, so
    // they describe one consistent generation of the cumulative state.

    // Streaming-config: one `BackendStageConfig` whose `aggregations` +
    // `readouts` are the concatenation of every cache entry's. The data
    // plane's swap installs this single multi-aggregation config
    // atomically, so ALL roles for ALL metrics survive.
    let cumulative_be = BackendStageConfig {
        aggregations: cumulative_entries
            .iter()
            .flat_map(|(_, c)| c.aggregations.iter().cloned())
            .collect(),
        readouts: cumulative_entries
            .iter()
            .flat_map(|(_, c)| c.readouts.iter().cloned())
            .collect(),
    };
    let streaming_body = match emit_backend_streaming_config_json(&cumulative_be, monitors) {
        Ok(doc) => doc.to_string(),
        Err(e) => {
            warn!(error = %e, "emit_backend_streaming_config_json failed; skipping coupled push");
            return PushOutcome::EmitFailed;
        }
    };

    // BackendPlan (design-backend-plan-wire-format.md): built from the
    // SAME `cumulative_be` snapshot as the legacy documents above, so all
    // three describe one consistent generation of planning state. Until
    // BackendPlan fully replaces the compatibility documents, publication
    // succeeds only when all three are accepted.
    let plan_bytes = match crate::backend_plan::from_stage_config(
        &cumulative_be,
        monitors,
        PLAN_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
        now_unix_ms(),
    ) {
        Ok(plan) => plan.encode_to_vec(),
        Err(e) => {
            warn!(error = %e, "backend_plan::from_stage_config failed; refusing partial publication");
            return PushOutcome::EmitFailed;
        }
    };

    // Storage-routing: the routing classifier (`build_routing_entry` in
    // `emit/stage_config.rs`) reads `cfg.aggregations` to derive shape
    // routing, so we MUST merge every role's aggregations for one metric
    // into a single `BackendStageConfig` before passing it through —
    // otherwise a metric with both DDSketch (Quantile) and ExactAgg (Sum)
    // would emit only the last-cached role's shape classifications and
    // route the siblings to archive.
    //
    // `emit_backend_storage_routing`'s signature is
    // `&[(String, &BackendStageConfig)]` — per-metric, NOT per-(metric,
    // role) — so the merge happens here.
    let mut by_metric: BTreeMap<String, BackendStageConfig> = BTreeMap::new();
    for ((m, _r), cfg) in cumulative_entries.iter() {
        let entry = by_metric
            .entry(m.clone())
            .or_insert_with(|| BackendStageConfig {
                aggregations: Vec::new(),
                readouts: Vec::new(),
            });
        entry.aggregations.extend(cfg.aggregations.iter().cloned());
        entry.readouts.extend(cfg.readouts.iter().cloned());
    }
    let routing_owned: Vec<(String, BackendStageConfig)> = by_metric.into_iter().collect();
    let routing_input: Vec<(String, &BackendStageConfig)> =
        routing_owned.iter().map(|(k, v)| (k.clone(), v)).collect();
    let routing_body = match emit_backend_storage_routing(&routing_input) {
        Ok(doc) => doc.to_string(),
        Err(e) => {
            warn!(error = %e, "emit_backend_storage_routing failed; skipping coupled push");
            return PushOutcome::EmitFailed;
        }
    };

    let role_label = role
        .map(|r| r.as_str().to_string())
        .unwrap_or_else(|| "*".to_string());
    info!(
        stage = "backend",
        metric = %metric,
        role = %role_label,
        aggregations = cumulative_be.aggregations.len(),
        readouts = cumulative_be.readouts.len(),
        cumulative_pairs = cumulative_entries.len(),
        cumulative_metrics = routing_owned.len(),
        "[USE_TYPED_STAGE_SPLIT] posting coupled streaming-config + storage-routing JSON"
    );

    // ── 3. Coupled push ───────────────────────────────────────────────────
    let Some(client) = backend_client else {
        info!(
            stage = "backend",
            "[USE_TYPED_STAGE_SPLIT] no backend client configured; \
             skipping JSON push (set CONTROLLER_BACKEND_ENDPOINT to enable)"
        );
        return PushOutcome::Skipped;
    };

    let (streaming_ok, routing_ok, attempts) =
        push_documents_coupled(client, streaming_body, routing_body).await;

    let plan_ok = push_backend_plan_required(client, plan_bytes).await;

    if streaming_ok && routing_ok && plan_ok {
        info!(
            stage = "backend",
            endpoint = %client.endpoint(),
            attempts,
            "[USE_TYPED_STAGE_SPLIT] coupled backend JSON push succeeded (both documents applied)"
        );
        PushOutcome::AllApplied
    } else {
        warn!(
            stage = "backend",
            endpoint = %client.endpoint(),
            attempts,
            streaming_ok,
            routing_ok,
            plan_ok,
            "[USE_TYPED_STAGE_SPLIT] coupled backend JSON push DESYNCED after retries \
             (one document landed, the other did not); next replan cycle re-POSTs both \
             cumulatively to restore consistency"
        );
        PushOutcome::Desynced {
            streaming_ok,
            routing_ok,
            plan_ok,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Retry-loop happy path with transient recovery: closure returns
    /// `Transient(404)` on the first call and `Ok` on the second. The
    /// helper must (a) reach attempt 2, (b) report final outcome Ok.
    /// This is the regression test for the controller-startup vs
    /// backend-route-bind race the parent PR addresses.
    #[tokio::test(start_paused = true)]
    async fn retry_transient_recovers_after_first_404() {
        let counter = AtomicU32::new(0);
        let (attempts, outcome) = retry_transient("test", || {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                if n == 1 {
                    Err(BackendPostError::Transient(anyhow::anyhow!(
                        "backend returned 404 for streaming-config JSON POST: <empty>"
                    )))
                } else {
                    Ok(())
                }
            }
        })
        .await;

        assert!(
            attempts > 1,
            "expected retry to happen at least once, got {attempts} attempt(s)"
        );
        assert_eq!(attempts, 2, "should succeed on the 2nd attempt");
        assert!(
            outcome.is_ok(),
            "expected Ok after recovery, got {outcome:?}"
        );
    }

    /// Permanent failures (4xx other than 404) MUST short-circuit on
    /// the first attempt — retrying a bad payload just floods the
    /// logs without ever succeeding.
    #[tokio::test(start_paused = true)]
    async fn retry_transient_does_not_retry_permanent_errors() {
        let counter = AtomicU32::new(0);
        let (attempts, outcome) = retry_transient("test", || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(BackendPostError::Permanent(anyhow::anyhow!(
                    "backend returned 400 for streaming-config JSON POST: bad payload"
                )))
            }
        })
        .await;

        assert_eq!(
            attempts, 1,
            "permanent error must not retry: got {attempts} attempts"
        );
        assert!(outcome.is_err(), "permanent error should surface as Err");
        assert!(
            !outcome.unwrap_err().is_transient(),
            "outcome must remain permanent"
        );
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "closure called exactly once"
        );
    }

    /// Exhausting all retries returns the final Transient error with
    /// `attempts == RETRY_MAX_ATTEMPTS` so the caller's WARN log can
    /// report how hard we tried.
    #[tokio::test(start_paused = true)]
    async fn retry_transient_exhausts_and_reports_attempts() {
        let counter = AtomicU32::new(0);
        let (attempts, outcome) = retry_transient("test", || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(BackendPostError::Transient(anyhow::anyhow!(
                    "connection refused"
                )))
            }
        })
        .await;

        assert_eq!(
            attempts, RETRY_MAX_ATTEMPTS,
            "all attempts should have fired"
        );
        assert!(outcome.is_err(), "exhausted retries should surface Err");
        assert!(
            outcome.unwrap_err().is_transient(),
            "final error must still be transient"
        );
        assert_eq!(counter.load(Ordering::SeqCst), RETRY_MAX_ATTEMPTS);
    }

    /// Happy path on the first attempt: zero retries, Ok outcome,
    /// attempts == 1. This protects the smoke-test invariant that the
    /// fire-and-forget happy path is unchanged when the backend is up
    /// before the controller's first POST.
    #[tokio::test(start_paused = true)]
    async fn retry_transient_no_retry_on_first_success() {
        let counter = AtomicU32::new(0);
        let (attempts, outcome) = retry_transient("test", || {
            counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(()) }
        })
        .await;

        assert_eq!(attempts, 1, "first-attempt success must not retry");
        assert!(outcome.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    /// Backoff schedule sanity-check: delays grow exponentially up to
    /// the cap. Doesn't assert exact ms (jitter makes that flaky); just
    /// asserts each delay is `<= RETRY_DELAY_CAP` and at least one
    /// later attempt has a larger nominal base than the first.
    #[test]
    fn backoff_delay_respects_cap() {
        let start = Instant::now();
        for attempt in 1..=RETRY_MAX_ATTEMPTS {
            let d = backoff_delay(attempt, start);
            assert!(
                d <= RETRY_DELAY_CAP,
                "attempt {attempt} delay {d:?} exceeds cap {:?}",
                RETRY_DELAY_CAP
            );
        }
    }

    fn make_be(metric: &str, agg_id: &str) -> BackendStageConfig {
        use crate::physical::colored_dag::emitter::{
            AggregationInput, BackendAggregation, BackendReadout,
        };
        use planner_types::post_asap::SketchQuery;
        use planner_types::post_asap::{SketchAlgorithm, SketchParams};
        BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: agg_id.to_string(),
                metric_name: metric.to_string(),
                sketch_kind: SketchAlgorithm::DDSketch.into(),
                sketch_params: SketchParams::DDSketch { alpha: 0.01 }.into(),
                grouping: vec![],
                item_label: None,
                spatial_filter: String::new(),
                window_secs: 60,
                aggregation_input: AggregationInput::SketchEnvelope,
                agg_type_override: None,
            }],
            readouts: vec![BackendReadout {
                aggregation_id: agg_id.to_string(),
                op: SketchQuery::Quantile { q: 0.99 },
            }],
        }
    }

    /// With no backend client, the helper logs but returns without
    /// panic. The cache is still updated (verified by a second call
    /// asserting cumulative behaviour).
    #[tokio::test]
    async fn no_client_still_updates_cache() {
        let cache = Mutex::new(HashMap::new());
        let be = make_be("m", "agg0");
        post_typed_backend_for_role(None, &cache, "m", AggRole::Quantile, be, &[]).await;
        let snap = cache.lock().await;
        assert_eq!(snap.len(), 1);
        assert!(snap.contains_key(&("m".to_string(), AggRole::Quantile)));
    }

    /// Two distinct `(metric, role)` calls produce a cumulative cache
    /// (size 2), not overwrite (size 1). This is the regression that
    /// motivated PR #287.
    #[tokio::test]
    async fn distinct_roles_accumulate_not_overwrite() {
        let cache = Mutex::new(HashMap::new());
        post_typed_backend_for_role(
            None,
            &cache,
            "http_requests_total",
            AggRole::Quantile,
            make_be("http_requests_total", "q"),
            &[],
        )
        .await;
        post_typed_backend_for_role(
            None,
            &cache,
            "http_requests_total",
            AggRole::Sum,
            make_be("http_requests_total", "s"),
            &[],
        )
        .await;
        let snap = cache.lock().await;
        assert_eq!(snap.len(), 2, "both roles must persist");
        assert!(snap.contains_key(&("http_requests_total".to_string(), AggRole::Quantile)));
        assert!(snap.contains_key(&("http_requests_total".to_string(), AggRole::Sum)));
    }

    /// Re-posting the same `(metric, role)` is idempotent at the
    /// cache level — the entry is replaced, not duplicated. This is
    /// the contract OpAMP on-connect ticks rely on (multiple
    /// reconnects must not bloat the cumulative POST).
    #[tokio::test]
    async fn same_pair_replaces_not_duplicates() {
        let cache = Mutex::new(HashMap::new());
        post_typed_backend_for_role(
            None,
            &cache,
            "m",
            AggRole::Quantile,
            make_be("m", "v1"),
            &[],
        )
        .await;
        post_typed_backend_for_role(
            None,
            &cache,
            "m",
            AggRole::Quantile,
            make_be("m", "v2"),
            &[],
        )
        .await;
        let snap = cache.lock().await;
        assert_eq!(snap.len(), 1);
        let entry = snap.get(&("m".to_string(), AggRole::Quantile)).unwrap();
        assert_eq!(entry.aggregations[0].aggregation_id, "v2");
    }

    // ── P2-3 / P0-1: coupled push + periodic re-POST against a mock backend ──

    use axum::extract::State;
    use axum::routing::post;
    use axum::Router;
    use std::sync::atomic::{AtomicU32 as StdAtomicU32, Ordering as StdOrdering};
    use std::sync::Arc as StdArc;

    /// Mock backend exposing BOTH the streaming-config and storage-routing
    /// endpoints. Counts hits per endpoint and lets each endpoint be
    /// configured to return a fixed status, so a test can make one side fail
    /// while the other succeeds (the P2-3 desync scenario).
    #[derive(Clone)]
    struct DualMock {
        streaming_hits: StdArc<StdAtomicU32>,
        routing_hits: StdArc<StdAtomicU32>,
        plan_hits: StdArc<StdAtomicU32>,
        streaming_status: axum::http::StatusCode,
        routing_status: axum::http::StatusCode,
    }

    async fn start_dual_mock(
        streaming_status: axum::http::StatusCode,
        routing_status: axum::http::StatusCode,
    ) -> (String, DualMock) {
        let mock = DualMock {
            streaming_hits: StdArc::new(StdAtomicU32::new(0)),
            routing_hits: StdArc::new(StdAtomicU32::new(0)),
            plan_hits: StdArc::new(StdAtomicU32::new(0)),
            streaming_status,
            routing_status,
        };
        let app = Router::new()
            .route(
                "/api/v1/streaming-config",
                post(
                    |State(m): State<DualMock>, _body: axum::body::Bytes| async move {
                        m.streaming_hits.fetch_add(1, StdOrdering::SeqCst);
                        m.streaming_status
                    },
                ),
            )
            .route(
                "/api/v1/storage_routing",
                post(
                    |State(m): State<DualMock>, _body: axum::body::Bytes| async move {
                        m.routing_hits.fetch_add(1, StdOrdering::SeqCst);
                        m.routing_status
                    },
                ),
            )
            .route(
                "/api/v1/backend-plan",
                post(
                    |State(m): State<DualMock>, _body: axum::body::Bytes| async move {
                        m.plan_hits.fetch_add(1, StdOrdering::SeqCst);
                        axum::http::StatusCode::OK
                    },
                ),
            )
            .with_state(mock.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        (format!("http://{addr}/api/v1/streaming-config"), mock)
    }

    /// Happy path: both endpoints return 2xx → `BothApplied`, and each
    /// endpoint is hit exactly once (no wasteful re-POST of an
    /// already-applied side).
    #[tokio::test]
    async fn coupled_push_both_ok_hits_each_endpoint_once() {
        let (url, mock) =
            start_dual_mock(axum::http::StatusCode::OK, axum::http::StatusCode::OK).await;
        let client = StdArc::new(BackendClient::new(url));
        let cache = Mutex::new(HashMap::new());
        let outcome = post_typed_backend_for_role(
            Some(&client),
            &cache,
            "latency",
            AggRole::Quantile,
            make_be("latency", "q"),
            &[],
        )
        .await;
        assert_eq!(outcome, PushOutcome::AllApplied);
        assert_eq!(mock.streaming_hits.load(StdOrdering::SeqCst), 1);
        assert_eq!(mock.routing_hits.load(StdOrdering::SeqCst), 1);
    }

    /// The dual-push also fires a best-effort `POST /api/v1/backend-plan`,
    /// alongside — not instead of — the legacy documents.
    #[tokio::test]
    async fn coupled_push_also_fires_backend_plan_push() {
        let (url, mock) =
            start_dual_mock(axum::http::StatusCode::OK, axum::http::StatusCode::OK).await;
        let client = StdArc::new(BackendClient::new(url));
        let cache = Mutex::new(HashMap::new());
        let outcome = post_typed_backend_for_role(
            Some(&client),
            &cache,
            "latency",
            AggRole::Quantile,
            make_be("latency", "q"),
            &[],
        )
        .await;
        assert_eq!(outcome, PushOutcome::AllApplied);
        assert_eq!(mock.plan_hits.load(StdOrdering::SeqCst), 1);
    }

    /// A BackendPlan push failure makes the publication generation
    /// explicitly incomplete even if both compatibility documents landed.
    #[tokio::test]
    async fn backend_plan_push_failure_is_reported_as_desync() {
        // A mock that only serves the legacy endpoints (no
        // `/api/v1/backend-plan` route) — the plan push 404s.
        let app = Router::new()
            .route(
                "/api/v1/streaming-config",
                post(|_body: axum::body::Bytes| async { axum::http::StatusCode::OK }),
            )
            .route(
                "/api/v1/storage_routing",
                post(|_body: axum::body::Bytes| async { axum::http::StatusCode::OK }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let client = StdArc::new(BackendClient::new(format!(
            "http://{addr}/api/v1/streaming-config"
        )));
        let cache = Mutex::new(HashMap::new());
        let outcome = post_typed_backend_for_role(
            Some(&client),
            &cache,
            "latency",
            AggRole::Quantile,
            make_be("latency", "q"),
            &[],
        )
        .await;
        assert_eq!(
            outcome,
            PushOutcome::Desynced {
                streaming_ok: true,
                routing_ok: true,
                plan_ok: false,
            },
            "publication must not report success when the authoritative plan is missing"
        );
    }

    /// P2-3: streaming-config succeeds (200) but storage-routing always
    /// returns a PERMANENT 400. The coupled push surfaces
    /// `Desynced { streaming_ok: true, routing_ok: false }` rather than a
    /// silent success, and — because 400 is permanent — the routing side is
    /// NOT retried (hit exactly once), while the already-applied streaming
    /// side is also not re-POSTed.
    #[tokio::test]
    async fn coupled_push_surfaces_desync_when_one_side_permanently_fails() {
        let (url, mock) = start_dual_mock(
            axum::http::StatusCode::OK,
            axum::http::StatusCode::BAD_REQUEST,
        )
        .await;
        let client = StdArc::new(BackendClient::new(url));
        let cache = Mutex::new(HashMap::new());
        let outcome = post_typed_backend_for_role(
            Some(&client),
            &cache,
            "latency",
            AggRole::Quantile,
            make_be("latency", "q"),
            &[],
        )
        .await;
        assert_eq!(
            outcome,
            PushOutcome::Desynced {
                streaming_ok: true,
                routing_ok: false,
                plan_ok: true,
            },
            "one-sided failure must surface as Desynced, not silent success"
        );
        // Streaming applied once; routing's permanent 400 stops further
        // attempts after the first.
        assert_eq!(mock.streaming_hits.load(StdOrdering::SeqCst), 1);
        assert_eq!(mock.routing_hits.load(StdOrdering::SeqCst), 1);
    }

    /// P0-1: a simulated backend RESET. The controller plans a (metric,
    /// role) (populating the shared cache), then the backend "restarts"
    /// (a fresh mock with zero hits). The periodic re-POST
    /// (`repost_cumulative_backend_config`) must re-send the FULL cumulative
    /// streaming-config + storage-routing from the cache WITHOUT any
    /// re-plan, so the restarted backend recovers its config.
    #[tokio::test]
    async fn repost_after_simulated_backend_reset_re_pushes_full_config() {
        // Phase 1: initial plan lands on the first backend instance.
        let (url1, mock1) =
            start_dual_mock(axum::http::StatusCode::OK, axum::http::StatusCode::OK).await;
        let client1 = StdArc::new(BackendClient::new(url1));
        let cache = Mutex::new(HashMap::new());
        post_typed_backend_for_role(
            Some(&client1),
            &cache,
            "http_requests_total",
            AggRole::Sum,
            make_be("http_requests_total", "s"),
            &[],
        )
        .await;
        assert_eq!(mock1.streaming_hits.load(StdOrdering::SeqCst), 1);
        assert_eq!(mock1.routing_hits.load(StdOrdering::SeqCst), 1);

        // Phase 2: the backend silently restarts — model it as a brand-new
        // mock with zero recorded hits. NOTHING expires, NO replan fires.
        let (url2, mock2) =
            start_dual_mock(axum::http::StatusCode::OK, axum::http::StatusCode::OK).await;
        let client2 = StdArc::new(BackendClient::new(url2));
        assert_eq!(mock2.streaming_hits.load(StdOrdering::SeqCst), 0);

        // The periodic re-POST reads the SAME cache and re-pushes everything.
        let outcome = repost_cumulative_backend_config(Some(&client2), &cache, &[]).await;
        assert_eq!(outcome, PushOutcome::AllApplied);
        assert_eq!(
            mock2.streaming_hits.load(StdOrdering::SeqCst),
            1,
            "restarted backend must receive the cumulative streaming-config again"
        );
        assert_eq!(
            mock2.routing_hits.load(StdOrdering::SeqCst),
            1,
            "restarted backend must receive the cumulative storage-routing again"
        );
    }

    /// P0-1 guard: re-POST on an EMPTY cache (controller hasn't planned
    /// anything yet) is a no-op `Skipped`, so a fresh controller never wipes
    /// a backend with an empty cumulative config.
    #[tokio::test]
    async fn repost_empty_cache_is_skipped() {
        let (url, mock) =
            start_dual_mock(axum::http::StatusCode::OK, axum::http::StatusCode::OK).await;
        let client = StdArc::new(BackendClient::new(url));
        let cache = Mutex::new(HashMap::new());
        let outcome = repost_cumulative_backend_config(Some(&client), &cache, &[]).await;
        assert_eq!(outcome, PushOutcome::Skipped);
        assert_eq!(mock.streaming_hits.load(StdOrdering::SeqCst), 0);
        assert_eq!(mock.routing_hits.load(StdOrdering::SeqCst), 0);
    }

    /// P0-1: no backend client → `Skipped` (the periodic ticker is a no-op
    /// when `CONTROLLER_BACKEND_ENDPOINT` isn't set).
    #[tokio::test]
    async fn repost_no_client_is_skipped() {
        let cache = Mutex::new(HashMap::new());
        post_typed_backend_for_role(None, &cache, "m", AggRole::Quantile, make_be("m", "q"), &[])
            .await;
        let outcome = repost_cumulative_backend_config(None, &cache, &[]).await;
        assert_eq!(outcome, PushOutcome::Skipped);
    }
}
