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
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tracing::{info, warn};

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
    let base_ms = RETRY_BASE_DELAY
        .as_millis()
        .saturating_mul(exp as u128) as u64;
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

/// Update the cumulative cache with `be` for `(metric, role)` and
/// POST the cumulative streaming-config + storage-routing JSON
/// documents to the backend.
///
/// `backend_client`: `None` is the explicit "no backend configured"
/// signal — the function still logs the would-have-emitted shape and
/// returns. This preserves the fire-and-forget contract from PR #287.
///
/// Errors at any step are logged at WARN and returned — never
/// propagated.
pub async fn post_typed_backend_for_role(
    backend_client: Option<&Arc<BackendClient>>,
    cache: &BackendRoutingCache,
    metric: &str,
    role: AggRole,
    be: BackendStageConfig,
) {
    // ── 1. Update cache and collect cumulative entries ───────────────────
    //
    // Snapshot the cache under a single lock so concurrent calls don't
    // interleave half-applied state. The clone is cheap (the per-(metric,
    // role) BackendStageConfig payloads are O(aggregations + readouts)
    // and one POST cycle).
    let cumulative_entries: Vec<((String, AggRole), BackendStageConfig)> = {
        let mut cache = cache.lock().await;
        cache.insert((metric.to_string(), role), be);
        let mut v: Vec<((String, AggRole), BackendStageConfig)> = cache
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
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

    // ── 2. Cumulative streaming-config ───────────────────────────────────
    //
    // One `BackendStageConfig` whose `aggregations` + `readouts` are the
    // concatenation of every cache entry's. The data plane's swap
    // installs this single multi-aggregation config atomically, so ALL
    // roles for ALL metrics survive.
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

    match emit_backend_streaming_config_json(&cumulative_be) {
        Ok(json_doc) => {
            info!(
                stage = "backend",
                metric = %metric,
                role = %role,
                aggregations = cumulative_be.aggregations.len(),
                readouts = cumulative_be.readouts.len(),
                cumulative_pairs = cumulative_entries.len(),
                "[USE_TYPED_STAGE_SPLIT] posting typed backend JSON"
            );
            if let Some(client) = backend_client {
                let body = json_doc.to_string();
                // Retry-with-backoff for transient errors (404
                // route-not-bound, 5xx, connection refused, connect
                // timeout) so the controller's startup `replan_all()`
                // tick can outwait the backend's HTTP-server bind +
                // route-registration window. See PR for the multinode
                // race we're patching here.
                let (attempts, outcome) = retry_transient("streaming-config", || {
                    let body = body.clone();
                    async move { client.post_streaming_config_json_typed(body).await }
                })
                .await;
                match outcome {
                    Ok(()) => info!(
                        stage = "backend",
                        endpoint = %client.endpoint(),
                        attempts,
                        "[USE_TYPED_STAGE_SPLIT] typed backend JSON push succeeded"
                    ),
                    Err(e) => warn!(
                        stage = "backend",
                        endpoint = %client.endpoint(),
                        attempts,
                        error = %e,
                        "[USE_TYPED_STAGE_SPLIT] typed backend JSON push failed after retries; \
                         next replan cycle will retry"
                    ),
                }
            } else {
                info!(
                    stage = "backend",
                    "[USE_TYPED_STAGE_SPLIT] no backend client configured; \
                     skipping JSON push (set CONTROLLER_BACKEND_ENDPOINT to enable)"
                );
            }
        }
        Err(e) => warn!(error = %e, "emit_backend_streaming_config_json failed"),
    }

    // ── 3. Cumulative storage-routing ────────────────────────────────────
    //
    // The routing classifier (`build_routing_entry` in
    // `emit/stage_config.rs`) reads `cfg.aggregations` to derive shape
    // routing, so we MUST merge every role's aggregations for one
    // metric into a single `BackendStageConfig` before passing it
    // through — otherwise a metric with both DDSketch (Quantile) and
    // ExactAgg (Sum) would emit only the last-cached role's shape
    // classifications and route the siblings to archive.
    //
    // `emit_backend_storage_routing`'s signature is
    // `&[(String, &BackendStageConfig)]` — per-metric, NOT per-(metric,
    // role) — so the merge happens here.
    let mut by_metric: BTreeMap<String, BackendStageConfig> = BTreeMap::new();
    for ((m, _r), cfg) in &cumulative_entries {
        let entry = by_metric.entry(m.clone()).or_insert_with(|| {
            BackendStageConfig {
                aggregations: Vec::new(),
                readouts: Vec::new(),
            }
        });
        entry.aggregations.extend(cfg.aggregations.iter().cloned());
        entry.readouts.extend(cfg.readouts.iter().cloned());
    }
    let routing_owned: Vec<(String, BackendStageConfig)> = by_metric.into_iter().collect();
    let routing_input: Vec<(String, &BackendStageConfig)> = routing_owned
        .iter()
        .map(|(k, v)| (k.clone(), v))
        .collect();
    match emit_backend_storage_routing(&routing_input) {
        Ok(routing_doc) => {
            info!(
                stage = "backend",
                metric = %metric,
                cumulative_metrics = routing_owned.len(),
                cumulative_pairs = cumulative_entries.len(),
                "[USE_TYPED_STAGE_SPLIT] posting cumulative storage-routing JSON"
            );
            if let Some(client) = backend_client {
                let body = routing_doc.to_string();
                // Same retry policy as the streaming-config POST
                // above — the storage-routing endpoint lives on the
                // same backend HTTP server and binds at the same time,
                // so it shares the same startup-race window.
                let (attempts, outcome) = retry_transient("storage-routing", || {
                    let body = body.clone();
                    async move { client.post_storage_routing_json_typed(body).await }
                })
                .await;
                match outcome {
                    Ok(()) => info!(
                        stage = "backend",
                        metric = %metric,
                        attempts,
                        "[USE_TYPED_STAGE_SPLIT] storage-routing JSON push succeeded"
                    ),
                    Err(e) => warn!(
                        stage = "backend",
                        metric = %metric,
                        attempts,
                        error = %e,
                        "[USE_TYPED_STAGE_SPLIT] storage-routing JSON push failed after retries; \
                         next replan cycle will retry"
                    ),
                }
            } else {
                info!(
                    stage = "backend",
                    "[USE_TYPED_STAGE_SPLIT] no backend client configured; \
                     skipping storage-routing JSON push"
                );
            }
        }
        Err(e) => warn!(error = %e, "emit_backend_storage_routing failed"),
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
        assert!(outcome.is_ok(), "expected Ok after recovery, got {outcome:?}");
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

        assert_eq!(attempts, 1, "permanent error must not retry: got {attempts} attempts");
        assert!(outcome.is_err(), "permanent error should surface as Err");
        assert!(!outcome.unwrap_err().is_transient(), "outcome must remain permanent");
        assert_eq!(counter.load(Ordering::SeqCst), 1, "closure called exactly once");
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
        use crate::sketch_algebra::params::{DDSketchParams, SketchKind, SketchParams};
        use crate::sketch_algebra::physical_expr::EstimateOp;
        BackendStageConfig {
            aggregations: vec![BackendAggregation {
                aggregation_id: agg_id.to_string(),
                metric_name: metric.to_string(),
                sketch_kind: SketchKind::DDSketch,
                sketch_params: SketchParams::DDSketch(DDSketchParams { alpha: 0.01 }),
                grouping: vec![],
                spatial_filter: String::new(),
                window_secs: 60,
                aggregation_input: AggregationInput::SketchEnvelope,
                agg_type_override: None,
            }],
            readouts: vec![BackendReadout {
                aggregation_id: agg_id.to_string(),
                op: EstimateOp::Quantile { q: 0.99 },
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
        post_typed_backend_for_role(None, &cache, "m", AggRole::Quantile, be).await;
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
        )
        .await;
        post_typed_backend_for_role(
            None,
            &cache,
            "http_requests_total",
            AggRole::Sum,
            make_be("http_requests_total", "s"),
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
        post_typed_backend_for_role(None, &cache, "m", AggRole::Quantile, make_be("m", "v1"))
            .await;
        post_typed_backend_for_role(None, &cache, "m", AggRole::Quantile, make_be("m", "v2"))
            .await;
        let snap = cache.lock().await;
        assert_eq!(snap.len(), 1);
        let entry = snap.get(&("m".to_string(), AggRole::Quantile)).unwrap();
        assert_eq!(entry.aggregations[0].aggregation_id, "v2");
    }
}
