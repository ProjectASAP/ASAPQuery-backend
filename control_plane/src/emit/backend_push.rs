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
use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::backend_client::BackendClient;
use crate::emit::{emit_backend_storage_routing, emit_backend_streaming_config_json};
use crate::physical::colored_dag::emitter::BackendStageConfig;
use crate::workload::AggRole;

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
                match client.post_streaming_config_json(body).await {
                    Ok(()) => info!(
                        stage = "backend",
                        endpoint = %client.endpoint(),
                        "[USE_TYPED_STAGE_SPLIT] typed backend JSON push succeeded"
                    ),
                    Err(e) => warn!(
                        stage = "backend",
                        endpoint = %client.endpoint(),
                        error = %e,
                        "[USE_TYPED_STAGE_SPLIT] typed backend JSON push failed; \
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
                match client.post_storage_routing_json(body).await {
                    Ok(()) => info!(
                        stage = "backend",
                        metric = %metric,
                        "[USE_TYPED_STAGE_SPLIT] storage-routing JSON push succeeded"
                    ),
                    Err(e) => warn!(
                        stage = "backend",
                        metric = %metric,
                        error = %e,
                        "[USE_TYPED_STAGE_SPLIT] storage-routing JSON push failed; \
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
