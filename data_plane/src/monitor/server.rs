//! tonic bidi-streaming `MonitorService` server. Each edge opens one
//! `Monitor` stream and multiplexes all of its monitors over it. The server
//! reads `EdgeToCoord` (register / report) messages, drives the per-monitor
//! [`Monitor`] state machine, and pushes the resulting `CoordToEdge`
//! (coordinated-sampling grant) back to the addressed edge's stream.
//! Global-threshold alerting is retired (see `coordinator` module docs) — this
//! server never fires one.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use asap_otel_proto::monitor::v1::{
    coord_to_edge, edge_to_coord,
    monitor_service_server::{MonitorService, MonitorServiceServer},
    CoordToEdge, EdgeToCoord, SlackGrant,
};

use super::coordinator::{Action, Monitor, MonitorConfig};

type MonKey = (u64, Vec<u8>);
type EdgeTx = mpsc::Sender<Result<CoordToEdge, Status>>;

/// Shared coordinator state behind the gRPC service. One per data-plane process.
pub struct MonitorCoordinator {
    /// Live per-monitor scalar state machines, created lazily from `cfgs`.
    monitors: Mutex<HashMap<MonKey, Monitor>>,
    /// Monitor specs (ε, window) from the streaming-config `monitors:`
    /// section. Behind an `RwLock` so the control plane's hot-reload
    /// (`reconfigure`) can add/update/remove monitors on a live coordinator
    /// without a process restart. Held only for brief, non-`await` critical
    /// sections, so a `std` lock is safe in async code.
    cfgs: std::sync::RwLock<HashMap<MonKey, MonitorConfig>>,
    /// Outbound stream sender per connected edge.
    edges: Mutex<HashMap<String, EdgeTx>>,
}

impl MonitorCoordinator {
    pub fn new(cfgs: Vec<MonitorConfig>) -> Arc<Self> {
        let cfgs = cfgs
            .into_iter()
            .map(|c| ((c.agg_id, c.key.clone()), c))
            .collect();
        Arc::new(Self {
            monitors: Mutex::new(HashMap::new()),
            cfgs: std::sync::RwLock::new(cfgs),
            edges: Mutex::new(HashMap::new()),
        })
    }

    /// Number of configured monitors (test/observability).
    pub fn monitor_count(&self) -> usize {
        self.cfgs.read().unwrap().len()
    }

    /// The coordinated-sampling grant currently computed for `edge_id` under
    /// monitor `(agg_id, key)`, or `None` if the monitor doesn't exist yet or
    /// that edge hasn't reported a rate (test/observability — see
    /// `Monitor::sample_p_for_edge`).
    pub async fn granted_sample_p(&self, agg_id: u64, key: &[u8], edge_id: &str) -> Option<f64> {
        let monitors = self.monitors.lock().await;
        monitors
            .get(&(agg_id, key.to_vec()))?
            .sample_p_for_edge(edge_id)
    }

    /// Hot-reload the monitor set from a freshly-pushed streaming-config.
    ///
    /// The coordinator originally read `monitors:` only at boot; a monitor the
    /// control plane published *after* start (via the `/api/v1/streaming-config`
    /// hot-reload POST) never reached it, so every edge registering for that
    /// monitor was rejected as "unconfigured". This applies the new spec list to
    /// the live coordinator:
    ///   * **added** specs become matchable immediately (the next register
    ///     lazily builds the `Monitor`);
    ///   * **changed** specs (different τ/ε/window for an existing key) drop the
    ///     stale live `Monitor` so the next register rebuilds it under the new
    ///     spec — the edge re-registers every epoch, so this self-heals within
    ///     one window;
    ///   * **removed** specs drop both the spec and any live state.
    /// Unchanged monitors keep their in-flight per-edge rate state untouched.
    ///
    /// Returns `(added, changed, removed)` counts for observability. Idempotent:
    /// re-applying the same specs is a no-op that returns `(0, 0, 0)`.
    pub async fn reconfigure(&self, specs: Vec<MonitorConfig>) -> (usize, usize, usize) {
        let new_cfgs: HashMap<MonKey, MonitorConfig> = specs
            .into_iter()
            .map(|c| ((c.agg_id, c.key.clone()), c))
            .collect();

        // Diff against the current specs to find keys to evict from live state
        // (removed, or changed so the live Monitor's τ is stale).
        let (added, changed, removed, evict): (usize, usize, usize, Vec<MonKey>) = {
            let old = self.cfgs.read().unwrap();
            let mut added = 0;
            let mut changed = 0;
            let mut evict = Vec::new();
            for (k, c) in new_cfgs.iter() {
                match old.get(k) {
                    None => added += 1,
                    Some(prev) if prev != c => {
                        changed += 1;
                        evict.push(k.clone());
                    }
                    Some(_) => {}
                }
            }
            let mut removed = 0;
            for k in old.keys() {
                if !new_cfgs.contains_key(k) {
                    removed += 1;
                    evict.push(k.clone());
                }
            }
            (added, changed, removed, evict)
        };

        if added == 0 && changed == 0 && removed == 0 {
            return (0, 0, 0);
        }

        // Swap the spec map first so any register racing the eviction below sees
        // the new spec (and rebuilds correctly), then drop stale live state.
        *self.cfgs.write().unwrap() = new_cfgs;
        if !evict.is_empty() {
            let mut monitors = self.monitors.lock().await;
            for k in &evict {
                monitors.remove(k);
            }
        }
        (added, changed, removed)
    }

    /// Apply a register and return the resulting actions, or None if no monitor
    /// is configured for (agg_id, key).
    async fn apply_register(
        &self,
        agg_id: u64,
        key: Vec<u8>,
        edge_id: &str,
        epoch_window_ms: u64,
        window_start_ms: u64,
    ) -> Option<Vec<Action>> {
        let mk = (agg_id, key.clone());
        let cfg = {
            let cfgs = self.cfgs.read().unwrap();
            match cfgs.get(&mk) {
                Some(c) => c.clone(),
                None => {
                    warn!(
                        agg_id,
                        req_key = ?key,
                        req_key_len = key.len(),
                        configured = ?cfgs.keys().map(|(a, k)| format!("{a}/len{}", k.len())).collect::<Vec<_>>(),
                        "apply_register MISS — (agg_id,key) not in cfgs"
                    );
                    return None;
                }
            }
        };
        let mut monitors = self.monitors.lock().await;
        let mon = monitors.entry(mk).or_insert_with(|| Monitor::new(cfg));
        Some(mon.on_register(edge_id, epoch_window_ms, window_start_ms))
    }

    async fn apply_report(
        &self,
        agg_id: u64,
        key: Vec<u8>,
        edge_id: &str,
        window_start_ms: u64,
        rate: f64,
    ) -> Option<Vec<Action>> {
        let mk = (agg_id, key);
        let mut monitors = self.monitors.lock().await;
        let mon = monitors.get_mut(&mk)?;
        Some(mon.on_report(edge_id, window_start_ms, rate))
    }

    /// Dispatch coordinator actions: grants to the addressed edge's stream.
    /// No alert path — this coordinator no longer makes alerting decisions
    /// (see `coordinator` module docs).
    async fn dispatch(&self, agg_id: u64, key: &[u8], actions: Vec<Action>) {
        for action in actions {
            let Action::Grant {
                edge_id,
                round,
                local_slack,
                window_start_ms,
                sample_p,
            } = action;
            let msg = CoordToEdge {
                msg: Some(coord_to_edge::Msg::Grant(SlackGrant {
                    agg_id,
                    key: key.to_vec(),
                    round,
                    local_slack,
                    window_start_ms,
                    sample_p,
                })),
            };
            let tx = {
                let edges = self.edges.lock().await;
                edges.get(&edge_id).cloned()
            };
            if let Some(tx) = tx {
                if tx.send(Ok(msg)).await.is_err() {
                    debug!(edge = %edge_id, "grant send failed (edge gone)");
                }
            }
        }
    }

    /// Handle one inbound envelope from an edge stream. Records the edge's
    /// sender on first register so grants can be routed back to it.
    async fn handle_msg(
        self: &Arc<Self>,
        env: EdgeToCoord,
        tx: &EdgeTx,
        my_edge_id: &mut Option<String>,
    ) {
        match env.msg {
            Some(edge_to_coord::Msg::Reg(reg)) => {
                if my_edge_id.is_none() {
                    *my_edge_id = Some(reg.edge_id.clone());
                    self.edges
                        .lock()
                        .await
                        .insert(reg.edge_id.clone(), tx.clone());
                }
                match self
                    .apply_register(
                        reg.agg_id,
                        reg.key.clone(),
                        &reg.edge_id,
                        reg.epoch_window_ms,
                        reg.window_start_ms,
                    )
                    .await
                {
                    Some(actions) => self.dispatch(reg.agg_id, &reg.key, actions).await,
                    None => warn!(
                        agg_id = reg.agg_id,
                        "register for unconfigured monitor — ignored"
                    ),
                }
            }
            Some(edge_to_coord::Msg::Report(rep)) => {
                // A rate report: reply to this edge alone with its freshly
                // computed coordinated-sampling grant (rep.local_value / seq
                // are no longer consulted — no countdown left to feed them
                // into; they still ride the wire message for compatibility).
                if let Some(actions) = self
                    .apply_report(rep.agg_id, rep.key.clone(), &rep.edge_id, rep.window_start_ms, rep.rate)
                    .await
                {
                    self.dispatch(rep.agg_id, &rep.key, actions).await;
                }
            }
            None => {}
        }
    }

    /// An edge stream closed: drop its sender and its membership in every
    /// monitor it was part of. No re-grant fan-out needed — each edge's grant
    /// is independent, so one edge leaving never perturbs another's.
    async fn handle_disconnect(&self, edge_id: &str) {
        self.edges.lock().await.remove(edge_id);
        let mut monitors = self.monitors.lock().await;
        for mon in monitors.values_mut() {
            mon.on_leave(edge_id);
        }
    }
}

/// The tonic service wrapper.
pub struct MonitorServiceImpl {
    coord: Arc<MonitorCoordinator>,
}

impl MonitorServiceImpl {
    pub fn new(coord: Arc<MonitorCoordinator>) -> Self {
        Self { coord }
    }

    /// Wrap into a tonic server service ready for `Server::add_service`.
    pub fn into_server(self) -> MonitorServiceServer<Self> {
        MonitorServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl MonitorService for MonitorServiceImpl {
    type MonitorStream = Pin<Box<dyn Stream<Item = Result<CoordToEdge, Status>> + Send + 'static>>;

    async fn monitor(
        &self,
        request: Request<Streaming<EdgeToCoord>>,
    ) -> Result<Response<Self::MonitorStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<CoordToEdge, Status>>(64);
        let coord = self.coord.clone();

        tokio::spawn(async move {
            let mut my_edge_id: Option<String> = None;
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(env) => coord.handle_msg(env, &tx, &mut my_edge_id).await,
                    Err(status) => {
                        debug!(?status, "monitor stream recv error");
                        break;
                    }
                }
            }
            if let Some(edge_id) = my_edge_id {
                info!(edge = %edge_id, "monitor stream closed");
                coord.handle_disconnect(&edge_id).await;
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

#[cfg(test)]
mod reconfigure_tests {
    use super::MonitorCoordinator;
    use crate::monitor::coordinator::MonitorConfig;

    fn cfg(agg_id: u64, key: &str, tau: f64) -> MonitorConfig {
        MonitorConfig {
            agg_id,
            key: key.as_bytes().to_vec(),
            tau,
            epsilon: 0.2,
            window_ms: 15_000,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn reconfigure_adds_changes_and_removes_specs() {
        // Boot with one sum monitor.
        let coord = MonitorCoordinator::new(vec![cfg(1, "", 100.0)]);
        assert_eq!(coord.monitor_count(), 1);

        // Add a cms_point monitor (agg 2, key s0) and change agg 1's τ; agg 1's
        // key "" stays but τ differs -> counts as a change. Nothing removed.
        let (added, changed, removed) = coord
            .reconfigure(vec![cfg(1, "", 200.0), cfg(2, "s0", 5000.0)])
            .await;
        assert_eq!((added, changed, removed), (1, 1, 0));
        assert_eq!(coord.monitor_count(), 2);

        // Idempotent: same specs -> no-op.
        assert_eq!(
            coord
                .reconfigure(vec![cfg(1, "", 200.0), cfg(2, "s0", 5000.0)])
                .await,
            (0, 0, 0)
        );

        // Drop agg 1, keep agg 2 unchanged.
        let (added, changed, removed) =
            coord.reconfigure(vec![cfg(2, "s0", 5000.0)]).await;
        assert_eq!((added, changed, removed), (0, 0, 1));
        assert_eq!(coord.monitor_count(), 1);
    }

    #[tokio::test]
    async fn reconfigure_evicts_live_state_for_changed_monitor() {
        let coord = MonitorCoordinator::new(vec![cfg(7, "s0", 5000.0)]);
        // A register materializes live Monitor state for (7, s0).
        let actions = coord
            .apply_register(7, b"s0".to_vec(), "edge-a", 15_000, 15_000)
            .await;
        assert!(actions.is_some(), "register for a configured monitor succeeds");
        assert_eq!(coord.monitors.lock().await.len(), 1);

        // Changing the spec (new τ) must drop the stale live state so the next
        // register rebuilds the Monitor under the new τ.
        let (_, changed, _) = coord.reconfigure(vec![cfg(7, "s0", 9000.0)]).await;
        assert_eq!(changed, 1);
        assert_eq!(
            coord.monitors.lock().await.len(),
            0,
            "changed monitor's live state evicted"
        );

        // An unconfigured key is still rejected after reconfigure.
        assert!(coord
            .apply_register(7, b"other".to_vec(), "edge-a", 15_000, 15_000)
            .await
            .is_none());
    }
}
