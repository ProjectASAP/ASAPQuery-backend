//! tonic bidi-streaming `MonitorService` server. Each edge opens one
//! `Monitor` stream and multiplexes all of its monitors over it. The server
//! reads `EdgeToCoord` (register / report) messages, drives the per-monitor
//! [`Monitor`] state machine, and pushes the resulting `CoordToEdge`
//! (slack-grant) messages back to the addressed edge's stream. Global-threshold
//! alerts go out-of-band through the control-plane violation sink.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use asap_otel_proto::monitor::v1::{
    coord_to_edge, edge_to_coord,
    monitor_service_server::{MonitorService, MonitorServiceServer},
    CoordToEdge, EdgeToCoord, MonitorReport, RefBroadcast, SlackGrant,
};

use super::alert::{global_threshold_violation, AlertSink};
use super::coordinator::{Action, Monitor, MonitorConfig};
use super::f2_coord::{F2CoordMonitor, F2Out};

type MonKey = (u64, Vec<u8>);
type EdgeTx = mpsc::Sender<Result<CoordToEdge, Status>>;

/// Shared coordinator state behind the gRPC service. One per data-plane process.
pub struct MonitorCoordinator {
    /// Live per-monitor scalar state machines, created lazily from `cfgs`.
    monitors: Mutex<HashMap<MonKey, Monitor>>,
    /// Live per-monitor whole-sketch (F2) state machines, created lazily from
    /// `cfgs` for monitors whose functional is `F2`.
    f2_monitors: Mutex<HashMap<MonKey, F2CoordMonitor>>,
    /// F2 communication accounting (for the eval): total msgpack sketch bytes
    /// received from edges, total reference-broadcast bytes egressed, and the
    /// message counts. `f2_bytes_out` counts each `RefBroadcast` once per
    /// recipient edge.
    f2_bytes_in: AtomicU64,
    f2_bytes_out: AtomicU64,
    f2_ships_in: AtomicU64,
    f2_refs_out: AtomicU64,
    /// Monitor specs (τ, ε, window) from the streaming-config `monitors:`
    /// section — the authoritative source of τ. Behind an `RwLock` so the
    /// control plane's hot-reload (`reconfigure`) can add/update/remove monitors
    /// on a live coordinator without a process restart. Held only for brief,
    /// non-`await` critical sections, so a `std` lock is safe in async code.
    cfgs: std::sync::RwLock<HashMap<MonKey, MonitorConfig>>,
    /// Outbound stream sender per connected edge.
    edges: Mutex<HashMap<String, EdgeTx>>,
    /// Alert egress (control-plane violation sink).
    alert_sink: AlertSink,
}

impl MonitorCoordinator {
    pub fn new(cfgs: Vec<MonitorConfig>, alert_sink: AlertSink) -> Arc<Self> {
        let cfgs = cfgs
            .into_iter()
            .map(|c| ((c.agg_id, c.key.clone()), c))
            .collect();
        Arc::new(Self {
            monitors: Mutex::new(HashMap::new()),
            f2_monitors: Mutex::new(HashMap::new()),
            f2_bytes_in: AtomicU64::new(0),
            f2_bytes_out: AtomicU64::new(0),
            f2_ships_in: AtomicU64::new(0),
            f2_refs_out: AtomicU64::new(0),
            cfgs: std::sync::RwLock::new(cfgs),
            edges: Mutex::new(HashMap::new()),
            alert_sink,
        })
    }

    /// F2 communication accounting `(sketch_bytes_in, ref_bytes_out, ships_in,
    /// refs_out)` — the eval reads this to compare geometric vs distributed cost.
    pub fn f2_comm(&self) -> (u64, u64, u64, u64) {
        (
            self.f2_bytes_in.load(Ordering::Relaxed),
            self.f2_bytes_out.load(Ordering::Relaxed),
            self.f2_ships_in.load(Ordering::Relaxed),
            self.f2_refs_out.load(Ordering::Relaxed),
        )
    }

    /// Number of configured monitors (test/observability).
    pub fn monitor_count(&self) -> usize {
        self.cfgs.read().unwrap().len()
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
    /// Unchanged monitors keep their in-flight slack-countdown state untouched.
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
            let mut f2 = self.f2_monitors.lock().await;
            for k in &evict {
                monitors.remove(k);
                f2.remove(k);
            }
        }
        (added, changed, removed)
    }

    fn monitor_id(agg_id: u64, key: &[u8]) -> String {
        if key.is_empty() {
            format!("agg:{agg_id}/sum")
        } else {
            format!("agg:{agg_id}/{}", String::from_utf8_lossy(key))
        }
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
        // Whole-sketch (F2) monitors do not run the scalar slack countdown:
        // lazily materialize their F2 state and return no grant actions (the
        // edge ships/gates sketches on its own window cadence).
        if cfg.functional.is_whole_sketch() {
            let mut f2 = self.f2_monitors.lock().await;
            f2.entry(mk).or_insert_with(|| {
                F2CoordMonitor::new(cfg.f2_mode, cfg.tau, cfg.epsilon, cfg.f2_d, cfg.f2_w)
            });
            return Some(Vec::new());
        }
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
        local_value: f64,
        seq: u64,
        rate: f64,
    ) -> Option<Vec<Action>> {
        let mk = (agg_id, key);
        let mut monitors = self.monitors.lock().await;
        let mon = monitors.get_mut(&mk)?;
        Some(mon.on_report(edge_id, window_start_ms, local_value, seq, rate))
    }

    /// Handle a whole-sketch (F2) report: decode the msgpack Count-Sketch cell
    /// matrix, feed the F2 coordinator, and dispatch the resulting actions
    /// (alert to the sink; geometric `RefBroadcast` to every edge). Byte
    /// accounting feeds the eval.
    async fn apply_f2_report(&self, rep: MonitorReport) {
        let mk = (rep.agg_id, rep.key.clone());
        let bytes_len = rep.sketch.len();
        // Wire form is the 3-element msgpack array `[rows, cols, matrix]` that
        // asapmsgpack.MarshalCountSketch (Go edge) emits — decode it as that exact
        // tuple (NOT the 4-field portable::CountSketch, whose `to_msgpack` also
        // writes a `topk` element the Go side does not read).
        let matrix = match rmp_serde::from_slice::<(u64, u64, Vec<Vec<f64>>)>(&rep.sketch) {
            Ok((_rows, _cols, m)) => m,
            Err(e) => {
                warn!(agg_id = rep.agg_id, edge = %rep.edge_id, error = %e,
                      "F2 report carried an undecodable Count-Sketch — dropped");
                return;
            }
        };
        self.f2_bytes_in.fetch_add(bytes_len as u64, Ordering::Relaxed);
        self.f2_ships_in.fetch_add(1, Ordering::Relaxed);
        let outs = {
            let mut f2 = self.f2_monitors.lock().await;
            match f2.get_mut(&mk) {
                Some(mon) => mon.on_sketch(&rep.edge_id, rep.window_start_ms, matrix),
                None => return, // register precedes reports
            }
        };
        self.dispatch_f2(rep.agg_id, &rep.key, outs).await;
    }

    /// Dispatch F2 coordinator actions. Alerts egress through the violation sink;
    /// a `RefBroadcast` is serialized once and sent to every connected edge (each
    /// tags its agg_id/key so non-subscribers ignore it).
    async fn dispatch_f2(&self, agg_id: u64, key: &[u8], outs: Vec<F2Out>) {
        for out in outs {
            match out {
                F2Out::Alert {
                    f2_estimate,
                    tau,
                    window_start_ms,
                } => {
                    let id = Self::monitor_id(agg_id, key);
                    info!(monitor = %id, f2_estimate, tau, window_start_ms,
                          "F2 global threshold crossed — firing alert");
                    (self.alert_sink)(global_threshold_violation(id, f2_estimate, tau));
                }
                F2Out::RefBroadcast {
                    round,
                    window_start_ms,
                    k,
                    c_ref,
                } => {
                    let rows = c_ref.len() as u64;
                    let cols = c_ref.first().map(|r| r.len()).unwrap_or(0) as u64;
                    // Serialize as the same 3-element `[rows, cols, matrix]` array the
                    // Go edge's asapmsgpack.UnmarshalCountSketch expects.
                    let bytes = match rmp_serde::to_vec(&(rows, cols, c_ref)) {
                        Ok(b) => b,
                        Err(e) => {
                            warn!(error = %e, "failed to serialize C_ref — skipping broadcast");
                            continue;
                        }
                    };
                    let msg = CoordToEdge {
                        msg: Some(coord_to_edge::Msg::Ref(RefBroadcast {
                            agg_id,
                            key: key.to_vec(),
                            round,
                            window_start_ms,
                            k,
                            c_ref: bytes.clone(),
                        })),
                    };
                    let txs: Vec<EdgeTx> = {
                        let edges = self.edges.lock().await;
                        edges.values().cloned().collect()
                    };
                    for tx in &txs {
                        if tx.send(Ok(msg.clone())).await.is_err() {
                            debug!("C_ref broadcast send failed (edge gone)");
                        }
                    }
                    self.f2_bytes_out
                        .fetch_add(bytes.len() as u64 * txs.len() as u64, Ordering::Relaxed);
                    self.f2_refs_out
                        .fetch_add(txs.len() as u64, Ordering::Relaxed);
                }
            }
        }
    }

    /// Dispatch coordinator actions: grants to the addressed edge's stream,
    /// alerts to the violation sink.
    async fn dispatch(&self, agg_id: u64, key: &[u8], actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::Grant {
                    edge_id,
                    round,
                    local_slack,
                    window_start_ms,
                    sample_p,
                } => {
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
                Action::Alert {
                    global_estimate,
                    tau,
                    window_start_ms,
                } => {
                    let id = Self::monitor_id(agg_id, key);
                    info!(monitor = %id, global_estimate, tau, window_start_ms,
                          "CDM global threshold crossed — firing alert");
                    (self.alert_sink)(global_threshold_violation(id, global_estimate, tau));
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
                // Whole-sketch (F2) reports carry a msgpack sketch payload and are
                // routed to the F2 coordinator; scalar reports drive the countdown.
                let is_f2 = {
                    let cfgs = self.cfgs.read().unwrap();
                    cfgs.get(&(rep.agg_id, rep.key.clone()))
                        .map(|c| c.functional.is_whole_sketch())
                        .unwrap_or(false)
                };
                if is_f2 {
                    self.apply_f2_report(rep).await;
                } else if let Some(actions) = self
                    .apply_report(
                        rep.agg_id,
                        rep.key.clone(),
                        &rep.edge_id,
                        rep.window_start_ms,
                        rep.local_value,
                        rep.seq,
                        rep.rate,
                    )
                    .await
                {
                    self.dispatch(rep.agg_id, &rep.key, actions).await;
                }
            }
            None => {}
        }
    }

    /// An edge stream closed: drop its sender and fold its last-known value into
    /// every monitor's departed mass, then route any re-grants. Actions are
    /// collected under the monitors lock and dispatched after releasing it, so
    /// no send is awaited while the lock is held.
    async fn handle_disconnect(&self, edge_id: &str) {
        self.edges.lock().await.remove(edge_id);
        let pending: Vec<(u64, Vec<u8>, Vec<Action>)> = {
            let mut monitors = self.monitors.lock().await;
            monitors
                .iter_mut()
                .map(|(mk, mon)| (mk.0, mk.1.clone(), mon.on_leave(edge_id)))
                .filter(|(_, _, actions)| !actions.is_empty())
                .collect()
        };
        for (agg_id, key, actions) in pending {
            self.dispatch(agg_id, &key, actions).await;
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
    use crate::monitor::alert::AlertSink;
    use crate::monitor::coordinator::MonitorConfig;
    use std::sync::Arc;

    fn sink() -> AlertSink {
        Arc::new(|_v| {})
    }

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
        let coord = MonitorCoordinator::new(vec![cfg(1, "", 100.0)], sink());
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
        let coord = MonitorCoordinator::new(vec![cfg(7, "s0", 5000.0)], sink());
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
