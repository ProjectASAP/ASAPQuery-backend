//! tonic bidi-streaming `MonitorService` server. Each edge opens one
//! `Monitor` stream and multiplexes all of its monitors over it. The server
//! reads `EdgeToCoord` (register / report) messages, drives the per-monitor
//! [`Monitor`] state machine, and pushes the resulting `CoordToEdge`
//! (slack-grant) messages back to the addressed edge's stream. Global-threshold
//! alerts go out-of-band through the control-plane violation sink.

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

use super::alert::{global_threshold_violation, AlertSink};
use super::coordinator::{Action, Monitor, MonitorConfig};

type MonKey = (u64, Vec<u8>);
type EdgeTx = mpsc::Sender<Result<CoordToEdge, Status>>;

/// Shared coordinator state behind the gRPC service. One per data-plane process.
pub struct MonitorCoordinator {
    /// Live per-monitor state machines, created lazily from `cfgs`.
    monitors: Mutex<HashMap<MonKey, Monitor>>,
    /// Static monitor specs (τ, ε, window) from the streaming-config
    /// `monitors:` section — the authoritative source of τ.
    cfgs: HashMap<MonKey, MonitorConfig>,
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
            cfgs,
            edges: Mutex::new(HashMap::new()),
            alert_sink,
        })
    }

    /// Number of configured monitors (test/observability).
    pub fn monitor_count(&self) -> usize {
        self.cfgs.len()
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
        let cfg = self.cfgs.get(&mk)?.clone();
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
    ) -> Option<Vec<Action>> {
        let mk = (agg_id, key);
        let mut monitors = self.monitors.lock().await;
        let mon = monitors.get_mut(&mk)?;
        Some(mon.on_report(edge_id, window_start_ms, local_value, seq))
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
                } => {
                    let msg = CoordToEdge {
                        msg: Some(coord_to_edge::Msg::Grant(SlackGrant {
                            agg_id,
                            key: key.to_vec(),
                            round,
                            local_slack,
                            window_start_ms,
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
                if let Some(actions) = self
                    .apply_report(
                        rep.agg_id,
                        rep.key.clone(),
                        &rep.edge_id,
                        rep.window_start_ms,
                        rep.local_value,
                        rep.seq,
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
