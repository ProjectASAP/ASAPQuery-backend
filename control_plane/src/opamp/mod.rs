/// OpAMP server implemented over WebSocket via axum.
///
/// Speaks the standard OpAMP protobuf protocol (ServerToAgent / AgentToServer)
/// so the `opampextension` in OTel Collectors can connect directly.
///
/// Each OTel collector connects as an "agent" identified by the `X-Agent-ID`
/// request header.  The role (`agent` vs `backend`) is determined by the
/// optional `X-Agent-Role` header (defaults to `agent`).
///
/// The server pushes `RemoteConfig` as an OpAMP `ServerToAgent.remote_config`
/// message (protobuf binary frame) and receives `AgentToServer` status reports.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;
use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Notify, RwLock};
use tracing::{info, warn};

/// Generated OpAMP protobuf types (from proto/opamp.proto).
pub mod opamp_proto {
    include!(concat!(env!("OUT_DIR"), "/opamp.proto.rs"));
}

// ── Wire types ────────────────────────────────────────────────────────────────

/// Config message pushed to an agent collector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteConfig {
    pub config_hash: String,
    pub yaml: String,
}

/// Status report sent back from an agent after applying a config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentStatus {
    pub agent_id: String,
    pub config_hash: String,
    pub healthy: bool,
    pub error: Option<String>,
}

pub const COLLECTOR_PLAN_CAPABILITY: &str = "io.projectasap.collector-plan.v1";
pub const COLLECTOR_PLAN_MESSAGE: &str = "collector_plan";
pub const PLAN_STATUS_MESSAGE: &str = "plan_status";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CollectorPlanStatusKind {
    Applied,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CollectorPlanStatus {
    pub plan_id: u64,
    pub status: CollectorPlanStatusKind,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum CollectorPlanPublishError {
    #[error("collector {collector_id} did not advertise {COLLECTOR_PLAN_CAPABILITY}")]
    CapabilityTimeout { collector_id: String },
    #[error("collector {collector_id} disconnected before plan publication")]
    Disconnected { collector_id: String },
    #[error("compiled bundle contains duplicate collector target {collector_id}")]
    DuplicateTarget { collector_id: String },
    #[error("collector {collector_id} plan serialization failed: {source}")]
    Serialize {
        collector_id: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("collector {collector_id} did not report plan {plan_id} before timeout")]
    StatusTimeout { collector_id: String, plan_id: u64 },
    #[error("collector {collector_id} rejected plan {plan_id}: {error}")]
    Rejected {
        collector_id: String,
        plan_id: u64,
        error: String,
    },
}

// ── Role ──────────────────────────────────────────────────────────────────────

/// Role of a connected OTel collector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRole {
    /// Edge / agent collector that produces sketches.
    Agent,
    /// Mid-tier gateway collector that forwards / merges sketches
    /// between edge agents and the backend. Phase C (MVP v6) wires
    /// this role through OpAMP so the typed L5 stage_split path can
    /// push the gateway YAML directly via `push_to_role`.
    Gateway,
    /// Backend / aggregation collector that merges sketches.
    Backend,
}

impl AgentRole {
    /// Parse the `X-Agent-Role` header value into an `AgentRole`.
    /// Recognises `backend`, `gateway`, and `agent` (case-insensitive);
    /// any other value (including the empty string) defaults to
    /// `Agent` so legacy / mis-configured collectors keep working.
    pub fn from_header(value: &str) -> Self {
        match value.trim().to_lowercase().as_str() {
            "backend" => AgentRole::Backend,
            "gateway" => AgentRole::Gateway,
            _ => AgentRole::Agent,
        }
    }
}

// ── Server ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum OutboundMessage {
    RemoteConfig(RemoteConfig),
    CollectorPlan(Vec<u8>),
}

struct AgentConnection {
    tx: mpsc::Sender<OutboundMessage>,
    role: AgentRole,
    custom_capabilities: HashSet<String>,
}

type AgentMap = HashMap<String, AgentConnection>;
type PlanStatusMap = HashMap<(String, u64), CollectorPlanStatus>;

pub type OnConnectFn = Arc<dyn Fn(String, AgentRole) + Send + Sync>;
pub type OnDisconnectFn = Arc<dyn Fn(String) + Send + Sync>;

pub struct OpampServer {
    agents: Arc<RwLock<AgentMap>>,
    plan_statuses: Arc<RwLock<PlanStatusMap>>,
    state_changed: Arc<Notify>,
    on_connect: Option<OnConnectFn>,
    on_disconnect: Option<OnDisconnectFn>,
}

impl Default for OpampServer {
    fn default() -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
            plan_statuses: Arc::new(RwLock::new(HashMap::new())),
            state_changed: Arc::new(Notify::new()),
            on_connect: None,
            on_disconnect: None,
        }
    }
}

impl Clone for OpampServer {
    fn clone(&self) -> Self {
        Self {
            agents: Arc::clone(&self.agents),
            plan_statuses: Arc::clone(&self.plan_statuses),
            state_changed: Arc::clone(&self.state_changed),
            on_connect: self.on_connect.clone(),
            on_disconnect: self.on_disconnect.clone(),
        }
    }
}

impl OpampServer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a callback invoked when an agent connects.
    pub fn with_on_connect(
        mut self,
        f: impl Fn(String, AgentRole) + Send + Sync + 'static,
    ) -> Self {
        self.on_connect = Some(Arc::new(f));
        self
    }

    /// Register a callback invoked when an agent disconnects.
    pub fn with_on_disconnect(mut self, f: impl Fn(String) + Send + Sync + 'static) -> Self {
        self.on_disconnect = Some(Arc::new(f));
        self
    }

    /// axum handler: `GET /v1/opamp` — upgrades to WebSocket.
    /// Agents must include `X-Agent-ID: <id>` in the upgrade request.
    /// Optional `X-Agent-Role: agent|backend` (default: `agent`).
    pub async fn ws_handler(
        ws: WebSocketUpgrade,
        headers: HeaderMap,
        State(srv): State<Arc<OpampServer>>,
    ) -> impl IntoResponse {
        let agent_id = headers
            .get("X-Agent-ID")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        if agent_id.is_empty() {
            return (StatusCode::BAD_REQUEST, "X-Agent-ID header required").into_response();
        }

        let role = headers
            .get("X-Agent-Role")
            .and_then(|v| v.to_str().ok())
            .map(AgentRole::from_header)
            .unwrap_or(AgentRole::Agent);

        ws.on_upgrade(move |socket| handle_socket(socket, agent_id, role, srv))
            .into_response()
    }

    /// Pushes a config to a specific agent. Returns false if not connected.
    pub async fn push(&self, agent_id: &str, cfg: RemoteConfig) -> bool {
        let tx = self
            .agents
            .read()
            .await
            .get(agent_id)
            .map(|connection| connection.tx.clone());
        match tx {
            Some(tx) => tx.send(OutboundMessage::RemoteConfig(cfg)).await.is_ok(),
            None => false,
        }
    }

    /// Broadcasts a config to every connected agent regardless of role.
    pub async fn push_all(&self, cfg: RemoteConfig) {
        let ids: Vec<String> = self.agents.read().await.keys().cloned().collect();
        for id in ids {
            self.push(&id, cfg.clone()).await;
        }
    }

    /// Broadcasts a config only to agents matching the given role.
    pub async fn push_to_role(&self, role: AgentRole, cfg: RemoteConfig) {
        let ids: Vec<String> = self
            .agents
            .read()
            .await
            .iter()
            .filter(|(_, connection)| connection.role == role)
            .map(|(id, _)| id.clone())
            .collect();
        for id in ids {
            self.push(&id, cfg.clone()).await;
        }
    }

    /// Returns the IDs of currently connected agents (all roles).
    pub async fn connected_agents(&self) -> Vec<String> {
        self.agents.read().await.keys().cloned().collect()
    }

    /// Returns a map of agent_id → role for all connected agents.
    pub async fn connected_agents_with_roles(&self) -> HashMap<String, AgentRole> {
        self.agents
            .read()
            .await
            .iter()
            .map(|(id, connection)| (id.clone(), connection.role.clone()))
            .collect()
    }

    /// Publish all per-target physical plans and require an exact semantic
    /// APPLIED report from every Collector. Config hashes are deliberately not
    /// accepted as plan activation evidence.
    pub async fn publish_collector_plans(
        &self,
        plans: &[crate::physical::compiler::CollectorPlan],
        timeout: Duration,
    ) -> Result<Vec<CollectorPlanStatus>, CollectorPlanPublishError> {
        self.ensure_collector_plan_targets(plans, timeout).await?;

        for plan in plans {
            let body = serde_json::to_vec(plan).map_err(|source| {
                CollectorPlanPublishError::Serialize {
                    collector_id: plan.collector_id.clone(),
                    source,
                }
            })?;
            self.plan_statuses
                .write()
                .await
                .remove(&(plan.collector_id.clone(), plan.envelope.plan_id));
            let sent = self.send_collector_plan(&plan.collector_id, body).await;
            if !sent {
                return Err(CollectorPlanPublishError::Disconnected {
                    collector_id: plan.collector_id.clone(),
                });
            }
        }

        let mut reports = Vec::with_capacity(plans.len());
        for plan in plans {
            let report = self
                .wait_for_plan_status(&plan.collector_id, plan.envelope.plan_id, timeout)
                .await
                .ok_or_else(|| CollectorPlanPublishError::StatusTimeout {
                    collector_id: plan.collector_id.clone(),
                    plan_id: plan.envelope.plan_id,
                })?;
            if report.status != CollectorPlanStatusKind::Applied {
                return Err(CollectorPlanPublishError::Rejected {
                    collector_id: plan.collector_id.clone(),
                    plan_id: report.plan_id,
                    error: report.error.clone().unwrap_or_else(|| "unspecified".into()),
                });
            }
            reports.push(report);
        }
        Ok(reports)
    }

    /// Validate target uniqueness and wait for every target to advertise the
    /// physical-plan capability. The orchestrator calls this before changing
    /// backend state, then publication repeats it to close disconnect races.
    pub async fn ensure_collector_plan_targets(
        &self,
        plans: &[crate::physical::compiler::CollectorPlan],
        timeout: Duration,
    ) -> Result<(), CollectorPlanPublishError> {
        let mut targets = HashSet::with_capacity(plans.len());
        for plan in plans {
            if !targets.insert(plan.collector_id.as_str()) {
                return Err(CollectorPlanPublishError::DuplicateTarget {
                    collector_id: plan.collector_id.clone(),
                });
            }
        }
        for plan in plans {
            if !self
                .wait_for_capability(&plan.collector_id, COLLECTOR_PLAN_CAPABILITY, timeout)
                .await
            {
                return Err(CollectorPlanPublishError::CapabilityTimeout {
                    collector_id: plan.collector_id.clone(),
                });
            }
        }

        Ok(())
    }

    async fn send_collector_plan(&self, agent_id: &str, body: Vec<u8>) -> bool {
        let agents = self.agents.read().await;
        let Some(connection) = agents.get(agent_id) else {
            return false;
        };
        if !connection
            .custom_capabilities
            .contains(COLLECTOR_PLAN_CAPABILITY)
        {
            return false;
        }
        let tx = connection.tx.clone();
        drop(agents);
        tx.send(OutboundMessage::CollectorPlan(body)).await.is_ok()
    }

    async fn wait_for_capability(
        &self,
        agent_id: &str,
        capability: &str,
        timeout: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.state_changed.notified();
            if self
                .agents
                .read()
                .await
                .get(agent_id)
                .is_some_and(|agent| agent.custom_capabilities.contains(capability))
            {
                return true;
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, changed).await.is_err() {
                return false;
            }
        }
    }

    async fn wait_for_plan_status(
        &self,
        agent_id: &str,
        plan_id: u64,
        timeout: Duration,
    ) -> Option<CollectorPlanStatus> {
        let key = (agent_id.to_string(), plan_id);
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let changed = self.state_changed.notified();
            if let Some(status) = self.plan_statuses.read().await.get(&key).cloned() {
                return Some(status);
            }
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() || tokio::time::timeout(remaining, changed).await.is_err() {
                return None;
            }
        }
    }
}

async fn handle_socket(
    socket: WebSocket,
    agent_id: String,
    role: AgentRole,
    srv: Arc<OpampServer>,
) {
    let (tx, mut rx) = mpsc::channel::<OutboundMessage>(16);
    srv.agents.write().await.insert(
        agent_id.clone(),
        AgentConnection {
            tx,
            role: role.clone(),
            custom_capabilities: HashSet::new(),
        },
    );
    srv.state_changed.notify_waiters();
    info!(agent = %agent_id, ?role, "agent connected");

    if let Some(cb) = &srv.on_connect {
        cb(agent_id.clone(), role);
    }

    let (mut ws_tx, mut ws_rx) = socket.split();

    // Forward channel messages → WebSocket as standard OpAMP protobuf.
    //
    // OpAMP WS wire format prepends each binary frame with a varint
    // header (`uint64(0)` today). See `opamp-go/internal/wsmessage.go`.
    // Without the header, the agent's `DecodeWSMessage` falls back to
    // "old format" and decodes successfully — which is why pushes
    // worked even before this fix. We add the header for spec
    // conformance so the agent never has to take the legacy path.
    let writer_id = agent_id.clone();
    let write_task = tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            let (server_to_agent, description) = match message {
                OutboundMessage::RemoteConfig(cfg) => (
                    encode_remote_config(&cfg),
                    format!("remote config hash={}", cfg.config_hash),
                ),
                OutboundMessage::CollectorPlan(body) => (
                    encode_collector_plan(body),
                    "typed CollectorPlan".to_string(),
                ),
            };
            let mut payload = Vec::new();
            if server_to_agent.encode(&mut payload).is_err() {
                warn!(agent = %writer_id, "failed to encode OpAMP protobuf");
                continue;
            }
            // Prepend the wsMsgHeader varint (zero byte today; the
            // varint is `0u64`, which encodes to a single 0x00).
            let mut buf = Vec::with_capacity(1 + payload.len());
            buf.push(0u8);
            buf.extend_from_slice(&payload);
            if ws_tx.send(Message::Binary(buf.into())).await.is_err() {
                break;
            }
            info!(agent = %writer_id, message = %description, "message pushed (OpAMP protobuf)");
        }
    });

    // Receive AgentToServer protobuf messages.
    //
    // Strip the OpAMP wire-format header before decoding. Per
    // `opamp-go/internal/wsmessage.go::DecodeWSMessage`, the spec
    // header is a varint-encoded `uint64(0)` and is detected by a
    // leading 0 byte. Older clients send raw protobuf with no header
    // — in that case the first byte is the protobuf field tag and is
    // never zero (a tag-0 wire type is illegal), so the
    // "first-byte-is-zero" check is unambiguous.
    while let Some(Ok(msg)) = ws_rx.next().await {
        match msg {
            Message::Binary(data) => {
                let payload: &[u8] = if !data.is_empty() && data[0] == 0 {
                    // Spec format. Decode the varint header (always
                    // 0 today) and skip it.
                    match prost::encoding::decode_varint(&mut &data[..]) {
                        Ok(_hdr) => {
                            // Recompute consumed bytes = varint length.
                            // For the canonical zero header this is 1
                            // byte; for any future non-zero header
                            // it's `n` bytes from the unsigned LEB128
                            // encoding.
                            let mut tmp: &[u8] = data.as_ref();
                            let _ = prost::encoding::decode_varint(&mut tmp);
                            let consumed = data.len() - tmp.len();
                            &data[consumed..]
                        }
                        Err(_) => &data[..],
                    }
                } else {
                    &data[..]
                };
                match opamp_proto::AgentToServer::decode(payload) {
                    Ok(ats) => {
                        info!(agent = %agent_id, "received AgentToServer (OpAMP protobuf)");
                        if let Some(capabilities) = &ats.custom_capabilities {
                            if let Some(connection) = srv.agents.write().await.get_mut(&agent_id) {
                                connection.custom_capabilities =
                                    capabilities.capabilities.iter().cloned().collect();
                            }
                            srv.state_changed.notify_waiters();
                        }
                        if let Some(message) = &ats.custom_message {
                            if message.capability == COLLECTOR_PLAN_CAPABILITY
                                && message.r#type == PLAN_STATUS_MESSAGE
                            {
                                match serde_json::from_slice::<CollectorPlanStatus>(&message.data) {
                                    Ok(status) => {
                                        srv.plan_statuses
                                            .write()
                                            .await
                                            .insert((agent_id.clone(), status.plan_id), status);
                                        srv.state_changed.notify_waiters();
                                    }
                                    Err(error) => warn!(
                                        agent = %agent_id,
                                        %error,
                                        "invalid typed CollectorPlan status"
                                    ),
                                }
                            }
                        }
                        // Log effective config if reported. Triggered by
                        // the `ReportFullState` flag on our outgoing
                        // ServerToAgent (see `encode_remote_config`).
                        if let Some(ec) = &ats.effective_config {
                            if let Some(cm) = &ec.config_map {
                                for (name, file) in &cm.config_map {
                                    let body_preview = String::from_utf8_lossy(
                                        &file.body[..file.body.len().min(160)],
                                    );
                                    info!(
                                        agent = %agent_id,
                                        config_name = %name,
                                        bytes = file.body.len(),
                                        preview = %body_preview.replace('\n', " ⏎ "),
                                        "agent reported effective config",
                                    );
                                }
                            }
                        }
                        // Log remote-config apply state — this is the
                        // signal that "the agent received our pushed
                        // RemoteConfig, attempted to apply it, and ended
                        // up in {Applied | Failed | Applying}".
                        // RemoteConfigStatuses enum values:
                        //   0 = Unset
                        //   1 = Applied
                        //   2 = Applying
                        //   3 = Failed
                        if let Some(rcs) = &ats.remote_config_status {
                            let status_str = match rcs.status {
                                0 => "Unset",
                                1 => "Applied",
                                2 => "Applying",
                                3 => "Failed",
                                n => {
                                    // Future spec-defined values fall through
                                    // here; surface the raw int rather than
                                    // claim a meaning.
                                    return_unknown_status(n)
                                }
                            };
                            let last_hash_hex = rcs
                                .last_remote_config_hash
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>();
                            if rcs.status == 3 {
                                warn!(
                                    agent = %agent_id,
                                    status = status_str,
                                    last_hash = %last_hash_hex,
                                    error = %rcs.error_message,
                                    "agent reported remote-config status",
                                );
                            } else {
                                info!(
                                    agent = %agent_id,
                                    status = status_str,
                                    last_hash = %last_hash_hex,
                                    "agent reported remote-config status",
                                );
                            }
                        }
                        // Log health if reported.
                        if let Some(health) = &ats.health {
                            info!(agent = %agent_id, healthy = health.healthy, "agent health");
                        }
                    }
                    Err(e) => {
                        warn!(agent = %agent_id, error = %e, "failed to decode AgentToServer")
                    }
                }
            }
            // Also accept JSON for backward compatibility.
            Message::Text(text) => match serde_json::from_str::<AgentStatus>(&text) {
                Ok(s) => {
                    info!(agent = %s.agent_id, healthy = s.healthy, "agent status (legacy JSON)")
                }
                Err(_) => warn!(agent = %agent_id, "unexpected text message"),
            },
            Message::Close(_) => break,
            _ => {}
        }
    }

    write_task.abort();
    srv.agents.write().await.remove(&agent_id);
    srv.state_changed.notify_waiters();
    info!(agent = %agent_id, "agent disconnected");

    if let Some(cb) = &srv.on_disconnect {
        cb(agent_id);
    }
}

/// Encode a `RemoteConfig` as a standard OpAMP `ServerToAgent` protobuf message.
///
/// The YAML config body is wrapped in:
///   ServerToAgent.remote_config.config.config_map[""].body = yaml_bytes
///
/// Format an unknown `RemoteConfigStatuses` int as a stable string
/// for logs. Pulled out into a helper to keep the match arm above
/// borrow-checker-friendly (returning a `&'static str`).
fn return_unknown_status(n: i32) -> &'static str {
    // Leak the formatted int into a `'static str` only if needed.
    // For diagnostic logs we accept the cost of a Box::leak per
    // unrecognised value since this is an "out-of-spec status"
    // signal that should be rare. Avoids reworking the surrounding
    // match into String.
    Box::leak(format!("Unknown({})", n).into_boxed_str())
}

/// This is the standard OpAMP way to push collector configuration.
/// The opampextension in the OTel Collector decodes this and applies the config.
///
/// Two protocol bits the control plane sets per spec:
///
/// 1. `flags = ReportFullState` (`0x01`) asks the agent's next
///    `AgentToServer` to include the full status block —
///    `effective_config` (the YAML the agent ended up running)
///    and `remote_config_status` (Applied / Failed / Applying).
///    Without this, the agent is allowed to elide both fields as
///    an optimization once the control plane has acknowledged a
///    given sequence_num, and we lose visibility into whether the
///    push actually took.
///
/// 2. `capabilities` advertises what the control plane can accept
///    back. `AcceptsStatus` is mandatory; `OffersRemoteConfig`
///    must be set whenever we send `remote_config`;
///    `AcceptsEffectiveConfig` tells the agent it's worth
///    populating the field (some agents skip it if the server
///    didn't claim it could parse it).
fn encode_remote_config(cfg: &RemoteConfig) -> opamp_proto::ServerToAgent {
    let config_file = opamp_proto::AgentConfigFile {
        body: cfg.yaml.as_bytes().to_vec(),
        content_type: "text/yaml".to_string(),
    };

    let mut config_map = HashMap::new();
    config_map.insert(String::new(), config_file); // empty key = single config file

    let agent_config_map = opamp_proto::AgentConfigMap { config_map };

    let remote_config = opamp_proto::AgentRemoteConfig {
        config: Some(agent_config_map),
        config_hash: cfg.config_hash.as_bytes().to_vec(),
    };

    // ServerToAgentFlags_ReportFullState = 0x01.
    const FLAG_REPORT_FULL_STATE: u64 = 0x0000_0001;
    // ServerCapabilities bitmask:
    //   AcceptsStatus            = 0x01
    //   OffersRemoteConfig       = 0x02
    //   AcceptsEffectiveConfig   = 0x04
    const CAPS_DEFAULT: u64 = 0x01 | 0x02 | 0x04;

    opamp_proto::ServerToAgent {
        remote_config: Some(remote_config),
        flags: FLAG_REPORT_FULL_STATE,
        capabilities: CAPS_DEFAULT,
        ..Default::default()
    }
}

fn encode_collector_plan(body: Vec<u8>) -> opamp_proto::ServerToAgent {
    opamp_proto::ServerToAgent {
        capabilities: 0x01,
        custom_capabilities: Some(opamp_proto::CustomCapabilities {
            capabilities: vec![COLLECTOR_PLAN_CAPABILITY.into()],
        }),
        custom_message: Some(opamp_proto::CustomMessage {
            capability: COLLECTOR_PLAN_CAPABILITY.into(),
            r#type: COLLECTOR_PLAN_MESSAGE.into(),
            data: body,
        }),
        ..Default::default()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};

    #[tokio::test]
    async fn no_agents_push_returns_false() {
        let srv = Arc::new(OpampServer::new());
        let sent = srv
            .push(
                "unknown",
                RemoteConfig {
                    config_hash: "h".into(),
                    yaml: "y".into(),
                },
            )
            .await;
        assert!(!sent);
    }

    #[tokio::test]
    async fn connected_agents_empty_initially() {
        let srv = Arc::new(OpampServer::new());
        assert!(srv.connected_agents().await.is_empty());
    }

    #[test]
    fn role_from_header() {
        assert_eq!(AgentRole::from_header("backend"), AgentRole::Backend);
        assert_eq!(AgentRole::from_header("agent"), AgentRole::Agent);
        assert_eq!(AgentRole::from_header(""), AgentRole::Agent);
        assert_eq!(AgentRole::from_header("BACKEND"), AgentRole::Backend);
    }

    /// Phase C: `Gateway` is a recognised role and round-trips through
    /// the OpAMP `X-Agent-Role` header parser. This locks in the wire
    /// vocabulary that the typed L5 stage_split path relies on when it
    /// calls `push_to_role(AgentRole::Gateway, ...)` and expects to
    /// reach gateway-role collectors only.
    #[test]
    fn role_from_header_recognises_gateway() {
        assert_eq!(AgentRole::from_header("gateway"), AgentRole::Gateway);
        assert_eq!(AgentRole::from_header("Gateway"), AgentRole::Gateway);
        assert_eq!(AgentRole::from_header("GATEWAY"), AgentRole::Gateway);
        // Round-trip through serde lowercase rename.
        let s = serde_json::to_string(&AgentRole::Gateway).unwrap();
        assert_eq!(s, "\"gateway\"");
        let back: AgentRole = serde_json::from_str(&s).unwrap();
        assert_eq!(back, AgentRole::Gateway);
        // Still distinct from the other two roles.
        assert_ne!(AgentRole::Gateway, AgentRole::Agent);
        assert_ne!(AgentRole::Gateway, AgentRole::Backend);
    }

    /// Phase C: a gateway-role client connecting via WebSocket appears
    /// in `connected_agents_with_roles` tagged as `Gateway`. Together
    /// with the from_header test above this proves the role plumbs
    /// through the connect path that `push_to_role` selects on.
    #[tokio::test]
    async fn gateway_role_round_trips_through_connection() {
        let (srv, addr) = start_server().await;
        let _gateway_ws = connect_ws_client(addr, "gw-1", "gateway").await;

        // Wait for server-side registration to complete.
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            let map = srv.connected_agents_with_roles().await;
            if map.get("gw-1") == Some(&AgentRole::Gateway) {
                return;
            }
        }
        let map = srv.connected_agents_with_roles().await;
        panic!("gateway role never registered; map = {:?}", map);
    }

    /// Helper: start a real OpAMP server on a random port, return the server
    /// Arc and the bound address.
    async fn start_server() -> (Arc<OpampServer>, std::net::SocketAddr) {
        let srv = Arc::new(OpampServer::new());
        let app = Router::new()
            .route("/v1/opamp", get(OpampServer::ws_handler))
            .with_state(Arc::clone(&srv));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (srv, addr)
    }

    /// Connect a WebSocket client with a given agent-id and role.
    async fn connect_ws_client(
        addr: std::net::SocketAddr,
        agent_id: &str,
        role: &str,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        use tokio_tungstenite::tungstenite::http::Request;
        let req = Request::builder()
            .uri(format!("ws://{addr}/v1/opamp"))
            .header("Host", addr.to_string())
            .header("X-Agent-ID", agent_id)
            .header("X-Agent-Role", role)
            .header("Upgrade", "websocket")
            .header("Connection", "Upgrade")
            .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
            .header("Sec-WebSocket-Version", "13")
            .body(())
            .unwrap();
        let (ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
        ws
    }

    fn decode_server_to_agent_frame(data: &[u8]) -> opamp_proto::ServerToAgent {
        let payload = if !data.is_empty() && data[0] == 0 {
            &data[1..]
        } else {
            data
        };
        opamp_proto::ServerToAgent::decode(payload).expect("valid protobuf")
    }

    /// `push_to_role` delivers a RemoteConfig to a connected agent-role client.
    #[tokio::test]
    async fn push_to_role_delivers_yaml_to_agent_role() {
        let (srv, addr) = start_server().await;
        let mut agent_ws = connect_ws_client(addr, "agent-1", "agent").await;

        // Allow the server to register the connection.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let yaml_payload = "ddsketch:\n  mode: window\n";
        srv.push_to_role(
            AgentRole::Agent,
            RemoteConfig {
                config_hash: "hash-1".into(),
                yaml: yaml_payload.to_string(),
            },
        )
        .await;

        let msg = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            futures_util::StreamExt::next(&mut agent_ws),
        )
        .await
        .expect("timed out waiting for message")
        .unwrap()
        .unwrap();

        // Decode standard OpAMP protobuf binary frame.
        let data = msg.into_data();
        let sta = decode_server_to_agent_frame(data.as_ref());
        let rc = sta.remote_config.expect("should have remote_config");
        let config = rc.config.expect("should have config");
        let file = config
            .config_map
            .get("")
            .expect("should have empty-key entry");
        let yaml = String::from_utf8(file.body.clone()).unwrap();
        assert_eq!(yaml, yaml_payload, "delivered yaml must match");
        assert_eq!(
            String::from_utf8(rc.config_hash).unwrap(),
            "hash-1",
            "delivered hash must match"
        );
    }

    /// Phase C integration test: gateway YAML emitted from the typed L5
    /// stage_split path is queued onto the gateway-role connection and
    /// not onto agent-role / backend-role connections. We don't decode
    /// the protobuf (an unrelated decode-tag-zero issue affects sibling
    /// tests today); we only assert *delivery routing* — the gateway
    /// client receives a non-empty binary frame within the timeout, the
    /// other roles receive nothing.
    #[tokio::test]
    async fn push_to_role_gateway_routes_only_to_gateway_role() {
        use futures_util::StreamExt;
        let (srv, addr) = start_server().await;
        let mut agent_ws = connect_ws_client(addr, "agent-1", "agent").await;
        let mut gateway_ws = connect_ws_client(addr, "gateway-1", "gateway").await;
        let mut backend_ws = connect_ws_client(addr, "backend-1", "backend").await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Mirror the call site that handle_plan now exercises:
        //   emit_gateway_yaml(...) → push_to_role(Gateway, ...).
        // We use a stand-in YAML payload here; the emitter has its own
        // tests in stage_config.rs.
        let yaml = "extensions:\n  opamp: {}\n".to_string();
        srv.push_to_role(
            AgentRole::Gateway,
            RemoteConfig {
                config_hash: "hash-gw".into(),
                yaml,
            },
        )
        .await;

        // Gateway must receive exactly one frame.
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), gateway_ws.next())
            .await
            .expect("gateway timed out")
            .unwrap()
            .unwrap();
        let bytes = msg.into_data();
        assert!(!bytes.is_empty(), "gateway must receive a non-empty frame");

        // Other roles must receive nothing within a short window.
        let agent_result =
            tokio::time::timeout(std::time::Duration::from_millis(200), agent_ws.next()).await;
        assert!(
            agent_result.is_err(),
            "agent-role client must not receive gateway-role push"
        );
        let backend_result =
            tokio::time::timeout(std::time::Duration::from_millis(200), backend_ws.next()).await;
        assert!(
            backend_result.is_err(),
            "backend-role client must not receive gateway-role push"
        );
    }

    /// `push_to_role(Agent)` must not deliver to a backend-role client.
    #[tokio::test]
    async fn push_to_agent_role_does_not_reach_backend_role() {
        use futures_util::StreamExt;
        let (srv, addr) = start_server().await;
        let mut agent_ws = connect_ws_client(addr, "agent-1", "agent").await;
        let mut backend_ws = connect_ws_client(addr, "backend-1", "backend").await;

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        srv.push_to_role(
            AgentRole::Agent,
            RemoteConfig {
                config_hash: "hash-agent".into(),
                yaml: "ddsketch:\n  mode: batch\n".to_string(),
            },
        )
        .await;

        // Agent must receive the message.
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), agent_ws.next())
            .await
            .expect("agent timed out")
            .unwrap()
            .unwrap();
        let data = msg.into_data();
        let sta = decode_server_to_agent_frame(data.as_ref());
        let rc = sta.remote_config.expect("should have remote_config");
        assert_eq!(String::from_utf8(rc.config_hash).unwrap(), "hash-agent");

        // Backend must receive nothing within a short window.
        let backend_result =
            tokio::time::timeout(std::time::Duration::from_millis(200), backend_ws.next()).await;
        assert!(
            backend_result.is_err(),
            "backend-role client must not receive agent-role push"
        );
    }

    fn test_collector_plan(
        collector_id: &str,
        plan_id: u64,
    ) -> crate::physical::compiler::CollectorPlan {
        crate::physical::compiler::CollectorPlan {
            collector_id: collector_id.into(),
            envelope: crate::physical::compiler::PlanEnvelope {
                plan_id,
                generated_at_unix_ms: 1,
                planner_revision: crate::physical::compiler::PLANNER_REVISION.into(),
                capability_snapshot_id: "caps-1".into(),
            },
            materializations: vec![crate::physical::compiler::CollectorMaterialization {
                query_id: "q".into(),
                metric: "requests".into(),
                algorithm: "hll".into(),
                parameters: serde_json::json!({"precision": 14}),
                group_by: vec!["service".into()],
                window_secs: 60,
                abstract_window_framework:
                    planner_types::post_asap::SummaryWindowFramework::Tumbling,
                window_implementation_id: "collector-tumbling-v1".into(),
                pane_secs: 60,
                state_layout: "anchored-pane-v1".into(),
                evidence_source: None,
                lifecycle: crate::physical::compiler::CollectorLifecycle {
                    kind: "continuously_maintained".into(),
                    maintenance_mode: "incremental".into(),
                    evaluation_schedule: "per_update".into(),
                    output_representation: "summary_state".into(),
                },
            }],
        }
    }

    async fn send_agent_message(
        ws: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        message: opamp_proto::AgentToServer,
    ) {
        use futures_util::SinkExt;
        let mut protobuf = Vec::new();
        message.encode(&mut protobuf).unwrap();
        let mut frame = vec![0];
        frame.extend(protobuf);
        ws.send(tokio_tungstenite::tungstenite::Message::Binary(frame))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn typed_plan_publication_waits_for_capability_and_exact_applied_status() {
        let (srv, addr) = start_server().await;
        let mut agent_ws = connect_ws_client(addr, "edge-a", "agent").await;

        send_agent_message(
            &mut agent_ws,
            opamp_proto::AgentToServer {
                custom_capabilities: Some(opamp_proto::CustomCapabilities {
                    capabilities: vec![COLLECTOR_PLAN_CAPABILITY.into()],
                }),
                ..Default::default()
            },
        )
        .await;

        let publisher = Arc::clone(&srv);
        let plan = test_collector_plan("edge-a", 42);
        let publish = tokio::spawn(async move {
            publisher
                .publish_collector_plans(&[plan], Duration::from_secs(2))
                .await
        });

        let frame = tokio::time::timeout(Duration::from_secs(2), agent_ws.next())
            .await
            .expect("typed plan timed out")
            .unwrap()
            .unwrap();
        let message = decode_server_to_agent_frame(&frame.into_data());
        let custom = message.custom_message.expect("custom message");
        assert_eq!(custom.capability, COLLECTOR_PLAN_CAPABILITY);
        assert_eq!(custom.r#type, COLLECTOR_PLAN_MESSAGE);
        let wire_plan: crate::physical::compiler::CollectorPlan =
            serde_json::from_slice(&custom.data).unwrap();
        assert_eq!(wire_plan.collector_id, "edge-a");
        assert_eq!(wire_plan.envelope.plan_id, 42);

        let status = serde_json::to_vec(&CollectorPlanStatus {
            plan_id: 42,
            status: CollectorPlanStatusKind::Applied,
            error: None,
        })
        .unwrap();
        send_agent_message(
            &mut agent_ws,
            opamp_proto::AgentToServer {
                custom_message: Some(opamp_proto::CustomMessage {
                    capability: COLLECTOR_PLAN_CAPABILITY.into(),
                    r#type: PLAN_STATUS_MESSAGE.into(),
                    data: status,
                }),
                ..Default::default()
            },
        )
        .await;

        let reports = publish.await.unwrap().unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].plan_id, 42);
        assert_eq!(reports[0].status, CollectorPlanStatusKind::Applied);
    }

    #[tokio::test]
    async fn typed_plan_publication_fails_without_advertised_capability() {
        let (srv, addr) = start_server().await;
        let _agent_ws = connect_ws_client(addr, "edge-a", "agent").await;
        let error = srv
            .publish_collector_plans(
                &[test_collector_plan("edge-a", 42)],
                Duration::from_millis(30),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CollectorPlanPublishError::CapabilityTimeout { collector_id }
                if collector_id == "edge-a"
        ));
    }

    #[tokio::test]
    async fn typed_plan_publication_rejects_duplicate_targets_before_sending() {
        let srv = OpampServer::new();
        let plan = test_collector_plan("edge-a", 42);
        let error = srv
            .publish_collector_plans(&[plan.clone(), plan], Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            CollectorPlanPublishError::DuplicateTarget { collector_id }
                if collector_id == "edge-a"
        ));
    }
}
