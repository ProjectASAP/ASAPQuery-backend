//! Forward mode (`--raw-store-url`, docs/design/demo-plan.md §10.1): every
//! Remote Write request the data plane accepts is also delivered, unchanged,
//! to a raw store (a VictoriaMetrics single-node), so queries the plan does
//! not answer can be proxied to it.
//!
//! A request is acknowledged (204) only once the engine has admitted its
//! samples and its body is queued for the raw store. The queue is bounded in
//! bytes (bodies in flight included); when it is full the request is refused
//! with 503 before the engine sees it, so a sender's retry never duplicates
//! samples in either store. Queued bodies are delivered by a few workers on
//! their own thread and retried until the raw store accepts them (2xx), with
//! an exponential backoff on 429, 5xx, timeouts and connection errors. Any
//! other answer (a 4xx: the raw store will never accept that body) drops the
//! body and counts it. The queue is in memory: a crash loses at most the
//! bytes queued.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use tokio::sync::Notify;

/// How raw samples are forwarded.
#[derive(Debug, Clone)]
pub struct RawForwardOptions {
    /// Base URL of the raw store; bodies go to `{url}/api/v1/write`.
    pub url: String,
    /// Bytes of request bodies queued or in flight beyond which writes are
    /// refused with 503.
    pub max_queued_bytes: u64,
    /// Requests delivered at once.
    pub concurrency: usize,
    /// Bound on one delivery attempt.
    pub timeout: Duration,
}

impl RawForwardOptions {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into().trim_end_matches('/').to_string(),
            max_queued_bytes: 256 << 20,
            concurrency: 4,
            timeout: Duration::from_secs(30),
        }
    }
}

/// The forwarder's counters, exported on `/metrics` and the status JSON.
#[derive(Default)]
struct Counters {
    queued_bytes: AtomicU64,
    queued_requests: AtomicU64,
    delivered: AtomicU64,
    delivered_bytes: AtomicU64,
    retried: AtomicU64,
    dropped: AtomicU64,
    refused: AtomicU64,
}

struct Shared {
    options: RawForwardOptions,
    queue: Mutex<VecDeque<Bytes>>,
    ready: Notify,
    counters: Counters,
    /// Set when the forwarder is dropped: the workers stop.
    closed: AtomicBool,
}

/// Delivers accepted Remote Write bodies to the raw store.
pub struct RawForwarder {
    shared: Arc<Shared>,
}

/// Room reserved in the queue for one body; released unless committed.
pub struct Reservation<'a> {
    forwarder: &'a RawForwarder,
    bytes: u64,
    committed: bool,
}

impl Reservation<'_> {
    /// Queues `body` (of the reserved size) for delivery.
    pub fn commit(mut self, body: Bytes) {
        self.committed = true;
        let shared = &self.forwarder.shared;
        shared
            .queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(body);
        shared
            .counters
            .queued_requests
            .fetch_add(1, Ordering::SeqCst);
        shared.ready.notify_one();
    }
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.forwarder
                .shared
                .counters
                .queued_bytes
                .fetch_sub(self.bytes, Ordering::SeqCst);
        }
    }
}

impl RawForwarder {
    /// Starts `options.concurrency` delivery workers on a thread of their own.
    pub fn start(options: RawForwardOptions) -> std::io::Result<Self> {
        let shared = Arc::new(Shared {
            options,
            queue: Mutex::new(VecDeque::new()),
            ready: Notify::new(),
            counters: Counters::default(),
            closed: AtomicBool::new(false),
        });
        let worker_shared = shared.clone();
        std::thread::Builder::new()
            .name("raw-forward".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a current-thread runtime");
                runtime.block_on(async move {
                    let client = reqwest::Client::builder()
                        .timeout(worker_shared.options.timeout)
                        .build()
                        .expect("reqwest client with only a timeout set");
                    let workers: Vec<_> = (0..worker_shared.options.concurrency.max(1))
                        .map(|_| tokio::spawn(deliver(worker_shared.clone(), client.clone())))
                        .collect();
                    for worker in workers {
                        let _ = worker.await;
                    }
                });
            })?;
        Ok(Self { shared })
    }

    /// Reserves room for a body of `bytes`; `None` (and the refusal is
    /// counted) when the queue is full. An empty queue always takes one
    /// body, so a bound smaller than one request cannot refuse it forever.
    pub fn reserve(&self, bytes: u64) -> Option<Reservation<'_>> {
        let counters = &self.shared.counters;
        let max = self.shared.options.max_queued_bytes;
        let mut current = counters.queued_bytes.load(Ordering::SeqCst);
        loop {
            let total = current.saturating_add(bytes);
            if current > 0 && total > max {
                counters.refused.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            match counters.queued_bytes.compare_exchange(
                current,
                total,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(Reservation {
                        forwarder: self,
                        bytes,
                        committed: false,
                    })
                }
                Err(now) => current = now,
            }
        }
    }

    /// The raw store's base URL.
    pub fn url(&self) -> &str {
        &self.shared.options.url
    }

    /// The counters as JSON, for `/api/v1/status/asap`.
    pub fn status(&self) -> serde_json::Value {
        let c = &self.shared.counters;
        serde_json::json!({
            "url": self.shared.options.url,
            "queued_bytes": c.queued_bytes.load(Ordering::Relaxed),
            "queued_requests": c.queued_requests.load(Ordering::Relaxed),
            "delivered_requests": c.delivered.load(Ordering::Relaxed),
            "delivered_bytes": c.delivered_bytes.load(Ordering::Relaxed),
            "retried_attempts": c.retried.load(Ordering::Relaxed),
            "dropped_requests": c.dropped.load(Ordering::Relaxed),
            "refused_writes": c.refused.load(Ordering::Relaxed),
        })
    }

    /// The counters in the Prometheus text format.
    pub fn render(&self, out: &mut String) {
        let c = &self.shared.counters;
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let _ = writeln!(
            out,
            "# HELP asap_raw_forward_queued_bytes Remote Write bytes queued or in flight to the raw store.\n# TYPE asap_raw_forward_queued_bytes gauge\nasap_raw_forward_queued_bytes {}",
            load(&c.queued_bytes)
        );
        let _ = writeln!(
            out,
            "# HELP asap_raw_forward_queued_requests Remote Write requests queued or in flight to the raw store.\n# TYPE asap_raw_forward_queued_requests gauge\nasap_raw_forward_queued_requests {}",
            load(&c.queued_requests)
        );
        let _ = writeln!(
            out,
            "# HELP asap_raw_forward_requests_total Remote Write requests forwarded to the raw store, by outcome.\n# TYPE asap_raw_forward_requests_total counter"
        );
        for (outcome, value) in [
            ("delivered", load(&c.delivered)),
            ("dropped", load(&c.dropped)),
        ] {
            let _ = writeln!(
                out,
                "asap_raw_forward_requests_total{{outcome=\"{outcome}\"}} {value}"
            );
        }
        for (name, help, value) in [
            (
                "asap_raw_forward_retries_total",
                "Delivery attempts to the raw store that failed and were retried.",
                load(&c.retried),
            ),
            (
                "asap_raw_forward_delivered_bytes_total",
                "Remote Write bytes the raw store accepted.",
                load(&c.delivered_bytes),
            ),
            (
                "asap_raw_forward_refused_writes_total",
                "Remote Write requests refused with 503 because the forward queue was full.",
                load(&c.refused),
            ),
        ] {
            let _ = writeln!(
                out,
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}"
            );
        }
    }
}

impl Drop for RawForwarder {
    /// Stops the workers; bodies still queued are not delivered.
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::SeqCst);
        self.shared.ready.notify_waiters();
    }
}

/// One delivery worker: takes the oldest queued body and delivers it,
/// retrying until the raw store accepts or rejects it.
async fn deliver(shared: Arc<Shared>, client: reqwest::Client) {
    let url = format!("{}/api/v1/write", shared.options.url);
    loop {
        let body = loop {
            let next = shared
                .queue
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .pop_front();
            match next {
                Some(body) => break body,
                None if shared.closed.load(Ordering::SeqCst) => return,
                None => {
                    let notified = shared.ready.notified();
                    if shared.closed.load(Ordering::SeqCst) {
                        return;
                    }
                    // Woken by a commit, or every second to see `closed`.
                    let _ = tokio::time::timeout(Duration::from_secs(1), notified).await;
                }
            }
        };
        let mut backoff = Duration::from_millis(100);
        loop {
            let attempt = client
                .post(&url)
                .header("Content-Encoding", "snappy")
                .header("Content-Type", "application/x-protobuf")
                .header("X-Prometheus-Remote-Write-Version", "0.1.0")
                .body(body.clone())
                .send()
                .await;
            let retry = match attempt {
                Ok(response) if response.status().is_success() => {
                    shared.counters.delivered.fetch_add(1, Ordering::Relaxed);
                    shared
                        .counters
                        .delivered_bytes
                        .fetch_add(body.len() as u64, Ordering::Relaxed);
                    false
                }
                Ok(response)
                    if response.status().as_u16() == 429 || response.status().is_server_error() =>
                {
                    true
                }
                Ok(response) => {
                    tracing::warn!(status = %response.status(), "raw store rejected a Remote Write body; dropped");
                    shared.counters.dropped.fetch_add(1, Ordering::Relaxed);
                    false
                }
                Err(error) => {
                    tracing::debug!(%error, "raw store unreachable; retrying");
                    true
                }
            };
            if !retry || shared.closed.load(Ordering::SeqCst) {
                break;
            }
            shared.counters.retried.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
        shared
            .counters
            .queued_bytes
            .fetch_sub(body.len() as u64, Ordering::SeqCst);
        shared
            .counters
            .queued_requests
            .fetch_sub(1, Ordering::SeqCst);
    }
}
