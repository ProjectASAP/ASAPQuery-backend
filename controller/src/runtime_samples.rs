//! Receiver for agents' [`sketch-runtime::GrpcExporter`]
//! batches — the **push side** of the controller's real-time
//! decision loop.
//!
//! Agents running an embedded `sketch-runtime` Sampler call the
//! `asap.runtime.v1.RuntimeSamples.Push` RPC with batched
//! records. The handler appends each record to a bounded ring
//! buffer keyed by `(source, sketch, impl)`. Decision loops
//! peek the tail of that buffer to see the freshest throughput
//! / latency / accuracy signal.
//!
//! ## Why gRPC (vs the earlier HTTP+JSONL)
//!
//! HTTP/2 flow control surfaces controller back-pressure to
//! the agent — critical when the decision loop is real-time.
//! See the design discussion thread for the full trade-off;
//! the summary is in
//! [`sketch-runtime::exporter::grpc`](https://github.com/ProjectASAP/sketch-bench/blob/main/sketch-runtime/src/exporter/grpc.rs).
//!
//! ## Why a ring buffer, not a stream
//!
//! Real-time decisions want the freshest N records, not a full
//! replay. Bounded memory, O(1) append + peek, FIFO eviction.
//! Longer windows live in the agent's `FileExporter` artifact.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Generated from proto/feedback.proto by build.rs.
pub mod feedback {
    tonic::include_proto!("asap.runtime.v1");
}
use feedback::runtime_samples_server::{RuntimeSamples, RuntimeSamplesServer};
use feedback::{PushAck, PushBatch};

/// One record as stored in the ring buffer. Kept as
/// `serde_json::Value` payload for schema forward-compat — new
/// fields on `sketch-core::report::Record` flow through without
/// a controller rebump. Decision loops extract numeric fields
/// on demand.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeRecord {
    pub source: String,
    pub sketch: String,
    #[serde(rename = "impl")]
    pub impl_name: String,
    #[serde(default)]
    pub schema_version: u32,
    /// The full `sketch-core::Record` payload, minus the
    /// labelling fields already in this struct. Parsed from the
    /// `RuntimeRecord.payload_json` field of the proto.
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SampleKey {
    pub source: String,
    pub sketch: String,
    pub impl_name: String,
}

#[derive(Debug, Default)]
pub struct RuntimeSamplesStats {
    pub batches_received: AtomicU64,
    pub records_stored: AtomicU64,
    pub records_evicted: AtomicU64,
    pub decode_errors: AtomicU64,
}

impl RuntimeSamplesStats {
    pub fn snapshot(&self) -> RuntimeSamplesStatsSnapshot {
        RuntimeSamplesStatsSnapshot {
            batches_received: self.batches_received.load(Ordering::Relaxed),
            records_stored: self.records_stored.load(Ordering::Relaxed),
            records_evicted: self.records_evicted.load(Ordering::Relaxed),
            decode_errors: self.decode_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RuntimeSamplesStatsSnapshot {
    pub batches_received: u64,
    pub records_stored: u64,
    pub records_evicted: u64,
    pub decode_errors: u64,
}

/// Bounded FIFO ring buffer of runtime records, keyed by
/// `(source, sketch, impl)`. Each key gets its own buffer so a
/// chatty source can't starve a quiet one.
pub struct RuntimeSamplesStore {
    buffers: RwLock<HashMap<SampleKey, VecDeque<RuntimeRecord>>>,
    per_key_capacity: usize,
    stats: Arc<RuntimeSamplesStats>,
}

impl RuntimeSamplesStore {
    pub fn new(per_key_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            buffers: RwLock::new(HashMap::new()),
            per_key_capacity,
            stats: Arc::new(RuntimeSamplesStats::default()),
        })
    }

    pub fn stats(&self) -> Arc<RuntimeSamplesStats> {
        Arc::clone(&self.stats)
    }

    pub fn stats_handle(&self) -> Arc<RuntimeSamplesStats> {
        Arc::clone(&self.stats)
    }

    #[cfg(test)]
    pub(crate) fn append_for_test(&self, rec: RuntimeRecord) {
        self.append(rec)
    }

    fn append(&self, rec: RuntimeRecord) {
        let key = SampleKey {
            source: rec.source.clone(),
            sketch: rec.sketch.clone(),
            impl_name: rec.impl_name.clone(),
        };
        let mut map = self.buffers.write();
        let buf = map
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(self.per_key_capacity));
        if buf.len() >= self.per_key_capacity {
            buf.pop_front();
            self.stats.records_evicted.fetch_add(1, Ordering::Relaxed);
        }
        buf.push_back(rec);
        self.stats.records_stored.fetch_add(1, Ordering::Relaxed);
    }

    /// Peek the latest record for a given key, or `None` if the
    /// key has never been seen. Used by the replanner /
    /// decision loop to read freshness signals.
    pub fn latest(&self, key: &SampleKey) -> Option<RuntimeRecord> {
        self.buffers.read().get(key).and_then(|b| b.back().cloned())
    }

    /// Snapshot the full ring for a key. O(n) clone; non-hot-path only.
    pub fn snapshot(&self, key: &SampleKey) -> Vec<RuntimeRecord> {
        self.buffers
            .read()
            .get(key)
            .map(|b| b.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn keys(&self) -> Vec<SampleKey> {
        self.buffers.read().keys().cloned().collect()
    }
}

/// tonic service impl. One instance wraps the shared store and
/// is added to a tonic `Server` listening on the runtime-samples
/// port.
pub struct RuntimeSamplesService {
    store: Arc<RuntimeSamplesStore>,
}

impl RuntimeSamplesService {
    pub fn new(store: Arc<RuntimeSamplesStore>) -> Self {
        Self { store }
    }

    /// Return the server as a tonic-routed service with gzip
    /// compression negotiated on both sides.
    pub fn into_server(self) -> RuntimeSamplesServer<Self> {
        RuntimeSamplesServer::new(self)
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .send_compressed(tonic::codec::CompressionEncoding::Gzip)
    }
}

#[tonic::async_trait]
impl RuntimeSamples for RuntimeSamplesService {
    async fn push(
        &self,
        request: tonic::Request<PushBatch>,
    ) -> Result<tonic::Response<PushAck>, tonic::Status> {
        self.store
            .stats
            .batches_received
            .fetch_add(1, Ordering::Relaxed);
        let batch = request.into_inner();
        let mut accepted = 0_u64;
        for pb in batch.records {
            // The agent's RuntimeRecord carries the full v1
            // `sketch-core::Record` as JSON in `payload_json`.
            // Parse it into our `Value`-flattened store struct;
            // reject individual malformed records rather than
            // failing the whole batch.
            let mut payload: Value = match serde_json::from_str(&pb.payload_json) {
                Ok(v) => v,
                Err(e) => {
                    self.store
                        .stats
                        .decode_errors
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %e, "runtime-samples: malformed payload_json, skipping");
                    continue;
                }
            };
            // Strip the labelling fields out of `payload` — our
            // RuntimeRecord carries them as typed fields, so
            // duplicates inside payload would confuse downstream
            // consumers.
            if let Some(obj) = payload.as_object_mut() {
                obj.remove("source");
                obj.remove("sketch");
                obj.remove("impl");
            }
            let rec = RuntimeRecord {
                source: pb.source,
                sketch: pb.sketch,
                impl_name: pb.impl_name,
                schema_version: pb.schema_version,
                payload,
            };
            self.store.append(rec);
            accepted += 1;
        }
        Ok(tonic::Response::new(PushAck { accepted }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feedback::RuntimeRecord as PbRecord;

    fn make_pb_record(source: &str, sketch: &str, impl_name: &str, tp: f64) -> PbRecord {
        let payload = serde_json::json!({
            "schema_version": 1,
            "mode": "runtime",
            "timestamp": "2026-04-21T19:00:00Z",
            "bench": {
                "throughput_items_per_sec": { "mean": tp, "stddev": 0.0 },
                "latency_ns": { "p50": 10, "p99": 100 },
                "memory_bytes": 2048
            }
        });
        PbRecord {
            source: source.into(),
            sketch: sketch.into(),
            impl_name: impl_name.into(),
            schema_version: 1,
            payload_json: payload.to_string(),
        }
    }

    #[tokio::test]
    async fn push_stores_records_under_per_key_rings() {
        let store = RuntimeSamplesStore::new(16);
        let svc = RuntimeSamplesService::new(Arc::clone(&store));
        let batch = PushBatch {
            records: vec![
                make_pb_record("dc-a", "cms", "oxide", 1e6),
                make_pb_record("dc-b", "hll", "lib", 2e6),
                make_pb_record("dc-a", "cms", "oxide", 1.5e6),
            ],
        };
        let resp = svc.push(tonic::Request::new(batch)).await.expect("ok");
        assert_eq!(resp.into_inner().accepted, 3);
        assert_eq!(store.keys().len(), 2);
        let key = SampleKey {
            source: "dc-a".into(),
            sketch: "cms".into(),
            impl_name: "oxide".into(),
        };
        assert_eq!(store.snapshot(&key).len(), 2);
    }

    #[tokio::test]
    async fn push_rejects_malformed_payload_individually_but_accepts_rest() {
        let store = RuntimeSamplesStore::new(16);
        let svc = RuntimeSamplesService::new(Arc::clone(&store));
        let good1 = make_pb_record("dc-a", "cms", "oxide", 1e6);
        let bad = PbRecord {
            source: "dc-a".into(),
            sketch: "cms".into(),
            impl_name: "oxide".into(),
            schema_version: 1,
            payload_json: "{not json".into(),
        };
        let good2 = make_pb_record("dc-a", "cms", "oxide", 2e6);
        let batch = PushBatch {
            records: vec![good1, bad, good2],
        };
        let resp = svc.push(tonic::Request::new(batch)).await.expect("ok");
        assert_eq!(resp.into_inner().accepted, 2);
        let snap = store.stats.snapshot();
        assert_eq!(snap.records_stored, 2);
        assert_eq!(snap.decode_errors, 1);
    }

    #[tokio::test]
    async fn ring_evicts_oldest_past_capacity() {
        let store = RuntimeSamplesStore::new(3);
        let svc = RuntimeSamplesService::new(Arc::clone(&store));
        for _ in 0..5 {
            svc.push(tonic::Request::new(PushBatch {
                records: vec![make_pb_record("dc-a", "cms", "oxide", 1e6)],
            }))
            .await
            .unwrap();
        }
        let snap = store.stats.snapshot();
        assert_eq!(snap.records_stored, 5);
        assert_eq!(snap.records_evicted, 2);
        let key = SampleKey {
            source: "dc-a".into(),
            sketch: "cms".into(),
            impl_name: "oxide".into(),
        };
        assert_eq!(store.snapshot(&key).len(), 3);
    }
}
