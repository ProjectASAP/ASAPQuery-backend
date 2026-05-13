//! Centralized series_id resolver — Phase 4 of the controller-into-backend
//! refactor (2026-05).
//!
//! The asap-query-backend host is the SOLE minter of `series_id`s in the
//! pipeline. Agents and gateway are transparent forwarders for sids whose
//! input identity matches the output identity; for rollup-output identities
//! at the gateway, the gateway itself calls back to this resolver (same
//! flow as agents — it just happens to be a hop closer).
//!
//! The resolver is content-addressable and idempotent: same `(metric_name,
//! attribute_set)` input always produces the same `series_id` for the
//! lifetime of the cache. This is the invariant that makes
//! attribute-fallback recovery work — a recovered agent re-emits with full
//! attributes, the resolver returns the same sid that was assigned before
//! the agent's crash, and sketch state under that sid stays coherent.
//!
//! Cache key: a deterministic fingerprint of `(metric_name, sorted
//! [attr_key, attr_value] pairs)`. Both sender and resolver MUST compute
//! the fingerprint the same way; the fingerprint algorithm here mirrors
//! the patched OTel-Go exporter's `attributesFingerprint` in
//! `opentelemetry-go-patch/exporters/otlp/otlpmetric/otlpmetricgrpc/
//! internal/series/dictionary.go`.
//!
//! See design doc §5.4 ("Idempotency invariant on `ResolveSeriesIDs`")
//! at `docs/design-controller-into-backend.md`.

use dashmap::DashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// Canonical sid identity — `(metric_name, attrs_fingerprint, agg_kind_canonical)`.
///
/// This is the same 3-tuple that the retired `compute_sketch_sid` /
/// `compute_sid` functions hashed over, just held as a string key
/// for the registry-allocated mint path.
///
/// - `attrs_fingerprint` — keys sorted lexicographically, then
///   `key=value;`-joined. Matches the patched OTel-Go exporter so
///   sender and receiver agree bit-exactly.
/// - `agg_kind_canonical` — the stable string form of `AggKind` (see
///   `sketch_db::data::AggKind::canonical_string`). Distinguishes
///   different aggregations over the same series — e.g. a DDSketch
///   and a Sum on the same `(metric, attrs)` get separate sids.
type CacheKey = (String, String, String);

/// Idempotent compute-or-mint resolver. Atomic per-key — concurrent
/// `resolve()` calls for the same `(metric, attrs)` from different agents
/// or different DataPoints in the same Export request always observe the
/// same sid, no spurious mints.
pub struct SeriesIdResolver {
    cache: DashMap<CacheKey, u64>,
    next_sid: AtomicU64,
    /// Durability hook. `NoopPersistence` is the default — fast, no I/O,
    /// loses every binding on restart (recovery path is the existing
    /// `unknown_series_ids` eviction signal). `FilePersistence` writes a
    /// WAL record per fresh mint and replays on construction, so the
    /// agent's cached sids stay valid across backend restarts.
    persistence: Arc<dyn SeriesResolverPersistence>,
}

impl SeriesIdResolver {
    /// Build an in-memory-only resolver. Equivalent to
    /// `with_persistence(NoopPersistence)`. Suitable for tests, for the
    /// `--persistence-enabled=false` deployment mode, and for any path
    /// where the caller doesn't need restart-survival semantics.
    pub fn new() -> Self {
        Self::with_persistence(Arc::new(NoopPersistence))
    }

    /// Build a resolver with an injected persistence backend. Used by
    /// tests that want to verify the trait contract against a mock, and
    /// by [`Self::open`] under the hood.
    pub fn with_persistence(persistence: Arc<dyn SeriesResolverPersistence>) -> Self {
        Self {
            cache: DashMap::new(),
            // sid=0 is reserved for "unresolved/uncached"; start minting at 1.
            next_sid: AtomicU64::new(1),
            persistence,
        }
    }

    /// Open a file-backed resolver at `path`, replay every durable
    /// binding into the in-memory cache, resume `next_sid` at
    /// `max(replayed_sid) + 1`, and return the warm resolver. The
    /// production constructor under `--persistence-enabled`.
    ///
    /// If the file does not exist, it is created with a fresh header
    /// and the resolver starts cold (no bindings, `next_sid = 1`).
    ///
    /// If a torn record is detected at EOF during replay (e.g. backend
    /// crashed mid-append), the file is truncated to the last durable
    /// record's offset. Replay returns the durable prefix.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        let persistence = Arc::new(FilePersistence::open(path)?);
        let records = persistence.replay()?;
        info!(
            replayed = records.len(),
            "series-resolver WAL replayed",
        );
        let cache: DashMap<CacheKey, u64> = DashMap::new();
        let mut max_sid: u64 = 0;
        for r in records {
            cache.insert(
                (r.metric, r.attrs_fingerprint, r.agg_kind_canonical),
                r.sid,
            );
            if r.sid > max_sid {
                max_sid = r.sid;
            }
        }
        // sid=0 is reserved; if the log was empty, max_sid is 0 and we
        // start minting at 1 (the same as a cold start).
        Ok(Self {
            cache,
            next_sid: AtomicU64::new(max_sid.saturating_add(1).max(1)),
            persistence,
        })
    }

    /// Resolve `(metric, attrs, agg_kind)` to a series_id. Returns the
    /// existing sid if this 3-tuple was already registered; otherwise
    /// mints a fresh sid, durably persists the binding (when a non-noop
    /// backend is wired), caches it, and returns the new value.
    ///
    /// `agg_kind_canonical` is the stable string form of
    /// [`sketch_db::data::AggKind`] (call its `canonical_string()`
    /// method at the call site). It's a string here so the resolver
    /// doesn't take a dep on storage_engines.
    ///
    /// Idempotent: repeated calls with the same input ALWAYS return the
    /// same sid for the lifetime of the cache. With `FilePersistence`,
    /// "lifetime of the cache" extends across backend restarts (the WAL
    /// is replayed on [`Self::open`]). With `NoopPersistence`, the
    /// cache resets per process; agents observe their cached sids as
    /// stale via `unknown_series_ids` and re-resolve with attrs.
    ///
    /// Persistence failures are logged at WARN and do NOT propagate —
    /// the resolver stays in-memory-correct. Next restart will not
    /// recover the lost mint, and the agent will hit the eviction
    /// recovery path (one extra round trip with attrs).
    pub fn resolve(
        &self,
        metric_name: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
    ) -> u64 {
        let key = (
            metric_name.to_string(),
            attrs_fingerprint.to_string(),
            agg_kind_canonical.to_string(),
        );
        // Fast path: read-only check on the cache before taking the
        // bucket's write lock. DashMap's `get` takes a shard read lock;
        // the common case (a hit on a known identity) never serializes
        // against other resolve calls.
        if let Some(existing) = self.cache.get(&key) {
            return *existing;
        }
        // Slow path: bucket write lock + mint + persist + insert.
        // `entry().or_insert_with` ensures only ONE caller runs the
        // closure for a given key, even under concurrent load. The
        // persistence append happens inside the closure so the binding
        // is durable before any caller observes the sid.
        let entry = self.cache.entry(key).or_insert_with(|| {
            let sid = self.next_sid.fetch_add(1, Ordering::Relaxed);
            if let Err(e) = self.persistence.append(
                sid,
                metric_name,
                attrs_fingerprint,
                agg_kind_canonical,
            ) {
                warn!(
                    metric = %metric_name,
                    sid,
                    error = %e,
                    "resolver persistence append failed; binding is \
                     in-memory-only and will not survive restart",
                );
            }
            sid
        });
        *entry
    }

    /// Look up an existing sid without minting. Returns `None` if the
    /// `(metric, attrs, agg_kind)` tuple is not in the cache.
    pub fn lookup(
        &self,
        metric_name: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
    ) -> Option<u64> {
        let key = (
            metric_name.to_string(),
            attrs_fingerprint.to_string(),
            agg_kind_canonical.to_string(),
        );
        self.cache.get(&key).map(|v| *v)
    }

    /// Reverse lookup: given a sid, is it known? Used at receive time
    /// when an Export carries a sid != 0 with empty attributes — backend
    /// must verify it knows the sid; otherwise stamp `unknown_series_ids`
    /// in the response.
    pub fn is_known(&self, sid: u64) -> bool {
        self.cache.iter().any(|kv| *kv.value() == sid)
    }

    /// Number of registered identities. Used for telemetry / debugging.
    pub fn len(&self) -> usize {
        self.cache.len()
    }
}

impl Default for SeriesIdResolver {
    fn default() -> Self {
        Self::new()
    }
}

// ── Persistence ──────────────────────────────────────────────────────────────
//
// The resolver's `(metric, fp, agg_kind) → sid` cache is in-memory only
// by default. Under `--persistence-enabled`, a `FilePersistence` backend
// writes a WAL record per fresh mint; the resolver replays it on startup
// so the agent's cached sids stay valid across backend restarts.
//
// WAL format v3 (current; v1 was attrs-only, never shipped to prod;
// v2 used `agg_kind_canonical` without spatial-filter, replaced when
// spatial_filter_canonical was folded into agg_kind_canonical to
// distinguish filter-distinct policies on the sid identity):
//   header: 8 bytes  → b"ASAPSRP\x03"
//   record: 8 bytes  → sid (u64 little-endian)
//           4 bytes  → metric_len (u32 LE)
//           metric_len bytes → metric utf8
//           4 bytes  → fp_len (u32 LE)
//           fp_len bytes → fp utf8
//           4 bytes  → agg_kind_len (u32 LE)
//           agg_kind_len bytes → agg_kind_canonical utf8
//
// v2 WALs are not auto-migrated — pre-prod constraint. A v2 header
// causes `FilePersistence::open` to fail with `InvalidData`; recovery
// is to delete the file and let the resolver cold-start (the
// `unknown_series_ids` eviction primitive handles the bandwidth blip).
//
// Append-only; sids are minted once and never rewritten, so the log size
// is proportional to live cardinality. At 100M sids (~5GB) compaction
// becomes worth scheduling; not implemented here.
//
// Crash safety: every `append` calls `fsync` before returning. A torn
// write at EOF (kernel buffered the bytes but the metadata flush was
// interrupted) is detected at replay via short-read on any record field
// — the file is truncated to the last durable record's offset and replay
// returns the durable prefix. No CRC: bit-rot is low-probability for an
// append-only WAL; add a CRC field if telemetry ever shows it firing.

/// One durable binding row read back from the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolverRecord {
    pub sid: u64,
    pub metric: String,
    pub attrs_fingerprint: String,
    pub agg_kind_canonical: String,
}

/// Durability hook for [`SeriesIdResolver`]. Implementations decide
/// whether mints survive process restart. `NoopPersistence` is fine for
/// tests and stateless deployments; `FilePersistence` is the production
/// answer under `--persistence-enabled`.
pub trait SeriesResolverPersistence: Send + Sync {
    /// Durably record a fresh `(sid, metric, fp, agg_kind)` binding.
    /// MUST be flushed to stable storage before returning `Ok` — the
    /// resolver only returns the sid to its caller after this returns.
    /// On error, the binding is in-memory-only; the caller logs and
    /// continues.
    fn append(
        &self,
        sid: u64,
        metric: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
    ) -> std::io::Result<()>;

    /// Read every durable binding in append order. Called once at
    /// resolver construction time.
    fn replay(&self) -> std::io::Result<Vec<ResolverRecord>>;
}

/// In-memory-only impl. Every `append` is a no-op; `replay` returns
/// empty. Resolver bindings reset on process restart; agents recover via
/// the `unknown_series_ids` eviction primitive.
pub struct NoopPersistence;

impl SeriesResolverPersistence for NoopPersistence {
    fn append(
        &self,
        _sid: u64,
        _metric: &str,
        _fp: &str,
        _agg_kind_canonical: &str,
    ) -> std::io::Result<()> {
        Ok(())
    }

    fn replay(&self) -> std::io::Result<Vec<ResolverRecord>> {
        Ok(Vec::new())
    }
}

/// File-backed WAL. Single-writer; the `Mutex<File>` serializes appends
/// to keep the on-disk order deterministic and so `fsync` ordering
/// matches mint order. Reads only happen at construction.
#[derive(Debug)]
pub struct FilePersistence {
    file: Mutex<File>,
    path: PathBuf,
}

const WAL_MAGIC: &[u8; 8] = b"ASAPSRP\x03";
/// Reject any single field whose length-prefix exceeds these caps. A
/// corrupted file might claim huge field lengths; without these bounds
/// the replay loop could allocate gigabytes of zeros before discovering
/// the lengths don't match the actual content. The caps are far above
/// any realistic input — metric names are tens of bytes, fingerprints
/// are hundreds, agg_kind canonical strings are tens.
const MAX_METRIC_LEN: usize = 16 * 1024;
const MAX_FP_LEN: usize = 64 * 1024;
const MAX_AGG_KIND_LEN: usize = 4 * 1024;

impl FilePersistence {
    /// Open or create the WAL at `path`. On a fresh file, writes the
    /// magic header and fsyncs. On an existing file, verifies the
    /// header matches and seeks to EOF for future appends.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            file.write_all(WAL_MAGIC)?;
            file.sync_all()?;
        } else {
            let mut hdr = [0u8; 8];
            file.seek(SeekFrom::Start(0))?;
            file.read_exact(&mut hdr)?;
            if &hdr != WAL_MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "resolver WAL header mismatch at {:?}: expected {:?}, got {:?}",
                        path, WAL_MAGIC, hdr,
                    ),
                ));
            }
        }
        // Position at EOF — appends start here.
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file: Mutex::new(file),
            path,
        })
    }

    /// Diagnostic accessor — the WAL path. Tests use this to inspect
    /// the on-disk file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SeriesResolverPersistence for FilePersistence {
    fn append(
        &self,
        sid: u64,
        metric: &str,
        fp: &str,
        agg_kind_canonical: &str,
    ) -> std::io::Result<()> {
        let metric_bytes = metric.as_bytes();
        let fp_bytes = fp.as_bytes();
        let agg_kind_bytes = agg_kind_canonical.as_bytes();
        let metric_len: u32 =
            metric_bytes.len().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "metric name longer than u32::MAX bytes",
                )
            })?;
        let fp_len: u32 = fp_bytes.len().try_into().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "fingerprint longer than u32::MAX bytes",
            )
        })?;
        let agg_kind_len: u32 =
            agg_kind_bytes.len().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "agg_kind_canonical longer than u32::MAX bytes",
                )
            })?;

        let mut f = self.file.lock().unwrap();
        f.write_all(&sid.to_le_bytes())?;
        f.write_all(&metric_len.to_le_bytes())?;
        f.write_all(metric_bytes)?;
        f.write_all(&fp_len.to_le_bytes())?;
        f.write_all(fp_bytes)?;
        f.write_all(&agg_kind_len.to_le_bytes())?;
        f.write_all(agg_kind_bytes)?;
        // Durability barrier: caller must not observe the sid until the
        // record is on stable storage. fsync is the slow part of the
        // mint path (a few ms on SSD) but it's amortized — minting is
        // once per identity, not per emit.
        f.sync_all()?;
        Ok(())
    }

    fn replay(&self) -> std::io::Result<Vec<ResolverRecord>> {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(0))?;
        let mut hdr = [0u8; 8];
        match f.read_exact(&mut hdr) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // File exists but is empty — caller likely opened it
                // moments ago without writing the header yet. Treat as
                // no records.
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        }
        if &hdr != WAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "resolver WAL header mismatch during replay",
            ));
        }
        let mut out = Vec::new();
        // After successful header read, the offset is 8.
        let mut safe_offset: u64 = 8;
        loop {
            match read_one_record(&mut *f) {
                ReadOne::Ok(record, new_offset) => {
                    out.push(record);
                    safe_offset = new_offset;
                }
                ReadOne::Eof => break,
                ReadOne::Torn => {
                    warn!(
                        path = %self.path.display(),
                        torn_at = safe_offset,
                        recovered = out.len(),
                        "resolver WAL: torn record at EOF — truncating to last durable offset",
                    );
                    f.set_len(safe_offset)?;
                    f.seek(SeekFrom::End(0))?;
                    return Ok(out);
                }
            }
        }
        // Clean EOF — seek back to end for future appends and return.
        f.seek(SeekFrom::End(0))?;
        Ok(out)
    }
}

/// Outcome of attempting to read a single WAL record. `Torn` means a
/// short read or out-of-range field length was detected mid-record;
/// the caller truncates the file to the last `Ok` offset.
enum ReadOne {
    Ok(ResolverRecord, u64),
    Eof,
    Torn,
}

fn read_one_record(f: &mut File) -> ReadOne {
    let mut sid_buf = [0u8; 8];
    match f.read_exact(&mut sid_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return ReadOne::Eof,
        Err(_) => return ReadOne::Torn,
    }
    let sid = u64::from_le_bytes(sid_buf);

    let mut len_buf = [0u8; 4];
    if f.read_exact(&mut len_buf).is_err() {
        return ReadOne::Torn;
    }
    let metric_len = u32::from_le_bytes(len_buf) as usize;
    if metric_len > MAX_METRIC_LEN {
        return ReadOne::Torn;
    }
    let mut metric_bytes = vec![0u8; metric_len];
    if f.read_exact(&mut metric_bytes).is_err() {
        return ReadOne::Torn;
    }

    if f.read_exact(&mut len_buf).is_err() {
        return ReadOne::Torn;
    }
    let fp_len = u32::from_le_bytes(len_buf) as usize;
    if fp_len > MAX_FP_LEN {
        return ReadOne::Torn;
    }
    let mut fp_bytes = vec![0u8; fp_len];
    if f.read_exact(&mut fp_bytes).is_err() {
        return ReadOne::Torn;
    }

    if f.read_exact(&mut len_buf).is_err() {
        return ReadOne::Torn;
    }
    let agg_kind_len = u32::from_le_bytes(len_buf) as usize;
    if agg_kind_len > MAX_AGG_KIND_LEN {
        return ReadOne::Torn;
    }
    let mut agg_kind_bytes = vec![0u8; agg_kind_len];
    if f.read_exact(&mut agg_kind_bytes).is_err() {
        return ReadOne::Torn;
    }

    let metric = match String::from_utf8(metric_bytes) {
        Ok(s) => s,
        Err(_) => return ReadOne::Torn,
    };
    let attrs_fingerprint = match String::from_utf8(fp_bytes) {
        Ok(s) => s,
        Err(_) => return ReadOne::Torn,
    };
    let agg_kind_canonical = match String::from_utf8(agg_kind_bytes) {
        Ok(s) => s,
        Err(_) => return ReadOne::Torn,
    };
    let new_offset = match f.stream_position() {
        Ok(p) => p,
        Err(_) => return ReadOne::Torn,
    };
    ReadOne::Ok(
        ResolverRecord {
            sid,
            metric,
            attrs_fingerprint,
            agg_kind_canonical,
        },
        new_offset,
    )
}

/// Compute the canonical attributes fingerprint matching the patched
/// OTel-Go exporter's `attributesFingerprint`. Both sides MUST produce
/// the same string for the same attribute set — sender uses it to look
/// up its local cache; receiver uses it as the resolver's cache key.
///
/// Format: `key1=value1;key2=value2;...` where keys are sorted
/// lexicographically. Mirrors
/// `opentelemetry-go-patch/exporters/otlp/otlpmetric/otlpmetricgrpc/
/// internal/series/dictionary.go::attributesFingerprint`.
pub fn canonical_attrs_fingerprint(attrs: &[(&str, &str)]) -> String {
    let mut sorted: Vec<(&str, &str)> = attrs.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(b.0));
    let mut buf = String::new();
    for (k, v) in sorted {
        buf.push_str(k);
        buf.push('=');
        buf.push_str(v);
        buf.push(';');
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stand-in canonical `AggKind` string. Tests don't care about the
    /// specific encoding — the resolver only uses the value for key
    /// equality. Production callers compute this via
    /// `AggKind::canonical_string()`.
    const TEST_AGG: &str = "sketch:DDSketch:D:0.01";

    #[test]
    fn idempotent_same_input_same_sid() {
        let r = SeriesIdResolver::new();
        let sid1 = r.resolve("http_requests_total", "zone=z0;", TEST_AGG);
        let sid2 = r.resolve("http_requests_total", "zone=z0;", TEST_AGG);
        assert_eq!(sid1, sid2, "same input must produce same sid");
    }

    #[test]
    fn distinct_inputs_distinct_sids() {
        let r = SeriesIdResolver::new();
        let s_z0 = r.resolve("metric_a", "zone=z0;", TEST_AGG);
        let s_z1 = r.resolve("metric_a", "zone=z1;", TEST_AGG);
        assert_ne!(s_z0, s_z1);
    }

    #[test]
    fn distinct_metrics_same_attrs_distinct_sids() {
        let r = SeriesIdResolver::new();
        let s_a = r.resolve("metric_a", "zone=z0;", TEST_AGG);
        let s_b = r.resolve("metric_b", "zone=z0;", TEST_AGG);
        assert_ne!(s_a, s_b);
    }

    #[test]
    fn distinct_agg_kinds_same_series_distinct_sids() {
        // Two aggregations over the same (metric, attrs) tuple — e.g.
        // a DDSketch and a Sum on `http_latency_ms{zone=z0}` — get
        // SEPARATE sids. This is the core property of Interpretation B:
        // sid identity is `(metric, attrs, agg_kind)`.
        let r = SeriesIdResolver::new();
        let s_dd = r.resolve("http_latency_ms", "zone=z0;", "sketch:DDSketch:D:0.01");
        let s_sum = r.resolve("http_latency_ms", "zone=z0;", "precompute:Sum:");
        assert_ne!(
            s_dd, s_sum,
            "different agg_kinds over the same series must mint distinct sids",
        );
    }

    #[test]
    fn fingerprint_sorts_keys() {
        let f1 = canonical_attrs_fingerprint(&[("zone", "z0"), ("rack", "r00")]);
        let f2 = canonical_attrs_fingerprint(&[("rack", "r00"), ("zone", "z0")]);
        assert_eq!(f1, f2, "fingerprint must be order-independent");
        assert_eq!(f1, "rack=r00;zone=z0;");
    }

    #[test]
    fn lookup_returns_existing_without_mint() {
        let r = SeriesIdResolver::new();
        let sid = r.resolve("m", "k=v;", TEST_AGG);
        assert_eq!(r.lookup("m", "k=v;", TEST_AGG), Some(sid));
        assert_eq!(r.lookup("m", "k=v2;", TEST_AGG), None);
        // Same (metric, attrs) but different agg_kind is a miss.
        assert_eq!(r.lookup("m", "k=v;", "precompute:Sum:"), None);
    }
}

#[cfg(test)]
mod persistence_tests {
    use super::*;
    use tempfile::TempDir;

    fn wal_path(dir: &TempDir) -> PathBuf {
        dir.path().join("series_resolver.wal")
    }

    #[test]
    fn empty_log_replays_empty() {
        let dir = TempDir::new().unwrap();
        let p = FilePersistence::open(wal_path(&dir)).unwrap();
        let records = p.replay().unwrap();
        assert!(records.is_empty());
    }

    const TEST_AGG: &str = "sketch:DDSketch:D:0.01";

    #[test]
    fn append_then_replay_round_trips() {
        let dir = TempDir::new().unwrap();
        let p = FilePersistence::open(wal_path(&dir)).unwrap();
        p.append(1, "metric_a", "zone=z0;", TEST_AGG).unwrap();
        p.append(2, "metric_a", "zone=z1;", TEST_AGG).unwrap();
        p.append(3, "metric_b", "zone=z0;", "precompute:Sum:")
            .unwrap();

        // Reopen to confirm durability across handle close.
        drop(p);
        let p2 = FilePersistence::open(wal_path(&dir)).unwrap();
        let records = p2.replay().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].sid, 1);
        assert_eq!(records[0].metric, "metric_a");
        assert_eq!(records[0].attrs_fingerprint, "zone=z0;");
        assert_eq!(records[0].agg_kind_canonical, TEST_AGG);
        assert_eq!(records[1].sid, 2);
        assert_eq!(records[2].metric, "metric_b");
        assert_eq!(records[2].agg_kind_canonical, "precompute:Sum:");
    }

    #[test]
    fn header_mismatch_errors_on_open() {
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        // Write a non-WAL file at the path.
        std::fs::write(&path, b"NOTAWAL!extra bytes").unwrap();
        let err = FilePersistence::open(path).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn torn_record_truncated_on_replay() {
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        // Write two clean records, then a torn third one (sid + len
        // header but truncated payload).
        {
            let p = FilePersistence::open(path.clone()).unwrap();
            p.append(1, "m", "k=v;", TEST_AGG).unwrap();
            p.append(2, "m", "k=w;", TEST_AGG).unwrap();
        }
        // Manually append a torn record: sid (8B) + metric_len=999
        // (claims 999 bytes of metric but we write 0 bytes after).
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&3u64.to_le_bytes()).unwrap();
            f.write_all(&999u32.to_le_bytes()).unwrap();
            // No payload bytes — replay reads metric_len=999 then
            // hits EOF.
            f.sync_all().unwrap();
        }
        let pre_size = std::fs::metadata(&path).unwrap().len();
        let p = FilePersistence::open(path.clone()).unwrap();
        let records = p.replay().unwrap();
        assert_eq!(records.len(), 2, "only the two clean records survive");
        let post_size = std::fs::metadata(&path).unwrap().len();
        assert!(
            post_size < pre_size,
            "torn tail truncated: pre={pre_size} post={post_size}"
        );
        // After truncation, subsequent appends pick up from the
        // truncated EOF — no gap, no rewrite of historical records.
        p.append(3, "m", "k=x;", TEST_AGG).unwrap();
        drop(p);
        let p2 = FilePersistence::open(path).unwrap();
        let records2 = p2.replay().unwrap();
        assert_eq!(records2.len(), 3);
        assert_eq!(records2[2].sid, 3);
    }

    #[test]
    fn out_of_range_metric_len_treated_as_torn() {
        // A corrupted file might claim a 4GB metric name. The replay
        // must NOT allocate that much; the bounds check rejects it as
        // torn instead.
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        {
            let p = FilePersistence::open(path.clone()).unwrap();
            p.append(1, "m", "k=v;", TEST_AGG).unwrap();
        }
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&2u64.to_le_bytes()).unwrap();
            // metric_len = MAX_METRIC_LEN + 1 — over the cap.
            f.write_all(&((MAX_METRIC_LEN as u32) + 1).to_le_bytes())
                .unwrap();
            f.sync_all().unwrap();
        }
        let p = FilePersistence::open(path).unwrap();
        let records = p.replay().unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn resolver_open_replays_existing_log() {
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        // First process: mint three bindings.
        {
            let r = SeriesIdResolver::open(path.clone()).unwrap();
            let s1 = r.resolve("m", "k=v0;", TEST_AGG);
            let s2 = r.resolve("m", "k=v1;", TEST_AGG);
            let s3 = r.resolve("m", "k=v2;", TEST_AGG);
            assert_eq!(s1, 1);
            assert_eq!(s2, 2);
            assert_eq!(s3, 3);
        }
        // Second process: reopen, same inputs return the same sids;
        // a fresh input mints sid=4 (max replayed + 1).
        {
            let r = SeriesIdResolver::open(path).unwrap();
            assert_eq!(r.resolve("m", "k=v0;", TEST_AGG), 1);
            assert_eq!(r.resolve("m", "k=v1;", TEST_AGG), 2);
            assert_eq!(r.resolve("m", "k=v2;", TEST_AGG), 3);
            let fresh = r.resolve("m", "k=v3;", TEST_AGG);
            assert_eq!(fresh, 4, "next_sid resumes at max(replayed)+1");
        }
    }

    #[test]
    fn resolver_open_distinguishes_agg_kinds_on_replay() {
        // Same (metric, attrs) but two agg_kinds — both replay to the
        // resolver as distinct keys, and re-resolving each returns its
        // original sid.
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        {
            let r = SeriesIdResolver::open(path.clone()).unwrap();
            let s_dd = r.resolve("m", "k=v0;", "sketch:DDSketch:D:0.01");
            let s_sum = r.resolve("m", "k=v0;", "precompute:Sum:");
            assert_ne!(s_dd, s_sum);
        }
        {
            let r = SeriesIdResolver::open(path).unwrap();
            assert_eq!(r.resolve("m", "k=v0;", "sketch:DDSketch:D:0.01"), 1);
            assert_eq!(r.resolve("m", "k=v0;", "precompute:Sum:"), 2);
        }
    }

    #[test]
    fn resolver_with_noop_persistence_does_not_persist() {
        // Sanity check: NoopPersistence is the back-compat path; resolver
        // bindings reset across construction.
        let r1 = SeriesIdResolver::new();
        let s1 = r1.resolve("m", "k=v;", TEST_AGG);
        drop(r1);
        let r2 = SeriesIdResolver::new();
        let s2 = r2.resolve("m", "k=v;", TEST_AGG);
        // Both resolvers start fresh, so both mint sid=1.
        assert_eq!(s1, 1);
        assert_eq!(s2, 1);
    }

    #[test]
    fn append_failure_logs_but_does_not_panic() {
        // A custom persistence impl that always returns an error.
        // The resolver should log + return the sid anyway (in-memory-
        // only); subsequent resolves for the same key hit the cache
        // and don't re-attempt append.
        struct FailingPersistence;
        impl SeriesResolverPersistence for FailingPersistence {
            fn append(&self, _: u64, _: &str, _: &str, _: &str) -> std::io::Result<()> {
                Err(std::io::Error::other("simulated I/O failure"))
            }
            fn replay(&self) -> std::io::Result<Vec<ResolverRecord>> {
                Ok(Vec::new())
            }
        }
        let r = SeriesIdResolver::with_persistence(Arc::new(FailingPersistence));
        let sid = r.resolve("m", "k=v;", TEST_AGG);
        assert_eq!(sid, 1, "resolver returns the sid despite persistence error");
        // Second call hits the cache; no second append attempt.
        let sid2 = r.resolve("m", "k=v;", TEST_AGG);
        assert_eq!(sid, sid2);
    }
}
