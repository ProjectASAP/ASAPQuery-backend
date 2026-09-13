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
//! in `docs/design_docs/series-identity.md`.

use dashmap::DashMap;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

/// Canonical sid identity: `(metric_name, attrs_fingerprint, agg_kind_canonical)`.
///
/// Sort attribute keys and join `key=value;` pairs exactly as the patched
/// OTel-Go exporter does. The aggregation kind separates different summaries
/// over the same metric and attributes.
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
        info!(replayed = records.len(), "series-resolver WAL replayed",);
        let cache: DashMap<CacheKey, u64> = DashMap::new();
        let mut max_sid: u64 = 0;
        for r in records {
            cache.insert((r.metric, r.attrs_fingerprint, r.agg_kind_canonical), r.sid);
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
    /// the returned identity is ephemeral and is not cached. Catalog-bound
    /// callers use `try_resolve` and receive the persistence failure instead.
    pub fn resolve(
        &self,
        metric_name: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
    ) -> u64 {
        match self.try_resolve(metric_name, attrs_fingerprint, agg_kind_canonical) {
            Ok(sid) => sid,
            Err(error) => {
                // Compatibility callers still receive an ephemeral ID, but it
                // must never enter the shared cache used by strict producers.
                let sid = self.next_sid.fetch_add(1, Ordering::Relaxed);
                warn!(%error, sid, "resolver persistence failed; returning uncached ephemeral identity");
                sid
            }
        }
    }

    /// Persist a new binding before exposing it to catalog-bound producers.
    pub fn try_resolve(&self, metric: &str, attrs: &str, kind: &str) -> std::io::Result<u64> {
        use dashmap::mapref::entry::Entry;
        let key = (metric.to_owned(), attrs.to_owned(), kind.to_owned());
        match self.cache.entry(key) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let sid = self
                    .next_sid
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                        value.checked_add(1)
                    })
                    .map_err(|_| std::io::Error::other("series ID exhausted"))?;
                self.persistence.append(sid, metric, attrs, kind)?;
                entry.insert(sid);
                Ok(sid)
            }
        }
    }

    /// Resolve a logical series and ask the storage lifecycle owner whether
    /// an explicit catalog activation requires a fresh physical lifetime.
    pub fn resolve_with_reactivation(
        &self,
        metric: &str,
        attrs: &str,
        kind: &str,
        authorize: impl FnOnce(u64) -> Result<Option<Arc<asap_types::sds::CatalogGeneration>>, String>,
    ) -> Result<u64, String> {
        let sid = self
            .try_resolve(metric, attrs, kind)
            .map_err(|error| error.to_string())?;
        match authorize(sid)? {
            None => Ok(sid),
            Some(generation) => self
                .rotate_for_catalog_activation(metric, attrs, kind, sid, &generation)
                .map_err(|error| error.to_string()),
        }
    }

    /// Advance a tombstoned physical series after the storage engine has
    /// authorized reactivation in a different installed catalog generation.
    /// The logical cache key remains unchanged. Persistence failure leaves
    /// the old binding intact. Concurrent activation requires fresh authorization.
    pub fn rotate_for_catalog_activation(
        &self,
        metric_name: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
        previous_sid: u64,
        generation: &asap_types::sds::CatalogGeneration,
    ) -> std::io::Result<u64> {
        let key = (
            metric_name.to_owned(),
            attrs_fingerprint.to_owned(),
            agg_kind_canonical.to_owned(),
        );
        let mut binding = self.cache.get_mut(&key).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "catalog activation requires an existing resolver binding",
            )
        })?;
        if *binding != previous_sid {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "physical series changed after catalog authorization; retry routing",
            ));
        }
        let replacement = self
            .next_sid
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |sid| {
                sid.checked_add(1)
            })
            .map_err(|_| std::io::Error::other("physical series ID space exhausted"))?;
        self.persistence.append_catalog_activation(
            replacement,
            metric_name,
            attrs_fingerprint,
            agg_kind_canonical,
            generation,
        )?;
        *binding = replacement;
        Ok(replacement)
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
// WAL v4: header b"ASAPSRP\x04", followed by u32-length-prefixed JSON
// records. CatalogGeneration records are normalized by snapshot digest;
// Binding records carry a reference only for explicitly authorized rotations.
// Opening a v3 WAL atomically migrates its durable prefix without changing
// physical IDs. v1/v2 remain unsupported because they used different identity
// semantics. Older binaries reject the v4 header rather than truncating it.
//
// Every append is fsynced before publication. Failed appends roll back to the
// previous offset; replay truncates incomplete trailing frames and rejects
// malformed or oversized complete frames. The append-only WAL has no garbage
// collection yet; lifetime rotations add bindings for the same logical key.

/// One durable binding row read back from the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolverRecord {
    pub sid: u64,
    pub metric: String,
    pub attrs_fingerprint: String,
    pub agg_kind_canonical: String,
    pub catalog_generation: Option<Arc<asap_types::sds::CatalogGeneration>>,
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

    /// Persist a replacement physical lifetime with its catalog provenance.
    /// Backends must explicitly implement this stronger contract; silently
    /// downgrading to a legacy binding append would lose authorization.
    fn append_catalog_activation(
        &self,
        _sid: u64,
        _metric: &str,
        _attrs_fingerprint: &str,
        _agg_kind_canonical: &str,
        _generation: &asap_types::sds::CatalogGeneration,
    ) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "resolver persistence does not support catalog activation",
        ))
    }

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

    fn append_catalog_activation(
        &self,
        _sid: u64,
        _metric: &str,
        _attrs_fingerprint: &str,
        _agg_kind_canonical: &str,
        _generation: &asap_types::sds::CatalogGeneration,
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
    generations: Mutex<BTreeMap<String, Arc<asap_types::sds::CatalogGeneration>>>,
}

const WAL_MAGIC: &[u8; 8] = b"ASAPSRP\x04";
const LEGACY_WAL_MAGIC: &[u8; 8] = b"ASAPSRP\x03";
const MAX_WAL_RECORD_BYTES: usize = 4 * 1024 * 1024;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WalRecord {
    CatalogGeneration {
        generation: Arc<asap_types::sds::CatalogGeneration>,
    },
    Binding {
        sid: u64,
        metric: String,
        attrs_fingerprint: String,
        agg_kind_canonical: String,
        #[serde(default)]
        generation_sha256: Option<String>,
    },
}

fn write_wal_record(file: &mut File, record: &WalRecord) -> std::io::Result<()> {
    let bytes = serde_json::to_vec(record).map_err(std::io::Error::other)?;
    if bytes.len() > MAX_WAL_RECORD_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "resolver WAL record exceeds size limit",
        ));
    }
    file.write_all(&(bytes.len() as u32).to_le_bytes())?;
    file.write_all(&bytes)
}
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
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        if file.metadata()?.len() == 0 {
            file.write_all(WAL_MAGIC)?;
            file.sync_all()?;
        } else {
            let mut header = [0; 8];
            file.read_exact(&mut header)?;
            if &header == LEGACY_WAL_MAGIC {
                let temporary = path.with_extension("v4.tmp");
                let mut migrated = File::create(&temporary)?;
                migrated.write_all(WAL_MAGIC)?;
                loop {
                    match read_one_record(&mut file) {
                        ReadOne::Ok(record, offset) => {
                            debug_assert!(offset >= 8);
                            write_wal_record(
                                &mut migrated,
                                &WalRecord::Binding {
                                    sid: record.sid,
                                    metric: record.metric,
                                    attrs_fingerprint: record.attrs_fingerprint,
                                    agg_kind_canonical: record.agg_kind_canonical,
                                    generation_sha256: None,
                                },
                            )?;
                        }
                        ReadOne::Eof | ReadOne::Torn => break,
                        ReadOne::Io(error) => return Err(error),
                        ReadOne::Corrupt => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "corrupt legacy resolver record",
                            ))
                        }
                    }
                }
                migrated.sync_all()?;
                std::fs::rename(&temporary, &path)?;
                if let Some(parent) = path.parent() {
                    File::open(parent)?.sync_all()?;
                }
                file = OpenOptions::new().read(true).write(true).open(&path)?;
            } else if &header != WAL_MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "resolver WAL header mismatch",
                ));
            }
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file: Mutex::new(file),
            generations: Mutex::new(BTreeMap::new()),
        })
    }

    fn append_binding(
        &self,
        sid: u64,
        metric: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
        generation: Option<&asap_types::sds::CatalogGeneration>,
    ) -> std::io::Result<()> {
        if sid == 0
            || metric.len() > MAX_METRIC_LEN
            || attrs_fingerprint.len() > MAX_FP_LEN
            || agg_kind_canonical.len() > MAX_AGG_KIND_LEN
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid resolver binding",
            ));
        }
        let mut file = self.file.lock().unwrap();
        let mut generations = self.generations.lock().unwrap();
        let offset = file.stream_position()?;
        let result = (|| {
            if let Some(generation) = generation {
                if let Some(existing) = generations.get(&generation.snapshot_sha256) {
                    if existing.as_ref() != generation {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            "conflicting catalog generation digest",
                        ));
                    }
                } else {
                    write_wal_record(
                        &mut file,
                        &WalRecord::CatalogGeneration {
                            generation: Arc::new(generation.clone()),
                        },
                    )?;
                }
            }
            write_wal_record(
                &mut file,
                &WalRecord::Binding {
                    sid,
                    metric: metric.into(),
                    attrs_fingerprint: attrs_fingerprint.into(),
                    agg_kind_canonical: agg_kind_canonical.into(),
                    generation_sha256: generation.map(|value| value.snapshot_sha256.clone()),
                },
            )?;
            file.sync_all()
        })();
        if result.is_err() {
            // Do not append behind an incomplete record after a failed write.
            file.set_len(offset)?;
            file.seek(SeekFrom::Start(offset))?;
            file.sync_all()?;
            return result;
        }
        if let Some(generation) = generation {
            generations
                .entry(generation.snapshot_sha256.clone())
                .or_insert_with(|| Arc::new(generation.clone()));
        }
        Ok(())
    }
}

impl SeriesResolverPersistence for FilePersistence {
    fn append(
        &self,
        sid: u64,
        metric: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
    ) -> std::io::Result<()> {
        self.append_binding(sid, metric, attrs_fingerprint, agg_kind_canonical, None)
    }

    fn append_catalog_activation(
        &self,
        sid: u64,
        metric: &str,
        attrs_fingerprint: &str,
        agg_kind_canonical: &str,
        generation: &asap_types::sds::CatalogGeneration,
    ) -> std::io::Result<()> {
        self.append_binding(
            sid,
            metric,
            attrs_fingerprint,
            agg_kind_canonical,
            Some(generation),
        )
    }

    fn replay(&self) -> std::io::Result<Vec<ResolverRecord>> {
        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0; 8];
        file.read_exact(&mut header)?;
        if &header != WAL_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "resolver WAL header mismatch",
            ));
        }
        let mut generations = BTreeMap::new();
        let mut records = Vec::new();
        let mut physical_keys = BTreeMap::new();
        let mut logical_sids = BTreeMap::new();
        let mut safe_offset = 8;
        loop {
            let mut length = [0; 4];
            match file.read_exact(&mut length) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    file.set_len(safe_offset)?;
                    break;
                }
                Err(error) => return Err(error),
            }
            let length = u32::from_le_bytes(length) as usize;
            if length > MAX_WAL_RECORD_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "resolver WAL record exceeds size limit",
                ));
            }
            let mut bytes = vec![0; length];
            if let Err(error) = file.read_exact(&mut bytes) {
                if error.kind() == std::io::ErrorKind::UnexpectedEof {
                    file.set_len(safe_offset)?;
                    break;
                }
                return Err(error);
            }
            let record: WalRecord = serde_json::from_slice(&bytes)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            match record {
                WalRecord::CatalogGeneration { generation } => {
                    let key = generation.snapshot_sha256.clone();
                    if generations
                        .get(&key)
                        .is_some_and(|existing| existing != &generation)
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "conflicting catalog generation digest",
                        ));
                    }
                    generations.insert(key, generation);
                }
                WalRecord::Binding {
                    sid,
                    metric,
                    attrs_fingerprint,
                    agg_kind_canonical,
                    generation_sha256,
                } => {
                    if sid == 0
                        || metric.len() > MAX_METRIC_LEN
                        || attrs_fingerprint.len() > MAX_FP_LEN
                        || agg_kind_canonical.len() > MAX_AGG_KIND_LEN
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "invalid resolver binding",
                        ));
                    }
                    let key = (
                        metric.clone(),
                        attrs_fingerprint.clone(),
                        agg_kind_canonical.clone(),
                    );
                    if physical_keys.get(&sid).is_some_and(|existing| {
                        existing != &(key.clone(), generation_sha256.clone())
                    }) || logical_sids.get(&key).is_some_and(|previous| {
                        *previous != sid && (generation_sha256.is_none() || sid < *previous)
                    }) {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "conflicting resolver binding",
                        ));
                    }
                    physical_keys.insert(sid, (key.clone(), generation_sha256.clone()));
                    logical_sids.insert(key, sid);
                    let catalog_generation = generation_sha256
                        .map(|key| {
                            generations.get(&key).cloned().ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "missing catalog generation reference",
                                )
                            })
                        })
                        .transpose()?;
                    records.push(ResolverRecord {
                        sid,
                        metric,
                        attrs_fingerprint,
                        agg_kind_canonical,
                        catalog_generation,
                    });
                }
            }
            safe_offset = file.stream_position()?;
        }
        file.seek(SeekFrom::End(0))?;
        *self.generations.lock().unwrap() = generations;
        Ok(records)
    }
}

/// Outcome of attempting to read a single WAL record. `Torn` means a
/// short read was detected mid-record; malformed complete fields are corruption.
/// Only an incomplete tail may be discarded;
/// the caller truncates the file to the last `Ok` offset.
enum ReadOne {
    Ok(ResolverRecord, u64),
    Eof,
    Torn,
    Corrupt,
    Io(std::io::Error),
}

fn read_one_record(f: &mut File) -> ReadOne {
    let mut sid_buf = [0u8; 8];
    match f.read_exact(&mut sid_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return ReadOne::Eof,
        Err(error) => return ReadOne::Io(error),
    }
    let sid = u64::from_le_bytes(sid_buf);
    if sid == 0 {
        return ReadOne::Corrupt;
    }

    let mut len_buf = [0u8; 4];
    if let Err(error) = f.read_exact(&mut len_buf) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadOne::Torn
        } else {
            ReadOne::Io(error)
        };
    }
    let metric_len = u32::from_le_bytes(len_buf) as usize;
    if metric_len > MAX_METRIC_LEN {
        return ReadOne::Corrupt;
    }
    let mut metric_bytes = vec![0u8; metric_len];
    if let Err(error) = f.read_exact(&mut metric_bytes) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadOne::Torn
        } else {
            ReadOne::Io(error)
        };
    }

    if let Err(error) = f.read_exact(&mut len_buf) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadOne::Torn
        } else {
            ReadOne::Io(error)
        };
    }
    let fp_len = u32::from_le_bytes(len_buf) as usize;
    if fp_len > MAX_FP_LEN {
        return ReadOne::Corrupt;
    }
    let mut fp_bytes = vec![0u8; fp_len];
    if let Err(error) = f.read_exact(&mut fp_bytes) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadOne::Torn
        } else {
            ReadOne::Io(error)
        };
    }

    if let Err(error) = f.read_exact(&mut len_buf) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadOne::Torn
        } else {
            ReadOne::Io(error)
        };
    }
    let agg_kind_len = u32::from_le_bytes(len_buf) as usize;
    if agg_kind_len > MAX_AGG_KIND_LEN {
        return ReadOne::Corrupt;
    }
    let mut agg_kind_bytes = vec![0u8; agg_kind_len];
    if let Err(error) = f.read_exact(&mut agg_kind_bytes) {
        return if error.kind() == std::io::ErrorKind::UnexpectedEof {
            ReadOne::Torn
        } else {
            ReadOne::Io(error)
        };
    }

    let metric = match String::from_utf8(metric_bytes) {
        Ok(s) => s,
        Err(_) => return ReadOne::Corrupt,
    };
    let attrs_fingerprint = match String::from_utf8(fp_bytes) {
        Ok(s) => s,
        Err(_) => return ReadOne::Corrupt,
    };
    let agg_kind_canonical = match String::from_utf8(agg_kind_bytes) {
        Ok(s) => s,
        Err(_) => return ReadOne::Corrupt,
    };
    let new_offset = match f.stream_position() {
        Ok(p) => p,
        Err(error) => return ReadOne::Io(error),
    };
    ReadOne::Ok(
        ResolverRecord {
            sid,
            metric,
            attrs_fingerprint,
            agg_kind_canonical,
            catalog_generation: None,
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
/// Select the population key protocol explicitly carried by the policy identity.
/// Legacy bytes remain unchanged; canonical keys reject ambiguous duplicate names.
pub fn population_attrs_fingerprint(
    encoding: asap_types::PopulationKeyEncoding,
    attrs: &[(&str, &str)],
) -> Result<String, String> {
    match encoding {
        asap_types::PopulationKeyEncoding::LegacyDelimited => {
            Ok(canonical_attrs_fingerprint(attrs))
        }
        asap_types::PopulationKeyEncoding::CanonicalLabelsV1 => {
            let mut labels = std::collections::BTreeMap::new();
            for (name, value) in attrs {
                if labels
                    .insert((*name).to_string(), (*value).to_string())
                    .is_some()
                {
                    return Err("duplicate population label name".into());
                }
            }
            asap_types::grouping_projection::encode_label_population_key(&labels)
        }
    }
}

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
    fn versioned_population_routing_separates_delimiter_collisions() {
        use asap_types::PopulationKeyEncoding::{CanonicalLabelsV1, LegacyDelimited};
        let a = [("a", "b;c=d")];
        let b = [("a", "b"), ("c", "d")];
        assert_eq!(
            population_attrs_fingerprint(LegacyDelimited, &a).unwrap(),
            "a=b;c=d;"
        );
        assert_eq!(
            population_attrs_fingerprint(LegacyDelimited, &a),
            population_attrs_fingerprint(LegacyDelimited, &b)
        );
        let ka = population_attrs_fingerprint(CanonicalLabelsV1, &a).unwrap();
        let kb = population_attrs_fingerprint(CanonicalLabelsV1, &b).unwrap();
        assert_ne!(ka, kb);
        assert_eq!(
            kb,
            population_attrs_fingerprint(CanonicalLabelsV1, &[("c", "d"), ("a", "b")]).unwrap()
        );
        let resolver = SeriesIdResolver::new();
        assert_ne!(
            resolver.resolve("m", &ka, TEST_AGG),
            resolver.resolve("m", &kb, TEST_AGG)
        );
        assert!(
            population_attrs_fingerprint(CanonicalLabelsV1, &[("a", "1"), ("a", "2")]).is_err()
        );
    }

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

    fn generation() -> asap_types::sds::CatalogGeneration {
        asap_types::sds::CatalogGeneration {
            schema_version: 1,
            plan_id: 7,
            plan_version: 2,
            snapshot_sha256: "new-catalog".into(),
        }
    }

    #[test]
    fn catalog_rotation_is_once_and_survives_restart_with_provenance() {
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        let resolver = Arc::new(SeriesIdResolver::open(path.clone()).unwrap());
        let previous = resolver.resolve("m", "group=a", TEST_AGG);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let resolver = Arc::clone(&resolver);
                std::thread::spawn(move || {
                    resolver.rotate_for_catalog_activation(
                        "m",
                        "group=a",
                        TEST_AGG,
                        previous,
                        &generation(),
                    )
                })
            })
            .collect();
        let ids: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        let successful: Vec<_> = ids
            .iter()
            .filter_map(|result| result.as_ref().ok())
            .copied()
            .collect();
        assert_eq!(successful.len(), 1);
        assert_ne!(successful[0], previous);
        assert!(ids
            .iter()
            .filter_map(|result| result.as_ref().err())
            .all(|error| error.kind() == std::io::ErrorKind::WouldBlock));
        drop(resolver);
        let reopened = SeriesIdResolver::open(path.clone()).unwrap();
        assert_eq!(reopened.resolve("m", "group=a", TEST_AGG), successful[0]);
        let records = FilePersistence::open(path).unwrap().replay().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[1].catalog_generation.as_deref(),
            Some(&generation())
        );
    }

    #[test]
    fn failed_catalog_rotation_keeps_old_resolver_mapping() {
        struct RejectRotation;
        impl SeriesResolverPersistence for RejectRotation {
            fn append(&self, _: u64, _: &str, _: &str, _: &str) -> std::io::Result<()> {
                Ok(())
            }
            fn replay(&self) -> std::io::Result<Vec<ResolverRecord>> {
                Ok(vec![])
            }
        }
        let resolver = SeriesIdResolver::with_persistence(Arc::new(RejectRotation));
        let previous = resolver.resolve("m", "group=a", TEST_AGG);
        assert!(resolver
            .rotate_for_catalog_activation("m", "group=a", TEST_AGG, previous, &generation())
            .is_err());
        assert_eq!(resolver.resolve("m", "group=a", TEST_AGG), previous);
    }

    #[test]
    fn legacy_v3_bindings_migrate_without_changing_physical_ids() {
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        let mut file = File::create(&path).unwrap();
        file.write_all(LEGACY_WAL_MAGIC).unwrap();
        file.write_all(&9u64.to_le_bytes()).unwrap();
        for field in ["m", "group=a", TEST_AGG] {
            file.write_all(&(field.len() as u32).to_le_bytes()).unwrap();
            file.write_all(field.as_bytes()).unwrap();
        }
        file.sync_all().unwrap();
        drop(file);
        let resolver = SeriesIdResolver::open(path.clone()).unwrap();
        assert_eq!(resolver.resolve("m", "group=a", TEST_AGG), 9);
        assert_eq!(resolver.resolve("m", "group=b", TEST_AGG), 10);
        assert_eq!(&std::fs::read(path).unwrap()[..8], WAL_MAGIC);
    }

    #[cfg(unix)]
    #[test]
    fn legacy_io_failure_is_not_an_incomplete_tail() {
        let dir = TempDir::new().unwrap();
        let mut unreadable_stream = File::open(dir.path()).unwrap();
        assert!(matches!(
            read_one_record(&mut unreadable_stream),
            ReadOne::Io(_)
        ));
    }

    #[test]
    fn corrupt_legacy_record_preserves_original_file() {
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        let mut bytes = LEGACY_WAL_MAGIC.to_vec();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&((MAX_METRIC_LEN + 1) as u32).to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        assert!(FilePersistence::open(path.clone()).is_err());
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn replay_rejects_invalid_or_conflicting_physical_bindings() {
        for second in [0, 1] {
            let dir = TempDir::new().unwrap();
            let path = wal_path(&dir);
            let persistence = FilePersistence::open(path.clone()).unwrap();
            persistence.append(1, "first", "", TEST_AGG).unwrap();
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            write_wal_record(
                &mut file,
                &WalRecord::Binding {
                    sid: second,
                    metric: "other".into(),
                    attrs_fingerprint: "".into(),
                    agg_kind_canonical: TEST_AGG.into(),
                    generation_sha256: None,
                },
            )
            .unwrap();
            assert!(persistence.replay().is_err());
        }
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
        // Write two clean frames, then a length prefix with no payload.
        {
            let p = FilePersistence::open(path.clone()).unwrap();
            p.append(1, "m", "k=v;", TEST_AGG).unwrap();
            p.append(2, "m", "k=w;", TEST_AGG).unwrap();
        }
        // The third frame claims 999 bytes but has no payload.
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&999u32.to_le_bytes()).unwrap();
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
    fn oversized_frame_fails_without_allocating_or_discarding_history() {
        // An oversized frame is corruption, not a torn tail. Reject it
        // before allocation and leave the durable file unchanged.
        let dir = TempDir::new().unwrap();
        let path = wal_path(&dir);
        {
            let p = FilePersistence::open(path.clone()).unwrap();
            p.append(1, "m", "k=v;", TEST_AGG).unwrap();
        }
        {
            use std::io::Write;
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&((MAX_WAL_RECORD_BYTES as u32) + 1).to_le_bytes())
                .unwrap();
            f.sync_all().unwrap();
        }
        let before = std::fs::metadata(&path).unwrap().len();
        let p = FilePersistence::open(path.clone()).unwrap();
        assert_eq!(
            p.replay().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(std::fs::metadata(path).unwrap().len(), before);
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
        assert!(r
            .resolve_with_reactivation("strict", "k=v;", TEST_AGG, |_| Ok(None))
            .is_err());
        assert!(!r
            .cache
            .contains_key(&("strict".into(), "k=v;".into(), TEST_AGG.into())));
        let sid = r.resolve("m", "k=v;", TEST_AGG);
        assert!(sid > 0, "legacy resolver returns an uncached ephemeral sid");
        assert!(r.try_resolve("m", "k=v;", TEST_AGG).is_err());
        assert_eq!(r.lookup("m", "k=v;", TEST_AGG), None);
        let sid2 = r.resolve("m", "k=v;", TEST_AGG);
        assert_ne!(sid, sid2);
    }
}
