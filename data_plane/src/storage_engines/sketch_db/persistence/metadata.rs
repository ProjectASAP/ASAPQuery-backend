//! Per-sid metadata sidecar for the durable warm-sketch tier.
//!
//! ## Why this exists
//!
//! On-disk parts (`part.rs`) store, per entry, only: the `sid` (as
//! `agg_id`), the label *values* (`KeyByLabelValues`), the
//! `sketch_type_name`, the encoding tag, the time bounds, and the
//! opaque sketch bytes. They do NOT carry the pieces of
//! [`SketchInstanceMetadata`](crate::storage_engines::sketch_db::index::SketchInstanceMetadata)
//! that the QUERY path needs to find and serve a series:
//!
//! * `metric_name` — the analyzer's
//!   [`instances_matching`](crate::storage_engines::sketch_db::index::SketchStore::instances_matching)
//!   filters on it.
//! * `group_by_keys` (label KEYS, not values) — needed both by
//!   `instances_matching` AND by the disk-union read path
//!   (`sid_group_by_keys` → `rebuild_label_map`), which zips the sorted
//!   keys against the stored value vector to reconstruct the label map.
//! * `capability` / `agg_kind` — the sketch reducer's
//!   `require_capability` gate.
//!
//! Without these, a `SketchStore` that is freshly reopened after a
//! restart recovers the parts manifest + part cache but registers NO
//! sids in its in-memory `instances` map (registration only ever happens
//! on the live ingest path, when a fresh DataPoint arrives). So
//! `instances_matching` enumerates nothing for the recovered metrics and
//! `query_range`'s disk-union early-returns on the missing
//! `sid_group_by_keys` → the query returns "No result" cluster-wide even
//! though the data is durable on disk.
//!
//! This sidecar closes that gap: the flusher upserts a compact,
//! self-describing record per flushed sid into `sid_metadata.json`, and
//! recovery replays it to re-register every disk-resident sid as a
//! queryable instance.
//!
//! ## Format
//!
//! A single JSON object `{ "<sid>": SidMetaRecord, ... }` written
//! atomically (tmp + rename) on every upsert. JSON (not the custom
//! binary part format) because the record count equals live sid
//! cardinality (small) and the schema is human-inspectable for
//! diagnosis. The record stores serializable PRIMITIVES — the
//! `Capability` / `AccuracyBound` are DERIVED on load from `agg_kind`
//! exactly as the ingest path derives them, so this module needs no
//! serde on the control-plane `Capability` / `SketchAlgorithm` enums.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::storage_engines::sketch_db::data::{
    AccuracyBound, AggKind, Capability, SketchAlgorithm, SketchConfig,
};
use crate::storage_engines::types::AggregationType;

use super::{PersistError, PersistResult};

/// File name of the sid-metadata sidecar under the persistence dir.
pub const SERIES_ID_METADATA_FILE: &str = "sid_metadata.json";

/// Serializable mirror of [`SketchConfig`]. Kept local (rather than
/// deriving serde on the control-plane `SketchConfig`) so the sidecar
/// schema is owned by the persistence layer and changes here can't
/// silently shift the on-disk format from an unrelated edit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SketchConfigRec {
    UnivMon {
        heap_size: u32,
        sketch_rows: u32,
        sketch_cols: u32,
        layers: u8,
    },
    DdSketch {
        relative_accuracy: f64,
    },
    Kll {
        k: u32,
    },
    Hll {
        precision: u32,
    },
    CountSketch {
        rows: i32,
        cols: i32,
    },
    CountMin {
        rows: i32,
        cols: i32,
    },
}

impl From<&SketchConfig> for SketchConfigRec {
    fn from(c: &SketchConfig) -> Self {
        match c {
            SketchConfig::DDSketch { relative_accuracy } => SketchConfigRec::DdSketch {
                relative_accuracy: *relative_accuracy,
            },
            SketchConfig::Kll { k } => SketchConfigRec::Kll { k: *k },
            SketchConfig::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } => SketchConfigRec::UnivMon {
                heap_size: *heap_size,
                sketch_rows: *sketch_rows,
                sketch_cols: *sketch_cols,
                layers: *layers,
            },
            SketchConfig::Hll { precision } => SketchConfigRec::Hll {
                precision: *precision,
            },
            SketchConfig::CountSketch { rows, cols } => SketchConfigRec::CountSketch {
                rows: *rows,
                cols: *cols,
            },
            SketchConfig::CountMin { rows, cols } => SketchConfigRec::CountMin {
                rows: *rows,
                cols: *cols,
            },
        }
    }
}

impl From<&SketchConfigRec> for SketchConfig {
    fn from(c: &SketchConfigRec) -> Self {
        match c {
            SketchConfigRec::DdSketch { relative_accuracy } => SketchConfig::DDSketch {
                relative_accuracy: *relative_accuracy,
            },
            SketchConfigRec::Kll { k } => SketchConfig::Kll { k: *k },
            SketchConfigRec::UnivMon {
                heap_size,
                sketch_rows,
                sketch_cols,
                layers,
            } => SketchConfig::UnivMon {
                heap_size: *heap_size,
                sketch_rows: *sketch_rows,
                sketch_cols: *sketch_cols,
                layers: *layers,
            },
            SketchConfigRec::Hll { precision } => SketchConfig::Hll {
                precision: *precision,
            },
            SketchConfigRec::CountSketch { rows, cols } => SketchConfig::CountSketch {
                rows: *rows,
                cols: *cols,
            },
            SketchConfigRec::CountMin { rows, cols } => SketchConfig::CountMin {
                rows: *rows,
                cols: *cols,
            },
        }
    }
}

/// Stable string form of a [`SketchAlgorithm`] for the sidecar. Mirrors
/// `sketch_algorithm_canonical` but is owned by the persistence layer so the
/// on-disk vocabulary is stable independent of any upstream rename.
fn sketch_algorithm_to_str(k: SketchAlgorithm) -> &'static str {
    match k {
        SketchAlgorithm::DDSketch => "DDSketch",
        SketchAlgorithm::Kll => "Kll",
        SketchAlgorithm::Hll => "Hll",
        SketchAlgorithm::UnivMon => "UnivMon",
        SketchAlgorithm::CountSketch => "CountSketch",
        SketchAlgorithm::Cms => "CountMin",
        SketchAlgorithm::CmsWithHeap => "CmsWithHeap",
        SketchAlgorithm::CountSketchWithHeap => "CountSketchWithHeap",
        SketchAlgorithm::Kmv => "Kmv",
        SketchAlgorithm::Theta => "Theta",
    }
}

fn sketch_algorithm_from_str(s: &str) -> Option<SketchAlgorithm> {
    Some(match s {
        "DDSketch" => SketchAlgorithm::DDSketch,
        "Kll" => SketchAlgorithm::Kll,
        "Hll" => SketchAlgorithm::Hll,
        "UnivMon" => SketchAlgorithm::UnivMon,
        "CountSketch" => SketchAlgorithm::CountSketch,
        "CountMin" => SketchAlgorithm::Cms,
        "CmsWithHeap" => SketchAlgorithm::CmsWithHeap,
        "CountSketchWithHeap" => SketchAlgorithm::CountSketchWithHeap,
        "Kmv" => SketchAlgorithm::Kmv,
        "Theta" => SketchAlgorithm::Theta,
        // `Any` was never a valid stored implementation. Reject legacy
        // sidecars that contain it instead of inventing an algorithm.
        "Any" => return None,
        _ => return None,
    })
}

/// Serializable mirror of the two [`AggKind`] branches.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum AggKindRec {
    Sketch {
        sketch_kind: String,
        config: SketchConfigRec,
        spatial_filter_canonical: String,
    },
    ExactAgg {
        agg_type: AggregationType,
        parameters_canonical: String,
        spatial_filter_canonical: String,
    },
}

impl From<&AggKind> for AggKindRec {
    fn from(a: &AggKind) -> Self {
        match a {
            AggKind::Sketch {
                algorithm: kind,
                config,
                spatial_filter_canonical,
            } => AggKindRec::Sketch {
                sketch_kind: sketch_algorithm_to_str(kind.clone()).to_string(),
                config: config.into(),
                spatial_filter_canonical: spatial_filter_canonical.clone(),
            },
            AggKind::ExactAgg {
                agg_type,
                parameters_canonical,
                spatial_filter_canonical,
            } => AggKindRec::ExactAgg {
                agg_type: *agg_type,
                parameters_canonical: parameters_canonical.clone(),
                spatial_filter_canonical: spatial_filter_canonical.clone(),
            },
        }
    }
}

impl AggKindRec {
    /// Rebuild the structured [`AggKind`]. Returns `None` if a sketch
    /// kind string is unrecognized (forward-compat: a newer writer added
    /// a kind this reader doesn't know — skip the sid rather than panic).
    fn to_agg_kind(&self) -> Option<AggKind> {
        Some(match self {
            AggKindRec::Sketch {
                sketch_kind,
                config,
                spatial_filter_canonical,
            } => AggKind::Sketch {
                algorithm: sketch_algorithm_from_str(sketch_kind)?,
                config: config.into(),
                spatial_filter_canonical: spatial_filter_canonical.clone(),
            },
            AggKindRec::ExactAgg {
                agg_type,
                parameters_canonical,
                spatial_filter_canonical,
            } => AggKind::ExactAgg {
                agg_type: *agg_type,
                parameters_canonical: parameters_canonical.clone(),
                spatial_filter_canonical: spatial_filter_canonical.clone(),
            },
        })
    }

    fn without_population_filter(&self) -> Self {
        let mut operator = self.clone();
        match &mut operator {
            Self::Sketch {
                spatial_filter_canonical,
                ..
            }
            | Self::ExactAgg {
                spatial_filter_canonical,
                ..
            } => spatial_filter_canonical.clear(),
        }
        operator
    }

    fn with_population_filter(&self, filter: &str) -> Self {
        let mut kind = self.clone();
        match &mut kind {
            Self::Sketch {
                spatial_filter_canonical,
                ..
            }
            | Self::ExactAgg {
                spatial_filter_canonical,
                ..
            } => *spatial_filter_canonical = filter.to_string(),
        }
        kind
    }
}

/// One durable sid-metadata row. The query-critical fields
/// (`metric_name`, `group_by_keys`, `agg_kind`) are stored explicitly;
/// `capability` and `accuracy` are DERIVED from `agg_kind` on load, the
/// same way the ingest path derives them, so the record stays minimal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SidMetaRecord {
    pub sid: u64,
    /// Authoritative identity and provenance, absent on legacy sidecars.
    #[serde(default)]
    pub summary_definition_id: Option<asap_types::sds::SummaryDefinitionId>,
    #[serde(default)]
    pub catalog_generation: Option<std::sync::Arc<asap_types::sds::CatalogGeneration>>,
    pub metric_name: String,
    /// Label KEY set, sorted (a `Vec` so the JSON stays compact; the
    /// store side rebuilds the `BTreeSet`).
    pub group_by_keys: Vec<String>,
    agg_kind: AggKindRec,
    pub first_seen_unix_ms: i64,
    #[serde(default)]
    pub retired_at_ms: Option<u64>,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
    #[serde(default)]
    pub removed: bool,
    /// No further publication may change a window ending at or before this bound.
    #[serde(default)]
    pub completed_through_ms: Option<u64>,
}

impl SidMetaRecord {
    /// Build a record from the live store-side fields. `agg_kind` is the
    /// structured `AggKind`; `capability`/`accuracy` are intentionally
    /// NOT stored (re-derived on load).
    pub fn new(
        sid: u64,
        metric_name: String,
        group_by_keys: Vec<String>,
        agg_kind: &AggKind,
        first_seen_unix_ms: i64,
    ) -> Self {
        Self {
            sid,
            summary_definition_id: None,
            catalog_generation: None,
            metric_name,
            group_by_keys,
            agg_kind: agg_kind.into(),
            first_seen_unix_ms,
            retired_at_ms: None,
            expires_at_ms: None,
            removed: false,
            completed_through_ms: None,
        }
    }

    /// Reconstruct the structured [`AggKind`], or `None` for an
    /// unrecognized sketch kind.
    pub fn agg_kind(&self) -> Option<AggKind> {
        self.agg_kind.to_agg_kind()
    }

    /// Derive the warm-tier [`Capability`] from `agg_kind`, mirroring the
    /// ingest path (`otel.rs`) and `ingest_precompute_with_series_id`. Returns
    /// `None` only when `agg_kind` itself fails to reconstruct.
    pub fn capability(&self) -> Option<Capability> {
        let agg_kind = self.agg_kind()?;
        Some(match agg_kind {
            AggKind::Sketch {
                algorithm: kind, ..
            } => match kind {
                SketchAlgorithm::DDSketch | SketchAlgorithm::Kll => {
                    Capability::QuantileApprox(Some(kind))
                }
                SketchAlgorithm::Hll | SketchAlgorithm::UnivMon => Capability::CardinalityApprox,
                SketchAlgorithm::CountSketch | SketchAlgorithm::Cms => {
                    Capability::FrequencyEstimate(Some(kind))
                }
                SketchAlgorithm::CmsWithHeap | SketchAlgorithm::CountSketchWithHeap => {
                    Capability::FrequencyTopk(Some(kind))
                }
                SketchAlgorithm::Kmv | SketchAlgorithm::Theta => Capability::CardinalityApprox,
            },
            AggKind::ExactAgg { agg_type, .. } => Capability::ExactAgg(agg_type),
        })
    }

    /// Derive the [`AccuracyBound`] — `Some` for sketch-backed sids,
    /// `None` for exact-agg sids (mirrors the ingest registration).
    pub fn accuracy(&self) -> Option<AccuracyBound> {
        match self.agg_kind()? {
            AggKind::Sketch { config, .. } => Some(AccuracyBound::from_config(&config)),
            AggKind::ExactAgg { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct DataDescriptorRec {
    metric_name: String,
    population_filter_canonical: String,
    group_by_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SidBindingRec {
    sid: u64,
    #[serde(default)]
    summary_definition_id: Option<asap_types::sds::SummaryDefinitionId>,
    #[serde(default)]
    catalog_generation_sha256: Option<String>,
    summary_descriptor_id: String,
    data_descriptor_id: String,
    first_seen_unix_ms: i64,
    #[serde(default)]
    retired_at_ms: Option<u64>,
    #[serde(default)]
    expires_at_ms: Option<u64>,
    #[serde(default)]
    removed: bool,
    #[serde(default)]
    completed_through_ms: Option<u64>,
}

/// Version-3 normalized sidecar with authoritative catalog provenance. Descriptors appear once and SeriesId bindings hold
/// foreign keys, mirroring the in-memory SDS registry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SdsSidecar {
    schema_version: u32,
    #[serde(default)]
    catalog_generations: HashMap<String, std::sync::Arc<asap_types::sds::CatalogGeneration>>,
    summary_descriptors: HashMap<String, AggKindRec>,
    data_descriptors: HashMap<String, DataDescriptorRec>,
    bindings: HashMap<String, SidBindingRec>,
}

impl SdsSidecar {
    fn from_records(records: impl IntoIterator<Item = SidMetaRecord>) -> Self {
        use crate::storage_engines::sketch_db::sds::{data_descriptor_id, summary_descriptor_id};

        let mut sidecar = Self {
            schema_version: 3,
            catalog_generations: HashMap::new(),
            summary_descriptors: HashMap::new(),
            data_descriptors: HashMap::new(),
            bindings: HashMap::new(),
        };
        for record in records {
            let Some(kind) = record.agg_kind() else {
                continue;
            };
            let summary_id = summary_descriptor_id(&kind).canonical().to_string();
            let filter = kind.spatial_filter_canonical().to_string();
            let data_id = data_descriptor_id(
                &record.metric_name,
                &filter,
                record.group_by_keys.iter().map(String::as_str),
            )
            .canonical()
            .to_string();
            sidecar
                .summary_descriptors
                .entry(summary_id.clone())
                .or_insert_with(|| record.agg_kind.without_population_filter());
            sidecar
                .data_descriptors
                .entry(data_id.clone())
                .or_insert_with(|| DataDescriptorRec {
                    metric_name: record.metric_name,
                    population_filter_canonical: filter,
                    group_by_keys: record.group_by_keys,
                });
            let generation_sha256 = record.catalog_generation.map(|generation| {
                let digest = generation.snapshot_sha256.clone();
                sidecar
                    .catalog_generations
                    .entry(digest.clone())
                    .or_insert(generation);
                digest
            });
            sidecar.bindings.insert(
                record.sid.to_string(),
                SidBindingRec {
                    sid: record.sid,
                    summary_definition_id: record.summary_definition_id,
                    catalog_generation_sha256: generation_sha256,
                    summary_descriptor_id: summary_id,
                    data_descriptor_id: data_id,
                    first_seen_unix_ms: record.first_seen_unix_ms,
                    retired_at_ms: record.retired_at_ms,
                    expires_at_ms: record.expires_at_ms,
                    removed: record.removed,
                    completed_through_ms: record.completed_through_ms,
                },
            );
        }
        sidecar
    }

    fn into_records(self) -> PersistResult<Vec<SidMetaRecord>> {
        self.bindings
            .into_values()
            .map(|binding| {
                let operator = self
                    .summary_descriptors
                    .get(&binding.summary_descriptor_id)
                    .ok_or_else(|| {
                        PersistError::Format(format!(
                            "SeriesId {} references missing summary descriptor {}",
                            binding.sid, binding.summary_descriptor_id
                        ))
                    })?;
                let data = self
                    .data_descriptors
                    .get(&binding.data_descriptor_id)
                    .ok_or_else(|| {
                        PersistError::Format(format!(
                            "SeriesId {} references missing data descriptor {}",
                            binding.sid, binding.data_descriptor_id
                        ))
                    })?;
                Ok(SidMetaRecord {
                    sid: binding.sid,
                    summary_definition_id: binding.summary_definition_id,
                    catalog_generation: binding
                        .catalog_generation_sha256
                        .as_ref()
                        .map(|digest| {
                            self.catalog_generations
                                .get(digest)
                                .cloned()
                                .ok_or_else(|| {
                                    PersistError::Format(format!(
                                        "SeriesId {} references missing catalog generation",
                                        binding.sid
                                    ))
                                })
                        })
                        .transpose()?,
                    metric_name: data.metric_name.clone(),
                    group_by_keys: data.group_by_keys.clone(),
                    agg_kind: operator.with_population_filter(&data.population_filter_canonical),
                    first_seen_unix_ms: binding.first_seen_unix_ms,
                    retired_at_ms: binding.retired_at_ms,
                    expires_at_ms: binding.expires_at_ms,
                    removed: binding.removed,
                    completed_through_ms: binding.completed_through_ms,
                })
            })
            .collect()
    }
}

/// File-backed sid-metadata sidecar. The whole map is rewritten on every
/// upsert (atomic tmp + rename). Live sid cardinality is small, so a full
/// rewrite per flush tick is cheap and keeps the on-disk file always
/// consistent with no log-replay machinery.
#[derive(Debug)]
pub struct SidMetadataStore {
    path: PathBuf,
    writer: std::sync::Mutex<()>,
}

impl SidMetadataStore {
    /// Open (or lazily create on first write) the sidecar at
    /// `<disk_path>/sid_metadata.json`.
    pub fn new(disk_path: &Path) -> Self {
        Self {
            path: disk_path.join(SERIES_ID_METADATA_FILE),
            writer: std::sync::Mutex::new(()),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load every durable record. Returns an empty vec when the sidecar
    /// doesn't exist yet (fresh dir, or parts written before this feature
    /// landed) or when it is unparsable (treated as "no recoverable
    /// metadata" — the live ingest path still re-registers on first DP).
    pub fn load(&self) -> PersistResult<Vec<SidMetaRecord>> {
        let mut f = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(PersistError::Io(e)),
        };
        let mut buf = String::new();
        f.read_to_string(&mut buf)?;
        if buf.trim().is_empty() {
            return Ok(Vec::new());
        }
        let value: serde_json::Value = match serde_json::from_str(&buf) {
            Ok(value) => value,
            Err(e) => {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %e,
                    "sid metadata sidecar unparsable; ignoring (live ingest will re-register)"
                );
                return Ok(Vec::new());
            }
        };
        if matches!(
            value.get("schema_version").and_then(|v| v.as_u64()),
            Some(2 | 3)
        ) {
            let sidecar: SdsSidecar = match serde_json::from_value(value) {
                Ok(sidecar) => sidecar,
                Err(error) => {
                    tracing::warn!(
                        path = %self.path.display(),
                        %error,
                        "SDS metadata sidecar is invalid; ignoring"
                    );
                    return Ok(Vec::new());
                }
            };
            return match sidecar.into_records() {
                Ok(records) => Ok(records),
                Err(error) => {
                    tracing::warn!(
                        path = %self.path.display(),
                        %error,
                        "SDS metadata sidecar has broken descriptor references; ignoring"
                    );
                    Ok(Vec::new())
                }
            };
        }
        // Version 1 was a flat SeriesId map. Read it and normalize on the next write.
        let map: HashMap<String, SidMetaRecord> = match serde_json::from_value(value) {
            Ok(map) => map,
            Err(error) => {
                tracing::warn!(path = %self.path.display(), %error, "legacy sid metadata is invalid; ignoring");
                return Ok(Vec::new());
            }
        };
        Ok(map.into_values().collect())
    }

    /// Upsert a batch of records, merging with whatever is already on
    /// disk (last write wins per sid). Atomic via tmp + rename + dir
    /// fsync, matching the manifest's durability discipline.
    pub fn upsert_all(&self, records: &[SidMetaRecord]) -> PersistResult<()> {
        let _writer = self.writer.lock().map_err(|_| {
            PersistError::Io(std::io::Error::other("summary metadata writer poisoned"))
        })?;
        if records.is_empty() {
            return Ok(());
        }
        let mut map: HashMap<String, SidMetaRecord> = self
            .load()?
            .into_iter()
            .map(|r| (r.sid.to_string(), r))
            .collect();
        let mut changed = false;
        for r in records {
            let key = r.sid.to_string();
            let mut next = r.clone();
            if let Some(existing) = map.get(&key) {
                // Lifecycle is monotone for a SeriesId. An older flush snapshot
                // must not resurrect a retired or removed persisted instance.
                next.removed |= existing.removed;
                next.completed_through_ms =
                    existing.completed_through_ms.max(next.completed_through_ms);
                next.retired_at_ms = existing.retired_at_ms.or(next.retired_at_ms);
                next.expires_at_ms = match (existing.expires_at_ms, next.expires_at_ms) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
            }
            if map.get(&key) != Some(&next) {
                map.insert(key, next);
                changed = true;
            }
        }
        if !changed {
            return Ok(());
        }
        let sidecar = SdsSidecar::from_records(map.into_values());
        let json = serde_json::to_string(&sidecar)
            .map_err(|e| PersistError::Serialize(format!("sid metadata: {e}")))?;
        self.write_atomic(json.as_bytes())
    }

    fn write_atomic(&self, bytes: &[u8]) -> PersistResult<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        {
            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sketch_meta(sid: u64) -> SidMetaRecord {
        SidMetaRecord::new(
            sid,
            "http_latency".into(),
            vec!["host".into(), "zone".into()],
            &AggKind::Sketch {
                algorithm: SketchAlgorithm::Kll,
                config: SketchConfig::Kll { k: 200 },
                spatial_filter_canonical: String::new(),
            },
            1234,
        )
    }

    fn exact_meta(sid: u64) -> SidMetaRecord {
        SidMetaRecord::new(
            sid,
            "http_requests_total".into(),
            vec!["zone".into()],
            &AggKind::ExactAgg {
                agg_type: AggregationType::Sum,
                parameters_canonical: String::new(),
                spatial_filter_canonical: String::new(),
            },
            5678,
        )
    }

    #[test]
    fn load_on_missing_file_is_empty() {
        let tmp = TempDir::new().unwrap();
        let s = SidMetadataStore::new(tmp.path());
        assert!(s.load().unwrap().is_empty());
    }

    #[test]
    fn authoritative_bindings_share_one_persisted_catalog_generation() {
        let directory = tempfile::tempdir().unwrap();
        let store = SidMetadataStore::new(directory.path());
        let generation = std::sync::Arc::new(asap_types::sds::CatalogGeneration {
            schema_version: 1,
            plan_id: 7,
            plan_version: 2,
            snapshot_sha256: "catalog".into(),
        });
        let mut first = sketch_meta(1);
        first.summary_definition_id = Some(asap_types::PolicyFingerprint(7).into());
        first.catalog_generation = Some(std::sync::Arc::clone(&generation));
        let mut second = first.clone();
        second.sid = 2;
        store.upsert_all(&[first, second]).unwrap();
        let json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(store.path()).unwrap()).unwrap();
        assert_eq!(json["catalog_generations"].as_object().unwrap().len(), 1);
        let records = store.load().unwrap();
        assert_eq!(records.len(), 2);
        assert!(std::sync::Arc::ptr_eq(
            records[0].catalog_generation.as_ref().unwrap(),
            records[1].catalog_generation.as_ref().unwrap()
        ));
        assert_eq!(
            records[0].summary_definition_id,
            Some(asap_types::PolicyFingerprint(7).into())
        );
    }

    #[test]
    fn upsert_then_load_round_trips() {
        let tmp = TempDir::new().unwrap();
        let s = SidMetadataStore::new(tmp.path());
        s.upsert_all(&[sketch_meta(1), exact_meta(2)]).unwrap();

        let mut got = s.load().unwrap();
        got.sort_by_key(|r| r.sid);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], sketch_meta(1));
        assert_eq!(got[1], exact_meta(2));

        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(s.path()).unwrap()).unwrap();
        assert_eq!(persisted["schema_version"], 3);
        assert_eq!(
            persisted["summary_descriptors"].as_object().unwrap().len(),
            2
        );
        assert_eq!(persisted["data_descriptors"].as_object().unwrap().len(), 2);
        assert_eq!(persisted["bindings"].as_object().unwrap().len(), 2);
    }

    #[test]
    fn equivalent_sids_persist_one_copy_of_each_descriptor() {
        let tmp = TempDir::new().unwrap();
        let store = SidMetadataStore::new(tmp.path());
        let mut second = sketch_meta(2);
        second.first_seen_unix_ms = 9999;
        store.upsert_all(&[sketch_meta(1), second]).unwrap();

        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(store.path()).unwrap()).unwrap();
        assert_eq!(
            persisted["summary_descriptors"].as_object().unwrap().len(),
            1
        );
        assert_eq!(persisted["data_descriptors"].as_object().unwrap().len(), 1);
        assert_eq!(persisted["bindings"].as_object().unwrap().len(), 2);
        assert_eq!(store.load().unwrap().len(), 2);
    }

    #[test]
    fn broken_descriptor_reference_does_not_partially_recover() {
        let tmp = TempDir::new().unwrap();
        let store = SidMetadataStore::new(tmp.path());
        store.upsert_all(&[sketch_meta(1), exact_meta(2)]).unwrap();

        let mut persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(store.path()).unwrap()).unwrap();
        let missing_id = persisted["bindings"]["1"]["summary_descriptor_id"]
            .as_str()
            .unwrap()
            .to_string();
        persisted["summary_descriptors"]
            .as_object_mut()
            .unwrap()
            .remove(&missing_id);
        std::fs::write(store.path(), serde_json::to_vec(&persisted).unwrap()).unwrap();

        assert!(store.load().unwrap().is_empty());
    }

    #[test]
    fn legacy_flat_sidecar_is_read_and_migrated_on_write() {
        let tmp = TempDir::new().unwrap();
        let store = SidMetadataStore::new(tmp.path());
        let legacy = HashMap::from([("1".to_string(), sketch_meta(1))]);
        std::fs::write(store.path(), serde_json::to_vec(&legacy).unwrap()).unwrap();

        assert_eq!(store.load().unwrap(), vec![sketch_meta(1)]);
        store.upsert_all(&[exact_meta(2)]).unwrap();
        let persisted: serde_json::Value =
            serde_json::from_slice(&std::fs::read(store.path()).unwrap()).unwrap();
        assert_eq!(persisted["schema_version"], 3);
        assert_eq!(store.load().unwrap().len(), 2);
    }

    #[test]
    fn upsert_merges_and_overwrites_per_sid() {
        let tmp = TempDir::new().unwrap();
        let s = SidMetadataStore::new(tmp.path());
        s.upsert_all(&[sketch_meta(1)]).unwrap();
        // New sid + updated metric for sid 1.
        let mut updated = sketch_meta(1);
        updated.metric_name = "http_latency_v2".into();
        s.upsert_all(&[updated.clone(), exact_meta(2)]).unwrap();

        let mut got = s.load().unwrap();
        got.sort_by_key(|r| r.sid);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].metric_name, "http_latency_v2");
        assert_eq!(got[1], exact_meta(2));
    }

    #[test]
    fn derives_capability_and_accuracy_from_agg_kind() {
        let kll = sketch_meta(1);
        assert!(matches!(
            kll.capability(),
            Some(Capability::QuantileApprox(Some(SketchAlgorithm::Kll)))
        ));
        assert!(kll.accuracy().is_some());

        let sum = exact_meta(2);
        assert!(matches!(
            sum.capability(),
            Some(Capability::ExactAgg(AggregationType::Sum))
        ));
        assert!(sum.accuracy().is_none());
    }

    #[test]
    fn unparsable_file_loads_as_empty() {
        let tmp = TempDir::new().unwrap();
        let s = SidMetadataStore::new(tmp.path());
        std::fs::write(s.path(), b"{not json").unwrap();
        assert!(s.load().unwrap().is_empty());
    }
}
