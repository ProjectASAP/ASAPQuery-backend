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
pub const SID_METADATA_FILE: &str = "sid_metadata.json";

/// Serializable mirror of [`SketchConfig`]. Kept local (rather than
/// deriving serde on the control-plane `SketchConfig`) so the sidecar
/// schema is owned by the persistence layer and changes here can't
/// silently shift the on-disk format from an unrelated edit.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SketchConfigRec {
    DdSketch { relative_accuracy: f64 },
    Kll { k: u32 },
    Hll { precision: u32 },
    CountSketch { rows: i32, cols: i32 },
    CountMin { rows: i32, cols: i32 },
}

impl From<&SketchConfig> for SketchConfigRec {
    fn from(c: &SketchConfig) -> Self {
        match c {
            SketchConfig::DDSketch { relative_accuracy } => SketchConfigRec::DdSketch {
                relative_accuracy: *relative_accuracy,
            },
            SketchConfig::Kll { k } => SketchConfigRec::Kll { k: *k },
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
}

/// One durable sid-metadata row. The query-critical fields
/// (`metric_name`, `group_by_keys`, `agg_kind`) are stored explicitly;
/// `capability` and `accuracy` are DERIVED from `agg_kind` on load, the
/// same way the ingest path derives them, so the record stays minimal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SidMetaRecord {
    pub sid: u64,
    pub metric_name: String,
    /// Label KEY set, sorted (a `Vec` so the JSON stays compact; the
    /// store side rebuilds the `BTreeSet`).
    pub group_by_keys: Vec<String>,
    agg_kind: AggKindRec,
    pub first_seen_unix_ms: i64,
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
            metric_name,
            group_by_keys,
            agg_kind: agg_kind.into(),
            first_seen_unix_ms,
        }
    }

    /// Reconstruct the structured [`AggKind`], or `None` for an
    /// unrecognized sketch kind.
    pub fn agg_kind(&self) -> Option<AggKind> {
        self.agg_kind.to_agg_kind()
    }

    /// Derive the warm-tier [`Capability`] from `agg_kind`, mirroring the
    /// ingest path (`otel.rs`) and `ingest_precompute_with_sid`. Returns
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
                SketchAlgorithm::Hll => Capability::CardinalityApprox,
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

/// File-backed sid-metadata sidecar. The whole map is rewritten on every
/// upsert (atomic tmp + rename). Live sid cardinality is small, so a full
/// rewrite per flush tick is cheap and keeps the on-disk file always
/// consistent with no log-replay machinery.
#[derive(Debug)]
pub struct SidMetadataStore {
    path: PathBuf,
}

impl SidMetadataStore {
    /// Open (or lazily create on first write) the sidecar at
    /// `<disk_path>/sid_metadata.json`.
    pub fn new(disk_path: &Path) -> Self {
        Self {
            path: disk_path.join(SID_METADATA_FILE),
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
        let map: HashMap<String, SidMetaRecord> = match serde_json::from_str(&buf) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(
                    path = %self.path.display(),
                    error = %e,
                    "sid metadata sidecar unparsable; ignoring (live ingest will re-register)"
                );
                return Ok(Vec::new());
            }
        };
        Ok(map.into_values().collect())
    }

    /// Upsert a batch of records, merging with whatever is already on
    /// disk (last write wins per sid). Atomic via tmp + rename + dir
    /// fsync, matching the manifest's durability discipline.
    pub fn upsert_all(&self, records: &[SidMetaRecord]) -> PersistResult<()> {
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
            match map.get(&key) {
                Some(existing) if existing == r => {}
                _ => {
                    map.insert(key, r.clone());
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(());
        }
        let json = serde_json::to_string(&map)
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
    fn upsert_then_load_round_trips() {
        let tmp = TempDir::new().unwrap();
        let s = SidMetadataStore::new(tmp.path());
        s.upsert_all(&[sketch_meta(1), exact_meta(2)]).unwrap();

        let mut got = s.load().unwrap();
        got.sort_by_key(|r| r.sid);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], sketch_meta(1));
        assert_eq!(got[1], exact_meta(2));
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
