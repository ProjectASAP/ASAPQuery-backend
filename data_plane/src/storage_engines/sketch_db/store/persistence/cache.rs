//! Tier-2 read-side part cache. Moka-backed, bounded by bytes,
//! W-TinyLFU eviction. Keyed on `PartId`; values are `Arc<PartReader>`
//! which holds the mmap handles for a part's three files.
//!
//! The cache has no consistency obligations (it is never dirty), so
//! the flusher can invalidate entries whenever it deletes a part.

use std::path::Path;
use std::sync::Arc;

use moka::sync::Cache;

use super::part::{part_dir_path, PartId, PartReader};
use super::PersistResult;

/// A cached, mmap-backed `PartReader`. Returned by [`PartCache::get`].
pub type LoadedPart = Arc<PartReader>;

/// Bounded cache of decoded parts. Cheap to clone — it's an `Arc`
/// inside.
#[derive(Clone)]
pub struct PartCache {
    inner: Option<Cache<PartId, LoadedPart>>,
    parts_root: std::path::PathBuf,
}

impl PartCache {
    /// Construct a cache with a byte budget. A budget of 0 disables
    /// caching entirely — every query path goes straight to disk.
    pub fn new(parts_root: std::path::PathBuf, byte_budget: u64) -> Self {
        let inner = if byte_budget == 0 {
            None
        } else {
            Some(
                Cache::builder()
                    .weigher(|_k: &PartId, v: &LoadedPart| -> u32 {
                        // Weight = sum of mmap'd bytes. Moka's weigher
                        // returns u32, so clamp large parts.
                        let len = v.meta.data_len + v.meta.index_len;
                        len.min(u32::MAX as u64) as u32
                    })
                    .max_capacity(byte_budget)
                    .build(),
            )
        };
        Self { inner, parts_root }
    }

    /// Get (or load) a `PartReader` for `part_id`. On miss, mmaps the
    /// part's files from disk.
    pub fn get_or_load(&self, part_id: PartId) -> PersistResult<LoadedPart> {
        if let Some(inner) = &self.inner {
            if let Some(hit) = inner.get(&part_id) {
                return Ok(hit);
            }
            let part_dir = part_dir_path(&self.parts_root, part_id);
            let reader = Arc::new(PartReader::open(&part_dir)?);
            inner.insert(part_id, Arc::clone(&reader));
            Ok(reader)
        } else {
            let part_dir = part_dir_path(&self.parts_root, part_id);
            Ok(Arc::new(PartReader::open(&part_dir)?))
        }
    }

    /// Drop the cached entry for `part_id`. Idempotent.
    pub fn invalidate(&self, part_id: PartId) {
        if let Some(inner) = &self.inner {
            inner.invalidate(&part_id);
        }
    }

    /// Diagnostic: approximate entry count (0 if caching disabled).
    pub fn entry_count(&self) -> u64 {
        self.inner.as_ref().map(|c| c.entry_count()).unwrap_or(0)
    }
}

/// Convenience helper: given a disk_path, return the parts root.
pub fn parts_root_of(disk_path: &Path) -> std::path::PathBuf {
    disk_path.join("parts")
}
