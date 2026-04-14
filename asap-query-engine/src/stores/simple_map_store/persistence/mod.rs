//! Persistence layer for `SimpleMapStorePerKey`.
//!
//! See `docs/design-simple-map-store-persistence.md` for the design rationale.
//!
//! ## Structure
//!
//! * [`config`] — [`SimpleMapStorePersistenceConfig`]
//! * [`part`] — on-disk part format (`meta.bin` + `data.bin` + `index.bin`),
//!   writer and reader.
//! * [`manifest`] — append-only log + periodic binary snapshot of live parts.
//! * [`flusher`] — background `std::thread` that walks sealed epochs and
//!   turns each tick's candidates into a single on-disk part.
//! * [`cache`] — moka-backed Tier-2 cache of decoded parts, keyed on
//!   `PartId`, bounded by bytes.
//! * [`recovery`] — startup: load snapshot, replay log, verify CRCs, sweep
//!   orphan part dirs.
//!
//! The submodule is intentionally decoupled from `SimpleMapStorePerKey`
//! via the [`EpochSource`] trait — the flusher knows nothing about the
//! store's internal types and can be unit-tested against a fake source.

pub mod config;
pub mod manifest;
pub mod part;
pub mod source;

pub mod cache;
pub mod flusher;
pub mod recovery;

pub use config::SimpleMapStorePersistenceConfig;
pub use manifest::{Manifest, PartEntry};
pub use part::{PartId, PartReader, PartWriter, SnapshotEntry};
pub use source::{EpochSource, SealedEpochRef};

/// Convenience result alias used across the persistence layer.
pub type PersistResult<T> = Result<T, PersistError>;

#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("part format error: {0}")]
    Format(String),

    #[error("manifest corruption: {0}")]
    Manifest(String),

    #[error("unsupported accumulator type for persistence: {0}")]
    UnsupportedAccumulator(String),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("internal error: {0}")]
    Internal(String),
}
