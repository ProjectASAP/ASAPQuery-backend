//! The data plane's stores: the raw samples the plan's `Scan`s read and the
//! materialized views its precompute writes ([`store`]), the views' window
//! cache and memory budget, their on-disk segments and persistence (and
//! the `persist-N` threads that write them, [`persist_pool`]), the state
//! codecs, and the process-wide series interner. [`boundary`] puts the
//! stores behind `common`'s store traits.

pub mod boundary;
pub mod memory_budget;
pub mod persist;
pub mod persist_pool;
pub mod segment;
pub mod series_ids;
mod state_codec;
pub mod store;
pub mod window_cache;
