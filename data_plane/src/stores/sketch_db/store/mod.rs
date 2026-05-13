//! Phase 5 M2.3.6g — the legacy `SketchStore` enum, `SketchStoreGlobal`,
//! `SketchStorePerKey`, and the non-generic `MutableEpoch`/`SealedEpoch`
//! in `common.rs` are gone. The six index optimizations they carried
//! now live generically in `index/epoch_columnar.rs`; the persistence
//! layer below is reused by `SketchIndex::start_persistence`.
//!
//! After the final M2.3.6g rename pass, this module's path will be
//! `stores::sketch_db::persistence` directly; for now the `store/`
//! subdirectory stays so the rename can be done with one mechanical
//! sweep.

pub mod persistence;
