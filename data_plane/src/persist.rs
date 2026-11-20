//! Materialized views on disk: one append-only file per view,
//! `view-<node id>.log`, one record per refreshed window holding its
//! `(start, end]` and rows, synced to disk before the next window is
//! refreshed. At startup the records still within each view's retention
//! are loaded back into the view store; open windows and raw samples are
//! not persisted.
//!
//! A record is a little-endian `u32` length and a MessagePack payload. A
//! row's plain values are stored as the executor's `Value`s and an exact
//! summary state (sum, count, min, max, rate, increase) by its serde form.
//! Sketch states have no byte encoding yet, so a window of a view holding
//! one is not persisted (counted in `asap_view_windows_not_persisted_total`).

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::summary_kernels::exact::ExactAccumulator;
use asap_executor::summary_kernels::IncreaseAccumulator;
use asap_executor::values::{Batch, SchemaRef, Value};
use serde::{Deserialize, Serialize};

use crate::store::ViewStore;

/// One stored value of a view row.
#[derive(Serialize, Deserialize)]
enum StoredValue {
    Plain(Value),
    Exact(ExactAccumulator),
    Increase(IncreaseAccumulator),
}

/// One refreshed window of a view.
#[derive(Serialize, Deserialize)]
struct Record {
    start_ms: i64,
    end_ms: i64,
    rows: Vec<Vec<StoredValue>>,
}

/// What persisting one window wrote.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Written {
    pub bytes: u64,
    pub sync_ns: u64,
    /// Views whose window could not be encoded (a sketch state).
    pub not_persisted: u64,
}

/// The view files in one directory.
pub struct ViewFiles {
    dir: PathBuf,
    files: BTreeMap<PhysicalASAPNodeId, File>,
}

impl ViewFiles {
    /// The view files in `dir`, created if missing.
    pub fn open(dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            files: BTreeMap::new(),
        })
    }

    fn path(&self, view: PhysicalASAPNodeId) -> PathBuf {
        self.dir.join(format!("view-{view}.log"))
    }

    /// Appends `batches` as `view`'s rows of `(start, end]` and syncs the
    /// file; a window with a sketch state is not written.
    pub fn append(
        &mut self,
        view: PhysicalASAPNodeId,
        (start_ms, end_ms): (i64, i64),
        batches: &[Batch],
    ) -> std::io::Result<Written> {
        let Some(rows) = encode_rows(batches) else {
            return Ok(Written {
                not_persisted: 1,
                ..Written::default()
            });
        };
        let payload = rmp_serde::to_vec(&Record {
            start_ms,
            end_ms,
            rows,
        })
        .map_err(std::io::Error::other)?;
        let path = self.path(view);
        let file = match self.files.entry(view) {
            std::collections::btree_map::Entry::Occupied(file) => file.into_mut(),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(OpenOptions::new().create(true).append(true).open(path)?)
            }
        };
        let length = u32::try_from(payload.len()).map_err(std::io::Error::other)?;
        let mut record = Vec::with_capacity(4 + payload.len());
        record.extend_from_slice(&length.to_le_bytes());
        record.extend_from_slice(&payload);
        file.write_all(&record)?;
        let synced = Instant::now();
        file.sync_data()?;
        Ok(Written {
            bytes: record.len() as u64,
            sync_ns: synced.elapsed().as_nanos() as u64,
            not_persisted: 0,
        })
    }

    /// Loads into `views` each of `views_to_load`' records ending within its
    /// retention of its newest record, oldest first, and returns how many
    /// windows were loaded. A file that ends in a partial record (a crash
    /// while writing) is read up to it; a record that does not decode, or
    /// is not after the ones loaded, is skipped.
    pub fn load(
        &self,
        views_to_load: &[(MaterializedView, SchemaRef)],
        views: &mut ViewStore,
    ) -> std::io::Result<u64> {
        let mut loaded = 0;
        for (view, schema) in views_to_load {
            let path = self.path(view.node_id);
            let Ok(file) = File::open(&path) else {
                continue;
            };
            let records = read_records(file)?;
            let Some(newest_end) = records.iter().map(|r| r.end_ms).max() else {
                continue;
            };
            let retention_ms = i64::try_from(view.retention_ms()).unwrap_or(i64::MAX);
            let oldest_kept_end = newest_end.saturating_sub(retention_ms);
            for record in records
                .into_iter()
                .filter(|r| r.end_ms > oldest_kept_end || r.end_ms == newest_end)
            {
                let range = (record.start_ms, record.end_ms);
                let Some(batch) = decode_rows(schema, record.rows) else {
                    tracing::warn!(
                        view = view.node_id,
                        ?range,
                        "skipping a view record that does not decode"
                    );
                    continue;
                };
                if views.append(view, range, vec![batch]).is_ok() {
                    loaded += 1;
                }
            }
        }
        Ok(loaded)
    }
}

/// Every record of a view file, up to a partial one at its end.
fn read_records(file: File) -> std::io::Result<Vec<Record>> {
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    loop {
        let mut length = [0u8; 4];
        match reader.read_exact(&mut length) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let mut payload = vec![0u8; u32::from_le_bytes(length) as usize];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        if let Ok(record) = rmp_serde::from_slice(&payload) {
            records.push(record);
        }
    }
    Ok(records)
}

/// The rows of `batches`, or `None` when one holds a state with no byte
/// encoding.
fn encode_rows(batches: &[Batch]) -> Option<Vec<Vec<StoredValue>>> {
    batches
        .iter()
        .flat_map(Batch::to_rows)
        .map(|row| {
            row.into_iter()
                .map(|value| match value {
                    Value::Summary { state, .. } => {
                        let any = state.as_any();
                        if let Some(exact) = any.downcast_ref::<ExactAccumulator>() {
                            Some(StoredValue::Exact(exact.clone()))
                        } else {
                            any.downcast_ref::<IncreaseAccumulator>()
                                .map(|increase| StoredValue::Increase(increase.clone()))
                        }
                    }
                    plain => Some(StoredValue::Plain(plain)),
                })
                .collect()
        })
        .collect()
}

/// `rows` as one batch of `schema`, each state of its field's family.
fn decode_rows(schema: &SchemaRef, rows: Vec<Vec<StoredValue>>) -> Option<Batch> {
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .zip(&schema.fields)
                .map(|(value, field)| match value {
                    StoredValue::Plain(value) => value,
                    StoredValue::Exact(state) => Value::Summary {
                        family: field.dtype.clone(),
                        state: Arc::new(state),
                    },
                    StoredValue::Increase(state) => Value::Summary {
                        family: field.dtype.clone(),
                        state: Arc::new(state),
                    },
                })
                .collect()
        })
        .collect();
    Batch::try_new(schema.clone(), rows).ok()
}
