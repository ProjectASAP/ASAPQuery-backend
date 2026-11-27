//! View file segments and the columnar fragments in them
//! (docs/design/tiered-view-storage.md, "On-disk format" and "Read path").
//!
//! A segment is an append-only file of frames, each an 8-byte head (a
//! little-endian `u32` payload length and a `u32` kind) and its payload,
//! padded to a multiple of 8 bytes. The first frame is the segment's
//! header; each later frame is one refreshed window's record: its parts
//! (the texts new to the segment, the Hydra-Count-Min grids, one fragment
//! per view), then its head (MessagePack) and the head's length as the last
//! 4 bytes.
//!
//! A fragment holds one view's rows of one window by column, compressed
//! (zstd), so a cold read decodes one (view, window) without the record's
//! other views. State and text columns are stored compact
//! ([`crate::state_codec`]) and rebuilt in the executor's mapped state
//! layout when read. A text column holding each row's series label is not
//! stored: it is rebuilt from the series identity column.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

use asap_executor::arrow::array::{
    Array, ArrayRef, DictionaryArray, PrimitiveArray, StringArray, StringBuilder, UInt32Array,
};
use asap_executor::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
use asap_executor::arrow::datatypes::{
    ArrowPrimitiveType, DataType as ArrowType, Float64Type, Int64Type, TimeUnit,
    TimestampMillisecondType, UInt32Type,
};
use asap_executor::planner::ir::schema::{DataType, FieldDataType, Schema, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::plan::physical_asap_dag::PhysicalASAPNodeId;
use asap_executor::planner::summary_type::{ExactKind, SketchAlgorithm};
use asap_executor::summary_kernels::HydraCms;
use asap_executor::values::{
    family_has_mapped_form, state_view_bytes, Batch, BatchColumn, SchemaRef, Value,
};
use asap_executor::Error;
use serde::{Deserialize, Serialize};

use crate::persist::{decode_stored_state, encode_state, Grids, StoredValue};
use crate::series_ids::labels_of_identity;
use crate::state_codec::{
    decode_states, decode_text_keys, encode_states, encode_text_keys, CompactKind,
};

/// The view file format version these segments are.
pub(crate) const FORMAT_VERSION: u32 = 4;

/// Frame kinds.
const HEADER_FRAME: u32 = 0;
const RECORD_FRAME: u32 = 1;
/// A frame longer than this is taken as torn: no window's rows are this
/// large, and a garbage length must not size an allocation.
const MAX_FRAME_BYTES: u64 = 1 << 31;

/// Column kinds in a fragment.
/// Text: each row's key into the segment's texts, a signed varint delta
/// from the previous row's.
const TEXT: u32 = 1;
const FLOAT64: u32 = 2;
const INT64: u32 = 3;
const TIMESTAMP: u32 = 4;
/// Any other plain column: MessagePack of its values.
const GENERIC: u32 = 5;
const STATES: u32 = 6;
/// A state column in the executor's mapped layout: `u32` offsets in the
/// data section, the states (each 8-byte aligned) in the second section.
/// For a family with a mapped form but no compact one (KLL, Count-Min).
const MAPPED_STATES: u32 = 7;
/// A text column whose every row holds its series identity's label of the
/// column's name (`""` when the series has none): no data, rebuilt on read
/// from the identity column, whose index is the descriptor's flags.
const IDENTITY_LABEL: u32 = 8;
/// A state column in compact form ([`crate::state_codec`]); its flags are
/// the [`CompactKind`] code. Read back into the mapped layout.
const COMPACT_STATES: u32 = 9;
/// Flag: a timestamp column whose rows all hold one value, stored once.
const CONSTANT: u32 = 1;
/// Bytes of one column descriptor: kind, flags, data offset and length,
/// validity offset and length.
const DESCRIPTOR_BYTES: usize = 40;
/// The zstd level fragments are compressed at: level 1 compresses view
/// fragments within a few percent of level 3 at about half the CPU.
const ZSTD_LEVEL: i32 = 1;

/// How a cold window's fragment is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ViewRead {
    /// Sealed segments mapped, each fragment decompressed from the
    /// mapping; the active segment read with `pread`.
    #[default]
    Mmap,
    /// Every fragment read with `pread` into a buffer.
    Pread,
}

/// The first frame of a segment.
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(crate) struct SegmentHeader {
    /// A hash of the plan file the views were computed for.
    pub plan_hash: u64,
    pub group: usize,
    pub version: u32,
}

/// Where each part of a record is: absolute offsets and lengths in the
/// segment.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct RecordHead {
    pub start_ms: i64,
    pub end_ms: i64,
    /// MessagePack `Vec<String>`: texts new to the segment, in key order.
    pub texts: (u64, u32),
    /// MessagePack `Vec<ByteBuf>`: the Hydra-Count-Min grids its fragments'
    /// states read, as ASAPv1.
    pub grids: (u64, u32),
    pub views: Vec<(PhysicalASAPNodeId, u64, u32)>,
}

/// A read-only mapping of a sealed segment, unmapped when dropped.
struct Mapping {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is read-only and never changes (only sealed segments,
// whose length never changes, are mapped); it is unmapped only on drop.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}
impl std::panic::RefUnwindSafe for Mapping {}

impl Mapping {
    fn new(file: &File, len: usize) -> std::io::Result<Self> {
        use std::os::fd::AsRawFd;
        if len == 0 {
            return Err(std::io::Error::other("an empty segment is not mapped"));
        }
        // SAFETY: a fresh read-only shared mapping of `len` bytes of an
        // open file; the file is at least `len` long and never shortened
        // while mapped (sealed segments only).
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `ptr` is the mapping just made; advice only.
        unsafe { libc::madvise(ptr, len, libc::MADV_RANDOM) };
        Ok(Self {
            ptr: NonNull::new(ptr.cast()).expect("mmap returned null"),
            len,
        })
    }

    fn bytes(&self) -> &[u8] {
        // SAFETY: the mapping is `len` readable bytes for as long as `self`.
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the mapping made in `new`, unmapped once.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

/// One segment file: its records' bytes, its texts, and (once sealed) its
/// mapping. Shared by the persist thread writing it and every window
/// location in it; the file stays open (and readable after an unlink) until
/// the last holder drops it.
pub struct Segment {
    path: PathBuf,
    file: File,
    /// Bytes of whole frames written.
    len: AtomicU64,
    sealed: AtomicBool,
    /// Whether its pages were advised cold ([`Segment::mark_cold`]).
    cold: AtomicBool,
    mapping: OnceLock<Result<Arc<Mapping>, String>>,
    /// The texts of its records, by key.
    texts: RwLock<Vec<Arc<str>>>,
    /// The texts as one Arrow array, shared by every window read from the
    /// segment; rebuilt when texts were added since.
    dictionary: Mutex<Option<ArrayRef>>,
    /// Every text dictionary built, the current one and those a window
    /// read before a rebuild may still hold, to count each live one once.
    dictionaries_built: Mutex<Vec<Weak<dyn Array>>>,
    /// What [`IDENTITY_LABEL`] columns read from the segment are built
    /// from ([`Segment::label_values`]).
    identity_label_values: Mutex<IdentityLabelValues>,
    /// The newest window end of its records.
    max_end_ms: AtomicI64,
}

impl std::fmt::Debug for Segment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Segment")
            .field("path", &self.path)
            .field("len", &self.len.load(Ordering::Relaxed))
            .field("sealed", &self.sealed.load(Ordering::Relaxed))
            .finish()
    }
}

/// `n` rounded up to a multiple of 8.
fn padded(n: usize) -> usize {
    n.div_ceil(8) * 8
}

/// One frame: its head, `payload` and padding.
fn frame(kind: u32, payload: &[u8]) -> std::io::Result<Vec<u8>> {
    let length = u32::try_from(payload.len())
        .ok()
        .filter(|&len| u64::from(len) <= MAX_FRAME_BYTES)
        .ok_or_else(|| std::io::Error::other("view record too large"))?;
    let mut bytes = Vec::with_capacity(8 + padded(payload.len()));
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&kind.to_le_bytes());
    bytes.extend_from_slice(payload);
    bytes.resize(8 + padded(payload.len()), 0);
    Ok(bytes)
}

impl Segment {
    /// A new, empty segment in `dir` whose first window ends at `end_ms`,
    /// with its header written: `<end_ms>.seg`, or `<end_ms>_<n>.seg` when
    /// that file exists (a recovered segment is never overwritten).
    pub(crate) fn create_new(
        dir: &Path,
        end_ms: i64,
        header: &SegmentHeader,
    ) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(dir)?;
        let mut n = 0u32;
        let (path, file) = loop {
            let name = match n {
                0 => format!("{end_ms}.seg"),
                n => format!("{end_ms}_{n}.seg"),
            };
            let path = dir.join(name);
            match std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => break (path, file),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => n += 1,
                Err(error) => return Err(error),
            }
        };
        let path = path.as_path();
        let bytes = frame(
            HEADER_FRAME,
            &rmp_serde::to_vec(header).map_err(std::io::Error::other)?,
        )?;
        file.write_all_at(&bytes, 0)?;
        Ok(Arc::new(Self {
            path: path.to_path_buf(),
            file,
            len: AtomicU64::new(bytes.len() as u64),
            sealed: AtomicBool::new(false),
            cold: AtomicBool::new(false),
            mapping: OnceLock::new(),
            texts: RwLock::new(Vec::new()),
            dictionary: Mutex::new(None),
            dictionaries_built: Mutex::default(),
            identity_label_values: Mutex::default(),
            max_end_ms: AtomicI64::new(i64::MIN),
        }))
    }

    /// Opens the segment at `path` written earlier, sealed: its header and
    /// every whole record up to the first that is torn or does not decode,
    /// which is cut off with everything after it (before anything maps the
    /// file). `None` when its header is not `expected` (the file
    /// is then left for the caller to set aside).
    pub(crate) fn open(
        path: &Path,
        expected: &SegmentHeader,
    ) -> std::io::Result<Option<(Arc<Self>, Vec<RecordHead>)>> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;
        let end = file.metadata()?.len();
        let mut offset = 0u64;
        let mut heads = Vec::new();
        let mut texts: Vec<Arc<str>> = Vec::new();
        let mut header_ok = false;
        let mut head_bytes = [0u8; 8];
        while offset + 8 <= end {
            file.read_exact_at(&mut head_bytes, offset)?;
            let length = u64::from(u32::from_le_bytes(head_bytes[..4].try_into().unwrap()));
            let kind = u32::from_le_bytes(head_bytes[4..].try_into().unwrap());
            let total = 8 + padded(length as usize) as u64;
            if length > MAX_FRAME_BYTES || offset + total > end {
                break;
            }
            let mut payload = vec![0u8; length as usize];
            file.read_exact_at(&mut payload, offset + 8)?;
            if offset == 0 {
                let header: Option<SegmentHeader> = (kind == HEADER_FRAME)
                    .then(|| rmp_serde::from_slice(&payload).ok())
                    .flatten();
                if header.as_ref() != Some(expected) {
                    return Ok(None);
                }
                header_ok = true;
            } else {
                // The texts part, decoded now: every fragment's keys index
                // the segment's texts. A frame that is not a record, or a
                // record whose head or texts do not decode, ends the
                // segment there: a later record's keys would index the
                // texts shifted.
                let Some((head, added)) = (kind == RECORD_FRAME)
                    .then(|| record_head(&payload))
                    .flatten()
                    .filter(|head| {
                        // Every range the head names lies in this frame.
                        let frame = offset + 8..=offset + 8 + length;
                        let inside = |(at, len): (u64, u32)| {
                            frame.contains(&at)
                                && at
                                    .checked_add(u64::from(len))
                                    .is_some_and(|end| frame.contains(&end))
                        };
                        inside(head.texts)
                            && (head.grids.1 == 0 || inside(head.grids))
                            && head.views.iter().all(|&(_, at, len)| inside((at, len)))
                    })
                    .and_then(|head| {
                        let start = usize::try_from(head.texts.0.checked_sub(offset + 8)?).ok()?;
                        let bytes =
                            payload.get(start..start.checked_add(head.texts.1 as usize)?)?;
                        let added: Vec<String> = rmp_serde::from_slice(bytes).ok()?;
                        Some((head, added))
                    })
                else {
                    break;
                };
                texts.extend(added.into_iter().map(Arc::from));
                heads.push(head);
            }
            offset += total;
        }
        if !header_ok {
            return Ok(None);
        }
        if offset != end {
            tracing::warn!(path = %path.display(), good_len = offset, "cutting off a torn or undecodable view record");
            file.set_len(offset)?;
            file.sync_data()?;
        }
        let max_end_ms = heads.iter().map(|h| h.end_ms).max().unwrap_or(i64::MIN);
        Ok(Some((
            Arc::new(Self {
                path: path.to_path_buf(),
                file,
                len: AtomicU64::new(offset),
                sealed: AtomicBool::new(true),
                cold: AtomicBool::new(false),
                mapping: OnceLock::new(),
                texts: RwLock::new(texts),
                dictionary: Mutex::new(None),
                dictionaries_built: Mutex::default(),
                identity_label_values: Mutex::default(),
                max_end_ms: AtomicI64::new(max_end_ms),
            }),
            heads,
        )))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Bytes of whole frames written.
    pub fn len(&self) -> u64 {
        self.len.load(Ordering::SeqCst)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn is_sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    /// The newest window end of its records.
    pub(crate) fn max_end_ms(&self) -> i64 {
        self.max_end_ms.load(Ordering::SeqCst)
    }

    /// Ends appending: syncs it; from now on it never changes, and reads map it.
    pub(crate) fn seal(&self) -> std::io::Result<()> {
        self.file.sync_data()?;
        self.sealed.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Whether its pages were advised cold.
    pub fn is_cold(&self) -> bool {
        self.cold.load(Ordering::SeqCst)
    }

    /// Advises the kernel, once, that a sealed segment's pages are cold:
    /// no query reads them often any more, so they should leave the
    /// active LRU list (which, without memory pressure, the kernel never
    /// ages) and be the first page cache reclaimed. `MADV_COLD` deactivates
    /// the pages its mapping maps and keeps them cached; the pages read
    /// with `pread` while it was the active segment are cached but not
    /// mapped, out of `MADV_COLD`'s reach, so `POSIX_FADV_DONTNEED` drops
    /// those (they are clean: a sealed segment is synced). A later read
    /// maps or reads them again: answers never change. Returns whether
    /// this call marked it.
    pub(crate) fn mark_cold(&self) -> bool {
        use std::os::fd::AsRawFd;
        if !self.is_sealed() || self.cold.swap(true, Ordering::SeqCst) {
            return false;
        }
        if let Some(Ok(mapping)) = self.mapping.get() {
            // SAFETY: the whole live mapping; advice only.
            unsafe { libc::madvise(mapping.ptr.as_ptr().cast(), mapping.len, libc::MADV_COLD) };
        }
        // SAFETY: a valid open descriptor; advice only (mapped pages are
        // left to `MADV_COLD`).
        unsafe {
            libc::posix_fadvise(
                self.file.as_raw_fd(),
                0,
                self.len() as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        true
    }

    pub(crate) fn sync(&self) -> std::io::Result<()> {
        self.file.sync_data()
    }

    /// How many texts it has: the next new text's key.
    pub(crate) fn text_count(&self) -> usize {
        self.texts.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Appends one record: `parts` laid out from the frame's payload start
    /// (each padded to 8 bytes), then the head `make_head` builds from the
    /// parts' absolute offsets. Adds `new_texts` to its texts once written.
    /// Returns the head and the frame's length.
    pub(crate) fn append_record(
        &self,
        parts: &[&[u8]],
        new_texts: Vec<Arc<str>>,
        end_ms: i64,
        make_head: impl FnOnce(&[(u64, u32)]) -> RecordHead,
    ) -> std::io::Result<(RecordHead, u64)> {
        let frame_start = self.len();
        let mut payload = Vec::new();
        let mut placed = Vec::with_capacity(parts.len());
        for part in parts {
            placed.push((frame_start + 8 + payload.len() as u64, part.len() as u32));
            payload.extend_from_slice(part);
            payload.resize(padded(payload.len()), 0);
        }
        let head = make_head(&placed);
        let head_bytes = rmp_serde::to_vec(&head).map_err(std::io::Error::other)?;
        payload.extend_from_slice(&head_bytes);
        payload.extend_from_slice(&(head_bytes.len() as u32).to_le_bytes());
        let bytes = frame(RECORD_FRAME, &payload)?;
        self.file.write_all_at(&bytes, frame_start)?;
        // Start writing the record back now, without waiting: its pages
        // become clean (reclaimable) within moments instead of piling up as
        // dirty page cache until the next sync, which under a memory limit
        // stalls every allocating thread in reclaim.
        {
            use std::os::fd::AsRawFd;
            // SAFETY: a valid open descriptor and a written range; advice
            // to the kernel only (an error leaves the pages to the next sync).
            unsafe {
                libc::sync_file_range(
                    self.file.as_raw_fd(),
                    frame_start as libc::off64_t,
                    bytes.len() as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                )
            };
        }
        self.texts
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .extend(new_texts);
        self.max_end_ms.fetch_max(end_ms, Ordering::SeqCst);
        self.len
            .store(frame_start + bytes.len() as u64, Ordering::SeqCst);
        Ok((head, bytes.len() as u64))
    }

    /// The segment's mapping, made on first use; only for a sealed segment.
    fn mapping(&self) -> Result<Arc<Mapping>, Error> {
        self.mapping
            .get_or_init(|| {
                Mapping::new(&self.file, self.len() as usize)
                    .map(Arc::new)
                    .map_err(|e| e.to_string())
            })
            .clone()
            .map_err(|e| Error::Invalid(format!("mapping {}: {e}", self.path.display())))
    }

    /// Bytes of its text dictionaries still in memory (the texts, and the
    /// label values of [`IDENTITY_LABEL`] columns): each is one array
    /// shared by the windows read from the segment while it was current,
    /// so it is counted once for the segment. Several of one kind only
    /// while the active segment's texts grew under a window still held.
    pub(crate) fn dictionary_bytes(&self) -> usize {
        let mut built = self
            .dictionaries_built
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        built.retain(|dictionary| dictionary.strong_count() > 0);
        built
            .iter()
            .filter_map(Weak::upgrade)
            .map(|dictionary| dictionary.get_array_memory_size())
            .sum()
    }

    /// The texts with keys below `needed`, as one array shared by the
    /// windows read from the segment.
    fn dictionary(&self, needed: usize) -> Result<ArrayRef, Error> {
        let mut dictionary = self.dictionary.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(values) = dictionary.as_ref().filter(|v| v.len() >= needed) {
            return Ok(values.clone());
        }
        let texts = self.texts.read().unwrap_or_else(|e| e.into_inner());
        if texts.len() < needed {
            return Err(Error::Invalid(format!(
                "{}: a text key past the segment's texts",
                self.path.display()
            )));
        }
        let values: ArrayRef = Arc::new(StringArray::from_iter_values(texts.iter().map(|t| &**t)));
        *dictionary = Some(values.clone());
        self.dictionaries_built
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::downgrade(&values));
        Ok(values)
    }
}

/// Per label name, one value per segment text: that label of the text
/// taken as a series identity (`""` without it, or for a text that is no
/// identity). The dictionary of every [`IDENTITY_LABEL`] column of that
/// name, indexed by the rows' identity keys.
#[derive(Default)]
struct IdentityLabelValues {
    values_by_label: HashMap<String, ArrayRef>,
}

impl Segment {
    /// The values of `label` for the texts with keys below `needed` taken
    /// as series identities, as one array shared by the windows read from
    /// the segment ([`IdentityLabelValues`]); extended by the texts added
    /// since it was built, each parsed once per label.
    fn label_values(&self, label: &str, needed: usize) -> Result<ArrayRef, Error> {
        let mut identity = self
            .identity_label_values
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let built = identity.values_by_label.get(label).cloned();
        if let Some(values) = built.as_ref().filter(|v| v.len() >= needed) {
            return Ok(values.clone());
        }
        let built_len = built.as_ref().map_or(0, |values| values.len());
        // The texts added since, copied out so appends do not wait for
        // their parsing.
        let added: Vec<Arc<str>> = {
            let texts = self.texts.read().unwrap_or_else(|e| e.into_inner());
            if texts.len() < needed {
                return Err(Error::Invalid(format!(
                    "{}: a text key past the segment's texts",
                    self.path.display()
                )));
            }
            texts[built_len..].to_vec()
        };
        let built = built
            .as_ref()
            .map(|values| values.as_any().downcast_ref::<StringArray>())
            .unwrap_or_default();
        let added: Vec<String> = added
            .iter()
            .map(|text| {
                parse_identity(text)
                    .and_then(|labels| labels.get(label).cloned())
                    .unwrap_or_default()
            })
            .collect();
        let mut builder = StringBuilder::with_capacity(
            built_len + added.len(),
            built.map_or(0, |built| built.value_data().len())
                + added.iter().map(String::len).sum::<usize>(),
        );
        for value in built.into_iter().flat_map(|built| built.iter()) {
            builder.append_value(value.unwrap_or_default());
        }
        for value in &added {
            builder.append_value(value);
        }
        let values: ArrayRef = Arc::new(builder.finish());
        identity
            .values_by_label
            .insert(label.to_string(), values.clone());
        self.dictionaries_built
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::downgrade(&values));
        Ok(values)
    }
}

/// The labels of the series whose identity text is `text`: an interned
/// series' own, else parsed (and not kept); `None` for a text that is no
/// label set's JSON. The labels the writer's `labels_of_identity` gives,
/// without its process-wide cache (a segment's texts outlive no reader).
fn parse_identity(text: &str) -> Option<Arc<BTreeMap<String, String>>> {
    match crate::series_ids::by_identity(text) {
        Some(series) => Some(series.labels().clone()),
        None if text.starts_with('{') => serde_json::from_str(text).ok().map(Arc::new),
        None => None,
    }
}

/// The head of a record frame's payload.
fn record_head(payload: &[u8]) -> Option<RecordHead> {
    let n = payload.len();
    let head_len = u32::from_le_bytes(payload.get(n.checked_sub(4)?..)?.try_into().ok()?) as usize;
    rmp_serde::from_slice(payload.get(n.checked_sub(4 + head_len)?..n - 4)?).ok()
}

/// Where one view's rows of one window are on disk: its fragment in a
/// segment, and its record's grids.
#[derive(Debug, Clone)]
pub struct WindowLocation {
    segment: Arc<Segment>,
    offset: u64,
    len: u32,
    grids: (u64, u32),
}

/// Bytes read for a decode: borrowed from a mapping, or owned.
enum Bytes {
    Mapped(Arc<Mapping>, usize, usize),
    Owned(Vec<u8>),
}

impl Bytes {
    fn slice(&self) -> &[u8] {
        match self {
            Bytes::Mapped(mapping, offset, len) => &mapping.bytes()[*offset..*offset + *len],
            Bytes::Owned(bytes) => bytes,
        }
    }
}

impl WindowLocation {
    pub(crate) fn new(segment: Arc<Segment>, (offset, len): (u64, u32), grids: (u64, u32)) -> Self {
        Self {
            segment,
            offset,
            len,
            grids,
        }
    }

    /// The segment file holding the fragment.
    pub fn segment_path(&self) -> &Path {
        &self.segment.path
    }

    /// The fragment's byte offset in its segment.
    #[cfg(test)]
    pub(crate) fn offset(&self) -> u64 {
        self.offset
    }

    /// The fragment's length in bytes.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether the fragment is empty (never, for a written window).
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether its segment is sealed (and so read through a mapping).
    #[cfg(test)]
    pub(crate) fn is_sealed(&self) -> bool {
        self.segment.is_sealed()
    }

    /// Whether its segment's pages were advised cold.
    #[cfg(test)]
    pub(crate) fn is_cold(&self) -> bool {
        self.segment.is_cold()
    }

    /// `len` bytes at `offset` of the segment.
    fn bytes(&self, (offset, len): (u64, u32), how: ViewRead) -> Result<Bytes, Error> {
        let len = len as usize;
        if offset
            .checked_add(len as u64)
            .is_none_or(|end| end > self.segment.len())
        {
            return Err(Error::Invalid(format!(
                "{}: a fragment past the segment's end",
                self.segment.path.display()
            )));
        }
        if how == ViewRead::Mmap && self.segment.is_sealed() {
            let mapping = self.segment.mapping()?;
            // The fragment is read whole: fault it in at once rather than
            // page by page (the mapping is MADV_RANDOM).
            let page = 4096;
            let start = offset as usize / page * page;
            let end = (offset as usize + len).min(mapping.len);
            // SAFETY: a range inside the mapping; advice only.
            unsafe {
                libc::madvise(
                    mapping.ptr.as_ptr().add(start).cast(),
                    end - start,
                    libc::MADV_WILLNEED,
                )
            };
            return Ok(Bytes::Mapped(mapping, offset as usize, len));
        }
        let mut bytes = vec![0u8; len];
        self.segment
            .file
            .read_exact_at(&mut bytes, offset)
            .map_err(|e| Error::Invalid(format!("{}: {e}", self.segment.path.display())))?;
        Ok(Bytes::Owned(bytes))
    }

    /// The fragment's rows as one batch of `schema`, and the bytes its
    /// state columns hold: they are rebuilt in the mapped layout, which
    /// [`Batch::memory_bytes`] counts as held outside the run, so a cache
    /// holding the batch charges them itself.
    pub fn read(&self, schema: &SchemaRef, how: ViewRead) -> Result<(Batch, usize), Error> {
        let grids: Vec<Option<Arc<HydraCms>>> = if self.grids.1 == 0 {
            Vec::new()
        } else {
            let bytes = self.bytes(self.grids, ViewRead::Pread)?;
            let grids: Vec<serde_bytes::ByteBuf> = rmp_serde::from_slice(bytes.slice())
                .map_err(|e| Error::Invalid(format!("view record grids: {e}")))?;
            grids
                .iter()
                .map(|bytes| {
                    let grid = asap_sketchlib::Hydra::from_asapv1(bytes).ok()?;
                    HydraCms::restore_from_disk(grid).ok().map(Arc::new)
                })
                .collect()
        };
        let compressed = self.bytes((self.offset, self.len), how)?;
        decode_fragment(
            &decompress(compressed.slice())?,
            schema,
            &self.segment,
            &grids,
        )
    }

    /// The segment holding the fragment.
    pub(crate) fn segment(&self) -> &Arc<Segment> {
        &self.segment
    }

    /// The fragment's length, for counting bytes read.
    pub(crate) fn read_bytes(&self) -> u64 {
        u64::from(self.len) + u64::from(self.grids.1)
    }
}

/// The texts new to a segment while one record is encoded.
pub(crate) struct SegmentTexts<'a> {
    /// The segment's texts so far, by text.
    pub known: &'a HashMap<Arc<str>, u32>,
    /// The first new text's key.
    pub base: u32,
    pub new: Vec<Arc<str>>,
    pub new_index: HashMap<Arc<str>, u32>,
    /// The labels of each series identity the segment's records held, by
    /// identity text: looked up in the interner once per segment, not once
    /// per row of every window.
    pub labels_by_identity: &'a mut LabelsByIdentity,
}

/// Per series identity text, its labels (`None`: no label set's JSON).
pub(crate) type LabelsByIdentity = HashMap<Arc<str>, Option<Arc<BTreeMap<String, String>>>>;

impl SegmentTexts<'_> {
    fn key(&mut self, text: &Arc<str>) -> u32 {
        if let Some(&key) = self.known.get(text).or_else(|| self.new_index.get(text)) {
            return key;
        }
        let key = self.base + self.new.len() as u32;
        self.new.push(text.clone());
        self.new_index.insert(text.clone(), key);
        key
    }

    /// The labels of the series whose identity text is `identity`
    /// ([`labels_of_identity`]), cached for the segment.
    fn identity_labels(&mut self, identity: &Arc<str>) -> Option<Arc<BTreeMap<String, String>>> {
        if let Some(labels) = self.labels_by_identity.get(identity) {
            return labels.clone();
        }
        let labels = labels_of_identity(identity);
        self.labels_by_identity
            .insert(identity.clone(), labels.clone());
        labels
    }

    /// Drops the new texts after the first `mark` (a fragment left out).
    pub(crate) fn forget_after(&mut self, mark: usize) {
        for text in self.new.drain(mark..) {
            self.new_index.remove(&text);
        }
    }
}

/// Appends `section` 8-byte aligned to `out`; returns its offset and length.
fn section(out: &mut Vec<u8>, section: &[u8]) -> (u64, u64) {
    out.resize(padded(out.len()), 0);
    let offset = out.len() as u64;
    out.extend_from_slice(section);
    (offset, section.len() as u64)
}

/// A validity bitmap (Arrow's LSB order), if any row is null.
fn validity(nulls: &[bool]) -> Option<Vec<u8>> {
    if !nulls.iter().any(|&null| null) {
        return None;
    }
    let mut bits = vec![0u8; nulls.len().div_ceil(8)];
    for (row, &null) in nulls.iter().enumerate() {
        if !null {
            bits[row / 8] |= 1 << (row % 8);
        }
    }
    Some(bits)
}

/// `rows` (of `schema`) as one compressed columnar fragment; `None` when a
/// state has no byte encoding.
pub(crate) fn encode_fragment(
    schema: &Schema,
    rows: &[Vec<Value>],
    texts: &mut SegmentTexts,
    grids: &mut Grids,
) -> Option<Vec<u8>> {
    let columns = schema.fields.len();
    let mut out = Vec::new();
    out.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    out.extend_from_slice(&(columns as u32).to_le_bytes());
    let descriptors_at = out.len();
    out.resize(descriptors_at + columns * DESCRIPTOR_BYTES, 0);
    let identity_labels = IdentityLabels::of(schema, rows, texts);
    for (column, field) in schema.fields.iter().enumerate() {
        let values = rows.iter().map(|row| &row[column]);
        let nulls: Vec<bool> = rows
            .iter()
            .map(|row| matches!(row[column], Value::Null))
            .collect();
        // The second section: the validity bitmap, or a mapped state
        // column's states.
        let mut second: Option<Vec<u8>> = None;
        // The identity column, when this column holds its series' label.
        let label_of_identity_column = identity_labels
            .as_ref()
            .filter(|labels| {
                field.dtype == FieldDataType::Plain(DataType::Utf8)
                    && labels.holds_label(rows, column, &field.name)
            })
            .map(|labels| labels.column as u32);
        // A state column of a family with a compact form, compacted from
        // each state's mapped layout (`None` when a state has none, e.g. a
        // keyed exact state, or the column has no compact form).
        let mut compact = compact_kind(&field.dtype).and_then(|kind| {
            let views = rows
                .iter()
                .map(|row| match &row[column] {
                    Value::Summary { state, .. } => state_view_bytes(state.as_ref()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;
            Some((kind, encode_states(kind, views.iter())?))
        });
        // Otherwise the column in the mapped layout, when it has one.
        let mut mapped = match &field.dtype {
            FieldDataType::Plain(_) => None,
            _ if compact.is_some() => None,
            family if family_has_mapped_form(family) => mapped_states(rows, column),
            _ => None,
        };
        let (kind, mut flags, data): (u32, u32, Vec<u8>) = match &field.dtype {
            _ if label_of_identity_column.is_some() => (
                IDENTITY_LABEL,
                label_of_identity_column.expect("checked"),
                Vec::new(),
            ),
            FieldDataType::Plain(DataType::Utf8) => {
                let keys = values.map(|value| match value {
                    Value::Utf8(text) => texts.key(text),
                    _ => 0,
                });
                (TEXT, 0, encode_text_keys(keys))
            }
            FieldDataType::Plain(DataType::Float64) => {
                let mut data = Vec::with_capacity(rows.len() * 8);
                for value in values {
                    let v = match value {
                        Value::Float64(v) => *v,
                        _ => 0.0,
                    };
                    data.extend_from_slice(&v.to_le_bytes());
                }
                (FLOAT64, 0, data)
            }
            FieldDataType::Plain(DataType::Int64) => {
                let mut data = Vec::with_capacity(rows.len() * 8);
                for value in values {
                    let v = match value {
                        Value::Int64(v) => *v,
                        _ => 0,
                    };
                    data.extend_from_slice(&v.to_le_bytes());
                }
                (INT64, 0, data)
            }
            FieldDataType::Plain(DataType::Timestamp) => {
                let times: Vec<i64> = values
                    .map(|value| match value {
                        Value::Timestamp(t) => *t,
                        _ => 0,
                    })
                    .collect();
                let constant = !times.is_empty()
                    && !nulls.iter().any(|&n| n)
                    && times.iter().all(|&t| t == times[0]);
                let data: Vec<u8> = if constant {
                    times[0].to_le_bytes().to_vec()
                } else {
                    times.iter().flat_map(|t| t.to_le_bytes()).collect()
                };
                (TIMESTAMP, if constant { CONSTANT } else { 0 }, data)
            }
            FieldDataType::Plain(_) => {
                let values: Vec<&Value> = values.collect();
                (GENERIC, 0, rmp_serde::to_vec(&values).ok()?)
            }
            _ if compact.is_some() => {
                let (compact_kind, compact) = compact.take().expect("checked");
                (COMPACT_STATES, compact_kind.code(), compact)
            }
            _ if mapped.is_some() => {
                let (offsets, states) = mapped.take().expect("checked");
                second = Some(states);
                (MAPPED_STATES, 0, offsets)
            }
            _ => {
                // States: u32 offsets[rows + 1], then each state's stored bytes.
                let mut offsets = Vec::with_capacity(rows.len() + 1);
                let mut bytes = Vec::new();
                offsets.push(0u32);
                for value in values {
                    let Value::Summary { state, .. } = value else {
                        return None;
                    };
                    let stored = encode_state(state.as_ref(), grids)?;
                    bytes.extend(rmp_serde::to_vec(&stored).ok()?);
                    offsets.push(u32::try_from(bytes.len()).ok()?);
                }
                let mut data: Vec<u8> = offsets.iter().flat_map(|o| o.to_le_bytes()).collect();
                data.extend(bytes);
                (STATES, 0, data)
            }
        };
        let (data_offset, data_len) = section(&mut out, &data);
        let (valid_offset, valid_len) = match (kind, validity(&nulls)) {
            (MAPPED_STATES, _) => section(&mut out, &second.take().expect("states")),
            (STATES | GENERIC | COMPACT_STATES, _) => (0, 0),
            (_, Some(bits)) => section(&mut out, &bits),
            (_, None) => (0, 0),
        };
        if kind == TIMESTAMP && valid_len > 0 {
            flags &= !CONSTANT;
        }
        let at = descriptors_at + column * DESCRIPTOR_BYTES;
        for (i, word) in [
            u64::from(kind) | u64::from(flags) << 32,
            data_offset,
            data_len,
            valid_offset,
            valid_len,
        ]
        .iter()
        .enumerate()
        {
            out[at + i * 8..at + i * 8 + 8].copy_from_slice(&word.to_le_bytes());
        }
    }
    compress(&out)
}

/// The compact form of a state family's column, if it has one.
fn compact_kind(family: &FieldDataType) -> Option<CompactKind> {
    Some(match family {
        FieldDataType::Sketch(SketchAlgorithm::DDSketch) => CompactKind::DDSketch,
        FieldDataType::ExactAggregate(kind, _) => match kind {
            ExactKind::Sum => CompactKind::Sum,
            ExactKind::Count => CompactKind::Count,
            ExactKind::Min | ExactKind::Max => CompactKind::MinMax,
            ExactKind::Counter => CompactKind::Counter,
            ExactKind::IRate => CompactKind::LastTwoSamples,
        },
        _ => return None,
    })
}

/// The labels of each row's series, from the series identity column, for
/// telling which text columns are its labels ([`IDENTITY_LABEL`]).
struct IdentityLabels {
    /// The series identity column's index.
    column: usize,
    /// Each row's labels; `None` when a row's identity is none.
    by_row: Vec<Option<Arc<BTreeMap<String, String>>>>,
}

impl IdentityLabels {
    /// `None` when there are no rows, or `schema` has no series identity
    /// text column or no other text column.
    fn of(schema: &Schema, rows: &[Vec<Value>], texts: &mut SegmentTexts) -> Option<Self> {
        if rows.is_empty() {
            return None;
        }
        let is_text = |dtype: &FieldDataType| *dtype == FieldDataType::Plain(DataType::Utf8);
        let column = schema
            .fields
            .iter()
            .position(|field| field.name == PROMQL_SERIES_IDENTITY && is_text(&field.dtype))?;
        if !schema
            .fields
            .iter()
            .enumerate()
            .any(|(index, field)| index != column && is_text(&field.dtype))
        {
            return None;
        }
        let by_row = rows
            .iter()
            .map(|row| match &row[column] {
                Value::Utf8(identity) => texts.identity_labels(identity),
                _ => None,
            })
            .collect();
        Some(Self { column, by_row })
    }

    /// Whether every row's `column` is its series' label `name` (`""`
    /// without it), so the column need not be stored.
    fn holds_label(&self, rows: &[Vec<Value>], column: usize, name: &str) -> bool {
        column != self.column
            && rows
                .iter()
                .zip(&self.by_row)
                .all(|(row, labels)| match (&row[column], labels) {
                    (Value::Utf8(text), Some(labels)) => {
                        **text == *labels.get(name).map_or("", String::as_str)
                    }
                    _ => false,
                })
    }
}

/// Column `column` of `rows` in the executor's mapped state layout: the
/// `u32` offsets (`rows + 1`) and the states back to back, each padded to 8
/// bytes; `None` when a state has no mapped form.
fn mapped_states(rows: &[Vec<Value>], column: usize) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut offsets = Vec::with_capacity((rows.len() + 1) * 4);
    let mut states = Vec::new();
    offsets.extend_from_slice(&0u32.to_le_bytes());
    for row in rows {
        let Value::Summary { state, .. } = &row[column] else {
            return None;
        };
        states.extend(state_view_bytes(state.as_ref())?);
        states.resize(padded(states.len()), 0);
        offsets.extend_from_slice(&u32::try_from(states.len()).ok()?.to_le_bytes());
    }
    Some((offsets, states))
}

/// A little-endian `u64` at `at` of `bytes`.
fn word(bytes: &[u8], at: usize) -> Result<u64, Error> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .ok_or_else(damaged)?
            .try_into()
            .map_err(|_| damaged())?,
    ))
}

fn damaged() -> Error {
    Error::Invalid("a damaged view fragment".into())
}

/// `bytes[offset..offset + len]` copied into a buffer of its own, 8-byte
/// aligned (for any fixed-width type) and charged exactly its size.
fn buffer(bytes: &[u8], offset: usize, len: usize) -> Result<Buffer, Error> {
    let slice = bytes.get(offset..offset + len).ok_or_else(damaged)?;
    let mut words = vec![0u64; len.div_ceil(8)];
    // SAFETY: `words` is at least `len` writable bytes.
    unsafe { std::ptr::copy_nonoverlapping(slice.as_ptr(), words.as_mut_ptr().cast::<u8>(), len) };
    Ok(Buffer::from(words).slice_with_length(0, len))
}

/// A stored fragment: the length of its columnar bytes (`u32`), then
/// those bytes compressed.
/// `None` past [`MAX_FRAME_BYTES`], which a read rejects as damaged.
fn compress(columnar: &[u8]) -> Option<Vec<u8>> {
    if columnar.len() as u64 > MAX_FRAME_BYTES {
        return None;
    }
    let mut out = u32::try_from(columnar.len()).ok()?.to_le_bytes().to_vec();
    // One zstd context per thread, not one per fragment.
    let compressed = COMPRESSOR.with(|compressor| {
        let mut compressor = compressor.borrow_mut();
        if compressor.is_none() {
            *compressor = Some(zstd::bulk::Compressor::new(ZSTD_LEVEL).ok()?);
        }
        compressor.as_mut()?.compress(columnar).ok()
    })?;
    out.extend(compressed);
    Some(out)
}

thread_local! {
    static COMPRESSOR: RefCell<Option<zstd::bulk::Compressor<'static>>> = const { RefCell::new(None) };
    static DECOMPRESSOR: RefCell<Option<zstd::bulk::Decompressor<'static>>> =
        const { RefCell::new(None) };
}

/// The columnar bytes of a stored fragment.
fn decompress(stored: &[u8]) -> Result<Vec<u8>, Error> {
    let length = u32::from_le_bytes(stored.get(..4).ok_or_else(damaged)?.try_into().unwrap());
    // The length is checked against the frame's own content size before it
    // sizes an allocation.
    let content_size = zstd::zstd_safe::get_frame_content_size(&stored[4..]);
    if u64::from(length) > MAX_FRAME_BYTES
        || !matches!(content_size, Ok(Some(size)) if size == u64::from(length))
    {
        return Err(damaged());
    }
    let columnar = DECOMPRESSOR
        .with(|decompressor| {
            let mut decompressor = decompressor.borrow_mut();
            if decompressor.is_none() {
                *decompressor = Some(zstd::bulk::Decompressor::new().ok()?);
            }
            decompressor
                .as_mut()?
                .decompress(&stored[4..], length as usize)
                .ok()
        })
        .ok_or_else(damaged)?;
    if columnar.len() != length as usize {
        return Err(damaged());
    }
    Ok(columnar)
}

/// A primitive column of `rows` values at `offset`, with nulls.
fn primitive<T: ArrowPrimitiveType>(
    source: &[u8],
    (offset, len): (usize, usize),
    rows: usize,
    nulls: Option<NullBuffer>,
) -> Result<PrimitiveArray<T>, Error> {
    if len != rows * std::mem::size_of::<T::Native>() {
        return Err(damaged());
    }
    let values = ScalarBuffer::<T::Native>::new(buffer(source, offset, len)?, 0, rows);
    PrimitiveArray::<T>::try_new(values, nulls).map_err(|e| Error::Invalid(e.to_string()))
}

/// One column's descriptor in a fragment: its kind and flags, and its data
/// and second sections (offset and length, inside the fragment).
struct Descriptor {
    kind: u32,
    flags: u32,
    data: (usize, usize),
    valid: (usize, usize),
}

/// Column `column`'s descriptor in fragment `bytes`; damaged when a
/// section lies past the fragment's end.
fn descriptor(bytes: &[u8], column: usize) -> Result<Descriptor, Error> {
    let at = 8 + column * DESCRIPTOR_BYTES;
    let kind_flags = word(bytes, at)?;
    let section = |at: usize| -> Result<(usize, usize), Error> {
        let (offset, len) = (word(bytes, at)? as usize, word(bytes, at + 8)? as usize);
        if offset.checked_add(len).is_none_or(|end| end > bytes.len()) {
            return Err(damaged());
        }
        Ok((offset, len))
    };
    Ok(Descriptor {
        kind: (kind_flags & 0xffff_ffff) as u32,
        flags: (kind_flags >> 32) as u32,
        data: section(at + 8)?,
        valid: section(at + 24)?,
    })
}

/// The columnar fragment `bytes` as one batch of `schema`, and the bytes
/// its state columns hold.
fn decode_fragment(
    bytes: &[u8],
    schema: &SchemaRef,
    segment: &Segment,
    grids: &[Option<Arc<HydraCms>>],
) -> Result<(Batch, usize), Error> {
    let mut state_bytes = 0;
    let header = word(bytes, 0)?;
    let rows = (header & 0xffff_ffff) as usize;
    let columns = (header >> 32) as usize;
    // Every row takes at least a bit of the fragment (a constant column
    // alone takes none, but no view is one): a larger count is damage, and
    // would allocate without bound.
    if columns != schema.fields.len()
        || 8 + columns * DESCRIPTOR_BYTES > bytes.len()
        || rows > bytes.len() * 8
    {
        return Err(damaged());
    }
    // The Arrow layout of each plain column, as the executor builds it.
    let empty = Batch::try_new(schema.clone(), Vec::new())?;
    let mut out = Vec::with_capacity(columns);
    // The series identity column's index and keys, decoded once for every
    // label column read back from it.
    let mut identity_keys: Option<(usize, ScalarBuffer<u32>)> = None;
    for (column, field) in schema.fields.iter().enumerate() {
        let Descriptor {
            kind,
            flags,
            data,
            valid,
        } = descriptor(bytes, column)?;
        let nulls = if valid.1 == 0 || kind == MAPPED_STATES || kind == COMPACT_STATES {
            None
        } else {
            if valid.1 != rows.div_ceil(8) {
                return Err(damaged());
            }
            let bits = Buffer::from(bytes[valid.0..valid.0 + valid.1].to_vec());
            Some(NullBuffer::new(BooleanBuffer::new(bits, 0, rows)))
        };
        let expected = empty.column(column).data_type().clone();
        let array: ArrayRef = match kind {
            TEXT => {
                let keys =
                    decode_text_keys(&bytes[data.0..data.0 + data.1], rows).ok_or_else(damaged)?;
                let keys = UInt32Array::try_new(keys.into(), nulls)
                    .map_err(|e| Error::Invalid(e.to_string()))?;
                let needed = keys.iter().flatten().max().map_or(0, |k| k as usize + 1);
                if field.name == PROMQL_SERIES_IDENTITY && keys.null_count() == 0 {
                    identity_keys = Some((column, keys.values().clone()));
                }
                let values = segment.dictionary(needed)?;
                Arc::new(
                    DictionaryArray::<UInt32Type>::try_new(keys, values)
                        .map_err(|e| Error::Invalid(e.to_string()))?,
                )
            }
            IDENTITY_LABEL => {
                // The identity column's keys, as this column's keys into
                // the label's values by text.
                let identity_column = flags as usize;
                if identity_column >= columns {
                    return Err(damaged());
                }
                let identity = descriptor(bytes, identity_column)?;
                if identity.kind != TEXT || identity.valid.1 != 0 || data.1 != 0 || valid.1 != 0 {
                    return Err(damaged());
                }
                // Each label column holds a copy of the identity column's
                // keys, so its charge is what it holds.
                let keys = match &identity_keys {
                    Some((at, keys)) if *at == identity_column => keys.clone(),
                    _ => {
                        let keys = ScalarBuffer::from(
                            decode_text_keys(
                                &bytes[identity.data.0..identity.data.0 + identity.data.1],
                                rows,
                            )
                            .ok_or_else(damaged)?,
                        );
                        identity_keys = Some((identity_column, keys.clone()));
                        keys
                    }
                };
                let keys = UInt32Array::from(keys.to_vec());
                let needed = keys.values().iter().max().map_or(0, |&k| k as usize + 1);
                let values = segment.label_values(&field.name, needed)?;
                Arc::new(
                    DictionaryArray::<UInt32Type>::try_new(keys, values)
                        .map_err(|e| Error::Invalid(e.to_string()))?,
                )
            }
            FLOAT64 => Arc::new(primitive::<Float64Type>(bytes, data, rows, nulls)?),
            INT64 => Arc::new(primitive::<Int64Type>(bytes, data, rows, nulls)?),
            TIMESTAMP if flags & CONSTANT != 0 => {
                if data.1 != 8 {
                    return Err(damaged());
                }
                let t = i64::from_le_bytes(bytes[data.0..data.0 + 8].try_into().unwrap());
                Arc::new(PrimitiveArray::<TimestampMillisecondType>::from(vec![
                    t;
                    rows
                ]))
            }
            TIMESTAMP => Arc::new(primitive::<TimestampMillisecondType>(
                bytes, data, rows, nulls,
            )?),
            GENERIC => {
                let values: Vec<Value> = rmp_serde::from_slice(&bytes[data.0..data.0 + data.1])
                    .map_err(|_| damaged())?;
                if values.len() != rows {
                    return Err(damaged());
                }
                one_column(field, values.into_iter().map(|v| vec![v]).collect())?.0
            }
            COMPACT_STATES => {
                let compact = CompactKind::from_code(flags)
                    .filter(|&kind| Some(kind) == compact_kind(&field.dtype))
                    .ok_or_else(damaged)?;
                let (states, offsets) =
                    decode_states(compact, &bytes[data.0..data.0 + data.1], rows)
                        .ok_or_else(damaged)?;
                state_bytes += states.len() * 8 + offsets.len() * 4;
                let column = asap_executor::values::StateColumn::from_mapped(
                    field.dtype.clone(),
                    Buffer::from(states),
                    ScalarBuffer::from(offsets),
                )?;
                out.push(BatchColumn::States {
                    indices: UInt32Array::from_iter_values(0..rows as u32),
                    states: Arc::new(column),
                });
                continue;
            }
            MAPPED_STATES => {
                if data.1 != (rows + 1) * 4 {
                    return Err(damaged());
                }
                state_bytes += data.1 + valid.1;
                let offsets = ScalarBuffer::<u32>::new(buffer(bytes, data.0, data.1)?, 0, rows + 1);
                let states = buffer(bytes, valid.0, valid.1)?;
                let column = asap_executor::values::StateColumn::from_mapped(
                    field.dtype.clone(),
                    states,
                    offsets,
                )?;
                out.push(BatchColumn::States {
                    indices: UInt32Array::from_iter_values(0..rows as u32),
                    states: Arc::new(column),
                });
                continue;
            }
            STATES => {
                let slice = &bytes[data.0..data.0 + data.1];
                let offsets_len = (rows + 1) * 4;
                if slice.len() < offsets_len {
                    return Err(damaged());
                }
                let offset = |row: usize| {
                    u32::from_le_bytes(slice[row * 4..row * 4 + 4].try_into().unwrap()) as usize
                };
                let states = &slice[offsets_len..];
                let mut values = Vec::with_capacity(rows);
                for row in 0..rows {
                    let (from, to) = (offset(row), offset(row + 1));
                    let stored: StoredValue =
                        rmp_serde::from_slice(states.get(from..to).ok_or_else(damaged)?)
                            .map_err(|_| damaged())?;
                    let state = decode_stored_state(stored, grids).ok_or_else(damaged)?;
                    values.push(vec![Value::Summary {
                        family: field.dtype.clone(),
                        state,
                    }]);
                }
                let (indices, states) = one_column(field, values)?;
                let indices = indices
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .ok_or_else(damaged)?
                    .clone();
                out.push(BatchColumn::States {
                    indices,
                    states: states.ok_or_else(damaged)?,
                });
                continue;
            }
            _ => return Err(damaged()),
        };
        let array = if array.data_type() == &expected {
            array
        } else {
            match (&expected, kind) {
                (ArrowType::Timestamp(TimeUnit::Millisecond, _), TIMESTAMP) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
                        .ok_or_else(damaged)?
                        .clone()
                        .with_data_type(expected.clone());
                    Arc::new(array) as ArrayRef
                }
                _ => {
                    return Err(Error::Invalid(format!(
                        "a view fragment column {} is not in its Arrow layout",
                        field.name
                    )))
                }
            }
        };
        if array.len() != rows {
            return Err(damaged());
        }
        out.push(BatchColumn::Plain(array));
    }
    Ok((Batch::try_from_columns(schema.clone(), out)?, state_bytes))
}

/// `rows` of one column `field` built by the executor: its array, and its
/// states if it is a summary column.
fn one_column(
    field: &asap_executor::planner::ir::schema::Field,
    rows: Vec<Vec<Value>>,
) -> Result<(ArrayRef, Option<Arc<asap_executor::values::StateColumn>>), Error> {
    let schema = Arc::new(Schema::new(vec![field.clone()]));
    let batch = Batch::try_new(schema, rows)?;
    Ok((batch.column(0).clone(), batch.state_column(0).cloned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use asap_executor::planner::ir::schema::Field;

    /// A fragment or record range read from disk that is out of bounds
    /// (an overflowing offset, a row count no fragment of its length can
    /// hold) is an error, not a panic or a huge allocation.
    #[test]
    fn out_of_bounds_heads_are_errors() {
        let dir = std::env::temp_dir().join(format!("asap-segment-heads-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let header = SegmentHeader {
            plan_hash: 7,
            group: 0,
            version: FORMAT_VERSION,
        };
        let segment = Segment::create_new(&dir, 10, &header).unwrap();
        let schema = Arc::new(Schema::with_time_index(
            vec![Field::plain("ts", DataType::Timestamp, false)],
            0,
            vec![],
        ));
        let location = WindowLocation::new(segment, (u64::MAX - 2, 10), (0, 0));
        assert!(location.read(&schema, ViewRead::Pread).is_err());

        // u32::MAX rows of one constant timestamp, in 56 bytes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((1u64 << 32) | u64::from(u32::MAX)).to_le_bytes());
        for word in [
            (u64::from(CONSTANT) << 32) | u64::from(TIMESTAMP),
            48,
            8,
            0,
            0,
        ] {
            bytes.extend_from_slice(&word.to_le_bytes());
        }
        bytes.extend_from_slice(&10i64.to_le_bytes());
        let segment = Segment::create_new(&dir, 20, &header).unwrap();
        assert!(decode_fragment(&bytes, &schema, &segment, &[]).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
