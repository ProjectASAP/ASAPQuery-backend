//! A view window read back from its file is charged to the window cache
//! what it holds in memory, the same as when it was written: its own
//! columns and states, not the segment's text dictionary its text columns
//! share with every other window of the segment. That dictionary is
//! counted once per segment in the view bytes, as is each older one a
//! window still holds after the active segment's dictionary grew.
//! Measured with a counting global allocator (one test per binary, so no
//! other test allocates).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use asap_executor::planner::ir::schema::{DataType, Field, Schema, PROMQL_SERIES_IDENTITY};
use asap_executor::planner::plan::materialized_view::MaterializedView;
use asap_executor::values::{Batch, Value};
use data_plane::persist::{ViewFiles, ViewRead};
use data_plane::store::{ViewStore, ViewTier};
use data_plane::window_cache::{WindowCache, WindowKey};

/// Counts the bytes allocated now.
struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            CURRENT.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            CURRENT.fetch_add(new_size, Ordering::Relaxed);
            CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const SERIES: usize = 2_000;
const WINDOWS: i64 = 8;
const WIDTH_MS: i64 = 1_000;
const VIEW: usize = 1;

/// A per-series exact view of `data` carrying three labels, as the demo's
/// 1 s views: the series identity and `job`, `region`, `service` as text
/// columns, and the value.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::with_time_index(
        vec![
            Field::plain("ts", DataType::Timestamp, false),
            Field::plain(PROMQL_SERIES_IDENTITY, DataType::Utf8, false),
            Field::plain("job", DataType::Utf8, false),
            Field::plain("region", DataType::Utf8, false),
            Field::plain("service", DataType::Utf8, false),
            Field::plain("value", DataType::Float64, false),
        ],
        0,
        vec![],
    ))
}

/// `series` series of the view's window ending `end_ms`.
fn window(end_ms: i64, series: usize) -> Batch {
    let rows = (0..series)
        .map(|s| {
            let (job, region, service) = (s % 200, s % 4, s % 20);
            vec![
                Value::Timestamp(end_ms),
                Value::Utf8(
                    format!(
                        r#"{{"__name__":"data","job":"job-{job}","region":"region-{region}","series":"series-{s}","service":"service-{service}"}}"#
                    )
                    .into(),
                ),
                Value::Utf8(format!("job-{job}").into()),
                Value::Utf8(format!("region-{region}").into()),
                Value::Utf8(format!("service-{service}").into()),
                Value::Float64(s as f64 + end_ms as f64),
            ]
        })
        .collect();
    Batch::try_new(schema(), rows).unwrap()
}

fn view() -> MaterializedView {
    serde_json::from_value(serde_json::json!({
        "node_id": VIEW,
        "refresh": "on_first_read",
        "coverage": {"definition_id": "test-view", "selection": []},
        "inputs": {"materialized_views": []},
        "retention_ms": 1_000_000,
    }))
    .unwrap()
}

/// The heap bytes `read` leaves allocated, and how much the view bytes
/// grew over it.
fn held_by(views: &ViewStore, read: impl FnOnce()) -> (usize, usize) {
    let view_bytes_before = views.bytes();
    let heap_before = CURRENT.load(Ordering::Relaxed);
    read();
    let heap_held = CURRENT.load(Ordering::Relaxed).saturating_sub(heap_before);
    (heap_held, views.bytes() - view_bytes_before)
}

fn assert_close(what: &str, a: u64, b: u64) {
    let ratio = a as f64 / b as f64;
    assert!((0.8..=1.25).contains(&ratio), "{what}: {a} B vs {b} B");
}

/// Writes `WINDOWS` windows of `SERIES` series, seals the segment, drops
/// them from the cache and reads them back `how`; checks each read-back
/// window is charged what it was when written and, for `pread` (where
/// every byte read is on the heap), that the view bytes grow by what the
/// read holds.
fn sealed_windows_read_back(how: ViewRead) {
    let dir = std::env::temp_dir().join(format!("view_cache_charge-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let view = view();
    let tier = Arc::new(ViewTier::new(
        WindowCache::new(None, None),
        how,
        Duration::ZERO,
    ));
    let mut views = ViewStore::tiered(tier.clone());
    let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
    for w in 1..=WINDOWS {
        let range = ((w - 1) * WIDTH_MS, w * WIDTH_MS);
        views
            .append(&view, range, vec![window(range.1, SERIES)])
            .unwrap();
        let slot = views.slot(VIEW, range.1).unwrap();
        let batches = views.rows()[&VIEW].last().unwrap().1.clone();
        let written = files.append(0, range, &[(VIEW, &batches)]).unwrap();
        slot.stored(written.locations[0].1.clone());
    }
    files.seal(0).unwrap();
    let written_charge = tier.cache.stats().bytes / WINDOWS as u64;
    assert_eq!(tier.cache.stats().entries, WINDOWS as u64);

    // Every window leaves the cache, then is read back from the file.
    tier.cache.set_target_bytes(Some(0));
    tier.cache.set_target_bytes(None);
    assert_eq!(tier.cache.stats().bytes, 0);
    let (heap_held, view_bytes_added) = held_by(&views, || {
        assert_eq!(views.rows()[&VIEW].len(), WINDOWS as usize);
    });
    let stats = tier.cache.stats();
    assert_eq!(
        (stats.entries, stats.loads),
        (WINDOWS as u64, WINDOWS as u64)
    );
    let read_back_charge = stats.bytes / WINDOWS as u64;
    println!(
        "{how:?}, sealed: per window, written charge {written_charge} B, read-back charge \
         {read_back_charge} B; {WINDOWS} windows read back: heap held {heap_held} B, view \
         bytes added {view_bytes_added} B"
    );
    // The same rows, keys and values: its keys index a larger dictionary,
    // the segment's, which is not the window's own.
    assert_close(
        &format!("{how:?}: read-back vs written charge per window"),
        read_back_charge,
        written_charge,
    );
    // A mapped read borrows the fixed-width columns from the file's pages
    // (charged, not on the heap).
    if how == ViewRead::Pread {
        assert_close(
            "view bytes added vs heap held",
            view_bytes_added as u64,
            heap_held as u64,
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Reads back each window from the active segment right after writing it,
/// each with series new to the segment: each read rebuilds the segment's
/// dictionary, and the cached windows keep the older ones alive. The view
/// bytes count every dictionary still held, once.
fn active_windows_read_back_while_the_dictionary_grows() {
    let dir = std::env::temp_dir().join(format!("view_cache_grow-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let view = view();
    let tier = Arc::new(ViewTier::new(
        WindowCache::new(None, None),
        ViewRead::Mmap,
        Duration::ZERO,
    ));
    let mut views = ViewStore::tiered(tier.clone());
    let mut files = ViewFiles::open(&dir, 7, Duration::ZERO).unwrap();
    let (mut heap_held, mut view_bytes_added) = (0, 0);
    for w in 1..=WINDOWS {
        let range = ((w - 1) * WIDTH_MS, w * WIDTH_MS);
        let series = SERIES + w as usize * 100;
        views
            .append(&view, range, vec![window(range.1, series)])
            .unwrap();
        let slot = views.slot(VIEW, range.1).unwrap();
        let batches = views.rows()[&VIEW].last().unwrap().1.clone();
        let written = files.append(0, range, &[(VIEW, &batches)]).unwrap();
        slot.stored(written.locations[0].1.clone());
        tier.cache.remove(WindowKey {
            view: VIEW,
            end_ms: range.1,
        });
        let (heap, added) = held_by(&views, || {
            views.rows();
        });
        heap_held += heap;
        view_bytes_added += added;
    }
    assert_eq!(tier.cache.stats().loads, WINDOWS as u64);
    println!(
        "active segment, growing: {WINDOWS} windows read back: heap held {heap_held} B, view \
         bytes added {view_bytes_added} B"
    );
    assert_close(
        "view bytes added vs heap held",
        view_bytes_added as u64,
        heap_held as u64,
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_read_back_window_is_charged_what_it_holds() {
    sealed_windows_read_back(ViewRead::Pread);
    sealed_windows_read_back(ViewRead::Mmap);
    active_windows_read_back_while_the_dictionary_grows();
}
