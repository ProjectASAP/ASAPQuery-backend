use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_histogram_vec, CounterVec, HistogramTimer, HistogramVec,
};

// Status labels for query requests. `fallback` means the local engine
// returned None and the fallback client handled the query; `error` covers
// parse failures, disabled-handling short-circuits, and fallback failures.
pub const QUERY_STATUS_OK: &str = "ok";
pub const QUERY_STATUS_FALLBACK: &str = "fallback";
pub const QUERY_STATUS_ERROR: &str = "error";
pub const QUERY_STATUS_UNSUPPORTED: &str = "unsupported";

pub const QUERY_TYPE_INSTANT: &str = "instant";
pub const QUERY_TYPE_RANGE: &str = "range";

lazy_static! {
    pub static ref QUERY_REQUESTS_TOTAL: CounterVec = register_counter_vec!(
        "asap_query_requests_total",
        "Total query-server requests, labelled by query type and outcome",
        &["type", "status"]
    )
    .unwrap();

    // Buckets span the range the paper expects: sketch hits land
    // sub-ms (§6.3), cold-fallback hits land 10ms–1s.
    pub static ref QUERY_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "asap_query_duration_seconds",
        "End-to-end query request duration",
        &["type"],
        vec![
            0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0
        ]
    )
    .unwrap();

    pub static ref INGEST_SAMPLES_TOTAL: CounterVec = register_counter_vec!(
        "asap_ingest_samples_total",
        "Raw samples accepted by the ingest server, labelled by wire protocol",
        &["protocol"]
    )
    .unwrap();

    pub static ref INGEST_BATCH_DURATION_SECONDS: HistogramVec = register_histogram_vec!(
        "asap_ingest_batch_duration_seconds",
        "Time to decode + route one ingest batch",
        &["protocol"],
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap();

    pub static ref INGEST_DECODE_ERRORS_TOTAL: CounterVec = register_counter_vec!(
        "asap_ingest_decode_errors_total",
        "Ingest batches rejected before routing (decode / parse failures)",
        &["protocol"]
    )
    .unwrap();
}

// Touching each lazy_static so all metric families show up on the first
// `/metrics` scrape even if no traffic has hit the server yet. Prometheus
// treats "metric appeared after scrape N" as a reset, which breaks rate().
pub fn register_all() {
    lazy_static::initialize(&QUERY_REQUESTS_TOTAL);
    lazy_static::initialize(&QUERY_DURATION_SECONDS);
    lazy_static::initialize(&INGEST_SAMPLES_TOTAL);
    lazy_static::initialize(&INGEST_BATCH_DURATION_SECONDS);
    lazy_static::initialize(&INGEST_DECODE_ERRORS_TOTAL);
}

pub fn start_query_timer(query_type: &str) -> HistogramTimer {
    QUERY_DURATION_SECONDS
        .with_label_values(&[query_type])
        .start_timer()
}

pub fn record_query_outcome(query_type: &str, status: &str) {
    QUERY_REQUESTS_TOTAL
        .with_label_values(&[query_type, status])
        .inc();
}

pub fn start_ingest_timer(protocol: &str) -> HistogramTimer {
    INGEST_BATCH_DURATION_SECONDS
        .with_label_values(&[protocol])
        .start_timer()
}

pub fn record_ingest_samples(protocol: &str, count: u64) {
    INGEST_SAMPLES_TOTAL
        .with_label_values(&[protocol])
        .inc_by(count as f64);
}

pub fn record_ingest_decode_error(protocol: &str) {
    INGEST_DECODE_ERRORS_TOTAL
        .with_label_values(&[protocol])
        .inc();
}
