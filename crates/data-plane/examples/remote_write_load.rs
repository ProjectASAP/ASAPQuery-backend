//! A Remote Write load generator for scaling runs: `THREADS` threads, each
//! with its own keep-alive connection to `ADDR`, send the samples of their
//! share of series `[FROM, TO)` of every metric in `METRICS`, one sample per
//! series every `1000 / HZ` ms of event time from `START_MS` for
//! `DURATION_S` seconds. A tick is sent once the wall clock reaches its
//! timestamp, never ahead; a thread that falls behind sends its ticks as
//! fast as the data plane acknowledges them, so the acknowledged rate is the
//! data plane's under overload. A 503 is counted and retried after an
//! exponential backoff (10 ms doubling to 1 s), as Prometheus backs off.
//! Sending stops at `START_MS + DURATION_S` of wall-clock time (or
//! `DEADLINE_MS`), behind or not. Labels and values follow `ingest_bench`
//! (counters, `*_total`,
//! never reset). Requests are built from per-series protobuf prefixes
//! encoded once, so one sender core offers several million samples/s.
//!
//! Prints one JSON line: samples acknowledged, 503 retries, elapsed seconds,
//! how far the last tick was behind the wall clock, and when it finished.
//!
//! `remote_write_load ADDR METRICS FROM TO HZ DURATION_S START_MS THREADS [DEADLINE_MS]`

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn string_field(out: &mut Vec<u8>, tag: u8, text: &str) {
    out.push(tag);
    varint(out, text.len() as u64);
    out.extend_from_slice(text.as_bytes());
}

/// `TimeSeries.labels` (field 1) of one series, encoded.
fn labels(metric: &str, i: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for (name, value) in [
        ("__name__", metric.to_string()),
        ("instance", format!("i{i}")),
        ("job", format!("job{}", i % 16)),
        ("region", format!("r{}", i % 4)),
        ("service", format!("s{}", i % 8)),
    ] {
        let mut label = Vec::new();
        string_field(&mut label, 0x0a, name);
        string_field(&mut label, 0x12, &value);
        out.push(0x0a);
        varint(&mut out, label.len() as u64);
        out.extend_from_slice(&label);
    }
    out
}

/// The snappy-compressed `WriteRequest` of one tick: one sample per series.
fn body(series: &[(Vec<u8>, usize, bool)], tick: i64, timestamp: i64) -> Vec<u8> {
    let mut request = Vec::with_capacity(series.len() * 96);
    let mut sample = Vec::with_capacity(16);
    for (labels, i, counter) in series {
        let gauge = ((*i as i64 * 7919 + tick * 104_729) % 100_003) as f64 / 100.0;
        let value = if *counter {
            gauge + (tick * 1001) as f64
        } else {
            gauge
        };
        sample.clear();
        sample.push(0x09);
        sample.extend_from_slice(&value.to_le_bytes());
        sample.push(0x10);
        varint(&mut sample, timestamp as u64);
        let length = labels.len() + 2 + sample.len();
        request.push(0x0a);
        varint(&mut request, length as u64);
        request.extend_from_slice(labels);
        request.push(0x12);
        request.push(sample.len() as u8);
        request.extend_from_slice(&sample);
    }
    snap::raw::Encoder::new()
        .compress_vec(&request)
        .expect("snappy compresses any input")
}

/// POSTs `body`; returns the status code.
fn post(stream: &mut BufReader<TcpStream>, host: &str, body: &[u8]) -> std::io::Result<u16> {
    let head = format!(
        "POST /api/v1/write HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/x-protobuf\r\nContent-Encoding: snappy\r\nX-Prometheus-Remote-Write-Version: 0.1.0\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let socket = stream.get_mut();
    socket.write_all(head.as_bytes())?;
    socket.write_all(body)?;
    let mut line = String::new();
    stream.read_line(&mut line)?;
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut length = 0usize;
    loop {
        line.clear();
        stream.read_line(&mut line)?;
        if line == "\r\n" || line.is_empty() {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut rest = vec![0; length];
    stream.read_exact(&mut rest)?;
    Ok(status)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addr = args[1].clone();
    let metrics: Vec<String> = args[2].split(',').map(String::from).collect();
    let (from, to): (usize, usize) = (args[3].parse().unwrap(), args[4].parse().unwrap());
    let hz: i64 = args[5].parse().unwrap();
    let duration_s: i64 = args[6].parse().unwrap();
    let start_ms: i64 = args[7].parse().unwrap();
    let threads: usize = args[8].parse().unwrap();
    // Optional: stop sending at this wall-clock time (ms) instead of
    // `START_MS + DURATION_S`, for a stream stamped in the past and sent as
    // fast as the data plane takes it.
    let deadline_ms: i64 = args
        .get(9)
        .map_or(start_ms + duration_s * 1000, |a| a.parse().unwrap());
    let period_ms = 1000 / hz;
    let ticks = duration_s * hz;
    let per_thread = (to - from).div_ceil(threads);
    let started = Instant::now();
    let workers: Vec<_> = (0..threads)
        .map(|t| {
            let (addr, metrics) = (addr.clone(), metrics.clone());
            std::thread::spawn(move || {
                let lo = from + t * per_thread;
                let hi = (lo + per_thread).min(to);
                let series: Vec<(Vec<u8>, usize, bool)> = metrics
                    .iter()
                    .flat_map(|m| (lo..hi).map(move |i| (labels(m, i), i, m.ends_with("_total"))))
                    .collect();
                let mut stream = BufReader::new(TcpStream::connect(&addr).unwrap());
                stream.get_ref().set_nodelay(true).unwrap();
                let (mut acked, mut busy, mut lag) = (0u64, 0u64, 0i64);
                for tick in 0..ticks {
                    let timestamp = start_ms + (tick + 1) * period_ms;
                    let wait = timestamp - now_ms();
                    if wait > 0 {
                        std::thread::sleep(Duration::from_millis(wait as u64));
                    }
                    // Stop at the deadline even when behind: the run measures
                    // what the data plane takes in a fixed wall-clock time.
                    if now_ms() > deadline_ms {
                        break;
                    }
                    let body = body(&series, tick, timestamp);
                    let mut backoff_ms = 10;
                    loop {
                        match post(&mut stream, &addr, &body) {
                            Ok(204) | Ok(200) => break,
                            Ok(503) => {
                                // Back off as Prometheus does (exponential, capped),
                                // so retries do not re-decode the request in a loop.
                                busy += 1;
                                std::thread::sleep(Duration::from_millis(backoff_ms));
                                backoff_ms = (backoff_ms * 2).min(1000);
                            }
                            Ok(status) => panic!("Remote Write answered {status}"),
                            Err(_) => {
                                stream = BufReader::new(TcpStream::connect(&addr).unwrap());
                            }
                        }
                    }
                    acked += series.len() as u64;
                    lag = now_ms() - timestamp;
                }
                (acked, busy, lag)
            })
        })
        .collect();
    let (mut acked, mut busy, mut lag) = (0, 0, 0);
    for worker in workers {
        let (a, b, l) = worker.join().unwrap();
        acked += a;
        busy += b;
        lag = lag.max(l);
    }
    println!(
        "{{\"acked\": {acked}, \"retries_503\": {busy}, \"elapsed_s\": {:.3}, \"final_lag_ms\": {lag}, \"finished_ms\": {}}}",
        started.elapsed().as_secs_f64(),
        now_ms()
    );
}
