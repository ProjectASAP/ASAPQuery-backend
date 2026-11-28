//! Remote Write decode cost on captured request bodies: reads every file
//! of a directory as one snappy-compressed Remote Write body (as a sender
//! posted it), then prints nanoseconds per sample of
//!
//! - `snappy`: decompressing the bodies only, into a new buffer per body
//!   and into one reused buffer;
//! - `decode`: [`decode_samples`], the write handler's decode (snappy,
//!   protobuf walk, series lookup), on one thread with its series cache
//!   warm;
//! - `hand-off`: splitting the decoded samples by ingest shard, as the
//!   engine's `try_ingest` does;
//! - `threaded`: `THREADS` threads decoding the bodies concurrently and
//!   sending each request's samples, split by shard, to `SHARDS` consumer
//!   threads that drop them, as the write handler and the ingest shards
//!   do; thread CPU time of the decoding threads per sample.
//!
//! With `DECODE_ONLY` set, only decodes the bodies `ROUNDS` times, for a
//! profiler.
//!
//! Bodies are decoded in file-name order, so series appear as they did
//! when captured.
//!
//! `cargo run --release -p data_plane --example rw_corpus_bench -- DIR [ROUNDS] [THREADS] [SHARDS]`

/// The data plane's allocator, as `main.rs` sets it.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::sync::mpsc;
use std::time::Instant;

use data_plane::remote_write::{decode_samples, Limits};
use internal_metrics::thread_cpu_ns;
use storage::store::RawSample;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = args.get(1).expect("a directory of Remote Write bodies");
    let rounds: usize = args.get(2).map_or(5, |a| a.parse().unwrap());
    let threads: usize = args.get(3).map_or(8, |a| a.parse().unwrap());
    let shards: usize = args.get(4).map_or(4, |a| a.parse().unwrap());
    let mut paths: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    paths.sort();
    let bodies: Vec<Vec<u8>> = paths.iter().map(|p| std::fs::read(p).unwrap()).collect();
    let limits = Limits::default();
    let compressed: usize = bodies.iter().map(Vec::len).sum();

    // Interns every series once, as a steady stream would.
    let samples: usize = bodies
        .iter()
        .map(|b| decode_samples(b, &limits).unwrap().len())
        .sum();
    let mut decompressed = 0;
    let mut series = 0;
    for body in &bodies {
        let bytes = snap::raw::Decoder::new().decompress_vec(body).unwrap();
        decompressed += bytes.len();
        series += count_series(&bytes);
    }
    println!(
        "{} bodies, {samples} samples, {series} TimeSeries, {:.1} MB compressed, {:.1} MB decompressed",
        bodies.len(),
        compressed as f64 / 1e6,
        decompressed as f64 / 1e6
    );
    if std::env::var_os("DECODE_ONLY").is_some() {
        // For a profiler: the decode alone.
        for _ in 0..rounds {
            for body in &bodies {
                std::hint::black_box(decode_samples(body, &limits).unwrap());
            }
        }
        return;
    }
    let per_sample = |cpu_ns: u64, rounds: usize| cpu_ns as f64 / (rounds * samples) as f64;

    let started = thread_cpu_ns();
    for _ in 0..rounds {
        for body in &bodies {
            std::hint::black_box(snap::raw::Decoder::new().decompress_vec(body).unwrap());
        }
    }
    println!(
        "snappy:   {:7.1} ns/sample (a new buffer per body)",
        per_sample(thread_cpu_ns() - started, rounds)
    );
    let mut buffer = Vec::new();
    let started = thread_cpu_ns();
    for _ in 0..rounds {
        for body in &bodies {
            let len = snap::raw::decompress_len(body).unwrap();
            if buffer.len() < len {
                buffer.resize(len, 0);
            }
            let written = snap::raw::Decoder::new()
                .decompress(body, &mut buffer)
                .unwrap();
            std::hint::black_box(&buffer[..written]);
        }
    }
    println!(
        "snappy:   {:7.1} ns/sample (one buffer, reused)",
        per_sample(thread_cpu_ns() - started, rounds)
    );

    let mut decode_ns = 0;
    let mut hand_off_ns = 0;
    for _ in 0..rounds {
        for body in &bodies {
            let started = thread_cpu_ns();
            let decoded = decode_samples(body, &limits).unwrap();
            let split = thread_cpu_ns();
            std::hint::black_box(by_shard(decoded, shards));
            let dropped = thread_cpu_ns();
            decode_ns += split - started;
            hand_off_ns += dropped - split;
        }
    }
    println!("decode:   {:7.1} ns/sample", per_sample(decode_ns, rounds));
    println!(
        "hand-off: {:7.1} ns/sample (split by {shards} shards, then dropped here)",
        per_sample(hand_off_ns, rounds)
    );

    // Threaded: as the server runs it.
    let (senders, consumers): (Vec<_>, Vec<_>) = (0..shards)
        .map(|_| {
            let (send, receive) = mpsc::sync_channel::<Vec<RawSample>>(64);
            let consumer = std::thread::spawn(move || {
                for samples in receive {
                    drop(samples);
                }
            });
            (send, consumer)
        })
        .unzip();
    let wall = Instant::now();
    let cpu_ns: u64 = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|t| {
                let (bodies, limits, senders) = (&bodies, &limits, senders.clone());
                scope.spawn(move || {
                    // Warm this thread's cache first, untimed.
                    for body in bodies.iter().skip(t).step_by(threads) {
                        decode_samples(body, limits).unwrap();
                    }
                    let started = thread_cpu_ns();
                    for _ in 0..rounds {
                        for body in bodies.iter().skip(t).step_by(threads) {
                            let decoded = decode_samples(body, limits).unwrap();
                            for (shard, samples) in
                                by_shard(decoded, senders.len()).into_iter().enumerate()
                            {
                                if !samples.is_empty() {
                                    senders[shard].send(samples).unwrap();
                                }
                            }
                        }
                    }
                    thread_cpu_ns() - started
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).sum()
    });
    drop(senders);
    for consumer in consumers {
        consumer.join().unwrap();
    }
    println!(
        "threaded: {:7.1} ns/sample decode + hand-off CPU ({threads} threads, {shards} shards), {:.2} M samples/s wall",
        per_sample(cpu_ns, rounds),
        (rounds * samples) as f64 / wall.elapsed().as_secs_f64() / 1e6
    );
}

/// Each sample's shard, as `Engine::try_ingest` splits a request.
fn by_shard(samples: Vec<RawSample>, shards: usize) -> Vec<Vec<RawSample>> {
    let room = samples.len() / shards * 5 / 4 + 1;
    let mut by_shard: Vec<Vec<RawSample>> = Vec::new();
    by_shard.resize_with(shards, || Vec::with_capacity(room));
    for sample in samples {
        by_shard[sample.labels.id as usize % shards].push(sample);
    }
    by_shard
}

/// The number of top-level field-1 entries (TimeSeries) of a WriteRequest.
fn count_series(bytes: &[u8]) -> usize {
    let mut position = 0;
    let mut count = 0;
    let varint = |position: &mut usize| {
        let mut value = 0u64;
        let mut shift = 0;
        loop {
            let byte = bytes[*position];
            *position += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                return value;
            }
            shift += 7;
        }
    };
    while position < bytes.len() {
        let tag = varint(&mut position);
        assert_eq!(tag & 7, 2, "a length-delimited field");
        let len = varint(&mut position) as usize;
        position += len;
        count += usize::from(tag >> 3 == 1);
    }
    count
}
