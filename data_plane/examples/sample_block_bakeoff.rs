//! Sample-block bake-off: bytes per sample and CPU per sample of the data
//! plane's sample-block codec (`data_plane::sample_blocks`) on five data
//! shapes, 1,000 series × 3,600 samples (one hour at 1 s), through the
//! executor's `SampleBlockCodec` trait as runs use it. Prints a Markdown
//! table: the bytes of codec 0 alone, of codec 1 alone (when every block is
//! eligible), of the sealed blocks (the shorter codec per block) and of the
//! uncompressed reference codec, and the time to append, seal and decode.
//!
//! `cargo run --release -p data_plane --example sample_block_bakeoff`

/// The data plane's allocator, as `main.rs` sets it.
#[global_allocator]
static ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::time::Instant;

use asap_executor::summary_kernels::{PlainSampleBlockCodec, SampleBlockCodec};
use data_plane::sample_blocks::{codec, CompressedSamples, SampleCodec};

const SERIES: usize = 1_000;
const SAMPLES: i64 = 3_600;
const INTERVAL_MS: i64 = 1_000;

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Uniform in (0, 1].
fn uniform(state: &mut u64) -> f64 {
    ((splitmix64(state) >> 11) + 1) as f64 / (1u64 << 53) as f64
}

/// Standard normal (Box-Muller).
fn normal(state: &mut u64) -> f64 {
    let (u, v) = (uniform(state), uniform(state));
    (-2.0 * u.ln()).sqrt() * (2.0 * std::f64::consts::PI * v).cos()
}

/// Zipf(s) over 1..=n by inverse CDF.
struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(s: f64, n: usize) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut total = 0.0;
        for k in 1..=n {
            total += (k as f64).powf(-s);
            cdf.push(total);
        }
        for c in &mut cdf {
            *c /= total;
        }
        Self { cdf }
    }

    fn sample(&self, state: &mut u64) -> u64 {
        let u = uniform(state);
        self.cdf.partition_point(|&c| c < u) as u64 + 1
    }
}

/// One shape's samples: `SERIES` series of `SAMPLES` samples each.
fn shape(name: &str, zipf: &Zipf) -> Vec<Vec<(i64, f64)>> {
    let mut state = 862u64;
    (0..SERIES)
        .map(|series| {
            let scale = 1.0 / ((series + 1) as f64).powf(1.1);
            let mut counter = 1_000.0 * series as f64;
            (0..SAMPLES)
                .map(|i| {
                    let value = match name {
                        // The current benchmark generator's gauge: i.i.d.
                        // rank-scaled Pareto(2) doubles.
                        "random doubles" => scale * uniform(&mut state).powf(-0.5),
                        // Log-normal latencies in ms (median 20 ms),
                        // rounded to two decimals.
                        "two-decimal log-normal latency" => {
                            let ms = (20f64.ln() + 0.8 * normal(&mut state)).exp();
                            (ms * 100.0).round() / 100.0
                        }
                        "integer counter" => {
                            counter += (1 + splitmix64(&mut state) % 5) as f64;
                            counter
                        }
                        "Zipf(1.1, 1e6) integer gauge" => zipf.sample(&mut state) as f64,
                        "Zipf(1.1, 1e6) counter increments" => {
                            counter += zipf.sample(&mut state) as f64;
                            counter
                        }
                        _ => unreachable!("unknown shape {name}"),
                    };
                    (i * INTERVAL_MS, value)
                })
                .collect()
        })
        .collect()
}

fn per_sample(nanos: u128) -> f64 {
    nanos as f64 / (SERIES as f64 * SAMPLES as f64)
}

fn bytes_per_sample(bytes: usize) -> f64 {
    bytes as f64 / (SERIES as f64 * SAMPLES as f64)
}

fn main() {
    let zipf = Zipf::new(1.1, 1_000_000);
    let codec = codec();
    let plain = PlainSampleBlockCodec;
    println!(
        "| Data shape | codec 0 B/sample | codec 1 B/sample | sealed B/sample (blocks as codec 1) | plain B/sample | append ns | seal ns | decode ns |"
    );
    println!("| --- | --- | --- | --- | --- | --- | --- | --- |");
    for name in [
        "random doubles",
        "two-decimal log-normal latency",
        "integer counter",
        "Zipf(1.1, 1e6) integer gauge",
        "Zipf(1.1, 1e6) counter increments",
    ] {
        let data = shape(name, &zipf);

        let start = Instant::now();
        let open: Vec<_> = data
            .iter()
            .map(|samples| {
                let mut block = codec.new_block();
                for &(t, v) in samples {
                    block.append(t, v);
                }
                block
            })
            .collect();
        let append_ns = per_sample(start.elapsed().as_nanos());

        let start = Instant::now();
        let sealed: Vec<Vec<u8>> = open.iter().map(|block| block.seal()).collect();
        let seal_ns = per_sample(start.elapsed().as_nanos());

        let start = Instant::now();
        let mut checksum = 0.0;
        for block in &sealed {
            for (_, v) in codec.decode(block).expect("a block the codec wrote") {
                checksum += v;
            }
        }
        let decode_ns = per_sample(start.elapsed().as_nanos());
        std::hint::black_box(checksum);

        let blocks: Vec<CompressedSamples> = sealed
            .iter()
            .map(|bytes| CompressedSamples::from_bytes(bytes).unwrap())
            .collect();
        let as_codec = |c: SampleCodec| -> Option<usize> {
            blocks
                .iter()
                .map(|b| b.encoded_with(c).map(|e| e.encoded_bytes()))
                .sum()
        };
        let codec_1_blocks = blocks
            .iter()
            .filter(|b| b.codec() == SampleCodec::DecimalZstd)
            .count();
        let plain_bytes: usize = data
            .iter()
            .map(|samples| {
                let mut block = plain.new_block();
                for &(t, v) in samples {
                    block.append(t, v);
                }
                block.seal().len()
            })
            .sum();
        let codec_1 = as_codec(SampleCodec::DecimalZstd).map_or("not eligible".to_string(), |b| {
            format!("{:.2}", bytes_per_sample(b))
        });
        println!(
            "| {name} | {:.2} | {codec_1} | {:.2} ({codec_1_blocks}/{SERIES}) | {:.2} | {append_ns:.1} | {seal_ns:.1} | {decode_ns:.1} |",
            bytes_per_sample(as_codec(SampleCodec::Gorilla).unwrap()),
            bytes_per_sample(sealed.iter().map(Vec::len).sum()),
            bytes_per_sample(plain_bytes),
        );
    }
}
