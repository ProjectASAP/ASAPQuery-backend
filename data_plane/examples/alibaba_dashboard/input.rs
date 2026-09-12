use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
};

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Event {
    pub time: u32,
    pub upstream: u32,
    pub downstream: u32,
    pub latency: f64,
}

pub fn cpu_seconds() -> anyhow::Result<f64> {
    let mut timestamp = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: a successful call initializes the writable timespec.
    let status =
        unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, timestamp.as_mut_ptr()) };
    if status != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    let timestamp = unsafe { timestamp.assume_init() };
    Ok(timestamp.tv_sec as f64 + timestamp.tv_nsec as f64 * 1e-9)
}

pub fn visit(
    path: &Path,
    mut consume: impl FnMut(Event) -> anyhow::Result<()>,
) -> anyhow::Result<u64> {
    let mut reader = BufReader::with_capacity(
        1024 * 1024,
        GzDecoder::new(BufReader::new(File::open(path)?)),
    );
    let mut count = 0;
    let mut prior = 0;
    loop {
        let mut bytes = [0u8; 20];
        if reader.read(&mut bytes[..1])? == 0 {
            break;
        }
        reader.read_exact(&mut bytes[1..])?;
        let event = Event {
            time: u32::from_le_bytes(bytes[0..4].try_into()?),
            upstream: u32::from_le_bytes(bytes[4..8].try_into()?),
            downstream: u32::from_le_bytes(bytes[8..12].try_into()?),
            latency: f64::from_le_bytes(bytes[12..20].try_into()?),
        };
        anyhow::ensure!(
            count == 0 || event.time >= prior,
            "replay is not chronological"
        );
        anyhow::ensure!(event.downstream != u32::MAX, "invalid downstream sentinel");
        prior = event.time;
        consume(event)?;
        count += 1;
    }
    Ok(count)
}

pub fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

/// Deterministic reservoir used only for explicitly budgeted calibration.
/// Held-out replay must never use this function or discard observations.
pub fn calibration(
    directory: &Path,
    files: usize,
    limit: usize,
    seed: u64,
) -> anyhow::Result<(Vec<Vec<Event>>, u64)> {
    anyhow::ensure!(limit > 0, "empty calibration budget");
    let mut rng = seed;
    let mut sample = Vec::with_capacity(limit);
    let mut seen = 0u64;
    for index in 0..files {
        visit(
            &directory.join(format!("observations_{index}.bin.gz")),
            |e| {
                anyhow::ensure!(
                    (e.time as usize) / 180000 == index,
                    "file interval mismatch"
                );
                seen += 1;
                if sample.len() < limit {
                    sample.push((seen, e));
                } else {
                    let slot = random(&mut rng) % seen;
                    if slot < (limit as u64) {
                        sample[slot as usize] = (seen, e);
                    }
                }
                Ok(())
            },
        )?;
    }
    // Retain a deterministic chronological replay, without sorting by key.
    sample.sort_by_key(|(ordinal, e)| (e.time, *ordinal));
    let mut panes = vec![Vec::new(); files * 3];
    for (_, e) in sample {
        panes[e.time as usize / 60000].push(e);
    }
    Ok((panes, seen))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn binary_reader_preserves_zero_latency_and_rejects_truncation() {
        let path =
            std::env::temp_dir().join(format!("alibaba-input-test-{}.gz", std::process::id()));
        let mut bytes = Vec::new();
        bytes.extend(42u32.to_le_bytes());
        bytes.extend(1u32.to_le_bytes());
        bytes.extend(2u32.to_le_bytes());
        bytes.extend(0f64.to_le_bytes());
        let mut gzip = flate2::write::GzEncoder::new(
            File::create(&path).unwrap(),
            flate2::Compression::fast(),
        );
        gzip.write_all(&bytes).unwrap();
        gzip.finish().unwrap();
        assert_eq!(
            visit(&path, |e| {
                assert_eq!(e.time, 42);
                assert_eq!(e.latency, 0.);
                Ok(())
            })
            .unwrap(),
            1
        );
        let mut gzip = flate2::write::GzEncoder::new(
            File::create(&path).unwrap(),
            flate2::Compression::fast(),
        );
        gzip.write_all(&bytes[..19]).unwrap();
        gzip.finish().unwrap();
        assert!(visit(&path, |_| Ok(())).is_err());
        std::fs::remove_file(path).unwrap();
    }
}
