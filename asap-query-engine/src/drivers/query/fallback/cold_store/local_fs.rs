//! Local-filesystem [`ColdStore`] impl.
//!
//! Walks the same directory layout an S3 bucket would hold, so a
//! future `S3ColdStore` can drop in without the adapter caring.
//! Used today by tests and by the single-node evaluation
//! deployment.
//!
//! Concurrency: scans read each file via `tokio::fs::read`, so
//! multiple overlapping scans can progress in parallel without
//! serialization.

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use super::format::{hour_prefixes, parse_jsonl};
use super::{ColdStore, ColdStoreError, RawSample};

/// Cold store backed by a local directory tree.
pub struct LocalFsColdStore {
    root: PathBuf,
}

impl LocalFsColdStore {
    /// Create a store rooted at `root`. The directory must exist;
    /// producing raw dumps is the exporter's job, not the reader's.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// List the JSONL parts inside `prefix_dir`, sorted by file name
    /// so scans over the same input are deterministic.
    async fn list_parts(&self, prefix_dir: &Path) -> Result<Vec<PathBuf>, ColdStoreError> {
        let mut out = Vec::new();
        let mut rd = match tokio::fs::read_dir(prefix_dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if path
                .extension()
                .and_then(|s| s.to_str())
                .is_some_and(|ext| ext == "jsonl")
            {
                out.push(path);
            }
        }
        out.sort();
        Ok(out)
    }
}

#[async_trait]
impl ColdStore for LocalFsColdStore {
    async fn scan(
        &self,
        metric: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Vec<RawSample>, ColdStoreError> {
        let mut out = Vec::new();
        for prefix in hour_prefixes(metric, start_ms, end_ms) {
            let dir = self.root.join(&prefix);
            for part in self.list_parts(&dir).await? {
                let bytes = tokio::fs::read(&part).await?;
                let mut samples = parse_jsonl(&bytes, start_ms, end_ms)?;
                out.append(&mut samples);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn hour_ms(year: i32, month: u32, day: u32, hour: u32) -> i64 {
        Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
            .unwrap()
            .timestamp_millis()
    }

    async fn write_part(root: &Path, rel: &str, lines: &[RawSample]) {
        let dir = root.join(rel);
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let mut buf = String::new();
        for s in lines {
            buf.push_str(&serde_json::to_string(s).unwrap());
            buf.push('\n');
        }
        tokio::fs::write(dir.join("part-000001.jsonl"), buf)
            .await
            .unwrap();
    }

    fn sample(ts_ms: i64, labels: &[(&str, &str)], value: f64) -> RawSample {
        RawSample {
            ts_ms,
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect::<BTreeMap<_, _>>(),
            value,
        }
    }

    #[tokio::test]
    async fn scan_returns_samples_in_range() {
        let tmp = TempDir::new().unwrap();
        let base = hour_ms(2026, 4, 21, 8);
        write_part(
            tmp.path(),
            "raw/http_requests_total/2026/04/21/08/",
            &[
                sample(base + 1_000, &[("zone", "a")], 1.0),
                sample(base + 2_000, &[("zone", "b")], 2.0),
            ],
        )
        .await;

        let s = LocalFsColdStore::new(tmp.path());
        let got = s
            .scan("http_requests_total", base, base + 60_000)
            .await
            .unwrap();
        assert_eq!(got.len(), 2);
    }

    #[tokio::test]
    async fn scan_prunes_by_time() {
        let tmp = TempDir::new().unwrap();
        let base = hour_ms(2026, 4, 21, 8);
        write_part(
            tmp.path(),
            "raw/m/2026/04/21/08/",
            &[
                sample(base + 1_000, &[], 1.0),
                sample(base + 60_000, &[], 2.0),
            ],
        )
        .await;

        let s = LocalFsColdStore::new(tmp.path());
        let got = s.scan("m", base, base + 30_000).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value, 1.0);
    }

    #[tokio::test]
    async fn scan_missing_prefix_is_empty_not_error() {
        let tmp = TempDir::new().unwrap();
        let s = LocalFsColdStore::new(tmp.path());
        let got = s.scan("never_written", 0, 1).await.unwrap();
        assert!(got.is_empty());
    }

    #[tokio::test]
    async fn scan_spans_hour_boundary() {
        let tmp = TempDir::new().unwrap();
        let h8 = hour_ms(2026, 4, 21, 8);
        let h9 = hour_ms(2026, 4, 21, 9);
        write_part(
            tmp.path(),
            "raw/m/2026/04/21/08/",
            &[sample(h8 + 3_599_000, &[], 1.0)],
        )
        .await;
        write_part(
            tmp.path(),
            "raw/m/2026/04/21/09/",
            &[sample(h9 + 1_000, &[], 2.0)],
        )
        .await;

        let s = LocalFsColdStore::new(tmp.path());
        let got = s.scan("m", h8 + 3_598_000, h9 + 2_000).await.unwrap();
        assert_eq!(got.len(), 2);
    }
}
