//! Discovery output must be accepted by the production snapshot planner.
use control_plane::physical::compiler::BackendLocalPlanningInput;
use std::{fs, path::PathBuf, process::Command};

struct TempDirectory(PathBuf);
impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn discovered_snapshot_plans_with_observed_cadence_and_promql_history() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let temporary = TempDirectory(
        std::env::temp_dir().join(format!("asap-discovery-snapshot-{}", std::process::id())),
    );
    fs::create_dir(&temporary.0).unwrap();
    let corpus = temporary.0.join("corpus.json");
    let metrics = temporary.0.join("metrics.prom");
    let output = temporary.0.join("snapshot.json");
    fs::write(&corpus, r#"{"upstream_revision":"test","queries":[{"id":"q","query":"max_over_time(m[1m] offset 1h)","eval_timestamp_ms":120000}]}"#).unwrap();
    fs::write(&metrics, "m{job=\"test\"} 1 60\nm{job=\"test\"} 2 120\n").unwrap();
    let result = Command::new("python3")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .arg(root.join("tools/o11y-execution/discover_snapshot.py"))
        .arg("--corpus")
        .arg(&corpus)
        .arg("--metrics")
        .arg(&metrics)
        .arg("--template")
        .arg(root.join("docs/examples/asapquery-planning-snapshot.json"))
        .arg("--output")
        .arg(&output)
        .args(["--repetitions", "1"])
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let snapshot: BackendLocalPlanningInput =
        serde_json::from_str(&fs::read_to_string(&output).unwrap()).unwrap();
    assert_eq!(snapshot.physical_inputs.scrape_interval_ms, 60_000);
    let (request, _) = snapshot
        .clone()
        .into_physical_compilation_request()
        .unwrap();
    assert_eq!(request.scrape_interval_ms, Some(60_000));
    assert_eq!(request.queries[0].query_lookback_seconds, 3660);
    let roundtrip: BackendLocalPlanningInput =
        serde_json::from_value(serde_json::to_value(&snapshot).unwrap()).unwrap();
    assert_eq!(snapshot, roundtrip);
}
