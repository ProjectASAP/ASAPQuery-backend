//! Every producer of a planning snapshot must declare the one schema version
//! the backend accepts.
//!
//! The version had drifted before: two tools in `tools/o11y-execution/`
//! overwrote it with a stale literal while the backend had moved on, so a
//! snapshot built from a current template was relabelled to an old version and
//! then rejected by the backend that had just been handed it. Nothing failed
//! until a test deep in the stack did, with a message about the version rather
//! than about the producer that wrote it.
//!
//! Producers are written in Rust, Python and JSON, so they cannot share a
//! constant. They can share this test.
use control_plane::physical::compiler::WORKLOAD_SNAPSHOT_VERSION;
use std::path::{Path, PathBuf};

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("control_plane sits in the workspace")
        .to_path_buf()
}

/// Shipped snapshots a user or a tool starts from.
const EXAMPLE_SNAPSHOTS: &[&str] = &[
    "docs/examples/asapquery-planning-snapshot.json",
    "docs/examples/asapquery-compatibility-demo-snapshot.json",
];

/// Tools that accept or emit a snapshot and state the version they expect.
const TOOLS: &[&str] = &[
    "tools/o11y-execution/discover_snapshot.py",
    "tools/o11y-execution/calibrate.py",
    "tools/shared-workload/planned_run.py",
];

#[test]
fn shipped_snapshots_declare_the_supported_version() {
    let root = repository_root();
    for relative in EXAMPLE_SNAPSHOTS {
        let path = root.join(relative);
        let snapshot: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{relative}: {error}")),
        )
        .unwrap_or_else(|error| panic!("{relative}: {error}"));
        assert_eq!(
            snapshot["snapshot_version"].as_u64(),
            Some(u64::from(WORKLOAD_SNAPSHOT_VERSION)),
            "{relative} declares a version the backend does not accept"
        );
    }
}

#[test]
fn snapshot_tools_expect_the_supported_version() {
    let root = repository_root();
    let expected = format!("snapshot_version\") != {WORKLOAD_SNAPSHOT_VERSION}");
    for relative in TOOLS {
        let path = root.join(relative);
        let source =
            std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{relative}: {error}"));
        assert!(
            source.contains("snapshot_version"),
            "{relative} no longer mentions snapshot_version; \
             remove it from TOOLS or restore the check"
        );
        assert!(
            source.contains(&expected),
            "{relative} does not reject snapshots other than version \
             {WORKLOAD_SNAPSHOT_VERSION}; a producer that guesses the version is \
             how it drifted last time"
        );
        // A tool that restates the version while writing is how the literal got
        // stale: it changes content, not schema, so it must not relabel.
        assert!(
            !source.contains("[\"snapshot_version\"] ="),
            "{relative} overwrites snapshot_version; carry the input's version \
             through instead of restating it"
        );
    }
}
