use std::path::{Path, PathBuf};
use std::process::Command;

fn git_output(repo: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn git_path(repo: &Path, name: &str) -> Option<PathBuf> {
    let path = PathBuf::from(git_output(repo, &["rev-parse", "--git-path", name])?);
    let path = if path.is_absolute() {
        path
    } else {
        repo.join(path)
    };
    Some(path.canonicalize().unwrap_or(path))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let repo = manifest_dir.parent().unwrap_or(&manifest_dir);
    let lock_path = repo.join("Cargo.lock");
    println!("cargo:rerun-if-changed={}", lock_path.display());
    let lock: toml::Value = std::fs::read_to_string(&lock_path)?.parse()?;
    let planner_source = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .into_iter()
        .flatten()
        .find(|package| package.get("name").and_then(toml::Value::as_str) == Some("asap-types"))
        .and_then(|package| package.get("source"))
        .and_then(toml::Value::as_str)
        .ok_or("Cargo.lock has no resolved ASAPPlanner types source")?;
    let planner_revision = planner_source
        .rsplit_once('#')
        .filter(|(_, revision)| {
            revision.len() == 40 && revision.bytes().all(|b| b.is_ascii_hexdigit())
        })
        .map(|(_, revision)| revision)
        .ok_or("ASAPPlanner source must resolve to a Git commit")?;
    println!("cargo:rustc-env=ASAPPLANNER_REVISION={planner_revision}");
    let revision = std::env::var("ASAPQUERY_BACKEND_REVISION")
        .ok()
        .or_else(|| git_output(repo, &["rev-parse", "HEAD"]))
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=ASAPQUERY_BACKEND_REVISION={revision}");
    println!("cargo:rerun-if-env-changed=ASAPQUERY_BACKEND_REVISION");
    if let Some(head) = git_path(repo, "HEAD") {
        println!("cargo:rerun-if-changed={}", head.display());
    }
    if let Some(symbolic_ref) = git_output(repo, &["symbolic-ref", "-q", "HEAD"]) {
        if let Some(reference) = git_path(repo, &symbolic_ref) {
            println!("cargo:rerun-if-changed={}", reference.display());
        }
    }
    prost_build::compile_protos(&["proto/opamp.proto"], &["proto/"])?;
    // asap.runtime.v1.RuntimeSamples service — receives
    // PushExporter batches from agents. Must stay in lockstep
    // with `sketch-bench/sketch-runtime/proto/feedback.proto`.
    tonic_build::configure()
        .build_server(true)
        // Keep the generated client available for black-box process E2E tests
        // and for downstream agents that share this crate's wire contract.
        .build_client(true)
        .compile_protos(&["proto/feedback.proto"], &["proto/"])?;
    Ok(())
}
