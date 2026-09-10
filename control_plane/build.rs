fn main() -> Result<(), Box<dyn std::error::Error>> {
    let revision = std::env::var("ASAPQUERY_BACKEND_REVISION")
        .ok()
        .or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "HEAD"])
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|value| value.trim().to_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=ASAPQUERY_BACKEND_REVISION={revision}");
    println!("cargo:rerun-if-env-changed=ASAPQUERY_BACKEND_REVISION");
    println!("cargo:rerun-if-changed=../.git/HEAD");
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
