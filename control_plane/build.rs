fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/opamp.proto"], &["proto/"])?;
    // `BackendPlan` wire contract — see
    // `control_plane/docs/design-backend-plan-wire-format.md`. Needs
    // `--experimental_allow_proto3_optional` for the `optional` scalar
    // fields (`WindowSpec.slide_ms`, `RetentionPolicy.num_aggregates_to_retain`).
    prost_build::Config::new()
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/backend_plan.proto"], &["proto/"])?;
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
