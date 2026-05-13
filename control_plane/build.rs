fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::compile_protos(&["proto/opamp.proto"], &["proto/"])?;
    // asap.runtime.v1.RuntimeSamples service — receives
    // PushExporter batches from agents. Must stay in lockstep
    // with `sketch-bench/sketch-runtime/proto/feedback.proto`.
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&["proto/feedback.proto"], &["proto/"])?;
    Ok(())
}
