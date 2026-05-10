fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = "proto";
    let proto_files = [
        "proto/opentelemetry/proto/common/v1/common.proto",
        "proto/opentelemetry/proto/resource/v1/resource.proto",
        "proto/opentelemetry/proto/metrics/v1/metrics.proto",
        "proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto",
        // sketchlib.v1 delta messages — vendored locally because
        // the upstream asap_sketchlib crate only exposes state
        // types today, not delta types. See the .proto file
        // headers for the sketchlib-go source of truth.
        "proto/sketchlib_delta/ddsketch_delta.proto",
        "proto/sketchlib_delta/hll_delta.proto",
        "proto/sketchlib_delta/countsketch_delta.proto",
        "proto/sketchlib_delta/countminsketch_delta.proto",
    ];
    for f in &proto_files {
        println!("cargo:rerun-if-changed={f}");
    }
    println!("cargo:rerun-if-changed=build.rs");

    // Use the vendored protoc binary so the build is independent of the
    // host's system protoc version. Many distros still ship protoc < 3.15
    // which rejects proto3 `optional` keyword used in opentelemetry-proto.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile_protos(&proto_files, &[proto_root])?;
    Ok(())
}
