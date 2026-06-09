//! Cross-language Phase-1 gate: decode the wire bytes produced by the Go edge
//! (asap-precompute-go/monitor/grpcclient/monitorpb interop_test.go) and assert
//! field-for-field equality. Proves the vendored Go copy and the canonical Rust
//! proto agree on field numbers / encoding.
use asap_otel_proto::monitor::v1::{edge_to_coord::Msg, EdgeToCoord};
use prost::Message;

#[test]
fn decodes_go_fixture() {
    // FIXTURE_HEX emitted by the Go TestInteropFixture.
    let hex = "122c0a06656467652d37102a1a0c7376633d636865636b6f75742080d095ffbc312900000000004a934030033809";
    let bytes: Vec<u8> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();

    let env = EdgeToCoord::decode(&*bytes).expect("decode");
    let report = match env.msg.expect("msg") {
        Msg::Report(r) => r,
        other => panic!("expected report, got {other:?}"),
    };
    assert_eq!(report.edge_id, "edge-7");
    assert_eq!(report.agg_id, 42);
    assert_eq!(report.key, b"svc=checkout");
    assert_eq!(report.window_start_ms, 1_700_000_000_000);
    assert_eq!(report.local_value, 1234.5);
    assert_eq!(report.round, 3);
    assert_eq!(report.seq, 9);
}
