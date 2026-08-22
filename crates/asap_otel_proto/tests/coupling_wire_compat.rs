//! Cross-language wire-compat gate for the dynamic coordinator<->sampling
//! coupling fields: bytes are emitted by the Go edge
//! (asap-precompute-go/monitor/grpcclient/monitorpb TestCouplingFixture) and
//! MUST decode field-for-field here via prost — proving SlackGrant.sample_p
//! (coord->edge) and MonitorReport.rate (edge->coord) cross the wire.
use asap_otel_proto::monitor::v1::{coord_to_edge, edge_to_coord, CoordToEdge, EdgeToCoord};
use prost::Message;

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn go_grant_sample_p_decodes_in_rust() {
    let bytes = unhex("0a1d082a1807210000000000000c402880d095ffbc3131000000000000d03f");
    let env = CoordToEdge::decode(&bytes[..]).expect("decode CoordToEdge");
    match env.msg.expect("msg") {
        coord_to_edge::Msg::Grant(g) => {
            assert_eq!(g.agg_id, 42);
            assert_eq!(g.round, 7);
            assert!((g.sample_p - 0.25).abs() < 1e-12, "sample_p={}", g.sample_p);
        }
        other => panic!("expected Grant, got {:?}", other),
    }
}

#[test]
fn go_report_rate_decodes_in_rust() {
    let bytes = unhex("12200a06656467652d37102a2900000000004a934030033809410000000000c09240");
    let env = EdgeToCoord::decode(&bytes[..]).expect("decode EdgeToCoord");
    match env.msg.expect("msg") {
        edge_to_coord::Msg::Report(r) => {
            assert_eq!(r.edge_id, "edge-7");
            assert_eq!(r.agg_id, 42);
            assert!((r.rate - 1200.0).abs() < 1e-9, "rate={}", r.rate);
        }
        other => panic!("expected Report, got {:?}", other),
    }
}

#[test]
fn rust_grant_sample_p_emit_for_go() {
    // The REAL coord->edge direction: Rust(prost) encodes a grant; Go decodes it.
    use asap_otel_proto::monitor::v1::SlackGrant;
    let env = CoordToEdge {
        msg: Some(coord_to_edge::Msg::Grant(SlackGrant {
            agg_id: 99,
            round: 4,
            local_slack: 1.0,
            window_start_ms: 1_700_000_000_000,
            sample_p: 0.3,
            ..Default::default()
        })),
    };
    let mut buf = Vec::new();
    env.encode(&mut buf).unwrap();
    println!(
        "RUST_GRANT_HEX={}",
        buf.iter().map(|b| format!("{:02x}", b)).collect::<String>()
    );
}
