//! Validate the replay adapter's wire format with the production decoding libraries.
use data_plane::drivers::ingest::prometheus_remote_write::WriteRequest;
use prost::Message;

// The Python adapter encodes x{job="a"} 2 at 1.234 OpenMetrics seconds.
#[test]
fn replay_wire_preserves_labels_value_and_millisecond_timestamp() {
    let hex =
        "29a00a270a0d0a085f5f6e616d655f5f1201780a080a036a6f62120161120c09000000000000004010d209";
    let bytes: Vec<_> = (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect();
    let protobuf = snap::raw::Decoder::new().decompress_vec(&bytes).unwrap();
    let write = WriteRequest::decode(protobuf.as_slice()).unwrap();
    assert_eq!(write.timeseries.len(), 1);
    let series = &write.timeseries[0];
    assert_eq!(
        series
            .labels
            .iter()
            .map(|l| (l.name.as_str(), l.value.as_str()))
            .collect::<Vec<_>>(),
        vec![("__name__", "x"), ("job", "a")]
    );
    assert_eq!(series.samples.len(), 1);
    assert_eq!(series.samples[0].value, 2.0);
    assert_eq!(series.samples[0].timestamp, 1234);
}
