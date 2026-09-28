# Workload statistics validation

This test PR isolates the consumer contract from ranking and sketch accuracy.
`control_plane/tests/workload_statistics.rs` verifies units, retained-series
cardinality with zero arrivals, expired/future/missing observations, and bounded
population accounting. Its controlled observations are not live telemetry.

The intended measurement sources remain remote_write accepted-sample counters,
query-tracker executions, and Prometheus series observations. Rates need explicit
observation windows and counter-reset handling; series scope must match the input
computation. Query cadence and ingestion cadence are different quantities.

The existing discovery replay adapter reports a derived finite-replay rate and
declared query recurrence. Those are not live ingestion/query measurements. This
PR does not claim the three production collectors are fully wired together.
Level 3 must preserve original trace timing, observation provenance and scope;
missing observations make the real-evidence run incomplete.
