//! Continuous distributed monitoring (CDM) coordinator — the backend side of
//! Discipline B from ASAPCollector
//! docs/continuous-monitoring-tumbling-cost-analysis.md.
//!
//! Edges run the slack-countdown protocol locally and stream reports over the
//! bidirectional `MonitorService` gRPC (see `monitor.proto`); this module hosts
//! that service, maintains the per-monitor global state machine, and fires an
//! alert through the control-plane violation sink when a global aggregate
//! crosses its threshold τ.
//!
//! Layout:
//!   - [`coordinator`] — the pure, unit-tested CMY round/slack state machine.
//!   - [`epoch`]       — tumbling-epoch alignment (matches the edge formula).
//!   - [`alert`]       — alert egress via the control-plane `Violation` sink.
//!   - [`server`]      — the tonic bidi-streaming `MonitorService` server.
//!   - [`sampling_alloc`] — the coordinated update-sampling law (ε-floor).
//!   - [`f2`]          — whole-sketch L2/F2 *threshold* monitor (Count-Sketch +
//!     geometric safe-zone). NOTE: F2 here is a MONITORED quantity (alert), not a
//!     sampling driver — see the sampling law below.
//!
//! # Two orthogonal axes — DON'T conflate them
//!
//! 1. **What to monitor / alert on** (the `functional`): `sum`, `cms_point`
//!    (a declared point `f(x)`), or `f2` (whole-sketch L2). This sets the
//!    THRESHOLD `g vs τ` and what `known_value` means. Identity is `(agg_id,key)`
//!    — `cms_point` carries a key; `sum`/`f2` are whole-stream (`key=""`).
//!
//! 2. **How hard to sample** (the per-edge `p_i`): a SINGLE law, the whole-sketch
//!    **ε-floor** `p_i = 1/(1+ε²·rate_i)` (see [`sampling_alloc`]). It depends
//!    ONLY on each edge's rate, NEVER on the monitored functional — because the
//!    sampling protects a SKETCH, whose accuracy is bounded by the stream norm
//!    (rate/L2), not by any single key. A per-key `√(f/rate)` allocation is NOT a
//!    valid sketch-sampling regime (it needs exact-counting outside the sketch),
//!    so it has been retired. Applies to additive sketches — Count-Min, Count-
//!    Sketch, DDSketch, KLL, Sum — but NOT HLL (sampling biases cardinality).

pub mod alert;
pub mod coordinator;
pub mod epoch;
pub mod f2;
pub mod sampling_alloc;
pub mod server;

pub use alert::{global_threshold_violation, AlertSink};
pub use coordinator::{Action, Monitor, MonitorConfig};
pub use sampling_alloc::epsilon_sample_floor;
pub use server::{MonitorCoordinator, MonitorServiceImpl};
