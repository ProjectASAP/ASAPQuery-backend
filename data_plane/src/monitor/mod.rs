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

pub mod alert;
pub mod coordinator;
pub mod epoch;
pub mod sampling_alloc;
pub mod server;

pub use alert::{global_threshold_violation, AlertSink};
pub use coordinator::{Action, Monitor, MonitorConfig};
pub use sampling_alloc::{allocate_sample_rates, epsilon_sample_floor, uniform_sample_rate};
pub use server::{MonitorCoordinator, MonitorServiceImpl};
