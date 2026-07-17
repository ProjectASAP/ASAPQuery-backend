//! Coordinated update-sampling — the backend side of what used to be called
//! "Discipline B" in ASAPCollector
//! docs/continuous-monitoring-tumbling-cost-analysis.md.
//!
//! Global-threshold ALERTING (the CMY slack-countdown: register → grant
//! `(slack, sample_p)` → countdown → report → alert) is RETIRED as of the
//! 2026-07 insert-time-GOS redesign — see the `coordinator` module doc
//! comment for the full rationale. An edge/collector must never be the thing
//! that fires an alert; that decision belongs entirely to query-time reads of
//! the backend's synced sketch state.
//!
//! What's left: edges report their per-window observation RATE on their own
//! periodic cadence (decoupled from any value/slack threshold), and this
//! module answers each one with its coordinated-sampling grant — the
//! whole-sketch ε-floor `p_i = 1/(1+ε²·rate_i)`, a pure per-edge function with
//! no cross-edge coordination or global state involved.
//!
//! Layout:
//!   - [`coordinator`] — the pure, unit-tested per-edge sampling state machine.
//!   - [`epoch`]       — tumbling-epoch alignment (matches the edge formula).
//!   - [`server`]      — the tonic bidi-streaming `MonitorService` server.
//!   - [`sampling_alloc`] — the coordinated update-sampling law (ε-floor).
//!   - [`f2`]          — whole-sketch L2/F2 *threshold* monitor (Count-Sketch +
//!     geometric safe-zone). NOTE: F2 here is a MONITORED quantity (alert), not a
//!     sampling driver — see the sampling law below.
//!
//! **How hard to sample** (the per-edge `p_i`): a SINGLE law, the whole-sketch
//! **ε-floor** `p_i = 1/(1+ε²·rate_i)` (see [`sampling_alloc`]). It depends
//! ONLY on each edge's rate, NEVER on any monitored functional/key — because
//! the sampling protects a SKETCH, whose accuracy is bounded by the stream
//! norm (rate/L2), not by any single key. Applies to additive sketches —
//! Count-Min, Count-Sketch, DDSketch, KLL, Sum — but NOT HLL (sampling biases
//! cardinality).

pub mod coordinator;
pub mod epoch;
pub mod f2;
pub mod sampling_alloc;
pub mod server;

pub use coordinator::{Action, Functional, Monitor, MonitorConfig};
pub use sampling_alloc::epsilon_sample_floor;
pub use server::{MonitorCoordinator, MonitorServiceImpl};
