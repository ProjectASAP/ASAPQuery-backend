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
//!   - [`f2`]          — distributed L2/F2 (whole-sketch) monitor + geometric layer.
//!
//! # Which monitor for which job — the decision rule
//!
//! The choice between a **whole-sketch** monitor ([`f2`]) and a **single-point**
//! monitor (`cms_point`, the `(agg_id, key)` path in [`coordinator`]/[`server`])
//! is NOT a style preference — it is forced by **whether the queried point is
//! known when you sample**:
//!
//! * **Query point UNKNOWN a priori** (ad-hoc point queries — you can't predict
//!   which `x` a user will ask). A point estimate's error is bounded by the
//!   *sketch norm*, not by any single counter:
//!   ```text
//!     Count-Min :   f̂(x) ≤ f(x) + ε·‖f‖₁
//!     Count-Sketch: |f̂(x) − f(x)| ≤ ε·‖f‖₂
//!   ```
//!   So to keep EVERY future `x` within ε you must bound the *whole-sketch norm*
//!   (`‖f‖₁` resp. `F2 = ‖f‖₂²`). That is **ONE CDM per sketch**, identity =
//!   **`agg_id` (no key)**, one per-edge admission `p_i ∝ √(‖f_i‖ / rate_i)`
//!   driven by the edge's *whole local norm* `‖f_i‖` — see
//!   [`f2::DistributedF2Monitor`] (Count-Sketch / L2). This is the right tool for
//!   the warm-tier sketch that serves arbitrary PromQL point queries.
//!
//! * **Query point PRE-DECLARED** (a standing threshold alert on a *specific*
//!   `x` the control plane named upfront — e.g. "alert when freq(endpoint=/api/x)
//!   ≥ τ"). Only then do you know `x`, and only then is it worth tracking/
//!   sampling for that one point: `cms_point`, identity = **`(agg_id, key)`**,
//!   per-edge `f_i` = that key's local frequency. The `key` is a *declared*
//!   monitoring point, never an ad-hoc query — do NOT reach for `cms_point` to
//!   back a general query surface (it keeps only one point accurate; every other
//!   `x` gets no accuracy guarantee).
//!
//! | use | point known? | monitor | identity | per-edge sampling signal |
//! |-----|--------------|---------|----------|--------------------------|
//! | serve arbitrary point queries | no  | [`f2`] whole-sketch (L1/L2) | `agg_id` | `‖f_i‖` |
//! | declared threshold alert on one `x` | yes | `cms_point` | `(agg_id, key)` | that key's `f_i` |
//!
//! Both share the CDM principle — *communicate only when a local condition
//! breaks*. Whole-sketch monitoring is that idea applied to the entire counter
//! vector (cf. OctoSketch, NSDI'24: per-counter change thresholds that jointly
//! hold one whole-sketch ε budget) rather than to a single scalar function
//! (Cormode–Muthukrishnan–Yi functional monitoring). The geometric safe-zone in
//! [`f2`] is the whole-sketch local condition; the slack countdown in
//! [`coordinator`] is the single-point one.

pub mod alert;
pub mod coordinator;
pub mod epoch;
pub mod f2;
pub mod sampling_alloc;
pub mod server;

pub use alert::{global_threshold_violation, AlertSink};
pub use coordinator::{Action, Monitor, MonitorConfig};
pub use sampling_alloc::{allocate_sample_rates, epsilon_sample_floor, uniform_sample_rate};
pub use server::{MonitorCoordinator, MonitorServiceImpl};
