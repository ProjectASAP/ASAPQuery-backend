//! Deployment cost inputs that survive outside the planner.
//!
//! * [`online`] — EMA-smoothed per-sketch cost observations fed by agent
//!   runtime samples, exposed through `GET /api/v1/cost-model`.
//! * [`tco`] — standalone cloud total-cost estimator behind `POST /api/v1/tco`.
//! * [`wire`] — wire-cost table the post-ASAP cost model prices against.

pub mod online;
pub mod tco;
pub mod wire;
