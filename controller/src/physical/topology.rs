//! L5 deployment-topology descriptors.
//!
//! Per `controller/docs/design.md` §5 / §6 `core::physical::topology`:
//! the topology declares which stages exist (3-stage / 1-stage /
//! 0-stage). Stages are roles, not instances — see also
//! [`crate::physical::colored_dag::stage_id`] for the underlying
//! `StageId` + `Topology` enums.
//!
//! Refactor 2026-05: the `StageId` + `Topology` types currently live in
//! [`super::colored_dag::stage_id`] (phase E delivered the typed-L5
//! framework there). This module re-exports them so the design.md §5
//! `core::physical::topology` entry point exists in code; future
//! deployment-topology descriptor extensions land here without
//! disturbing the colored-DAG framework.

pub use super::colored_dag::stage_id::{StageId, Topology};
