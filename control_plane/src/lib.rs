//! Backend control-plane library and standalone service.
//!
//! ASAPPlanner owns canonical semantic IR and legal logical selection. This
//! crate adapts workload/evidence inputs, compiles physical candidates, checks
//! provider quotes, and publishes one consistent catalog-backed generation.
//! Shared execution and installation contracts live in `asap_types`.
//!
//! `physical::compiler`, `physical::workload_cost`, and `clickhouse` are the
//! current compilation paths. Metric stage emission consumes the same canonical
//! workload model through a registration adapter. Public modules are integration APIs,
//! not an independent wire schema or a second semantic planner.

#![allow(
    clippy::collapsible_match,
    clippy::doc_lazy_continuation,
    clippy::doc_overindented_list_items,
    clippy::field_reassign_with_default,
    clippy::large_enum_variant,
    clippy::map_identity,
    clippy::result_large_err,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::vec_init_then_push
)]

pub mod accuracy;
pub mod backend_client;
pub mod clickhouse;
pub mod emit;
pub mod epsilon_alloc;
pub mod metrics_exposer;
pub mod opamp;
pub mod physical;
pub mod planner_selection;
pub mod query_parser;
pub mod query_plan;
pub mod runtime_samples;
pub mod types;
pub mod workload;

/// Crate-wide test-only utilities for serialising access to process-global
/// state (environment variables).
///
/// `cargo test` runs `#[test]`/`#[tokio::test]` functions on multiple
/// threads within a SINGLE process, and `std::env::set_var` /
/// `std::env::remove_var` mutate the one process-global environment table.
/// Those C `setenv`/`unsetenv` calls are not thread-safe — concurrent
/// invocations (even on different keys) are documented as unsound and can
/// corrupt the table — and any test that READS an env var via
/// `std::env::var` can transiently observe a value another test set
/// mid-emit. Both classes of failure are non-deterministic and surface as
/// flaky test results.
///
/// Every unit test in this crate that reads OR writes a process-global env
/// var (`ASAP_EDGE_FUSED`, `ASAP_AGENT_MEMORY_LIMIT_MIB`,
/// `USE_TYPED_STAGE_SPLIT`, …) must serialise behind the SINGLE
/// [`env_lock()`] below — one shared lock so no two such tests ever run
/// concurrently, regardless of which module they live in. Tests that
/// mutate env should use [`EnvVarGuard`] so the prior value is restored on
/// drop (panic-safe), preventing state from leaking between tests.
#[cfg(test)]
pub(crate) mod test_support {
    use std::rc::Rc;

    use planner_types::pre_asap::{CompareOpKind, Predicate, QueryExpr, ScalarValue, Schema};

    pub(crate) fn label_eq_predicate(
        label: &str,
        value: &str,
        schema: &Schema,
    ) -> Option<Predicate> {
        let column = schema.column_id(label)?;
        Some(Predicate(Rc::new(QueryExpr::Compare {
            left: Rc::new(QueryExpr::Column(column)),
            op: CompareOpKind::Eq,
            right: Rc::new(QueryExpr::Literal(ScalarValue::Utf8(value.to_string()))),
        })))
    }
}
