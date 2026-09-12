//! Control plane crate — library surface for the ASAPQuery-backend host.
//!
//! Refactor-2026-05 (Phase 9 / `refactor/controller-layered-cleanup`):
//! the controller previously ran as a standalone binary with its own
//! OpAMP server and HTTP API. After the controller crate moved into
//! ASAPQuery-backend, the same modules are exposed as a Rust library so
//! `asap-query-engine` can call them in-process — capability mapping,
//! plan emission, OpAMP push from the backend host. This `lib.rs`
//! declares the public module surface; the existing `main.rs` continues
//! to provide the standalone binary entrypoint for any deployments that
//! still want to run the control plane out-of-process.
//!
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

//! ## 2026-05 layered-cleanup refactor — old → new module mapping
//!
//! The internal module layout was restructured to mirror
//! `control_plane/docs/design.md` §5 target layout without splitting into
//! multiple crates:
//!
//! | Old path | New path |
//! |---|---|
//! | `controller/src/algebra/expr.rs` | `controller/src/intent_algebra/relational.rs` |
//! | `controller/src/algebra/lower.rs` | `controller/src/intent_algebra/legacy_lower.rs` |
//! | `controller/src/algebra/directory.rs` | `controller/src/physical/sketch_catalog.rs` |
//! | `controller/src/algebra/physical.rs` | `controller/src/physical/planner.rs` |
//! | `controller/src/algebra/allocator.rs` | `controller/src/physical/allocator.rs` |
//! | `controller/src/algebra/plan.rs` | `controller/src/physical/plan.rs` |
//! | `controller/src/algebra/optimizer.rs` | `controller/src/optimizer/engine.rs` |
//! | `controller/src/planner/cost_model.rs` | `controller/src/optimizer/cost/mod.rs` |
//! | `controller/src/planner/{delta,online}_cost_model.rs` | `controller/src/optimizer/cost/{delta,online}.rs` |
//! | `controller/src/planner/{pareto,tco,wire_cost}.rs` | `controller/src/optimizer/cost/{pareto,tco,wire}.rs` |
//! | `controller/src/planner/rules.rs` | `controller/src/optimizer/rules/mod.rs` |
//! | `controller/src/planner/baseline_planner.rs` | `controller/src/optimizer/baseline.rs` |
//! | `controller/src/planner/stage_split.rs` | `controller/src/physical/stage_split.rs` |
//! | `controller/src/analyzer.rs` | `controller/src/pipeline.rs` |
//! | `controller/src/stage_split/` | `controller/src/physical/colored_dag/` |
//! | `controller/src/query_language/` | `controller/src/query_parser/language/` |
//! | `controller/src/config/workloads.rs` | `controller/src/workload.rs` |
//! | `controller/src/config/{stage_config*,agent,backend,asapquery_backend,precompute}.rs` | `controller/src/emit/{...}.rs` |
//!
//! Public modules to consume from `asap-query-engine`:
//! - `intent_algebra` — `AggIntent` + `QueryExpr` DAG (canonical L3 IR;
//!   `relational` carries the L2 relational IR the parsers emit and
//!   `lower` lowers it to the canonical L3 types).
//! - `query_parser` — L1 entry point: `parse_query_expr_canonical`/`parse_query`
//!   call `asap_frontend_promql::lower_promql` directly (no local parser
//!   since design-target-architecture.md Part B; SQL not yet adopted).
//! - `physical` — L5 framework (allocator, planner, plan, sketch_catalog,
//!   colored_dag, stage_split, topology).
//! - `optimizer` — L4 rule engine + cost model traits/impls + baseline
//!   planner.
//! - `opamp` — OpAMP server (will be invoked from the backend's
//!   service startup once Phase 4 wires the in-process integration).
//! - `types`, `types_v2` — control-plane-internal data model.
//!
//! NOT intended for public consumption from outside the workspace —
//! these modules expose the control plane's L1–L5 internals and are not
//! part of any wire/protocol contract.

pub mod accuracy;
pub mod backend_client;
pub mod clickhouse;
pub mod emit;
pub mod epsilon_alloc;
pub mod metrics_exposer;
pub mod monitor;
pub mod opamp;
pub mod physical;
pub mod pipeline;
pub mod planner_selection;
pub mod query_parser;
pub mod query_plan;
pub mod query_planning;
pub mod replan;
pub mod runtime_samples;
pub mod sketch_selection;
pub mod store;
pub mod threshold_alloc;
pub mod types;
pub mod types_v2;
pub mod workload;

// 2026-05 layered-cleanup follow-up: the back-compat shims previously
// defined here (`pub use emit as config`, `pub use pipeline as analyzer`,
// `pub mod algebra { … }`, `pub use physical::colored_dag as stage_split`,
// `pub mod planner { … }`) have been removed. `main.rs` and other
// consumers now reference the canonical module names directly
// (`emit`, `pipeline`, `intent_algebra::relational`, `optimizer`,
// `physical`, `physical::colored_dag`, etc.) per the layered-cleanup
// follow-up task.
/// PromQL → ASAPPlanner's canonical post-ASAP plan via
/// `asap_aware_mapping::bind::implement_tree`.
pub mod asap_tier_implement;

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
    use std::sync::{Mutex, MutexGuard, OnceLock};

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

    /// The one shared lock guarding all process-global env access across
    /// every test module in this crate. Lazily initialised so it can be a
    /// non-`const` `Mutex`.
    fn env_mutex() -> &'static Mutex<()> {
        static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_LOCK.get_or_init(|| Mutex::new(()))
    }

    /// Acquire the crate-wide env lock for the duration of a test body.
    ///
    /// Bind it to a named local (e.g. `let _env = env_lock();`) so the
    /// guard lives until end of scope. Recovers from a poisoned lock (a
    /// panicking test still releases the mutex) so one failing test does
    /// not cascade into spurious failures elsewhere.
    pub(crate) fn env_lock() -> MutexGuard<'static, ()> {
        env_mutex().lock().unwrap_or_else(|p| p.into_inner())
    }

    /// RAII helper: set/unset a process-global env var for the lifetime of
    /// the guard, restoring the prior value (or unsetting if it was unset)
    /// on drop. Holds the crate-wide [`env_lock()`] so concurrent tests
    /// never trample each other's env writes.
    pub(crate) struct EnvVarGuard {
        key: &'static str,
        previous: Option<String>,
        _lock: MutexGuard<'static, ()>,
    }

    impl EnvVarGuard {
        /// Set `key=value` for the lifetime of the returned guard.
        pub(crate) fn set(key: &'static str, value: &str) -> Self {
            let lock = env_lock();
            let previous = std::env::var(key).ok();
            // SAFETY: the crate-wide lock is held, so no other test thread
            // is concurrently touching the process environment.
            unsafe {
                std::env::set_var(key, value);
            }
            Self {
                key,
                previous,
                _lock: lock,
            }
        }

        /// Unset `key` for the lifetime of the returned guard.
        pub(crate) fn unset(key: &'static str) -> Self {
            let lock = env_lock();
            let previous = std::env::var(key).ok();
            // SAFETY: see `set`.
            unsafe {
                std::env::remove_var(key);
            }
            Self {
                key,
                previous,
                _lock: lock,
            }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: the guard still holds the crate-wide env lock.
            unsafe {
                match &self.previous {
                    Some(v) => std::env::set_var(self.key, v),
                    None => std::env::remove_var(self.key),
                }
            }
        }
    }
}
