//! Control-plane planning, configuration emission, and deployment services.
//!
//! These modules support in-process integration and the standalone control-plane
//! binary. Internal module paths are not a wire-protocol compatibility contract.

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
pub mod monitor;
pub mod opamp;
pub mod physical;
pub mod pipeline;
pub mod planner_selection;
pub mod query_parser;
pub mod query_plan;
pub mod replan;
pub mod runtime_samples;
pub mod store;
pub mod types;
pub mod workload;

/// PromQL → ASAPPlanner's canonical post-ASAP plan via
/// `asap_aware_mapping::bind::implement_tree`.

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
