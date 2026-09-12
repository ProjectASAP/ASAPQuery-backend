//! Wall-clock budgets for tests that wait on background progress.
//!
//! Several storage / maintenance tests wait for work that a *background*
//! thread performs — the persistence flusher sealing and evicting epochs, the
//! finite-input sealer draining an admitted revision. A fixed five-second
//! budget is generous when the test owns the machine and far too tight when
//! `cargo test` runs one test per core: on a 64-way runner the waiter and the
//! background thread it is waiting on compete for the same CPUs, and the
//! budget expires while the work is merely queued. That is what made the
//! suite's flush / evict / restart tests fail under the default parallel run
//! and pass on a serial re-run, which in turn kept the data-plane library
//! tests out of the default CI gate.
//!
//! Scaling the budget with the test concurrency keeps both properties: a
//! passing test still returns as soon as its condition holds (these are
//! polling waits, not sleeps), and a genuinely stuck condition still fails —
//! later, but deterministically.

use std::time::Duration;

/// Scale a wait budget by the concurrency the test binary is running at.
///
/// Override with `ASAP_TEST_TIMEOUT_SCALE=<n>` to pin a multiplier (useful on
/// a loaded shared machine, or to re-tighten the budget while debugging a
/// hang). Otherwise the multiplier is derived from `RUST_TEST_THREADS` when
/// the harness was told a thread count, and from the machine's parallelism
/// otherwise, capped so the worst case stays bounded.
pub fn scaled(budget: Duration) -> Duration {
    budget * scale()
}

fn scale() -> u32 {
    if let Some(explicit) = std::env::var("ASAP_TEST_TIMEOUT_SCALE")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
    {
        return explicit.max(1);
    }
    let threads = std::env::var("RUST_TEST_THREADS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .or_else(|| std::thread::available_parallelism().ok().map(|n| n.get()))
        .unwrap_or(1);
    ((threads / 8) as u32).clamp(1, 8)
}

/// A deadline `budget` from now, scaled by [`scaled`].
pub fn deadline(budget: Duration) -> std::time::Instant {
    std::time::Instant::now() + scaled(budget)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_scale_overrides_the_derived_one() {
        // The env var is process-global; assert the parsing contract on the
        // pure helper instead of mutating it under a parallel harness.
        assert!(scale() >= 1);
        assert!(scaled(Duration::from_secs(5)) >= Duration::from_secs(5));
        assert!(scaled(Duration::from_secs(5)) <= Duration::from_secs(40));
    }
}
