# Repeated dashboard pane selection

The backend-local planning snapshot accepts `implementation.window_candidates`,
a map from the original registered PromQL text to a list of
`WindowImplementationCandidate` objects. Omitting a query keeps the existing
single lookback-sized candidate. Unknown query keys and empty candidate lists
are rejected. Each candidate supplies its own implementation ID, Tumbling
framework, query `window_secs`, `pane_secs`, state layout, and complete fresh
`ImplementationCostEvidence`. Pane sizes must be positive divisors of lookback.
Distributed Collector deployments still require pane size equal to lookback.

For example, a 60-second query may offer 10-, 20-, and 60-second panes, with
separate provider quotes for each. The cost model returns the concrete ID in
Planner's `CompleteSummaryCandidateEstimate`; the compiler installs the ID
returned by Planner. It does not choose another size after planning. Actual
pane width enters the state fingerprint, precompute configuration, and query
binding; the query's 60-second lookback remains unchanged. Shared DAG consumers
must agree on the physical deployment contract. Distinct logical-window cohorts
that collide only after selecting a smaller shared pane are rejected until
joint lifecycle evidence is available for the resulting physical state. The
compiler never keeps only the first cohort's consumer count or cost quote.
Sharing within one already-priced logical cohort remains supported.

The original QueryWorkload, including recurrence, time scope, predictability,
requirements and data evidence, reaches lifecycle planning. Shared consumers'
reads are combined without multiplying state updates. Legacy direct requests
without a QueryWorkload retain their previous synthesized demand. Refresh cadence
alone does not prove evaluation phase alignment, so it is not a configuration
rejection condition. Candidate costs must account for the supplied workload;
the backend does not manufacture measured costs or an automatic pane-size cost
formula. Version 2 still requires complete workload-versus-exact quotes.

## Execution guarantees and limits

Backend-local raw plans encode `promql_right_closed: true` in state parameters
and therefore in the fingerprint. Raw workers assign boundary samples to
PromQL's `(start, end]` panes, while preserving original timestamps inside
accumulators. Legacy half-open states have a different identity.

At every instant/range evaluation, serving checks each binding's pane alignment.
Multi-pane reads require contiguous stored pane ends for every matched stored
series before merging state. Partial, missing/open, or interior missing panes
fail closed to exact fallback. Without explicit empty-pane completion evidence,
a sparse interval is conservatively a fallback rather than an assumed zero.
Whole panes outside the current lookback are excluded even when retained in
storage; this is logical window expiration, not a claim of immediate physical
state reclamation. Normal retention remains responsible for reclaiming state.

This change does not add operators or min/max-specific paths. Existing operator,
source, and predicate limitations continue to apply.

## Reproduction

From the repository root:

```sh
cargo test -p control_plane --lib
cargo test -p data_plane --lib -- --test-threads=1
cargo test -p data_plane --test asapquery_compatibility_process_e2e -- --test-threads=1
```

The conformance tests change candidate costs and assert the selected ID and
installed pane width; execute advancing stored-state queries; reject missing and
partial panes; and run production RemoteWrite, planning, installation, SUM,
COUNT, and a ratio DAG with 5-second panes and 10-second lookbacks. Boundary
samples verify left exclusion and right inclusion. These are synthetic
correctness tests, not an o11ybench performance or benefit report.
