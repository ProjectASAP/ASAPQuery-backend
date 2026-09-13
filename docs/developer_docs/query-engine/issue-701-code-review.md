# Issues 701 and 702: Planner candidates and backend execution

Audience: developers reviewing Planner PR 404 and backend PR 700. The issues used
removed version-1 snapshots. Deployment now uses schema 2 with complete physical
workload cost evidence; these fixes do not restore the unpriced deployment path.

## Rule ownership

ASAPPlanner constructs the candidate DAG and its accuracy contract. The backend
lowers its typed operators, shares compatible physical state, prices complete
alternatives, and enforces runtime population and coverage constraints.

- `ExactKind::Min` distinguishes minimum from the legacy maximum family. The
  compiler emits the matching accumulator subtype and minimum readout.
- The temporal-average rule emits independently maintained sum and observation
  count, exact finalization, and division. The backend can share their sum/count
  producer without inventing an average rewrite.
- Current-series rules emit `MaintainPopulation` and `ReadPopulation` for
  quantile, TopK, sum, count, and average, globally or grouped. The backend keeps
  each series' latest live value and handles replacement, stale markers, expiry,
  and bounded resources. This is exact current-population state, not an
  append-only temporal DDSketch. Counts include equal-valued distinct series.
- Explicitly exact TopK over temporal aggregates can consume maintained exact
  values. Existing approximate heap-sketch candidates retain their selection path.
- Current-series alternatives can coexist with temporal materializations in the
  same workload. Equivalent current-only alternatives are deduplicated so they do
  not produce ambiguous cost quotes.

## Sliding-window execution

The semantic lookback and evaluation cadence generate distinct pane and
full-window alternatives. Complete cost evidence chooses the physical layout.
Full-window bindings retain the slide and validate the requested phase/range.
Only the snapshot ending at the requested evaluation is read; overlapping full
snapshots must not be merged or treated as required pending work for that read.
Pane layouts retain their non-overlapping coverage checks and exclude the legacy
index's preceding carry-in frame from the query population. The bound-read
regression includes an out-of-range preceding population and verifies that its
counts and values do not enter the result.

Unsigned storage excludes pre-epoch window starts from admission and publication.
Finite empty-window proofs use exact window identity for full-window layouts,
retain the replay/retention floor, and never equate missing unclosed input with
an empty population. Historical process checks explicitly declare their required
staleness/retention margin.

## Quantile expression guarantees

Point quantiles with compatible population and accuracy requirements share state.
Sharing alone does not make division preserve a component error bound.
For numerator error `a` and denominator error `b < 1`, a sufficient relative
ratio bound is `(a + b) / (1 - b)`. Independence is unnecessary for this
algebraic bound; probabilistic failures still require joint accounting.

Planner's checked relative-division candidate sizes DDSketch operands against the
whole expression budget. A 1% ratio needs component accuracy slightly below
`0.01 / 2.01`, approximately 0.4975%, including floating-point slack. Average /
quantile uses the same composition rule with an exact numerator. Rank-only KLL
certificates do not establish this relative-value guarantee.

The compiler lowers the typed checked division. Runtime requires finite operands,
a nonzero denominator, and a finite normal result. Unmet domain or coverage
conditions route to exact execution. An ordinary division is not silently given
a stronger guarantee.

## Acceptance coverage

Compiler regressions cover typed minimum, shared quantiles, average, and both
ratio forms. Runtime regressions cover current-series membership, checked-division
domain failures, storage-domain windows, pending full-window isolation, and finite
empty-window evidence. The process workload combines the issue query families,
ten quantiles, 15-minute and 5-minute windows, and consecutive evaluations while
input remains open. `ASAP_CURRENT_SERIES_PROMETHEUS_URL` enables real Prometheus
result comparison against Prometheus 3.x; the boundary-aligned fixture rejects a
2.x oracle because that version includes the left boundary, unlike the installed
PromQL window contract. See the [Prometheus migration guide](https://prometheus.io/docs/prometheus/3.5/migration/).
Test quotes are synthetic correctness preferences, not measured performance or
speedup evidence.

The UnivMon integration fixture brackets one complete calibration population and
uses the same window cadence for the producer observation. A partial sliding tail
must not be asserted to match the complete calibration distribution.

Validation on 2026-09-12:

- Planner `cargo +1.98.0 test --workspace`: 1,097 passed.
- Backend `cargo +1.98.0 test --workspace --lib`: 2,098 passed; the subsequent
  focused pane-population regression also passed.
- Compatibility process suite with a fresh Prometheus 3.5.0 remote-write oracle:
  14 passed, 1 previously ignored Collector-schema integration. The mixed test
  checks 46 queries at two successive evaluations and two zero-denominator
  fallbacks; the current-series test also compares replacement/staleness updates.
- Workspace/all-targets clippy passed with warnings denied; affected data-plane
  checks were repeated after the final pane-read correction.

These are correctness results. No measured performance win is claimed. The
separately ignored Collector schema integration remains outside these fixes.

## Review follow-up: lookback boundary and average overflow

The exact lower lookback boundary now expires current-series members, matching
Prometheus 3.5. A regression covers count, sum, average, quantile and TopK when
all but one series reach that boundary.

Temporal average no longer exports an unconditional sum/count logical rewrite.
Planner marks the physical division with `checked_finite_division`; the compiler
retains it as `FiniteDiv`. Nonfinite operands/results or a zero divisor trigger
the original-query fallback. A zero or subnormal finite average remains eligible.
The production HTTP test first waits for both sum and count state to become warm,
then verifies that overflowing `1e308` samples return the native finite average.

Validation on 2026-09-13: backend workspace library tests passed (2,102); the
Prometheus 3.5 compatibility process suite passed 15 tests with the existing
Collector-schema test ignored; workspace/all-targets clippy passed with warnings
denied. Planner workspace unit/integration tests passed, and the workspace
doctests passed on a separate run after a transient cached-crate lookup failure.
The Planner's GitHub test and format/lint checks also passed.
