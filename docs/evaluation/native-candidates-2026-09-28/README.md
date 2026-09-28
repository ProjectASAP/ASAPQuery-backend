# Native physical candidates and bound SDS validation

Scope: supported native candidate installation, durable bound-query execution,
and actual plans for human review. Ad-hoc SDS discovery is deferred.

Planner #462: `176c1bd565e0c400f9a35996c2bd65de32475e72`.
Backend implementation #761: `32095855`.
Level 1 export source #728: `d26043a1` (all 44 plans are unchanged by the preparation fixes).
Execution runner source for the recorded run: `13e90c8d`; the data-plane binary is from `ed944356`, which has identical runtime code.

## Behavior covered

Planner exposes exact ranking and evidence-dependent CMS/CountSketch heap
candidates for Rate TopK, with query-time or precomputed heap construction.
Spatial TopK supports exact ranking and signed CountSketch heap over a complete
current-series snapshot. Updates, decreases, staleness and expiration replace
snapshot eligibility instead of accumulating historical gauge values.

Grouped Rate exposes query-time and precomputed Sum. Both finalize each series'
Rate before grouping; pooled raw counters are not a valid substitute. Complete
60-second windows can overlap at a five-second recurrence. Stored grouped heaps
and Sum publish one atomic batch per window, retaining grouping inside the batch.
Bound reads validate the installed output/definition, generation, exact window,
format and schema. Recovery restores the physical program without logical lowering.

## Problems corrected

- Precomputed Rate heap and grouped Sum placements were missing from the native
  deployment path. Their maintenance and query graphs now survive installation
  and recovery, and both query and maintenance resources reach costing.
- Maintenance-only source retention was missing from costs. Source retention now
  follows its bound consumers, including derived stored outputs.
- A 60-second lookback incorrectly reduced production to once per minute for a
  five-second query recurrence. Window width and stride are now independent.
- Neighboring overlapping full-window snapshots were treated as ambiguous input.
  Bound reads select exact full-window coordinates before validating coverage.
- Missing grouping labels failed partition ownership or leaked as empty labels.
  Missing and empty labels share ownership; grouped results omit empty labels.
- Nested legacy Sum could be marked as derived state without a complete native
  maintenance graph. Such installation now requires the Planner-compiled graph.
- The differential runner still required resource model v1. It now validates v2
  and rejects old/provider-only reports while retaining resource, total-cost,
  provenance, local-execution and minimum-cost checks.

The runner now publishes the exact costed typed deployment plan. Backend startup
loads `*.install.json`; it does not repeat search from the input snapshot. This
keeps the executed generation aligned with the retained plan and avoids repeating
large-workload planning inside the service-readiness deadline.

Candidate preparation also reuses unchanged window requirements and shares
immutable diagnostics. The 66-query aggregation workload retains the same 214
forests and 1,018 deployment candidates. Population bindings are resolved once
per candidate rather than rescanning the workload for each consumer.

## Automated checks

| Check | Result |
| --- | --- |
| Planner weighted physical binding suite | 8 passed; strict all-target Clippy passed |
| Backend type / control-plane / data-plane libraries | 120 / 448 / 896 passed |
| Native Rate and spatial candidate integration | 6 + 3 passed |
| Real-process Rate heap and Sum execution/restart | 6 passed |
| Historical #728 combined assertions | 3 passed before the test split |
| #756 diagnostics and #766 controls | Strict all-target Clippy passed after merging #761 |

The process tests send Remote Write, read durable counter or aggregate SDS,
query HTTP results, restart the process and repeat the queries. They cover both
heap families and both Sum placements, counter reset, changed window rankings,
unreferenced labels, missing groups and unavailable windows. Sum evaluates at
60, 65 and 120 seconds with a five-second production stride.

[Human-review plans](../issue754-human-review/README.md) contain 10 selected plans
and 34 successfully compiled candidates. Operator dependencies, window contracts,
value/group columns, sort expressions, stored boundaries and admission/cost
results are retained. Missing heap accuracy evidence remains an explicit
rejection in the default fixture; certified tests exercise heap deployment.
These assertions and exports do not constitute human approval.

## Boundaries

Precomputed grouped results require finite complete-input closure. A timer alone
does not establish population completeness. Other PromQL shapes retain documented
Backend adapters; candidate search does not enumerate every joint workload
combination. This validation does not add partitioned execution, spill or ad-hoc
SDS discovery.

The [earlier Level 3 performance failure](../bound-sds-2026-09-26/README.md#local-level-3-result-not-passed)
is preserved. Correctness checks and debug-build runs do not establish a release
speedup, a passed performance gate, or manual production verification.

## Completed execution run and test split

All seven installed-plan differential suites subsequently passed; see
[raw execution reports and source provenance](../execution-2026-09-28/README.md).
These are finite-fixture execution checks, distinct from real-evidence selection.
#728 now asserts structure/admission only; #742 tests synthetic ranking. Separate
PRs own workload-statistics, accuracy-evidence and resource-measurement contracts.
See [test responsibilities](../../design_docs/planning-test-layers.md).
