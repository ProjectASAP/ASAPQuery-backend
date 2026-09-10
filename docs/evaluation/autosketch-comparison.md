# ASAPQuery vs. AutoSketch evaluation plan

Status: proposed experiments, not measured results. Audience: researchers and
developers implementing the evaluation. Scope: ASAPPlanner configuration and
ASAPQuery execution; ClickHouse is excluded.

## 1. Claims and priorities

The primary question is whether workload-aware planning pays off when multiple
queries repeatedly read overlapping windows. Broader sketch support and
competitive single-sketch sizing are separate supporting claims.

| Priority | Hypothesis | Evidence needed |
| --- | --- | --- |
| E1: primary | For recurring, overlapping window queries, ASAPQuery reduces total execution cost at comparable accuracy and freshness. | End-to-end measurements plus materialization, pane, retention, and sharing ablations. |
| E2: breadth | ASAP's supported candidate catalog serves a broader set of query/accuracy contracts than the evaluated AutoSketch implementation. | Audited support matrix, successful execution, and restricted-catalog ablations. |
| E3: configuration | For one common sketch and fixed query/window, ASAPPlanner configuration quality is competitive with AutoSketch-style search. | Identical implementations and candidate grids, held-out accuracy, and distance from a finite-grid oracle. |

These are hypotheses. Do not describe superiority or non-inferiority as
established before collecting results. Negative results and unsupported cases
remain in the report.

## 2. Baseline scope and attribution

AutoSketch is application-local, not merely a counter-size tuner: an application
can contain multiple operators. Its objective weights stateful ALUs and register
memory, subject to accuracy and hardware constraints. The published implementation
maps reduce to CMS/Count Sketch and distinct to Bloom/Counting Bloom filters,
and constructs sketch-like UDF states. It does not demonstrate KLL/DDSketch
support; general windows and multi-application joint search are future work.
See [AutoSketch, NSDI 2024, Sections 4–6](https://www.usenix.org/system/files/nsdi24-sun.pdf).

Our baseline adapts Algorithm 4's LHS initialization, feasibility-directed neighbor
search, pruning, and stopping to the common software runtime. Pin the
[source revision](https://github.com/N2-Sys/AutoSketch) and document deviations,
especially removing stage/page constraints and replacing hardware cost.
Name it **AutoSketch-Adapted**, not native AutoSketch.

The adaptation is a configuration baseline, not a reproduction of its whole P4
compiler. Unsupported original operators do not prove a fundamental limitation
of its search method. Likewise, a candidate listed in ASAP code does not prove
that the chosen deployment can execute its queries correctly.

Use the same ASAPQuery executor for both sets of configurations. This isolates
planning from hardware/runtime differences. In E1, give the adapted baseline a
legal fixed-window adapter; record its maintenance and merge costs. This adapter
is evaluation infrastructure, not a claimed AutoSketch window feature.

## 3. Shared experimental contract

- Freeze code revisions for backend, Planner, sketch-bench, sketch library, and
  AutoSketch adaptation. Record hardware, build flags, thread counts, and limits.
- Use identical input streams, query schedules, sketch implementations, hash
  seeds, candidate bounds, and accuracy/freshness requirements within each pair.
- Separate calibration, development, and final test streams. Configuration and
  ERP construction cannot inspect final-test answers.
- Use independent data and sketch seeds: initially ten paired runs per cell.
  Aggregate uncertainty across runs, not correlated queries within one run.
  Increase replication if intervals are too wide to resolve the hypothesis.
- Derive exact answers from the same events and window boundaries. Specify
  right/left closure, lateness, empty windows, and missing keys before execution.
- Query lookback is semantic and fixed. Optimize physical panes and retained
  state, never silently shorten a requested window or relax freshness.
- Hold ingestion and offered query schedules fixed. Count all offered queries,
  errors, timeouts, fallbacks, and queueing; do not measure only completed fast
  queries. Separate warm-up and steady state and also report startup costs.
- Count all maintained panes, sidecars, indices, buffers, replicas, and fallback
  resources. Report state bytes separately from process RSS.
- Save installed plans and verify which materializations actually served each
  query. An exact fallback can preserve correctness but is not a sketch hit.

Use uniform and Zipf key frequencies (suggested exponents 0.8 and 1.2), plus one
pinned representative trace if available. For quantiles add light-tailed and
heavy-tailed values. Missing trace availability must not be hidden by claiming
synthetic-only results generalize to production.

## 4. E1 — recurring multi-query windows (main experiment)

### Workload

Begin with a common frequency-estimation task and one CMS implementation. Query
the frequency of specified keys within each lookback. Define the task explicitly;
do not assume an arbitrary `count_over_time` expression means a CMS point query.
Validate the actual frontend expression and grouping before admitting a cell.

| Dimension | Initial settings | Purpose |
| --- | --- | --- |
| Lookbacks | 1m, 5m, 15m, 1h | Overlapping historical coverage |
| Execution periods | 1s, 10s, 60s, independently of lookback | Recurrence and materialization break-even |
| Logical query count | 1, 4, 16, 64 | Scaling with consumers; vary keys/lookbacks rather than counting duplicate registrations |
| Shareability | 0%, 50%, 100% of queries in compatible source/filter/grouping cohorts | Sharing benefit and negative control |
| Cardinality | 1k, 100k keys | State pressure and collisions |
| Ingest rate | 10k, 100k events/s initially | Maintenance pressure; reduce if the fixed hardware cannot sustain them |
| Pane candidates | 1s, 5s, 10s, 30s, 60s where semantically and operationally legal | Update/retention/read trade-off |

Do not execute the full Cartesian product initially. Establish one default cell,
then vary recurrence, lookbacks, shareability, cardinality, and load individually.
Follow with selected interactions. Also include staggered query phases and
non-aligned lookbacks (e.g. 70s and 310s): perfectly aligned windows alone can
overstate sharing benefits.

For 1h lookback, ingest at least a full history before steady-state measurement;
then measure at least 30 minutes and 30 evaluations of the slowest query.
Accelerated replay is acceptable for correctness checks, not as a substitute
for steady-state wall-clock CPU/latency measurements at the declared load.

### Baselines and ablations

| Baseline | Sketch configuration | Materialization | Pane / retention | Cross-query sharing |
| --- | --- | --- | --- | --- |
| Exact | None | Exact execution | Required exact history | Fixed exact backend |
| AutoSketch-PerQuery | Adapted search | One continuously maintained state set per logical query | Fixed legal window adapter; minimum retention needed for the contract | Off |
| ASAP-FixedLayout | ERP | Same always-materialize decision | Same layout as PerQuery | Off |
| ASAP-NoSharing | ERP | Cost-driven exact/materialized choice | Planner-selected supported layout | Off |
| ASAP-Full | ERP | Cost-driven | Planner-selected supported layout | On |

PerQuery must keep state between executions; it must not rebuild on every read.
Queries needing sliding windows get sufficient panes/history, not a single
incorrect tumbling sketch. Tune the fixed adapter on development data and freeze
it; do not deliberately pick an inefficient layout.

First lock identical CMS parameters across E1 baselines. This removes sizing as
a confounder. Then repeat key cells allowing each method to choose parameters;
report actual query error after pane merges in both runs.

Additional mechanism controls, enabled independently from Full:

- **NoSharing:** forbid shared producer/state identities while preserving other
  choices. Verify duplicate states are physically distinct in the executor.
- **FixedPane:** pin a common legal pane width while preserving other choices.
- **FixedRetention:** retain the longest required horizon for every state, while
  preserving other choices. This isolates pruning of unnecessary retained panes.
- **AlwaysMaterialize:** disable exact-vs-materialized cost choice. Exact mode's
  data retention and execution cost must be included in the comparison.

For a stronger layout control, add **AutoSketch-SharedPane**: the same selected
parameters with a fixed, calibration-chosen shared-pane layout. This measures
how much a simple sharing heuristic explains without recurrence-aware planning.
It is our constructed control, not an original AutoSketch feature.

### Measurements and interpretation

Report total CPU-seconds, ingest/maintenance CPU, read/merge CPU, peak state
memory, RSS, byte-seconds, query p50/p95/p99, achieved throughput, and deadline
miss rate. Also report state count, updates, merges, exact-fallback share,
planning time, and accuracy by query/window. Include Collector and exact-backend
costs when used; keep placement fixed between baselines.

The primary plot is CPU and memory versus query period, stratified by lookback
and shareability, with accuracy/latency alongside. NoSharing versus Full isolates
sharing; the independent controls isolate pane, retention, and materialization
decisions. PerQuery versus Full alone is not sufficient for attribution.

Support the headline only where Full reduces resources while meeting the same
accuracy and service requirements. Show low-recurrence, non-overlapping, and
single-query cases even when gains disappear or planning overhead dominates.

## 5. E2 — query coverage and candidate-space breadth

Audit both implementations at pinned revisions before benchmarking. Distinguish
three support levels: frontend expression, legal planner mapping under the
accuracy contract, and successful runtime execution in the chosen placement.

| Query/contract family | ASAP candidates to verify, not assume | Evaluation |
| --- | --- | --- |
| Point frequency / keyed sums | CMS, Count Sketch where update and error semantics permit | Common-space comparison |
| Ranked keys | Heap/sidecar variants where key recovery is supported | Count candidate enumeration and sidecar memory |
| Quantiles with rank-error intent | KLL | Held-out rank oracle |
| Quantiles with relative-value-error intent | DDSketch | Relative value oracle on its admitted domain |
| Distinct/cardinality tasks | HLL where an end-to-end path exists | Exact distinct oracle; audit the other system's distinct semantics separately |

For each task record: original AutoSketch support, adapted support, ASAP support,
candidate identities, rejection reason, selected configuration, and actual
accuracy. Distinct/deduplication and cardinality estimation are not automatically
the same operator. Report unsupported cells as unsupported, not infinite latency
or zero throughput.

Run two complementary experiments:

1. **Coverage:** fraction and list of preregistered task/accuracy contracts that
   compile and execute correctly. Fix the task list before observing outcomes.
2. **Candidate-set ablation:** compare ASAP-FullCatalog with
   ASAP-CommonCatalog under identical planning/runtime. On common tasks, measure
   whether extra eligible candidates improve the cost–accuracy frontier. On
   additional tasks, report coverage and compare with exact execution.

Do not count a singleton KLL task as evidence of superior selection among
algorithms: it demonstrates coverage. Demonstrating selection requires at least
two eligible candidates for the same contract. KLL rank error and DDSketch value
error cannot be substituted without an explicit common output contract and
validated accuracy mapping.

If time permits, extend the adapted search to the additional software sketches.
Call this **AutoSketch-Adapted-Extended**. It separates catalog engineering from
search quality, without claiming that the original implementation supported them.

## 6. E3 — fixed-window single-sketch configuration

Fix one CMS query, a 5m window, one runtime/layout, and the same key workload.
Start with width in {64, 128, 256, 512, 1024, 2048, 4096} and depth in
{1, 2, 3, 4, 5, 6, 7, 8}: 56 configurations, filtered identically for runtime
legality. The small space makes exhaustive measurement feasible.

Compare theoretical sizing (when a matching contract exists), AutoSketch-Adapted,
ASAP-ERP, and the finite-grid oracle. The oracle selects the cheapest feasible
configuration using calibration evidence, then is evaluated on held-out data;
it cannot choose retrospectively using test answers.

Define normalized additive frequency error as `abs(estimate - exact) / N`, with
N the number of events in that query window. Initially test epsilon in
{0.005, 0.01, 0.02}; define feasibility as the maximum error over the declared
key/window calibration set meeting epsilon. Report held-out per-query errors,
run-level maxima, and violation rates. Empty windows must have a separate exact
policy because N is zero. These empirical checks do not establish formal delta.

Use two common objectives:

- Minimum state memory subject to the same empirical accuracy constraint.
- A shared software cost: `lambda_cpu * (Nu*tu + Nq*tq + Nm*tm)
  + lambda_mem * B*T`, where operation counts, per-operation CPU, retained bytes,
  and horizon have identical definitions. Fix weights before testing and show
  a CPU–memory trade-off curve rather than only one favorable weighting.

Run a shared-table comparison to isolate selection, then an actual calibration
comparison to measure costs. Record LHS/search seeds, benchmark calls, cache
hits, planning wall time, and all ERP construction costs. Report first-use and
amortized costs over 1, 10, and 100 planning requests; permit baseline caching.

Search adaptation requirements: implement initialization, neighbor direction,
deduplication, resource/incumbent pruning, stopping, and explicit no-feasible
outcomes. Freeze deviations from Algorithm 4 before results. Do not silently
count a best-effort inaccurate configuration as feasible. ERP and search must
consume the same kind of accuracy evidence, not different training datasets.

### Operational definition of “not worse”

Use paired runs and report feasible-selection rate, held-out accuracy, memory,
CPU cost, planning cost, and gap to the grid oracle. Before final testing,
preregister practical non-inferiority margins. Suggested starting margins are
5% resource overhead and 2 percentage points lower run-level accuracy pass rate;
these are proposed experimental tolerances, not established guarantees.

Claim non-inferiority only if the appropriate paired 95% confidence bounds fall
inside the frozen margins. Report infeasible selections separately rather than
dropping them from cost ratios. If results are inconclusive, say so. Configuration
quality and planning-time competitiveness are separate conclusions; building
ERP may cost more initially even if subsequent selection is faster.

## 7. Implementation gates and execution order

This is an evaluation specification, not a list of already available runner
flags. Implement and test each baseline switch before collecting measurements.

1. Audit installed Planner/runtime capabilities and pin all revisions. Record
   backend-local versus Collector deployment support separately.
2. Implement the common CMS oracle/benchmark contract and CMS ERP accuracy
   mapping. The ERP adapter alone is not proof that measured CMS parameters
   survive workload selection and physical compilation.
3. Implement AutoSketch-Adapted and the 56-point grid oracle; test initialization,
   boundaries, pruning, deterministic seeds, and no-feasible outcomes. Run E3
   as a calibration sanity check before the main experiment.
4. Build the recurring-window harness and E1 controls. Validate complete-window
   answers, no unintended fallback, and physical sharing/non-sharing identities.
   Compare locked-parameter plans first; then enable sizing.
5. Audit E2 and add executable coverage cells family by family. Do not publish
   a catalog-count result without runtime evidence.
6. Execute the frozen matrix, retain failures, and generate paired comparisons.

Relevant starting points:

- [ERP deployment contract](../../control_plane/docs/design-erp-deployment.md)
- [Physical compiler](../../control_plane/src/physical/compiler.rs)
- [Compatibility process tests](../../data_plane/tests/asapquery_compatibility_process_e2e.rs)
- [Sketch process oracles](../../data_plane/tests/all_sketches_process_oracle_e2e.rs)
- [ERP E2E follow-up PR #542](https://github.com/ProjectASAP/ASAPQuery-backend/pull/542)

The #542 KLL process regression is a correctness prerequisite, not an AutoSketch
comparison. Its empirical accuracy path is scoped to KLL rank error and
epsilon-only requests; CMS mapping, the search adaptation, and workload ablation
controls still need dedicated evaluation implementation. Do not assume that
theoretical candidate support implies empirical ERP coverage for every family.

## 8. Required artifacts and final presentation

Each run records a manifest containing revisions, machine settings, data hashes,
seeds, window/recurrence/grouping definitions, candidate grid, accuracy metric,
cost weights, calibration budget, and baseline controls. Save profiles, selected
plans, planner traces, exact answers, per-query results, raw resource time series,
failures, and a script that regenerates every figure.

Recommended paper order:

1. E1: resource/latency versus recurrence and sharing, with mechanism ablations.
2. E2: verified task coverage and common-catalog/full-catalog comparisons.
3. E3: single-sketch configuration quality, oracle gaps, and calibration cost.

The intended conclusion is that ASAP extends the decision scope beyond local
sketch configuration to recurring-window workload planning, while retaining
competitive configuration quality. Qualify each part by the actual workload,
accuracy contract, deployment support, and measured uncertainty. No ClickHouse,
native-switch speed comparison, or automatic drift-detection claim is required
for this evaluation.
