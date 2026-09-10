# AutoSketch versus ASAPPlanner: recurring Top-K dashboard evaluation

## Claim and scope

Implementation status (v2): measured configuration selection is executable via
ASAPPlanner's ERP selector. The runner chooses shared or independent **30-second
panes**, minimizing retained memory; arbitrary pane-width/sliding-framework
search remains a subsequent experiment. The measured catalog is produced by a
backend window benchmark adapter in sketch-bench's ERP-v1 record format, extended
with per-window recall loss and empirical shape descriptors. The adapter is
necessary because atomic frequency-estimation error does not certify merged
Top-K membership. It is not the production control-plane observer or an invocation
of the sketch-bench executable. The broader design below describes the target
experiment; only completed paths may be presented as measured results.

The v1 result files are withdrawn: their ERP configuration was hardcoded,
initial sampling was not LHS, updates preaggregated events through an unordered
map, exact and approximate endpoints differed, and memory accounting for exact
retention was incorrect. Do not cite v1 speedups or accuracy comparisons.

### Executable v2 selection and validation

- Build and persist a catalog before comparison. Synthetic benchmark seeds
  1000–1002 (Zipf) and 2000–2002 (uniform) are disjoint from evaluation seeds
  42–44. Custom-data profiling reads only the calibration prefix and reports
  its construction cost separately as a required cold-start cost.
- Observe cardinality, per-pane event rate, and top-1/10/100/1000 rank mass.
  Match the nearest catalog descriptor, independent of whether its smaller
  configurations pass accuracy. Reject distance above 0.5 or a distance gap
  below 0.05. This descriptor matching avoids a forced family label; it is not
  a calibrated statistical confidence estimator.
- Filter measured worst-case calibration Recall loss by window. Ask Planner
  to minimize retained memory under 128 KiB per sketch and 16 MiB total.
  Compare full sharing against independent configurations; sharing is optional.
  Record composed empirical update/merge/query cost as an estimate.
- Use the same 72-point CMS/CountSketch depth/width/heap grid and memory objective
  for the AutoSketch CPU adaptation. Use per-family LHS and one-dimension
  neighbors, with Algorithm 4 direction stopping and visited/resource pruning.
  Log infeasible calibration selections instead of claiming a guarantee.
- A miss goes to exact because a theoretical additive frequency bound alone
  does not imply a Recall@10 SLA. The analytical arm remains a separately scored
  reference. Report all held-out failures even when calibration passes.
- Record 400 timestamped query samples per trial and assert common endpoints,
  summed merge/readout times, and retained-memory limits. Logical memory excludes
  heap-string/allocator overhead and query temporaries; RSS remains future work.

Run commands and checked results are recorded under
`tools/autosketch-comparison/data/topk-dashboard-v2/`.

This experiment tests whether ASAPPlanner improves a recurring multi-window
dashboard by jointly choosing sketch parameters, window implementation,
retention, and cross-query sharing. AutoSketch is adapted without its P4
compiler and uses the same Rust sketch implementations, parameter candidates,
accuracy target, and memory constraints. Its natural baseline plans each query
independently.

The older frozen-state frequency experiment remains a microbenchmark. It is not
Figure 1 because no logical time advanced between repeated queries and pane
composition was hidden inside readout time.

## Dashboard workload

The dashboard refreshes every 30 seconds and contains four queries:

```text
TopK(10, frequency(key) over latest 1 minute)
TopK(10, frequency(key) over latest 5 minutes)
TopK(10, frequency(key) over latest 15 minutes)
TopK(10, frequency(key) over latest 60 minutes)
```

At each of 100 refreshes the runner ingests the next 30 seconds of events,
advances logical time, expires old state, and executes all four panels. It does
not sleep. Query execution first merges the selected window's sketch states and
candidate sets, then performs one global Top-K readout. Pane-local Top-K results
must not be merged because they can omit a globally heavy key.

Synthetic data contains exactly 10,000,000 events over 100,000 keys. The primary
shape is truncated Zipf `s=1.1`; the full matrix adds uniform, Zipf
`s={0.8,1.1,1.4}`, seeded traffic bursts, and calibration/evaluation drift. The
first 60 logical minutes are calibration and the following 100 refreshes are
held out. Google Cluster Data 2019 uses disjoint chronological calibration and
evaluation intervals from the downloaded cell-a shard; events are never
shuffled across the split.

## Baselines

| Baseline | Parameters | Window plan | Sharing |
|---|---|---|---|
| AutoSketch-PerQuery | Algorithm 4 per query | independent sliding state | no |
| ASAPPlanner-ERP (Sharing Disabled) | measured ERP | Planner, sharing prohibited | no |
| ASAPPlanner-ERP | measured ERP | Planner-selected sliding/tumbling panes | yes |
| ASAPPlanner-Analytical | theoretical sizing and analytical cost | same Planner candidates | yes |
| Exact-Production | exact grouped Top-K backend path | Planner-selected exact plan | when legal |
| Measured Oracle | exhaustive candidates | exhaustive | yes |

The deployable approximate families are `CmsWithHeap` and
`CountSketchWithHeap`. Candidate heap size is at least `k`; all baselines use the
same width/depth/heap grid and total retained-state budget. Sketch-bench supplies
error, bytes, and atomic update, merge, candidate-union, and readout costs for
the observed/matched data shape. ERP profile construction is offline and is not
charged as online Planner time.

## Planner choices

ASAPPlanner must produce the recorded physical plan; the runner must not
hard-code the winning layout. Candidates include direct sliding state,
30-second tumbling panes, one-minute tumbling panes, per-query materialization,
shared materialization, and their required retention. Cost composition includes
updates, state merges, candidate unions, Top-K readouts, retained bytes, and the
dashboard recurrence schedule.

## Measurements

Every refresh records separately:

- sketch update and window-eviction time;
- sketch-state merge time;
- candidate-set union/deduplication time;
- global Top-K readout time;
- total panel and dashboard-refresh latency;
- retained logical bytes and peak RSS; and
- Recall@10, Precision@10, NDCG@10, and SLA violation.

Every baseline reports initial planning time, candidate count, per-query and
total planning time, and drift-triggered replanning time. AutoSketch planning
includes candidate benchmarking plus LHS/neighbor search. ERP reports online
shape matching, filtering/ranking, and physical compilation separately from
offline profile generation. Exact rule selection and compilation are timed,
not represented as zero.

Report p50/p95/p99 across refreshes and independent trials. Raw JSON contains
dataset identity, generator parameters, seeds, code revisions, selected sketch
parameters, physical window plan, every timing sample, and accuracy results.

## Safety and validity

Calibration and held-out intervals never overlap. Exact dataset-digest matches
take priority; otherwise empirical observations retain multiple distribution
fits and match only within configured goodness, confidence, distance, and
ambiguity bounds. ERP miss, out-of-distribution input, or drift invokes Hybrid
theoretical sizing and then exact fallback. A plotted approximate point is
invalid if it violates the memory or accuracy constraint.

## Figure 1

Synthetic and Google variants each show maintenance CPU, p95 dashboard refresh
latency, retained state, Top-K accuracy violation rate, and initial planning
time for the five executable baselines. The Oracle appears as an optimality-gap
reference. The report must explicitly state when Exact wins on sparse data or a
Planner candidate is unavailable; no smoke-test result is plotted.

## Acceptance criteria

The artifact is reviewable only when release-mode synthetic and Google runs
complete, all methods use the same held-out refresh schedule, all timing
components reconcile with total latency, every planning time is non-placeholder,
and raw data, plotting input, generated figures, commands, and environment
provenance are committed to this PR.
