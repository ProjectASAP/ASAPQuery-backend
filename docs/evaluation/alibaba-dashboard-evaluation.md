# Alibaba dashboard comparison: execution and data contract

Audience: evaluation implementers and reviewers.

Status: the user accepted full-row-distinct call observations, explicitly not
unique logical requests. Full 12-hour preparation and evaluation are automated.
This design contains no final performance results; those belong in the generated
report only after complete real-data runs. Tests and the separate two-hour
qualification run are not final comparison evidence.

## Data and acceptance criteria

Use Alibaba `cluster-trace-microservices-v2022/CallGraph` archives. The official
fetch script maps file index `i` to a three-minute interval. First audit all 20
files in hour zero; the full experiment uses hours 0–6 for calibration
and 6–12 for evaluation. Do not choose intervals based on benchmark outcomes.

Source documentation:
https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2022

`tools/autosketch-comparison/prepare_alibaba.py` downloads full archives, computes
SHA-256 digests, and converts parseable rows to lossless string-column Parquet.
Malformed CSV rows are excluded with counts and examples; there is no speculative
column repair. The first hour's original archives and Parquet are retained.
For subsequent archives, validated compact replay and metadata are retained;
task-generated raw/Parquet intermediates are removed after conversion to bound
disk usage. Their public URLs and SHA-256 hashes permit recovery.
Fields are not interpreted as
resource metrics or periodic scrapes. The audit is read-only with respect to call
identity: it reports conflicting records and does not silently deduplicate them.

Hour zero (20 complete archives) contains 260,437,230 parseable rows and 21,320
malformed rows. Archives occupy 3.995 GiB and lossless Parquet 4.523 GiB. These
are preparation statistics, not held-out performance measurements. Retaining
both representations for 12 hours would take roughly 102 GiB if rates remain
similar, excluding replay files and audit spill space; available storage must
be rechecked before extending the download.

The first three-minute archive contains 13,331,267 parseable rows and 1,089
malformed rows. It has 11,329,212 distinct `(traceid, rpc_id)` pairs, but 485,472
pairs have conflicting `(service, um, dm)` values, 715,927 have conflicting
`(timestamp, rt)` values, and 463,958 have conflicting instance pairs. Therefore
`DISTINCT traceid, rpc_id` does not establish a unique logical-call stream.
The same archive has 12,502,001 distinct full rows and 5,067,940 rows with zero
latency. Removing only identical rows is therefore a different operation from
deduplicating RPC IDs; dropping zeros would also materially change quantiles.
Related upstream reports describe similar corruption but do not supply a
validated reconstruction contract:
https://github.com/alibaba/clusterdata/issues/230

**Accepted semantics:** evaluate call-observation records, removing only rows
identical across all 11 source string columns. Never collapse `(traceid,rpc_id)`.
The combined first-hour identity audit exceeded its bounded spill allowance;
it is not reported as completed. Full-row deduplication runs independently per
archive and rejects timestamps outside its disjoint three-minute interval, so
identical full rows cannot span accepted archives. Final counts describe
this policy and all exclusions. Invalid downstream IDs and invalid latency
values are counted separately; latency failures must not silently remove valid
count events. Zero latencies must be preserved in quantile semantics, including
when adapting a positive-only DDSketch implementation.

## Queries

Replay native timestamps, not an invented 100ms scrape schedule. Evaluate every
minute on the held-out interval, with windows 1m, 10m and 1h:

- A: count by downstream service, and Top-3 services by that count.
- B: latency p50/p75/p90/p95/p99 by downstream service, plus p90/p50.
- C: the count/Top-3 workload with `(upstream, downstream)` service-pair keys.

Every method must answer the same endpoints and label sets. Empty groups,
boundary ties, zero-denominator ratios, and missing/invalid records need explicit
correctness fixtures. Calls are merged in chronological timestamp order, with a
deterministic tie-breaker; sorting by `(pane,key)` is not acceptable.

The current `topk_dashboard_comparison` executable uses fixed Top-10 and
1/5/15/60-minute windows. Running it unchanged does **not** execute this plan.
Likewise, an append-only frequency CMS is not an implementation of arbitrary
instant-value PromQL TopK or latency quantiles.

## Baselines and evidence

AutoSketch's CPU adaptation searches independently per window query, with common
implementations, candidate spaces, memory constraints, and final-query accuracy
targets. A KLL/DDSketch extension must be labeled separately from paper support.
Unsupported query/baseline pairs are explicit, not infinite speedups.

The executable matrix is Exact-scan, Exact-pane, ASAP analytical sizing,
AutoSketch CPU extension, custom ERP no-sharing, and custom ERP shared-or-local.
The existing synthetic catalog is incompatible with this query/window/error
contract: mark it unavailable, not a successful nearest-shape match. Custom
profiling is a separate baseline with a separately reported construction cost.

The ERP path calls ASAPPlanner's real `ErpArtifact.select` API; it is not the
complete production planner/control-plane. The analytical path calls
`default_size_params`, not full analytical CPU-cost optimization. This experiment
alone cannot establish synthetic-catalog transfer, observed-shape fitting, live
drift fallback, arbitrary pane-width optimization or production-system speedup.

Report per-baseline planning, ingestion, eviction, merge, readout, query latency,
accuracy violations, retained payload, temporary query state, and peak RSS.
Describe timing exclusions. Report per-window input size/cardinality and the
actual selected configurations. Repeated runs of one trace are timing repetitions,
not independent trace samples. Store raw query samples and provenance before
generating tables and plots. No held-out tuning to manufacture a winning point.

## Fixed configuration and measurement contract

Calibration scans the entire first six hours and keeps a deterministic reservoir
of 10,000,000 observations (seed 42), in timestamp order and original minute panes.
Filtering missing upstream for edge queries and invalid latency for quantiles
occurs after sampling. No held-out events are used in search or profiling.
Reservoir sampling is not proof that sample cardinality or error generalizes;
report actual held-out failures without retuning.

Each replay ingests one hour of calibration warmup (hours 5–6) and all six
held-out hours (6–12), without event sampling. The 360 held-out refreshes are at
minutes 361–720. A window is `[t-W,t)`. One-minute tumbling summaries are merged
to answer sliding dashboard windows. TopK is evaluated **after** composition;
per-pane TopK answers are not summed. No sleep is inserted between replayed
refreshes: native event time models recurrence, not real-time load/concurrency.

Frequency candidates: CMS point counters (depth 3/5/7, width
128/256/512/1024), CMS-with-heap and CountSketch-with-heap with the same counter
grid and heap 16/32/64: 84 configurations total. A point-counter candidate keeps
the key universe needed for enumeration. Latency candidates: KLL capacities
128/256/512/1024/2048 and DDSketch relative accuracy .005/.01/.02/.05/.1.
Native implementations come from `asap_sketchlib`; DDSketch adds a zero counter
because the native implementation accepts only positive values.

AutoSketch uses independent per-window-query LHS initialization and numeric
neighbor search. LHS has three strata per family; no cross-query search cache.
Each query receives a share of the 16,000,000,000-byte budget proportional to
its retained window length. Explicit exact fallback is a CPU-adapter extension,
as are KLL/DDSketch: do not claim unmodified paper support for these features.

ERP catalogs measure all candidates plus exact fallback on five evenly spaced
complete calibration endpoints per window and three sketch seeds. Errors and
pane memory take maxima; CPU cost takes the mean. Online ERP selection minimizes
predicted retained bytes; it does not optimize CPU in this version. Full compares
one shared 60-pane summary with completely independent per-panel summaries; it
does not enumerate partial-sharing partitions. Exact is a fallback, not a
sample-memory competitor to sketches. The actual retained-payload budget is
checked during replay; Exact-scan is an uncapped reference.

Heap-only TopK estimates omit the external enumeration key set; shared/count
plans include it. CPU records are measured with the catalog's combined query
requirements and are informational in the memory-only selector.

Common final-query acceptance targets:

- Count: L1 error divided by total true count ≤2%.
- Top-3: tie-aware recall ≥80%; boundary ties are acceptable alternatives.
- Quantiles: each group's absolute error / max(|truth|, 1 ms) ≤10%.
- Ratio p90/p50: error / max(|truth|, 1) ≤20%; IEEE NaN/Inf classes must agree.

Quantiles use linear interpolation at `q*(n-1)` consistently across methods.
Undefined ratios are separately counted, not described as accurate finite
estimates. Per-group rank-distance diagnostics are additional outputs, not the
common KLL/DDSketch selection target.

Three timing repetitions use the same unchanged trace. AutoSketch search seeds
are 42/43/44; runtime sketch seeds derive deterministically from minute index.
These are not independent datasets. Trial order is recorded and runs execute
sequentially on a shared host. Offline calibration/profile costs are separate
from planning; AutoSketch planning includes search, ERP planning includes
catalog load and selection. Planning is reported even for fixed exact plans.

Measure wall time and Linux process CPU time separately for update, eviction,
composition and readout. Initial-pane cloning is included in composition. Shared
composition is charged once per dashboard merge group. Exact-scan group-by is
readout work. Input decoding, oracle generation/loading, and validation are
outside these operator timers. Dashboard latency here means summed timed query
operations, not HTTP end-to-end latency. Exact-pane generates a reusable oracle;
independent Exact-scan validates it on the same complete endpoints.

Memory reports retained logical payload and temporary query payload, not archive
size or allocator overhead. Exact-scan stores only query-required keys (8 bytes)
or service/latency pairs (16 bytes); exact count panes store distinct key/count
pairs. Whole-process high-water RSS includes harness and oracle memory and is
not an isolated sketch-memory measurement. Output maps, capacity slack and
allocator metadata are not all represented by the logical payload estimate.

## Reproduction

Dependencies: Python 3, PyArrow, DuckDB, curl. On this host DuckDB is isolated in
`/mydata/datasets/alibaba-microservices-2022/python-deps`.

```sh
PYTHONPATH=/mydata/datasets/alibaba-microservices-2022/python-deps \
  python3 tools/autosketch-comparison/prepare_alibaba.py \
  --directory /mydata/datasets/alibaba-microservices-2022/CallGraph \
  --intervals 20 --workers 2

PYTHONPATH=/mydata/datasets/alibaba-microservices-2022/python-deps \
  python3 -m unittest discover -s tools/autosketch-comparison \
  -p test_prepare_alibaba.py
```

```sh
PYTHONPATH=/mydata/datasets/alibaba-microservices-2022/python-deps \
  python3 tools/autosketch-comparison/project_alibaba.py \
  --directory /mydata/datasets/alibaba-microservices-2022/CallGraph \
  --intervals 240 --workers 2 --discard-new-intermediates

cargo +1.98.0 test -p data_plane --example alibaba_dashboard_comparison
cargo +1.98.0 build --release -p data_plane --example alibaba_dashboard_comparison
python3 tools/autosketch-comparison/run_alibaba.py \
  --directory /mydata/datasets/alibaba-microservices-2022/CallGraph \
  --output tools/autosketch-comparison/results/alibaba-dashboard-v1
```

Preparation stops before another archive if fewer than 12 GiB remain. The runner
waits for atomic per-file metadata, checkpoints immutable artifacts, and stops
on command errors, changed binaries, or invalid results. Its optional
`--publish-pr` flag is restricted to the full geometry and dedicated evaluation
branch; it publishes only after all 54 real runs pass artifact validation.
Accuracy violations remain in the report and do not by themselves invalidate a
measurement. Failed execution is not silently substituted with smoke evidence.

Correctness verification before full execution: seven Rust tests pass, including
all-workload measured-profile → ERP/AutoSketch plan → held-out replay fixtures
and independent exact scan versus cached exact-pane truth. Six Python tests pass
for malformed-row accounting, full-row identity, zero-preserving projection and
rejection of incomplete/smoke report evidence. These are implementation tests,
not performance measurements or independent external review.
