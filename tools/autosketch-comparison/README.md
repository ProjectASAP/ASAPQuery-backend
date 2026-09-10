# AutoSketch comparison: first executable slice

This runner starts E3 of the [evaluation plan (PR #545)](https://github.com/ProjectASAP/ASAPQuery-backend/pull/545).
It is a memory-constrained CMS/Count Sketch **shared-calibration-table selection experiment**, not a full
ASAPPlanner, recurring-window, or native AutoSketch benchmark.

## What runs

- Actual `asap_sketchlib::CountMinSketch` and `CountSketch` implementations,
  plus `asap_sketchlib::BloomFilter` and `CountingBloomFilter` for the paper's
  distinct operator (asap_sketchlib PR #94).
- Up to 56 configurations per family: widths 64 through 4096 in powers of two,
  depths 1 through 8. All methods see the same budget- and legality-filtered grid.
  Portable Count Sketch requires `depth * (log2(width) + 1) <= 64`; CMS supports
  larger hash layouts automatically.
- `--memory-budget-bytes` is a hard cap on f64 counter payload for the selected
  sketch, applied **before calibration and selection**. It is not an RSS cap or
  a multi-window total-memory constraint. Object/allocator headers and transient
  query scratch space are excluded. No populated key sidecar is used by these
  point-frequency updates. Objective remains minimum counter payload bytes.
- `--sketches cms,count-sketch` enables both families (default). Each family gets
  an independent search frontier; the cheapest feasible result wins globally.
  Use `--sketches cms` or `--sketches count-sketch` for family ablations.
- Exact frequency oracle across all declared keys, including absent keys.
  Error is maximum `abs(estimate - truth) / events` over those keys.
- A software AutoSketch-style LHS/neighbor search, the real ASAPPlanner
  `ErpArtifact::select`, and an independently enumerated finite-grid oracle.
- Separate calibration/test data seeds per run. Selection occurs before test
  data generation. The CMS hash function is fixed by the library; varying data
  seeds is **not** independent sketch-hash replication.
- Uniform or truncated Zipf event generation representing one complete window
  snapshot. No event-time/window machinery or query frontend runs yet.

## Run

The repository's normal Rust dependencies, including the sibling ASAPCollector
and sketch-library checkout, are required. No new dependencies are introduced.

```sh
cargo test -p data_plane --example autosketch_comparison
cargo run --release -p data_plane --example autosketch_comparison -- \
  --output /tmp/cms-uniform.json --backend-revision "$(git rev-parse HEAD)" \
  --events 10000 --cardinality 1000 --runs 10 --seed 42 --epsilon 0.01 \
  --sketches cms,count-sketch --memory-budget-bytes 4096
cargo run --release -p data_plane --example autosketch_comparison -- \
  --output /tmp/cms-zipf.json --backend-revision "$(git rev-parse HEAD)" \
  --events 10000 --cardinality 1000 --runs 10 --seed 42 --epsilon 0.01 --zipf 1.2 \
  --sketches cms,count-sketch --memory-budget-bytes 4096
```

Use new output paths: the runner refuses to overwrite artifacts. A failed run
may leave an empty file, not a successful report. The JSON records arguments,
source revision, Cargo.lock, build debug-assertion status, calibration rows,
ERP artifacts, visited configurations, selections, and held-out outcomes.
Debug runs are smoke evaluations, not publishable performance measurements.

Summarize without excluding failures:

```sh
python3 -m unittest discover -s tools/autosketch-comparison -p 'test_*.py'
python3 tools/autosketch-comparison/summarize.py /tmp/cms-uniform.json /tmp/cms-zipf.json
```

See [initial smoke results](smoke-results.md) for the first executed runs,
including held-out accuracy failures.

## Adaptation boundary

The reference is [AutoSketch Algorithm 4 and Section 5.2](https://www.usenix.org/system/files/nsdi24-sun.pdf).
Seven initial points per family have distinct width and depth coordinates. Feasible paths
decrease width/depth; infeasible paths increase them. Paths stop at a feasibility
crossing. Evaluations are cached, and out-of-grid neighbors are excluded.
Infeasible branches costing at least the feasible incumbent are pruned.
Illegal/over-budget seeds and neighbors are discarded. Each family also gets its
smallest legal in-budget point, so tight budgets cannot accidentally eliminate
the entire family through LHS filtering. A budget below 512 bytes admits no
configuration and produces explicit `no_feasible_configuration` outcomes.

Sketch selection exhaustively considers both registered frequency families and
performs parameter search within each. It is a software adaptation, not the
paper's separate sampled family-preselection phase. CMS and Count Sketch use
the same empirical absolute additive-error metric on nonnegative updates; this
does not equate their theoretical L1/L2 guarantees. No P4 code is generated.
Bloom rows use membership false-positive error rather than frequency error and
must be evaluated with a distinct-membership workload; they are not directly
comparable to CMS/Count Sketch frequency rows.

Differences from the hardware algorithm are intentional and must be retained in
results: there is no stage dimension, ALU budget, or 16-KiB page alignment;
neighbors halve/double width or change depth by one. Incumbent pruning is
conservative: a costly feasible node can still lead to a cheaper neighbor. The
runner reports no feasible configuration explicitly, never best effort as a
success. This heuristic is not claimed to find every non-monotone feasible
region or the global optimum.

## How to interpret results

The complete calibration grid is built once per run and shared. ERP therefore
should match the grid oracle; that is a contract check, not a research result
showing superior optimization. Search's visited count measures table evaluations,
not saved real benchmark time in this version. Selection wall time excludes
calibration; total grid calibration time is reported separately. Update/query
timings are wall-clock observations, not CPU time. The memory-only ERP records
use zero CPU fields explicitly as unused placeholders, not measured free work.

The `asapplanner_erp_selector` label does not mean measured CMS parameters have
passed full workload selection, physical compilation, or serving. CMS empirical
accuracy-contract integration is still needed. There is no theoretical-sizing
baseline yet because its confidence contract must be defined separately.

## Next gates

1. Lazy real benchmark callbacks and fair first-use/amortized accounting; multiple
   accuracy seeds/windows per profile and stronger search regression fixtures.
2. CMS ERP accuracy mapping through full Planner and backend execution.
3. Recurring {1m, 5m, 15m, 1h} workloads, independent query periods, and verified
   PerQuery/NoSharing/Full physical identities and exact-window answers.
4. Catalog coverage and sketch-family ablations. No performance or breadth claim
   is established by this starter runner.
