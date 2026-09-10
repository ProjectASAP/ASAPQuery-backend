# AutoSketch comparison: first executable slice

This runner starts E3 of the [evaluation plan (PR #545)](https://github.com/ProjectASAP/ASAPQuery-backend/pull/545).
It is a fixed-CMS **shared-calibration-table selection experiment**, not a full
ASAPPlanner, recurring-window, or native AutoSketch benchmark.

## What runs

- Actual `asap_sketchlib::CountMinSketch`, the portable f64 CMS used by the backend.
- The same 56 configurations for every method: widths 64 through 4096 in powers
  of two, depths 1 through 8. Objective: counter payload bytes, not process RSS.
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
  --events 10000 --cardinality 1000 --runs 10 --seed 42 --epsilon 0.01
cargo run --release -p data_plane --example autosketch_comparison -- \
  --output /tmp/cms-zipf.json --backend-revision "$(git rev-parse HEAD)" \
  --events 10000 --cardinality 1000 --runs 10 --seed 42 --epsilon 0.01 --zipf 1.2
```

Use new output paths: the runner refuses to overwrite artifacts. A failed run
may leave an empty file, not a successful report. The JSON records arguments,
source revision, Cargo.lock, build debug-assertion status, calibration rows,
ERP artifacts, visited configurations, selections, and held-out outcomes.
Debug runs are smoke evaluations, not publishable performance measurements.

## Adaptation boundary

The reference is [AutoSketch Algorithm 4 and Section 5.2](https://www.usenix.org/system/files/nsdi24-sun.pdf).
Seven initial points have distinct width and depth coordinates. Feasible paths
decrease width/depth; infeasible paths increase them. Paths stop at a feasibility
crossing. Evaluations are cached, and out-of-grid neighbors are excluded.
Infeasible branches costing at least the feasible incumbent are pruned.

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
