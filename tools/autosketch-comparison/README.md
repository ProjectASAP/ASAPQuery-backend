# AutoSketch comparison: first executable slice

This runner starts E3 of the [evaluation plan (PR #545)](https://github.com/ProjectASAP/ASAPQuery-backend/pull/545).
It is a memory-constrained frequency/membership **shared-calibration-table selection experiment**, not a full
ASAPPlanner, recurring-window, or native AutoSketch benchmark.

## What runs

- Actual `asap_sketchlib::CountMinSketch`, `CountSketch`, and packed-bit `Bloom`
  implementations. No sketch implementation is duplicated in the backend.
- `--query frequency` admits CMS and Count Sketch; `--query membership` admits
  Bloom. Incompatible families are filtered before measurement because their
  result semantics and error metrics are not interchangeable.
- Up to 56 configurations per family: widths 64 through 4096 in powers of two,
  depths 1 through 8. All methods see the same budget- and legality-filtered grid.
  Portable Count Sketch requires `depth * (log2(width) + 1) <= 64`; CMS supports
  larger hash layouts automatically.
- `--memory-budget-bytes` is a hard cap on the selected sketch's f64 counter or
  packed-bit payload, applied **before calibration and selection**. It is not an RSS cap or
  a multi-window total-memory constraint. Object/allocator headers and transient
  query scratch space are excluded. No populated key sidecar is used by these
  point-frequency updates. Objective remains minimum sketch payload bytes.
- `--sketches cms,count-sketch,bloom` registers all families by default, after
  which `--query` removes incompatible candidates. Each legal family gets
  an independent search frontier; the cheapest feasible result wins globally.
  Use `--sketches cms` or `--sketches count-sketch` for family ablations.
- Frequency uses an exact oracle across all declared keys and maximum
  `abs(estimate - truth) / events`. Membership checks zero false negatives and
  measures FPP over a disjoint absent-key set.
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
  --query frequency --sketches cms,count-sketch,bloom --memory-budget-bytes 4096
cargo run --release -p data_plane --example autosketch_comparison -- \
  --output /tmp/cms-zipf.json --backend-revision "$(git rev-parse HEAD)" \
  --events 10000 --cardinality 1000 --runs 10 --seed 42 --epsilon 0.01 --zipf 1.2 \
  --query frequency --sketches cms,count-sketch,bloom --memory-budget-bytes 4096

# Membership configuration using measured false-positive rate
cargo run --release -p data_plane --example autosketch_comparison -- \
  --output /tmp/bloom.json --backend-revision "$(git rev-parse HEAD)" \
  --query membership --sketches bloom --epsilon 0.01 --memory-budget-bytes 4096
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

## Repeated-window execution slice

`repeated_window_comparison` measures the systems distinction that the
single-window selector cannot: four recurring `{1m, 5m, 15m, 1h}` frequency
queries over one-minute panes. It uses one fixed, identical
`asap_sketchlib::CountMinSketch` configuration for all sketch methods:

- `autosketch_per_query`: the natural query-local deployment, one retained pane
  collection per query;
- `asap_no_sharing`: deliberately the same physical layout, to keep sketch
  configuration separate from sharing benefits;
- `asap_full_shared`: one retained pane collection shared across all windows;
- `exact_raw`: retained raw keys and exact scans.

```sh
cargo test -p data_plane --example repeated_window_comparison
cargo run --release -p data_plane --example repeated_window_comparison -- \
  --output /tmp/repeated-window.json --backend-revision "$(git rev-parse HEAD)" \
  --panes 120 --events-per-pane 5000 --cardinality 1000 \
  --width 256 --depth 4 --trials 7 --query-repetitions 10
```

This is a deterministic standalone execution experiment, not a claim that the
AutoSketch paper implements window sharing, nor an end-to-end Planner/control-
plane benchmark. Methods execute sequentially, so compare medians and structural
counts; do not interpret wall time as isolated CPU time. See
[executed results](repeated-window-results.md).

## Adaptation boundary

The reference is [AutoSketch Algorithm 4 and Section 5.2](https://www.usenix.org/system/files/nsdi24-sun.pdf).
Seven initial points per family have distinct width and depth coordinates. Feasible paths
decrease width/depth; infeasible paths increase them. Paths stop at a feasibility
crossing. Evaluations are cached, and out-of-grid neighbors are excluded.
Infeasible branches costing at least the feasible incumbent are pruned.
Illegal/over-budget seeds and neighbors are discarded. Each family also gets its
smallest legal in-budget point, so tight budgets cannot accidentally eliminate
the entire family through LHS filtering. A budget admitting no legal configuration
produces explicit `no_feasible_configuration` outcomes.

Sketch selection considers every query-compatible registered family and performs
parameter search within each. It is a software adaptation, not the
paper's separate sampled family-preselection phase. CMS and Count Sketch use
the same empirical absolute additive-error metric on nonnegative updates; this
does not equate their theoretical L1/L2 guarantees. Bloom uses empirical FPP and
is never compared to frequency sketches as if their errors were equivalent. No
P4 code is generated.

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
3. Connect the standalone recurring-window runner to generated Planner physical
   identities and independently scheduled query periods.
4. Catalog coverage and sketch-family ablations. No performance or breadth claim
   is established by this starter runner.
