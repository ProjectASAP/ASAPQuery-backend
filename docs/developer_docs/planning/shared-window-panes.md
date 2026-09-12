# Shared additive window panes

The fallback window provider prices each query range. After logical selection,
raw inputs of derived maintenance programs use full, non-overlapping windows,
matching the current maintenance executor. Explicit `window_candidates` remain
authoritative and are not rewritten.

For backend-local raw SUM states, the backend offers compatible selected pane
producers to ASAPPlanner's `pane_sharing::select_shared_panes`. The compatibility
key preserves source, population predicate, projection, accumulator parameters,
grouping, partitioning, pane width/origin, runtime policy and lifecycle evidence.
Planner compares independent producers with one producer retained for the longest
lookback, charging every readout. Only beneficial groups are installed.

The shared state's physical window is one pane. Logical readout windows stay on
their original QueryPlan bindings. Retention uses the largest bound lookback.
For `sum_over_time(a[1m]) / sum_over_time(a[10m])` evaluated every minute, one
60-second producer retains 11 published states; readouts merge one and ten panes.
Different metrics, predicates or phases remain separate. Derived inputs, explicit
window quotes, non-additive state and independently selected FullWindow layouts
are not rewritten. This pass reuses compatible selected panes; it does not search
all possible pane widths or re-optimize FullWindow choices jointly.

Layout pricing charges both published retained states and open worker
accumulators. A full window of width W and step S keeps ceil(W/S) open states;
a pane producer keeps one. These residency costs are separate from update CPU.
Nonfinite arithmetic remains invalid evidence rather than becoming a zero quote.

## Cross-repository validation

The backend pins Planner commit `ca7546de792d74aee8231e9a1100ca893d9e86d3`, which provides
`pane_sharing::select_shared_panes`. Normal builds use the Git dependency.
For coordinated local development, the optional validation script exports only
the optimizer crate while retaining the pinned IR/frontend revision and restores
Cargo.lock after the run.

```sh
python3 tools/test_shared_panes.py --planner /path/to/ASAPPlanner -- \
  test -p control_plane
python3 tools/test_shared_panes.py --planner /path/to/ASAPPlanner \
  --sketchlib /path/to/compatible/asap_sketchlib -- \
  test -p data_plane --lib compiled_shared_sum_panes_preserve_each_lookback
```

Use `--toolchain 1.98.0` if needed in the development environment. The data-plane
checkout requires sketchlib's standard-update guard and interpolated quantile
interfaces; validation used revision `8c03d7c` for those existing dependencies.
