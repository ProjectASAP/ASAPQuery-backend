# Issue 701: windows, extrema, and composed quantile guarantees

Reviewed against backend main `674b9573` plus PR 700 and Planner PR 404
(`ea721e889b79e9ca22741a4d0370e9929bcf5b89`). The issue's original output was
produced by the removed v1 inspection path. This review uses executable code.

## Window identity and runtime reads

The current compiler derives pane and full-window alternatives from the query
lookback and evaluation cadence when the cadence divides the lookback. Thus a
300-second lookback evaluated every 30 seconds need not use tumbling 300-second
state. Complete workload cost evidence still determines the selected alternative.

A separate runtime defect remained: full-window snapshots of width 5 seconds,
sliding every second, were retrieved through an overlap scan and cumulatively
merged. A process TopK regression returned 1500 instead of 200 for one item.
The query binding now carries the full-window slide explicitly, validates it
against the installed producer, checks evaluation phase on the slide grid, and
reads only the snapshot ending at the requested evaluation time. Missing snapshots
and incompatible ranges fail closed. Pane reads retain non-overlapping composition.
Old full-window artifacts without this binding must be recompiled before install.

## min_over_time

Planner currently represents Min and Max using the same ExactKind::MinMax family.
The backend's materialized readout supports Max; the physical compiler filters
out unsupported Min state. The legacy non-composable lowering then attempted to
bind the removed state and failed with `materialized query has no compiled
executable DAG`.

The fix retains an explicit native fallback when all selected state lacks a
physical implementation. It does not reinterpret Min as Max or claim accelerated
Min support. Composable lowering retains its existing native dependency behavior.
Full accelerated Min needs an unambiguous Planner readout contract and matching
backend lowering, maintenance, and serving support.

## Quantiles and division

Multiple quantiles of the same population can share one maintained sketch.
The compiler regression covers q=0.5, 0.9, 0.95, and 0.99 with one materialization.
This does not imply the ratio inherits a component's relative-error bound.

If both nonzero quantile values have relative errors at most alpha, the ratio
of their estimates differs from the true ratio by at most
`2 * alpha / (1 - alpha)` in relative terms. No independence assumption is used.
At alpha=1%, this sufficient bound is about 2.0202%. A sufficient component bound
for a 1% ratio target is alpha <= `0.01 / 2.01`, about 0.4975%, together with a
valid nonzero-denominator/domain contract. Sharing a sketch alone supplies no
proof of cancellation. Probabilistic guarantees also need a joint success bound;
rank error, such as a KLL guarantee, is not relative value error.

Planner currently lacks this domain-aware division proof and conservatively
retains native execution. Even adding the formula would not make 1%-component
sketches satisfy a requested 1% ratio guarantee in general.

## Remaining integration failures

After the snapshot-read fix, the compatibility process suite reports 10 passing,
3 failing, and 1 previously ignored Collector-schema test. The failures are:

- Counter range execution rejects missing full-pane coverage.
- Finite persisted-summary drain reports unpublished summary windows.
- UnivMon producer observations do not select UnivMon on replanning.

These remain open; this change does not claim the complete compatibility matrix
passes. The three previously failing window/TopK-related executions include two
TopK algorithms and a multi-pane fixture whose explicit slide required updating.
No latency or end-to-end speedup measurement is claimed here.
