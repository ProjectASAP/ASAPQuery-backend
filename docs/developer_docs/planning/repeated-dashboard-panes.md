# Repeated dashboard window planning

Snapshot and HTTP planning use one `window_cost_model` input with an
`implementation_id`, an implementation-cost metadata template (`cost`), and
optional measured `quotes`. HTTP queries also declare `evaluation_phase_ms`;
snapshots obtain it from the canonical workload's `fixed_interval_at` demand.
The former snapshot `window_candidates` and HTTP `window_implementations` inputs
are removed. There are no compatibility aliases. The snapshot's former
`window_implementation_id` and `implementation_cost` move under the model as
`implementation_id` and `cost` respectively.

After logical selection, the compiler collects each maintained state's window
and executor constraints. It generates feasible layouts before pricing them.
Raw states offer `Pane { gcd(W,E) }` and, when distinct and supported, complete
windows advancing every E. A derived maintenance cohort and its sources offer
only the full, nonoverlapping cohort supported by that executor. Queries keep
their original W, E and evaluation phase regardless of storage representation.
Each state selects only candidates meeting its own requirements.

Measured quotes carry their workload fingerprint and exact window, slide and
layout. They replace the corresponding generated quote, or offer another legal
pane width, without bypassing capability checks. Serialization does not confer
compiler provenance: incoming quotes are always measured/provider evidence.
Unquoted layouts use supplied lifecycle unit costs multiplied by structural
counts. Layout changes never inherit another layout's measured scalar cost.
Workload-versus-exact deployment evidence is still required by snapshot version 2.

Temporal requirements are derived from PromQL. A range selector supplies its
own readout window; a rangeless instant-vector expression uses required
`implementation.scrape_interval_ms`. Snapshot `time_selection.lookback` is
rejected because it duplicates and can conflict with query semantics. A
subquery or positive offset extends only the furthest lookback: evaluated at
`t`, `a[1m] offset 1h` selects `(t - 61m, t - 60m]`, not a continuous
61-minute interval.

## Cases

All ranges below use PromQL's `(start,end]` convention. W is the lookback and E
is the query evaluation interval, both in seconds. Origins are zero unless stated.

| Case | Query demand | Storage and behavior |
|---|---|---|
| 1. E divides W | W=60, E=20 | 20s panes: at 60 read `(0,20]+(20,40]+(40,60]`; at 80 read `(20,40]+(40,60]+(60,80]`. Complete windows are a separately priced alternative. |
| 2. E does not divide W | W=60, E=45 | 15s panes: at 90 read `(30,45]+(45,60]+(60,75]+(75,90]`. A 60s tumbling fallback cannot answer this boundary. |
| 3. E equals W | W=60, E=60 | One 60s pane per read: `(0,60]`, then `(60,120]`. |
| 4. E is a larger multiple | W=60, E=120 | 60s panes answer `(60,120]` and `(180,240]`. Query cadence stays 120s. Some continuously stored panes are unused. |
| 5. E is larger, not a multiple | W=60, E=90 | 30s panes answer `(30,60]+(60,90]`, then `(120,150]+(150,180]`. Complete windows may alternatively skip the gaps. |
| 6. Shifted demand | W=60, E=20, phase=5 | At 65 read `(5,25]+(25,45]+(45,65]`. Phase is part of the physical state identity. |
| 7. Ad hoc off-grid read | W=60, 20s epoch panes, evaluation=67 | `(7,67]` cannot be tiled; serving returns a capability miss for exact fallback. It never rounds the requested interval. |
| 8. Cross-query reuse | A: W=60/E=20; B: W=90/E=30 | Compare independent 20s/30s panes with one 10s producer. A merges 6 instead of 3 panes, B merges 9 instead of 3. Share only when the saved maintenance outweighs extra build, retention and read costs. |
| 9. Pane larger than E | W=60, E=20, proposed pane=30 | Illegal: it answers at 60 but cannot tile `(20,80]`. Hierarchical rollups remain unsupported and are rejected. |
| 10. Executor restrictions | Collector or derived cohort | Collectors exclude partial-window panes and unsupported sparse full-window schedules. Derived maintenance keeps its required full cohorts. Unsupported backend-local leaves or off-grid reads use exact fallback; an unsupported strict Collector plan is rejected. |
| 11. Measured costs | 20s pane costs 8; complete window costs 12 | Select the cheaper feasible quote. Rewriting into a shared 10s producer cannot reuse the measured cost 8. Explicitly quoted selected producers are protected from automatic repricing. |

Runtime layouts currently have whole-second precision. Fractional cadences are
not rounded: automatic backend-local planning leaves them to exact execution.
The integer cases above preserve E even when E exceeds W.

## Serving and verification

Bindings distinguish disjoint pane width from complete-window slide. Pane reads
require both boundaries on the pane grid. Complete-window reads require exactly
one window of width W, starting on its slide grid; the compiler converts the
query's end phase to the corresponding start phase. Sketch overlap lookup is
restricted to that window's end, so neighboring overlapping states are not merged.
Missing/open panes and unaligned boundaries remain capability misses. Sparse
series are not assumed to contain empty zero-valued panes without evidence.

Tests cover the five cadence relations, shifted phases, runtime bucket assignment,
raw-sample sums, off-grid misses, costed sharing, measured quotes and target
capabilities. The process dashboard test derives 5s panes from a 5s cadence for
10s lookbacks; it no longer injects a fabricated zero-cost candidate.
