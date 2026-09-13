# Shared additive window panes

The compiler generates and prices layouts from selected state requirements.
See [window planning](repeated-dashboard-panes.md) for the input contract and all
cadence examples.

After individual layout selection, backend-local raw SUM producers may share a
common-divisor pane. Compatibility preserves source, population predicate,
projection, accumulator parameters, grouping, partitioning, runtime policy,
accuracy and lifecycle unit-cost evidence. Evaluation intervals may differ;
each consumer's reads are charged at its own interval. Origins must agree modulo
the proposed common pane width.

The optimizer compares the original producers with one producer at the common
pane width, retained for the longest lookback. It recalculates build, maintenance,
retention, retirement and read costs for the new layout. Only a finite, strictly
cheaper group is installed. Derived maintenance inputs, explicitly quoted selected
layouts, non-additive states and independently selected complete-window layouts
are not rewritten. This is a bounded comparison of selected panes, not exhaustive
search over all layouts or hierarchical rollups.

The shared state's physical window is one pane. Logical readout windows remain
on the QueryPlan bindings. For `sum_over_time(a[1m]) / sum_over_time(a[10m])`
evaluated every minute, one 60s producer retains 11 published states, while its
readouts merge one and ten panes. For separate 60s/20s and 90s/30s demands, a
10s shared producer may win when maintenance is expensive; independent producers
remain when the extra reads or pane creation cost more.

Costs include published retained states and open worker accumulators. Complete
windows have at most `ceil(W/E)` open states; their rate-model average update
fanout is `W/E`, including sparse schedules. Pane producers have one active state.
These are supplied unit costs and structural estimates, not measurements.

If the entire compatibility group cannot share, the optimizer selects profitable
phase-compatible pairs, ordered by savings, and retries the remaining members.
An incompatible or expensive fine-cadence consumer therefore does not block reuse
between other consumers. This deterministic greedy selection does not claim a
globally optimal partition of all workload states.
