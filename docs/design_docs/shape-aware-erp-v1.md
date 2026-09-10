# Shape-aware ERP v1

The backend obtains an observed shape from the live runtime-samples feedback
path and asks ASAPPlanner for the nearest compatible benchmark profile. A
runtime record carries `erp_observed_shape` with cardinality, optional fitted
Zipf exponent, observed event count, and burst ratio. The planning request
selects the ring via `observed_shape_source`.

The edge observer counts sampled keys in a bounded map, fits the slope of the
log-rank/log-frequency curve, and records per-interval traffic. Exceeding the
cardinality cap is an error; it is never reported as a smaller cardinality.
Uniform and Zipf shapes do not cross-match. Profiles with too few benchmark
events or excessive log-cardinality/skew distance are misses.

On a hit, empirical parameters and measured atomic costs are used. On a miss,
malformed evidence, or drift, Hybrid mode retains the theoretical parameters;
if the runtime cannot deploy them or they exceed its memory limit, compilation
chooses exact execution. Empirical-only mode fails closed.

For a pane width `p`, query window `w`, query executions `q`, updates `u`,
retained panes `r`, and shared materializations `m`, Planner composes:

```text
updates = u * m
merges  = q * (ceil(w / p) - 1)
queries = q
state   = r * m
CPU     = updates*Cupdate + merges*Cmerge + queries*Cquery
```

This separates machine-specific atomic measurement from workload-specific
window planning and makes tumbling, sliding/pane, retention, and sharing costs
auditable.
