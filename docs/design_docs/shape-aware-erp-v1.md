# Shape-aware ERP v1

The backend obtains an observed shape from the live runtime-samples feedback
path and asks ASAPPlanner for the nearest compatible benchmark profile. A
runtime record carries `erp_observed_shape.observation` using Planner's shared
`ErpShapeObservation`: cardinality, observed event count, candidate fits and an
optional dataset fingerprint. The wrapper also reports burst ratio. The planning request
selects the ring via `observed_shape_source`.

The observer retains both uniform and fitted Zipf hypotheses, with total-variation
distance against the observed rank masses and a sample-count-adjusted fit score.
The score is a heuristic fit quality, not a statistical confidence interval or
sketch-error guarantee. Unnamed and mixed distributions are not forced into one
family. Planner jointly matches admissible hypotheses against ERP records.
Custom data first matches its fingerprint when available, otherwise it can use
the same bounded shape matching as synthetic data.

Key count, key length and occupied interval count are bounded. Sparse interval
IDs do not allocate a dense vector. Any overflow or cap violation permanently
invalidates that observation window; callers must start a fresh observer rather
than publishing a biased partial snapshot. Profiles with too few benchmark
events, poor fit, ambiguous confidence, or excessive cardinality/parameter
distance are misses.

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
