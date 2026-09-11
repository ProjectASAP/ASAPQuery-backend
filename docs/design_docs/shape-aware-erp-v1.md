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

### Observation payload migration

Runtime producers must replace the old single `shape` object with an
`observation` containing `cardinality`, `observed_events`, `fits`, and optional
`empirical_fingerprint`. Each fit supplies `family`, `parameters`,
`goodness_of_fit`, and `confidence`; shape-match policy must also supply the fit
quality and confidence thresholds. Old payloads are rejected rather than given
invented confidence. Publish a new observation after upgrading the producer.

The bounded observer models ranked key frequencies. Its output does not describe
the numeric spacing of KLL sample values and must not be advertised as a general
numeric-distribution observation. Equal frequencies produce the canonical
uniform fit only: Zipf exponent zero describes the same distribution and must
not create a false ambiguity. Near-uniform, genuinely distinct fits still pass
through the normal ambiguity policy.

### Population isolation in backend-local execution

A temporal scalar summary has one state per source series when its selected
`SummaryAgg` uses `Reduction::PerEntity`. An explicit reduction with no grouping
keys has one pooled population. These are different materializations even when
source, sketch parameters, and the visible grouping-key list are identical.

The compiler records shared `PopulationPartitioning` metadata in the runtime
configuration and DataDescriptor. Both identities include the partitioning;
installation checks it against the bound Planner DAG. Raw ingestion uses the
full source labels for per-entity routing and the configured grouping for pooled
routing. Memory estimates count per-entity states against source cardinality.
Legacy configurations without this metadata retain their existing routing rules.

This is a source-isolation contract, not permission to skip a maintenance update
expression. Only already-supported scalar update expressions pass the compiler's
per-entity admission check; other subDAG updates still require a real evaluator.
