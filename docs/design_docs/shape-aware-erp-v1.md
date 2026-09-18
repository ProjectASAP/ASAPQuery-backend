# Shape-aware ERP v1

Status: implemented planning contract. Audience: developers producing runtime
shape observations or matching Error–Resource Profile (ERP) evidence.

## Document map

1. [Design at a glance](#design-at-a-glance)
2. [Worked example](#worked-example)
3. [Observation and matching contract](#observation-and-matching-contract)
4. [Cost composition](#cost-composition)
5. [Miss and fallback behavior](#miss-and-fallback-behavior)
6. [Payload migration](#payload-migration)
7. [Population isolation](#population-isolation)

## Design at a glance

```text
runtime samples ──► bounded shape observer ──► ErpShapeObservation
                                                   │
benchmark ERP records ─────────────────────────────┤
                                                   ▼
                                  compatible evidence or miss
                                                   │
                                                   ▼
                                  Planner cost and parameter choice
```

The observer retains uniform and fitted Zipf hypotheses. Planner matches
admissible hypotheses to compatible benchmark records. A fit score is a heuristic
measure of shape similarity, not a confidence interval or sketch-error guarantee.

## Worked example

```yaml
observation:
  cardinality: 10000
  observed_events: 1000000
  empirical_fingerprint: null
  fits:
    - family: zipf
      parameters: {exponent: 1.1}
      goodness_of_fit: 0.03
      confidence: 0.92

match_policy:
  max_goodness_of_fit: 0.05
  min_confidence: 0.90
  max_cardinality_ratio: 2.0
```

If a fresh KLL benchmark record covers a compatible cardinality, event count,
parameters and Zipf fit, Planner uses its measured atomic update, merge and query
costs. A poorer fit, ambiguous hypothesis or excessive cardinality distance is a
miss. Custom data first matches an exact dataset fingerprint when one is present.

For a five-minute query using one-minute panes, five query evaluations and one
shared materialization, the abstract operation counts are:

```text
updates = observed updates
merges  = 5 * (ceil(5m / 1m) - 1) = 20
queries = 5
state   = retained panes * 1
```

Planner multiplies those counts by compatible measured atomic costs; the runtime
observation does not directly claim total latency.

## Observation and matching contract

`erp_observed_shape.observation` uses Planner's shared `ErpShapeObservation` and
contains cardinality, observed event count, candidate fits and an optional dataset
fingerprint. The wrapper also records burst ratio; `observed_shape_source`
selects the observation ring.

Each fit reports family, parameters, goodness of fit and confidence. Total-
variation distance is computed against observed rank masses with sample-count-
adjusted scoring. Unnamed or mixed distributions are not forced into one family.
Equal frequencies produce only the canonical uniform fit; Zipf exponent zero must
not create false ambiguity.

Key count, key length and occupied interval count are bounded. Sparse interval
IDs do not allocate dense vectors. Overflow or a cap violation invalidates the
entire observation window; callers start a fresh observer instead of publishing
a biased partial snapshot.

## Cost composition

For pane width `p`, query window `w`, query executions `q`, updates `u`, retained
panes `r` and shared materializations `m`:

```text
updates = u * m
merges  = q * (ceil(w / p) - 1)
queries = q
state   = r * m
CPU     = updates*Cupdate + merges*Cmerge + queries*Cquery
```

This separates machine-specific atomic measurements from workload-specific
window, retention and sharing decisions.

## Miss and fallback behavior

Profiles with too few events, poor fit, ambiguous confidence, stale evidence or
excessive cardinality/parameter distance are misses. Malformed evidence also
fails matching.

- **Hybrid mode:** retain theoretical parameters; choose exact execution if the
  runtime cannot deploy them or they exceed its memory limit.
- **Empirical-only mode:** fail closed when compatible evidence is unavailable.

## Payload migration

Runtime producers replace the old single `shape` object with `observation`,
including `cardinality`, `observed_events`, `fits` and optional
`empirical_fingerprint`. Match policy supplies fit-quality and confidence
thresholds. Old payloads are rejected rather than assigned invented confidence.

This ranked-frequency observation does not describe numeric spacing of KLL values
and must not be advertised as a general numeric-distribution model.

## Population isolation

`Reduction::PerEntity` creates one temporal scalar state per source series. An
explicit reduction with no grouping keys creates one pooled population. They are
different materializations even when source, parameters and visible grouping keys
look identical.

The compiler stores `PopulationPartitioning` in runtime configuration and
`DataDescriptor`; both identities include it. Installation checks it against the
bound Planner DAG. Ingestion uses full source labels for per-entity routing and
configured grouping for pooled routing. Memory estimates charge per-entity state
against source cardinality.

This isolation contract does not implement arbitrary maintenance expressions.
Only supported scalar update expressions pass per-entity admission; other subDAG
updates still require an evaluator.
