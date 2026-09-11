# UnivMon readout evidence through production execution

The backend ERP adapter now selects evidence by the logical readout. Sharing a
UnivMon state does not give all of its readouts the same accuracy guarantee.

| Readout | Artifact metric | Bound units |
| --- | --- | --- |
| Distinct | `max_cardinality_relative_error` | Relative distinct-count error |
| Frequency L2 | `max_frequency_l2_relative_error` | Relative error of `sqrt(sum frequency(value)^2)` |
| Frequency entropy | `max_frequency_entropy_absolute_bits_error` | Absolute Shannon entropy error, in bits |
| Count | No empirical metric needed | Exact total unit weight |

Sizing maps `AggIntent` to this contract; accuracy validation maps `SketchQuery`
to the same contract. Matching artifact rows still pass implementation, runtime
parameters, trial count and bounded shape checks. Missing readout evidence leaves
that readout to theoretical sizing or exact fallback. It cannot borrow another
readout's measurement. ERP v1 does not calibrate failure probability, so an
explicit epsilon/delta requirement cannot use these observations as a confidence
bound.

The existing `error_metric` field remains available to generic ERP adapter
callers. Production KLL and UnivMon readouts use their canonical typed metric
mapping. The recognized UnivMon implementation tag is
`asap-sketchlib-univmon-standard-v1`: standard unit-frequency updates over sample
value identities. Terminal promotion updates and numeric-value L2 have different
semantics and are not covered.

## Reproduce the correctness fixture

Run from the repository root:

```sh
cargo test -p data_plane --test asapquery_compatibility_process_e2e measured_readout_evidence -- --nocapture
```

The test measures two configurations using the real runtime accumulator, ten
separate key populations, 128 distinct values and 1,338 unit updates per trial.
Each trial merges two panes. The artifact records maximum observed error and
serialized retained state size; CPU is not measured and its objective weight is
zero. These are local correctness measurements, not a throughput, latency,
resident-memory or general accuracy certification.

| Configuration | Distinct relative error | L2 relative error | Entropy absolute bits error |
| --- | ---: | ---: | ---: |
| heap 8, columns 128, rows 5, layers 4 | 0.984375 | 0.0179301 | 0.545845 |
| heap 128, columns 1024, rows 5, layers 4 | 0.0078125 | 0.00003754 | 0.003554 |

The observer fits the held-out frequency population and the normal Planner
chooses configurations. With a 0.2 bound in each readout's stated units:

- Distinct selects HLL with precision 10 through its existing theoretical model.
- Frequency L2 selects the smaller UnivMon configuration.
- Entropy selects the larger UnivMon configuration.

The resulting physical plan is installed in a real backend process. Remote
write feeds held-out keys, the production drain endpoint completes finite replay,
and all three queries execute through the warm ASAP path. The fallback service
only implements health checks, so it cannot supply query results. Held-out errors
were 2.054% for HLL distinct, 0.7304% for UnivMon L2, and approximately
3.55e-15 bits for entropy. This demonstrates selection and execution, not speedup.

Removing only the entropy metric from the artifact makes entropy fall back while
preserving the L2 materialization. The test emits `UNIVMON_MEASURED`,
`UNIVMON_PLANNED`, and `UNIVMON_WARM` JSON records containing the artifact, observed
shape, selected parameters, installed DAG identities, lifecycle estimates and
actual query results.

The three queries need different physical configurations in this fixture; they
do not demonstrate a single shared state. Planner may share compatible equal-
parameter states. The finite replay barrier also does not establish continuous
multi-worker population completeness, which is a separate publication contract.

Finite runtime observations carry bounded empirical frequency counts and temporal
interval counts through the shared metadata contract. The data plane does not
classify distributions or depend on Planner fitting types; the control plane
fits these counts before profile matching. Raw keys are absent from feedback.
An accepted catalog activation establishes the observer generation. Delayed
samples from another generation are ignored and cannot reset current counts.
