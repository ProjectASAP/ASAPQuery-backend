# ERP deployment integration

The compile-and-publish request and backend-local planning snapshot may carry
an optional `erp` object. It contains the versioned sketch-bench artifact, the
deployment's current distribution descriptor, workload operation counts,
resource weights, error metric, minimum trial count, accuracy mode, and runtime
capabilities.

Planning is query-workload-aware: ASAPPlanner still owns legal families and the
materialization DAG, while the backend adapter translates the selected ERP
parameter JSON into its typed runtime `SketchParams`.

Hybrid mode has three observable outcomes:

1. exact distribution and implementation match, sufficient trials, acceptable
   measured error, and supported parameters: use the least-cost ERP point;
2. ERP miss, invalid artifact, unparseable parameters, or distribution drift:
   use ASAPPlanner's theoretical parameters when runtime capabilities accept
   them; and
3. neither empirical nor theoretical state is deployable: preserve the
   pre-ASAP subtree for exact execution.

Empirical mode does not silently claim a theoretical fallback: an ERP miss
goes directly to exact execution. Selection and fallback reasons are emitted as
structured tracing events. ERP observations remain empirical and must not be
rendered as formal `(epsilon, delta)` guarantees.

ERP v1 matches the complete distribution JSON by equality. A caller detects
drift by supplying its latest descriptor with every new plan generation. A
changed descriptor cannot reuse the old profile accidentally.

## Validation and current scope

This section is for developers validating the deployment contract. Workload
selection must use the empirical accuracy model as well as the ERP cost model;
otherwise the default theoretical bound rejects the smaller measured state.
The deployment currently admits empirical accuracy only for epsilon-only KLL
rank queries using `kll-percall` / `lib` and `max_rank_err`. Other metrics,
implementations, and explicit `(epsilon, delta)` requests take the mode's
theoretical/exact fallback. Empirical provenance carries an unknown failure
probability, never an invented confidence guarantee.

The checked-in [measured fixture](../tests/fixtures/erp-kll-measured.json)
comes from sketch-bench revision `dedc2d4`: KLL k=32, 1,000 uniform samples,
seed 42, and measured maximum rank error 0.051. Its ten resource trials are
**not** ten independent accuracy trials (`accuracy_runs` is one). Resource
timings are machine-specific. This is a reproducible integration fixture,
not evidence of a distribution-independent error bound.

Run from the backend repository:

```sh
cargo test -p control_plane measured_erp_kll_parameters_survive_workload_selection
cargo test -p data_plane --test asapquery_compatibility_process_e2e erp_measured_kll_collector_to_query_oracle
cargo test -p data_plane --test all_sketches_process_oracle_e2e --test e2e_controller_plans_and_backend_serves --test asapquery_compatibility_process_e2e
```

The first regression checks k=32 survives workload selection and physical
compilation; descriptor drift, implementation mismatch, and explicit delta
requirements select theoretical sizing with a distinct materialization
identity; an unsupported runtime preserves the exact subtree.

The process test installs a real physical plan, runs the Collector's KLL
precompute runtime over a separate deterministic 1,000-value input, sends its
compacted portable state through OTLP to the backend binary, and checks a warm
PromQL result against an exact rank oracle (error <= 0.06). It has no exact
fallback server. It covers the supported distributed-collector path, not an
ERP-enabled raw-ingest deployment or an online drift detector. The older
transport fixtures install explicit QueryPlan/SummaryCatalog bindings and
exercise actual frame validation; they do not substitute for planner E2E.

To regenerate the benchmark fixture with the matching sketch-bench revision,
use a new report path for each run (benchmark reports append):

```sh
approxbench sketchbench --variant kll-percall --library lib --config 'k=32' --dataset uniform --size 1000 --cardinality 1 --dtype f64 --runs 10 --warmup-runs 1 --operations insert,query,merge --metrics throughput,cpu,memory --report erp.raw.jsonl
approxbench sketchbench --variant kll-percall --library lib --config 'k=32' --dataset uniform --size 1000 --cardinality 1 --dtype f64 --runs 10 --warmup-runs 1 --operations query --metrics accuracy --report erp.raw.jsonl
approxbench flatten erp.raw.jsonl --output erp.flat.jsonl
approxbench erp erp.flat.jsonl --producer-version dedc2d4 --output erp.json
```
