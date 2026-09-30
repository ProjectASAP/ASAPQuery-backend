# Complete workload cost evidence

Planning snapshots use one schema, `snapshot_version: 3`. Versions 1 and 2 are rejected; the environment must declare its logical dataset identity.
Candidate discovery may omit `workload_cost_evidence`; compiling a deployable
snapshot requires complete, valid quotes and selects by complete workload cost.
There is no unquoted snapshot deployment path. The checked-in JSON examples are
discovery templates, not ready-to-deploy plans.

## Workflow

1. Prepare the canonical workload, deployment capabilities and implementation
   evidence as in `asapquery-planning-snapshot.json`. Set version 3.
2. Obtain requirements without deploying:

   ```sh
   cargo run -p control_plane --example workload_cost_manifest -- snapshot.json
   ```

   For distributed deployment, POST the normal compile-and-publish request,
   without quotes, to `/api/v1/physical-plan/cost-manifests`. This endpoint only
   compiles; it does not stage, publish to Collectors, or activate a generation.
   It returns manifests for the compilable candidates in this runtime profile.
3. Have the deployment's cost/capability provider price each manifest. Attach a
   `workload_cost_evidence` object to the snapshot or publication request:

   ```json
   {
     "data_snapshot_id": "deployment-input-generation-42",
     "model_version": "calibrated-provider-v1",
     "observed_at_unix_ms": 1780000000000,
     "valid_for_ms": 60000,
     "quotes": [{
       "manifest": "REPLACE with the complete returned manifest object",
       "executable": true,
       "unit_costs": { "REPLACE with every component ID": 1.0 }
     }]
   }
   ```

   This is a structural illustration, not runnable or calibrated evidence.
   `unit_costs` must contain exactly the manifest's component keys. A quote
   declares feasibility against the capability generation, including raw-data
   access and the exact service when a candidate uses native fallback.
4. Compile/start or publish. The backend selects the cheapest completely quoted,
   feasible candidate. The live response includes `cost_comparison`; startup
   logs the same report. Preserve it with the deployment's evidence records.

## What is priced

This is a flat coverage manifest over the existing physical projection, not
another semantic DAG. Planner supplies the legal post-ASAP computations; costs
can change which bound workload is committed, not rewrite its semantics.

- Source ingestion once per source/location over the horizon. Exact fallback
  includes every named input metric, including both sides of binary expressions;
  repeated references to a metric share one upkeep component. Queries whose
  sources cannot be fully enumerated are unavailable for complete costing.
- Each shared state's build, update/merge, residency/spill and retirement once
  per actual location, independent of the number of result consumers.
- Each Collector transmission rule over the horizon.
- Every reachable query operator, including reads, arithmetic, reductions and
  complete engine-native fallback, multiplied by the query's recurrence.
- Each result's output cost. Exact-service input upkeep/storage has a separate
  horizon component and is not silently omitted from a raw alternative.

All quotes use one provider model's common cost units. A horizon quote includes
the complete stated partition's work, data volume/cardinality, maintained
groups and retention. Do not reuse a global ingestion rate as a per-metric rate.
Per-evaluation quotes exclude upkeep already charged in horizon components.
Sunk infrastructure may explicitly cost zero under the provider's documented
decision boundary; unknown costs may not.

The manifest binds query requirements, demand, horizon, implementation details,
Planner revision, plan identity/version and capability generation. Evidence also
names its input generation, model and validity interval. A provider must refresh
quotes when those data assumptions change; this API does not discover or
authenticate telemetry itself. Freshness is evaluated at the supplied snapshot
decision time for replay, and at server time for live requests.

Missing, duplicate, negative, nonfinite, stale, mismatched or infeasible quotes
make a candidate unavailable. If no completely quoted candidate remains, the
decision fails closed. Tests use explicit synthetic unit costs, not production
measurements.

## Supported migration scope

The default inventory compares the Planner-selected workload with its
whole-workload exact fallback, maintained-population variants, and one
substitution per native physical realization that Planner's PlanSpace proposes
for a query (for example a heap over per-series Rate readouts). Planner's
global selection, not a quote per alternative, decides every other summary
choice such as the sketch family. Within a candidate, whether each summary state
is precomputed or rebuilt at query time is not a separate candidate: the
compiler chooses a summary-maintenance lifecycle per unique state from
`implementation.lifecycle_costs`, pricing shared state once. Continuously
maintained state costs build, per-update maintenance over the ingestion rate,
reads, retention and retirement; retention adds the state's estimated bytes
times its retained panes times `store_per_byte_second` (default 0). An
ephemeral state costs build, read and retirement per read, and is offered only
when the deployment can read raw series from Prometheus at query time (not
under `require_backend_local_execution`). A query may retain admissible native
branches beside raw inputs when their bounded-lag mixed assignment costs less;
otherwise its states retain their group placement. With no retained state it
runs natively over raw series. The manifest of the
resulting placement is quoted like any other. A state built from another
retained state's readouts (such as that heap) is placed the same way: retained,
it is maintained over complete per-series states each window; ephemeral, it is
rebuilt per query from the readouts. This does not
claim exhaustive search over every lifecycle, engine or Planner algorithm.
Retention prices use the chosen installed layout, including downstream
maintenance lookbacks and the configured retention margin. Raw additive
states within one query that share the same generated panes charge their
producer and longest retention once, while preserving each logical read cost.

`implementation.source_ingestion_rates` optionally maps metric names to
samples-per-second evidence, using the same `Evidence<Rate>` format as
`data_workload.ingestion_rate`. Fresh observations price both retained
maintenance and raw selector folds. Rates must be finite and nonnegative.
Missing or expired metric evidence uses the workload-wide rate as a
conservative bound; label filters do not imply an invented selectivity.

An exact alternative without an accessible native backend is unavailable even
if its numeric quote would be cheap.

This provider-priced binding boundary deliberately avoids inventing physical
statistics to populate Planner's generic physical-formula provider. It completes
cost selection for the supported execution profile; production calibration and
additional provider implementations remain deployment work.
