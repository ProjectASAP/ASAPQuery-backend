# Offline sketch evidence replay

Audience: developers evaluating planner integration without a deployed data plane.

`ControlPlaneCostModel::with_offline_evidence` accepts the planner's validated
offline provider. Candidate ordering compares update CPU nanoseconds only when
every candidate has compatible evidence for the exact parameters returned by
this deployment's sizing policy. Missing, stale, incompatible, or ambiguous
measurements preserve the existing order. Offline errors never change formal
accuracy guarantees. Existing physical and lifecycle costs retain their units;
CPU nanoseconds are not added to legacy dimensionless costs.

For explicit integer-key point-frequency queries,
`with_offline_frequency_comparison(evidence, request)` additionally compares
query-matched sketch measurements against an exact snapshot baseline. The
request supplies the observed mean-error budget, number of reads, retained
state count, horizon and CPU/memory weights. This is a fixed-snapshot offline
comparison: the caller asserts the recorded integer-key distribution and probe
population apply. The mean-error threshold applies to that recorded population,
not to each queried key or future live data.

The backend restricts candidates to measured power-of-two CMS layouts before
comparison and supplies its own formal minimum parameters from the tighter
workload and query accuracy. Observed error may select a larger measured sketch,
but never weakens formal sizing. Missing evidence, an unacceptable error budget,
or an exact winner yields `PassThrough`, preserving exact execution. Unfiltered
legacy frequency totals and `count_over_time` never receive point-frequency
error acceptance. `offline_frequency_recommendation(payload)` exposes the same
decision and rejection reasons used by the binder.

The typed frequency example reads real comparison artifacts without starting a
data plane, using the planner revision pinned in this backend:

```bash
cargo run -p control_plane --example offline_frequency_plan -- \
  comparison-evidence.json comparison-request.json 7 0.01
```

It binds a named integer-key source and reports the chosen parameters, exact
alternative, cost estimates and preserved point readout. It does not certify
that an existing Prometheus metric or deployed materialization uses that source.

Run the PromQL replay using the pinned planner dependency:

```bash
cargo run -p control_plane --example offline_planner_replay -- \
  o11y_bench_promql.txt planner-evidence.json context.json > control-plane-o11y.json
```

For development against unpublished planner changes, the optional local-checkout
wrapper supplies source patches and records the checkout revisions:

```bash
python3 tools/run-offline-planner-replay.py \
  --planner /path/to/ASAPPlanner \
  --queries /path/to/ASAPPlanner/crates/frontend-promql/tests/observability/data/o11y_bench_promql.txt \
  --evidence /path/to/planner-evidence.json \
  --context /path/to/context.json \
  --output /tmp/control-plane-o11y.json
```

The wrapper supplies local Cargo source patches for all three planner crates,
preserving a single set of IR types. It also changes Cargo.lock; normal use of
the pinned dependency requires no source patches. The normal backend sibling
dependencies (ASAPCollector and asap_sketchlib)
must remain available at the paths in its workspace manifests.

The replay calls the real control-plane parser and typed summary binder for
exact, default, and empirical modes. It records per-query rejection/fallback,
selected summary states, matching update/state evidence, provenance, and elapsed
planning time. The selected offline context is an explicit simulation assumption;
it is not an assertion that an o11y metric has that measured distribution.
Source scans beneath summaries count as raw subtrees, so they are not themselves
evidence that the whole query fell back. Root fallback is a separate field.
Bare selectors, sort roots and comparison/filter roots retain the complete
original query as `KeepPreAsap`. They bind successfully while preserving label
predicates, ordering and filtering. Executable query compilation marks these
roots `ExactFallback` and requests no summary materializations; successful
binding therefore does not mean they are served by the warm tier.

This is binding coverage, not successful deployment compilation or execution.
No collectors or query servers start. Point-frequency benchmark errors are
exported as observations with `error_applies_to_current_query: false`.
Whole-plan resource savings remain null without matched raw/residual physical
operator evidence. Unsupported summary binary operators lower to explicit warm
tier fallback; they are not silently executed with different semantics.
