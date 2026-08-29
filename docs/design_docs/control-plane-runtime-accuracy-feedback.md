# Runtime accuracy feedback and replanning

> Status: active monitoring/replanning path with family-specific evidence still evolving

## Responsibility

This component collects empirical summary behavior from Collector and Backend,
compares it with the plan's declared guarantees, and supplies versioned evidence
to the Planner/control-plane replanning loop. It does not replace the Planner's
semantic accuracy model with an observed average.

## Evidence

```text
Collector: updates, drops, window/plan application, encoding and wire bytes
Backend: ingest gaps, coverage, query provenance, latency, resource use
Evaluator: aligned approximate vs exact results and calculated error
```

Every observation is keyed by plan/materialization/SID lineage, family and
parameters, workload/query shape, window, source time, runtime versions, and
measurement environment. Unattributed measurements cannot safely tune another
materialization.

## Feedback loop

```text
runtime evidence -> validate/freshness-check -> empirical profile
       -> violation or cost change -> replan request -> ASAPPlanner
       -> new selected logical plan -> physical compile/publish
```

A violation can trigger replanning, rollback, exact fallback, or an operator
alert according to policy. Replanning creates a new plan version and explicit
state transition; it never mutates the meaning of already stored SID state.

## Accuracy rules

- Exact and approximate answers are aligned by labels and timestamps first.
- Missing/extra points are failures, not excluded samples.
- Theoretical guarantees remain authoritative legality constraints.
- Empirical results refine cost/risk estimates only for the measured family,
  parameters, data distribution, and runtime version.
- Stale, undersampled, or incomparable evidence is reported as unknown.

## Acceptance behavior

Tests cover aligned error computation, provenance validation, insufficient
sample handling, stale evidence, plan-version isolation, violation-triggered
replanning, and failure to publish the replacement plan. The multi-node harness
provides end-to-end accuracy, freshness, latency, and cost evidence.

See [Developing runtime feedback](../developer_docs/control-plane-runtime-accuracy-feedback.md).
