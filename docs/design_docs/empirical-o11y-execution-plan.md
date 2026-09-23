# Offline evidence and o11y planning evaluation

Status: historical offline-evidence milestone with a current backend execution
path. Audience: developers reproducing issue #322 and evaluating Planner/control-
plane decisions.

The current replay guide is [o11y replay](../user_guide/o11y-replay.md). Benchmark
production and execution tooling belong to ASAPQuery-backend; planner-only
coverage and synthetic cached-result timing do not satisfy this evaluation.

## Document map

1. [Evaluation at a glance](#evaluation-at-a-glance)
2. [Worked example](#worked-example)
3. [Evidence contract](#evidence-contract)
4. [Execution and ownership](#execution-and-ownership)
5. [Comparison rules](#comparison-rules)
6. [Acceptance](#acceptance)

## Evaluation at a glance

```text
sketch-bench measurements ──► versioned evidence artifact
                                      │
OpenMetrics workload ──► backend planning/binding ──► selected plan or fallback
                                      │
                                      ▼
                         decision and coverage report
```

Evidence includes measured CPU, elapsed time, state/memory, disk when measured,
and error against offline ground truth. It does not claim runtime ground truth,
posterior feedback or formal guarantees on unseen distributions.

## Worked example

Suppose sketch-bench measures KLL on one million Zipf-distributed values:

```yaml
evidence:
  algorithm: {kind: kll, k: 200}
  dataset: {distribution: zipf, exponent: 1.1, events: 1000000}
  environment: {cpu: example-x86, revision: abc123}
  measurements:
    update_cpu_ns: {mean: 48, samples: 20}
    readout_cpu_ns: {mean: 2100, samples: 20}
    serialized_state_bytes: {mean: 4096, samples: 20}
    rank_error: {p99: 0.008, samples: 20}
  observed_at: 2026-09-01T00:00:00Z
```

For a compatible o11y query, the backend binds this evidence to a concrete KLL
candidate and may rank it below exact execution. If the query uses a different
distribution, stale evidence or unsupported parameters, matching fails and the
planner retains its non-empirical choice or exact fallback. The report records
the match, selected plan and evidence provenance; it does not report missing
exact-baseline cost as zero.

Evidence for a query readout alone does not establish that its shared producer
can meet the selected maintenance guarantee and schedule/retention. The physical
compiler must validate the complete implementation combination before binding
the producer to a PrecomputePlan writer and QueryPlan readers.

## Evidence contract

Every artifact declares:

- schema version, units, algorithm and parameters;
- dataset, distribution and workload size;
- hardware/software environment and source revision;
- collection and validity times;
- sample count and dispersion;
- benchmark/model provenance;
- unavailable measurements explicitly.

CPU and elapsed time are distinct. Serialized size, heap memory, disk I/O and
network bytes are distinct. Evidence affects a public cost, ranking or lifecycle
boundary only after compatibility and freshness validation.

## Execution and ownership

1. **Evidence producer:** run actual algorithms on uniform and Zipf inputs,
   retain raw output and emit versioned artifacts.
2. **Evidence integration:** validate artifacts, match compatible records and
   expose measured costs through the public cost-model boundary.
3. **Replay runner:** send the o11y PromQL corpus through the backend parser,
   ASAPPlanner call and typed physical binder; record bindings and fallbacks.
4. **Integration owner:** pin revisions, run end-to-end checks and publish the
   evidence-supported comparison.

Artifact design, benchmarks and replay may proceed in parallel after agreeing on
the contract. Query-pattern changes require a demonstrated blocker and regression
coverage; unsupported semantics remain visible in the report.

## Comparison rules

Compare equivalent tasks, data, parameters, windows, horizons and evaluation
cadences. Whole-plan estimates include build/update/readout, retained state,
sharing and raw residual work where evidence exists. Algorithm microbenchmarks
remain separately labeled.

Disk or network savings require measurements or an explicit model. Repeated-query
break-even requires compatible exact baseline, sketch maintenance and readout
measurements. Missing components make whole-plan speedup unavailable rather than
free. Offline estimates do not establish deployed end-to-end latency improvement.

## Acceptance

- At least two real sketch algorithms are measured on uniform and skewed inputs.
- Missing, stale, malformed and mismatched evidence cannot change a decision.
- Matching evidence changes at least one public planning decision in a test.
- Exact fallback and accuracy guarantees remain unchanged.
- Replay exposes supported bindings, unsupported shapes and provenance.
- Commands, raw machine-readable results and a concise report are reproducible.
