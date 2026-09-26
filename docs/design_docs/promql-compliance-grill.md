# PromQL compliance design Q&A

> **Question:** should v1 be a strict differential suite for a deliberately supported, locally answered subset of PromQL—with unsupported queries required to fail—or should it allow Prometheus fallback?
>
> **Recommendation:** require local answers and reject fallback in v1. Otherwise Prometheus can answer on the backend’s behalf, yielding a green comparison that proves neither backend ingestion nor backend query semantics.

**Answer:** unsupported queries required to fail, yes

> **Question:** should the suite compare approximate sketch-backed values to Prometheus within an explicit per-query tolerance, or start with only result shapes/data sizes where equality is exact?
>
> **Recommendation:** support explicit per-query tolerances from day one, defaulting to exact equality. The backend is designed to return approximate sketch results, so pretending all valid local answers are exact would either constrain the suite to an unrepresentative corpus or create noisy failures.

**Answer:** yes

> **Question:** should v1 use a finite, timestamp-pinned fixture and explicitly call the backend’s `/api/v1/precompute/drain` before querying?
>
> **Recommendation:** yes. It makes ingestion completion deterministic and avoids flaky “did the asynchronous precompute path catch up?” failures. Live-scrape/streaming behavior should be a separate suite later, because `drain` deliberately closes the backend’s Remote Write input.

**Answer:** yes

> **Question:** before the backend can accept Remote Write, it needs a configuration called a PhysicalPlan that says what metrics/queries it should accelerate. Should this new test suite: (1) give the backend a small, fixed test configuration; or (2) start the planner/control plane too and have it create that configuration?
>
> **Recommendation:** option 1 for this issue. It keeps failures clear: if the test fails, it is likely ingestion or query behavior in the backend, not plan generation.

**Answer:** i wanna do option 2 actually. we can add option 1 later

> **Question:** should “planner/control plane” mean the backend repository’s `control_plane` binary and its normal plan-publication protocol, rather than importing ASAPQuery’s older `asap-planner-rs` service?
>
> **Recommendation:** use the backend’s own `control_plane`. It tests the deployment users of `ASAPQuery-backend` actually run and avoids coupling this repository’s suite to a separate repository’s planner implementation.

**Answer:** yes

> **Question:** how should the fixture seeder, comparator and Docker Compose runner integrate with the Rust backend?
>
> **Decision:** implement the runner in the Cargo workspace. Shared Rust modules provide deterministic Remote Write encoding, structured comparison, fixtures, tolerances, reports and Compose lifecycle handling. No Go toolchain is required.

**Answer:** yes

> **Question:** what should be the first required green corpus?
>
> **Recommendation:** begin with one small fixture and suite covering a selector, `rate`, `increase`, one aggregation, and both instant/range evaluations—plus one intentionally unsupported query that must fail locally. Add sparse/staleness, counter resets, and larger aggregation matrices after this vertical slice is stable.

**Answer:** Let's just add all corpora. I think we may not get any green righ tnow

> **Question:** when those corpora find differences, should the runner exit non-zero from day one?
>
> **Recommendation:** yes—never encode known failures as passes. Keep the full workflow manual/non-required initially, preserve its JSON reports as evidence, and only make a stable subset required in PR CI after the gaps are fixed.

**Answer:** yes

> **Question:** should every successful backend response be required to carry and pass a local-execution provenance check (for example, its existing ASAPQuery data-source marker), in addition to disabling fallback?
>
> **Recommendation:** yes. Disabling fallback catches most masking, but provenance makes the test’s claim explicit and will catch accidental routing changes that still return a successful response.

**Answer:** no, it's fine. just disable fallback

> **Question:** should the runner wait until the control plane has published and the backend reports an active Remote Write-ready plan before it sends any fixture data?
>
> **Recommendation:** yes. Then seed both targets, call the backend’s finite-input `drain`, and only then execute the fixed-time queries. This makes plan activation and ingestion completion explicit rather than timing-dependent.

**Answer:** yes

> **Question:** should the runner derive the control-plane workload/configuration from the same query-suite YAML it executes, rather than maintain a second hand-written plan configuration per corpus?
>
> **Recommendation:** yes. One source of truth prevents a test from querying expressions that the control plane was never asked to plan, and it makes adding a corpus a fixture-only change.

**Answer:** yes

> **Question:** on failure, should the runner retain the JSON report and collect service logs, while still tearing down containers by default?
>
> **Recommendation:** yes. CI should upload the report and logs as artifacts; local runs should offer `--keep-services` for interactive debugging. Default cleanup prevents stale volumes/ports from contaminating the next run.

**Answer:** yes

> **Question:** should `ASAPQuery-backend` own a copied/adapted version of the harness and fixtures, rather than invoke `ASAPQuery/promql-compliance` across repositories?
>
> **Recommendation:** own it in the backend repository. The backend needs different service wiring—its `control_plane`, PhysicalPlan lifecycle, and one HTTP listener—and an external cross-repo dependency would make local and CI runs less reproducible.

**Answer:** yes
