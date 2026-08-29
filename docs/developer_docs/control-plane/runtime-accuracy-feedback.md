# Developing runtime accuracy feedback

## Architecture

Runtime samples and monitoring collect Collector/Backend application evidence,
query accuracy/freshness/latency, and cost signals. The replanner validates and
keys evidence to the exact plan/materialization/family/runtime versions before
building a new workload/cost input.

## Interfaces

Feedback records need plan and materialization identity, SID lineage, family and
parameters, query shape, labels/timestamps alignment summary, sample count,
observed error/freshness/latency/resource values, collection time, and producer
version. A violation event references the declared threshold and observed value.

## Extension and verification

New evidence requires a unit, aggregation rule, freshness rule, cardinality,
privacy classification, and Planner consumer. Tests reject unattributed,
stale, under-sampled, wrong-version, or incomparable evidence. Replanning tests
verify a new plan version and fail-safe publication/rollback, not in-place
mutation of an active SID.

See [runtime feedback design](runtime-accuracy-feedback-design.md).
