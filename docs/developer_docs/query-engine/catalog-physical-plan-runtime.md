# Catalog-backed physical-plan installation

`BackendPlan` and its standalone protobuf and installation endpoint have been
removed. The backend now stages and atomically activates one physical-plan
envelope containing the authoritative `SummaryCatalog` snapshot plus the
`PrecomputePlan`, optional `CollectorPlan`, `TransmissionPlan`, and `QueryPlan`
that reference catalog materialization IDs.

The target semantic-definition and deployed-output separation is described in
[SDS architecture](../../design_docs/summary-catalog-sds-architecture.md). It is a
design contract, not evidence that the current catalog/storage path already
implements canonical semantic fingerprints or independent stored-output IDs.
The [migration gates](../../design_docs/asapplanner-migration-plan.md#5-bound-query-sds-implementation-across-the-pr-stack)
track the required code and recovery changes.
The HTTP lifecycle is exposed through `/api/v1/physical-plan`,
`/api/v1/physical-plan/activate`, `/api/v1/physical-plan/discard`, and
`/api/v1/physical-plan/status`.

The current V1 storage path reads only the installed plan version. Matching
definition identity does not authorize cross-version payload reuse; that would
require an explicit reader binding and separate compatibility support.
