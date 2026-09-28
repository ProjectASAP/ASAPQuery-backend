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

## Foundation scope (#749)

This branch validates that maintenance and query plans belong to one installed
catalog generation, retain the selected DAG provenance, and agree on writer/read
bindings and physical windows. It removes the Collector runtime dependency.

The identity representation is transitional: catalog schema 3 still uses policy
fingerprints and couples the stored-output ID to that identifier. It cannot yet
represent independent hot/rebuild outputs with one semantic definition or prove
that equal metric names refer to the same logical dataset. Do not use this stage
as the completed SDS implementation. #774 supplies the Planner semantic export,
independent definition/output identities and explicit logical dataset binding;
#763/#765 supply the remaining storage and execution integration. These are
required before claiming the target contract or supporting multi-dataset reuse.

Recovery at this stage is limited to the same installed catalog generation.
Installing a new plan version never adopts previous-version state, even for an
unchanged definition. New input must populate that version before it can serve
accelerated results; the query follows its installed fallback/unavailability
policy while cold. Fresh writes allocate a new physical series instead of using
the previous generation's completed series, both after restart and during live
activation. Cross-version adoption metadata is rejected.
