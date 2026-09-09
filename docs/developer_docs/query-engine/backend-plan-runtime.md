# Catalog-backed physical-plan installation

`BackendPlan` and its standalone protobuf and installation endpoint have been
removed. The backend now stages and atomically activates one physical-plan
envelope containing the authoritative `SummaryCatalog` snapshot plus the
`PrecomputePlan`, optional `CollectorPlan`, `TransmissionPlan`, and `QueryPlan`
that reference catalog materialization IDs.

The current contracts and ownership rules are documented in
[SummaryCatalog and SDS architecture](../../design_docs/summary-catalog-sds-architecture.md).
The HTTP lifecycle is exposed through `/api/v1/physical-plan`,
`/api/v1/physical-plan/activate`, `/api/v1/physical-plan/discard`, and
`/api/v1/physical-plan/status`.
