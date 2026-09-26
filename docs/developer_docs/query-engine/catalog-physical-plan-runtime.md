# Catalog-backed physical-plan installation

`BackendPlan` and its standalone protobuf and installation endpoint have been
removed. The backend now stages and atomically activates one physical-plan
envelope containing the authoritative `SummaryCatalog` snapshot plus the
`PrecomputePlan`, optional `CollectorPlan`, `TransmissionPlan`, and `QueryPlan`
that reference installed stored outputs and their semantic definitions.

Catalog schema 4 separates `StoredOutputId` (deployment routing) from
`SummaryDefinitionId` (versioned semantic SHA-256). `StoredOutputReference`
binds both. Planner exports the persisted output's typed semantic dependency
closure; explicit raw summary configurations use a restricted typed description.
Derived outputs require the complete Planner closure at installation.

Writes must match the installed output's operator and format. Durable metadata
records both identities and the immutable catalog snapshot. Recovery validates
that snapshot before restoring authoritative state. Old payload decoders remain
available, but legacy metadata without semantic identity cannot authorize bound
reads. Reinstall/rebuild those outputs rather than guessing their meaning.

See [SDS architecture](../../design_docs/summary-catalog-sds-architecture.md)
for the contract and [migration gates](../../design_docs/asapplanner-migration-plan.md#5-bound-query-sds-implementation-across-the-pr-stack)
for the downstream acceptance requirements. Ad-hoc discovery is not implemented.
The HTTP lifecycle is exposed through `/api/v1/physical-plan`,
`/api/v1/physical-plan/activate`, `/api/v1/physical-plan/discard`, and
`/api/v1/physical-plan/status`.
