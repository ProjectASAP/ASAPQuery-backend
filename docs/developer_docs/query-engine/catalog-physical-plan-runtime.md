# Catalog-backed physical-plan installation

`BackendPlan` and its standalone protobuf and installation endpoint have been
removed. The backend now stages and atomically activates one physical-plan
envelope containing the authoritative `SummaryCatalog` snapshot plus the
`PrecomputePlan`, optional `CollectorPlan`, `TransmissionPlan`, and `QueryPlan`
that reference installed stored outputs and their semantic definitions.

Catalog schema 6 separates `StoredOutputId` (deployment routing) from
`SummaryDefinitionId` (versioned semantic SHA-256). `StoredOutputReference`
binds both. Planner exports the persisted output's typed semantic dependency
closure; explicit raw summary configurations use a restricted typed description.
Derived outputs require the complete Planner closure at installation.

Writes must match the installed output's operator and format. Durable metadata
records both identities and the immutable catalog snapshot. Recovery validates
that snapshot before restoring authoritative state. Only the current metadata
schema is accepted. Unsupported versions, flat-map sidecars, corrupt metadata,
obsolete identity aliases, and untyped projection encodings are rejected. There
is no automatic format migration or cross-version state-adoption path. Rebuild
unsupported persisted state using the installed plan.

See [SDS architecture](../../design_docs/summary-catalog-sds-architecture.md)
for the contract and [migration gates](../../design_docs/asapplanner-migration-plan.md#5-bound-query-sds-implementation-across-the-pr-stack)
for the downstream acceptance requirements. Ad-hoc discovery is not implemented.
The HTTP lifecycle is exposed through `/api/v1/physical-plan`,
`/api/v1/physical-plan/activate`, `/api/v1/physical-plan/discard`, and
`/api/v1/physical-plan/status`.

## Foundation scope (#749)

This branch validates that maintenance and query plans belong to one installed
catalog generation, retain the selected DAG provenance, and agree on writer/read
bindings and physical windows. It removes the Collector runtime dependency.

#749 establishes the SDS identity contract. Planner-generated definitions contain
an explicit logical dataset/tenant identity and the canonical typed dependency
closure of the persisted output. Endpoint relocation preserves this identity;
a different dataset changes it even when the metric and expression are equal.
The installed input binding must agree with the exported dataset identity.

Semantic definition IDs and deployed output IDs are independent. Hot and rebuild
outputs can share one definition, but an installed query reads only its selected
output. Catalog schema 6 and planning snapshot schema 3 reject older metadata
rather than inventing a missing semantic identity. Later runtime PRs consume this
contract; they do not replace its identity model.

Recovery at this stage is limited to the same installed catalog generation.
Installing a new plan version never adopts previous-version state, even for an
unchanged definition. New input must populate that version before it can serve
accelerated results; the query follows its installed fallback/unavailability
policy while cold. Fresh writes allocate a new physical series instead of using
the previous generation's completed series, both after restart and during live
activation. Cross-version adoption metadata is rejected.
