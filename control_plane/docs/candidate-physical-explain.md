# Candidate and physical alternative diagnostics

The compiler preserves diagnostic associations from the original Planner search through physical quote comparison. These fields explain the existing decision; they do not alter candidate ranking, introduce a resource model, or enable provider-driven re-selection.

`logical_selection` contains the original ranked groups, legal candidates, accuracy rejections, and selected roots. Group ordinals and `query_index` are local to that invocation. `target_id`, `candidate_id`, and `logical_root_id` use versioned semantic digests instead. Post-ASAP digests reuse Planner's canonical executable exporter, including operator payloads, schemas, guarantees, edge roles and execution states. They exclude assigned executable node IDs and incidental `Rc` sharing. The Planner revision is part of the digest domain.

The trace's `roots` describe Planner selection. `committed_roots` and `deployment_overrides` describe any subsequent ERP exact fallback. Physical alternatives reference their own `logical_root_ids`; the added native alternative therefore does not masquerade as a selected summary candidate. Canonical values must round-trip without loss, so non-finite floats cannot alias through JSON null. No identity is fabricated when lossless canonical export is unavailable: the corresponding identity is null, and physical explanations include `identity_unavailable_reason`.

`AlternativeCost` retains an `alternative_id`, concrete `physical_alternative_id` once binding succeeds, and status:

* `bound`: manifest preparation succeeded; no quote decision yet.
* `selected` / `unselected`: a complete valid quote was considered by the existing selector.
* `bind_failed`: compilation or manifest construction failed; `unavailable_reason` is retained even without a plan ID.
* `evidence_missing` / `evidence_invalid`: no unambiguous matching quote, or malformed/incomplete component evidence.
* `rejected`: the provider reports that the implementation is unavailable.

Physical identity combines the logical/mask alternative with existing materialization IDs, selected window implementation IDs and ingest mode. Activation time, plan version and quoted numeric prices are not semantic identities. Quotes still match the complete existing manifest, including its generation constraints; diagnostic IDs never replace quote validation or catalog identity.

The read-only `/api/v1/physical-plan/cost-manifests` and MetricsQL equivalent retain their default manifest-array response. Add `"explain": true` to the existing request to receive `{ "manifests": [...], "alternatives": [...], "logical_selection": [...] }`. Failed alternatives remain alongside usable manifests. When none can bind or be completely priced, the error retains an `all_infeasible` report and every accumulated alternative rather than only a generic message.

Snapshot compilation exposes the same logical trace on `PhysicalPlan`; `inspect_physical_dag` prints it outside the install request. Compile-and-publish returns the trace without adding it to executable wire DTOs. SQL's existing selection trace gains the same semantic candidate/root identities.

These are bounded explanations: they cover the actual Planner search and the existing physical materialization/exact inventory, not every possible placement or resource-constrained cluster assignment. Missing numeric measurements remain missing. The next provider integration must occur before logical commitment and reuse Planner's provider/resource contracts.
