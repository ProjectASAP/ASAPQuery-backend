# Migration to Planner Physical DAG Deployment

Status: delivery plan for the [target design](asapplanner-integration.md).
Audience: backend implementers. This plan does not introduce a second operator
IR or backend lowering path.

## 1. Outcome

PrecomputePlan and QueryPlan carry Planner-compiled Physical DAGs and backend
boundary bindings. Both engines execute through the shared physical library.
The backend owns storage, scheduling, installation and serving; Planner owns
operator selection and materialization frontiers.

The migration also removes the backend dependency on ASAPCollector. Distributed
activation, CollectorPlan/TransmissionPlan compilation and new transport
protocols are outside this delivery.

## 2. Migration boundaries

```text
Before
  full logical DAG + backend semantic-node bindings
      → backend-specific computation and phase interpretation

After
  Planner-provided maintenance/query Physical DAGs
      → backend input/output bindings and operational policy
      → shared physical execution
```

The runtime accepts the new deployment artifact. Obsolete plan schemas are
rejected before activation rather than interpreted through a parallel logical-DAG
executor. Producers and fixtures move together.

Plan-format migration is separate from stored payload compatibility. Supported
historical payloads retain versioned decoders and fixtures; this does not require
retaining obsolete plan readers. Do not change sketch byte formats as a side
effect of moving execution code.

## 3. Delivery stages

| Stage | Work | Exit condition |
| --- | --- | --- |
| Inventory | Record current supported computation, state formats and deployment behavior. | Each supported path has a fixture or an explicit unsupported result. |
| Shared dependencies | Adopt shared operator/runtime and codec contracts; remove Collector dependencies. | Backend builds and tests without ASAPCollector. |
| Deployment binding | Consume Planner Physical DAGs; bind their typed boundaries and lifecycle. | No backend logical lowering or frontier selection remains in the new path. |
| Execution and installation | Drive both kinds of DAG through the shared executor and install one coherent bundle. | Identity, resource, failure and readiness tests pass. |
| Retirement | Switch publications and remove superseded computation paths. | Full-path and recovery tests pass; obsolete plans are rejected. |

### 3.1 Inventory and shared dependencies

Capture fixtures for full/delta decoding, reconstruction, maintenance and
readout, completion, restart, staging, activation and fallback. Record revision
and schema provenance. Use semantic checks when randomized bytes are unstable.
Fixtures may originate from Collector but must run independently of it.

Reuse `asap-physical-operators`, `asap_sketch_codec` and sketch-library APIs for
neutral execution and encoding work. Storage adapters, scheduling and publication
remain backend-owned. Remove reconstruct-serialize-decode detours and duplicate
family execution paths when replacing them, with parity evidence.

Inspect manifests, lockfiles, build scripts and tests for direct or transitive
Collector dependencies, including `asap-precompute-rs` and Collector patches.

### 3.2 Deployment binding

Adopt the Planner-owned semantic-description export and versioned canonicalization
contract for SDS definitions, independent of internal executable IR serialization.
Persist only the semantic dependency closure needed to interpret each output before records
can reference semantic fingerprints. Definitions derived from incomplete legacy
metadata must be reconstructed from authoritative plans or rejected for rebuild;
do not infer missing expressions from source and grouping alone.

Consume the selected Physical DAGs, physical boundary identities, query
associations and maintenance requirements. Replace semantic-node classification
with mappings from declared input/output boundaries to deployment resources.

- Bind raw slots to readers satisfying source, filter, grouping, window, schema and boundedness requirements.
- Assign version-scoped stored-output identities to persisted physical outputs.
- Bind stored inputs to matching outputs and validate grouping, format, coverage
  and revision requirements.
- Package the original physical computation with schedules, retention, result
  routing and publication policy.

The old `Materialization`, `MaintenanceInput`, `Query` and `QueryInput` semantic
classification is not a target contract. Logical provenance is diagnostic data
from Planner, not an instruction to rebuild operators or split a graph.

No build/readout phase whitelist is introduced. Follow the selected physical
candidate. If the backend cannot persist a selected scalar or result output,
report that capability limitation instead of moving operators across a boundary.

A new plan schema version expresses this boundary. Do not reinterpret an old
field under an unchanged version. Normalize supported legacy stored identities
during migration with an explicit mapping; preserve payload identity and reject
unresolved/conflicting mappings.

### 3.3 Installation and execution

Validate definitions and boundary bindings against the supplied Physical DAGs.
Verify all stored-output references, schemas, encodings, partitions and versions,
then perform deployment resource and capability checks.

Stage definitions and both plans as one snapshot. Failed staging leaves the
active version unchanged. Activation does not establish state readiness; runtime
input resolution checks actual committed state and applies the installed fallback
or unavailability policy.

Precompute and query engines resolve inputs and drive the shared executor. They
must not retain a second node traversal that recomputes shared producers. Plan
visualizations show the supplied DAGs connected by deployed stored-output bindings.

### 3.4 Retirement

Migrate publishers and consumers together with pinned dependencies and matching
rollback artifacts. Remove obsolete plan adapters, full-logical-DAG execution
and duplicated operators after the new path passes its gates.

Storage payload readers remain governed by the supported format policy. Reuse
across plan versions requires an explicit compatibility decision independently
of a binary rollback.

## 4. Acceptance evidence

Record tested revisions, supported families/output types and fixture results.
Acceptance includes:

- Planner computation and boundaries are preserved through installation.
- One producer serves multiple queries without duplicate maintenance; one query
  can consume multiple compatible outputs.
- Shared producers execute once per run; separate runs remain isolated.
- Supported query-time construction and precomputed finalized outputs follow the
  selected phases; unsupported output bindings fail explicitly.
- Missing, overlapping, incomplete or incompatible state fails eligibility.
- Staging failure, cancellation, resource limits, restart and version switching
  preserve documented behavior.
- Obsolete plans are rejected and backend builds/tests do not require Collector.

Trace a query from Planner selection through physical compilation, deployment
binding, state publication and query execution. Verify exact operations against
independent results and sketches against their supported guarantees. The design
is not accepted solely because example schemas parse or unit tests pass.

## 5. Bound-query SDS implementation across the PR stack

The SDS document is a target contract. The existing definition-keyed storage
path must not be described as implementing independent deployed-output identity.
The current migration implements bound queries only; ad-hoc discovery is deferred.

| Implementation owner | Required change | Regression/acceptance gate |
| --- | --- | --- |
| Planner shared types and physical integration (#462) | Export a versioned canonical semantic description for a selected persisted output; exclude placement and temporary node IDs. | Different input expressions differ; renumbering preserves identity; state definitions exclude downstream readout parameters. |
| Backend plan/schema foundation (#749), completed with the shared semantic contract in #774 | Separate semantic definitions from deployed-output bindings; remove the requirement that stored-output ID equals definition ID; version the changed plan contract. | Same-version hot/rebuild outputs can share one definition without aliasing; tampered definitions and mismatched bindings fail installation. |
| Planner dependency integration (#774) | Consume the shared semantic export and propagate it from selected physical outputs into deployment compilation. | No backend expression normalization or synthetic semantic fingerprint from incomplete config fields. |
| Precompute/storage integration (#763) | Persist definitions and output-scoped records; authorize writes against installed bindings and recover them consistently. | Restart retains semantic descriptions; wrong-output writes fail; replacement metadata and payload remain consistent. |
| Query integration (#765) | Resolve the installed deployed output and validate definition, revision, format and coverage before invoking shared execution. | A hot-bound query never reads rebuild state; stale, missing or incompatible records take the explicit failure route. |
| Acceptance PRs (#728, #742, #759) | Update fixtures and process tests for the new contract; retain existing behavioral and performance gates. | End-to-end producer → persisted definition/record → recovery → bound read, with negative identity and coverage cases. |

This table assigns work, not completed implementation. PR ordering must follow
actual dependency commits; it must not be inferred from an outdated stack list.
A semantic definition cannot be replaced by a policy fingerprint containing
physical layout or cadence. Conversely, relaxing an output-reference validator
without changing storage keys and authorization is insufficient and unsafe.

The implementation must preserve supported payload decoders independently of
plan-schema retirement. Keep implementation guides accurate to the code until
each stage lands; then update the APIs, persistence descriptions and test evidence
in the same implementation PR.

The open shared-library integration PR is #774, replacing the already merged
#770. The active order after #771 is #774 → #763 → #765 → #761 → #728
→ #742 → #759; old #770 base metadata is not part of this chain.
