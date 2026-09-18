# PrecomputePlan and QueryPlan migration plan

Status: proposed delivery sequence. Audience: backend implementers.

## Goal and scope

Migrate the backend from a complete semantic DAG stored under PrecomputePlan to
separate executable PrecomputePlan and QueryPlan subgraphs connected by explicit
SDS state references.

This migration also removes the backend build/runtime dependency on ASAPCollector.
Shared envelope, schema and sketch reconstruction code moves to neutral libraries.

CollectorPlan, TransmissionPlan, distributed activation, new transport behavior
and a general ASAPPlanner API redesign are deferred.

Completion requires:

- `SummaryEstimate` and other query-only work appear only in QueryPlan;
- maintenance work terminates in explicit stored-state writes;
- both plans share one catalog/materialization/schema decision;
- the catalog and both plans install as one backend generation;
- supported legacy payloads and plans retain documented behavior;
- backend builds and required tests do not fetch, build or run ASAPCollector.

## Delivery stages

| Stage | Change | Exit gate |
| --- | --- | --- |
| 1. Inventory and fixtures | Record current contracts, imports, payloads and execution behavior | Every scoped path has a compatibility fixture or explicit unsupported result |
| 2. Extract common code | Move runtime-independent contracts and reconstruction to neutral libraries | Backend dependency graph and required tests contain no ASAPCollector |
| 3. Bind and split plans | Compile one decision into catalog entries, maintenance subgraphs and query subgraphs | Executable ownership and state references match selected semantics |
| 4. Validate and install | Add cross-plan validation, atomic generation switching and two-plan visualization | Invalid publications fail before activation; previous generation survives failure |
| 5. Migrate and retire | Normalize old artifacts and remove superseded execution paths | Compatibility and end-to-end gates pass |

## 1. Inventory and fixtures

Inventory the pinned Planner output, PrecomputePlan/QueryPlan/SDS types, state
schemas, envelope definitions, all `asap_precompute_rs` imports, Cargo patches,
build scripts and tests that invoke Collector.

Capture fixtures for supported:

- raw input and summary reconstruction;
- full, delta and legacy bare-state payloads;
- maintenance updates and query readout;
- completion, restart and recovery;
- plan staging, activation and fallback.

Fixtures may originate from Collector but must run without a Collector checkout or
process. Record their source revision and schema provenance. Use semantic readout
assertions where randomized sketch bytes are not stable.

The backend capability profile is an input to Planner selection. Preserve complete
lifecycle commitments, even when the only supported choice is batch construction
from data at rest. Do not infer incremental support from recurring query demand.

## 2. Extract common contracts and codecs

Use narrow neutral-library boundaries:

| Library responsibility | Must exclude |
| --- | --- |
| Envelope metadata, shared IDs/schema references and validation | Planner optimization and backend/Collector executors |
| Sketch payload schemas, encode/decode/reconstruction and supported state operations | Window scheduling, host adapters and backend storage |

Prefer existing sketch-library APIs. Move reusable DDSketch/KLL reconstruction
out of Collector wrappers and remove unnecessary reconstruct-serialize-decode
round trips. Preserve legacy readers and other family-specific backend paths until
replacement APIs have parity evidence.

Remove `asap-precompute-rs` and obsolete Collector-specific Cargo patches. Check
manifests, lockfiles, dependency graphs, scripts and required tests for direct and
transitive Collector dependencies.

Do not change payload bytes during extraction. Version any later wire/schema
change separately.

## 3. Bind once and split executable subgraphs

Create compiler-local bindings for selected semantic nodes, summary definitions,
materializations, state schemas and read/write references. Derive the catalog and
both plans from those bindings.

Apply the [materialization-boundary rules](asapplanner-integration.md#executable-subgraphs-and-materialization-boundaries):

- PrecomputePlan contains maintenance inputs/operators and stored-state sinks.
- QueryPlan contains state reads, `SummaryEstimate`, exact residuals and result
  composition.
- Derived maintenance uses explicit completed-state references.
- Shared producers retain one materialization identity and update path.
- Absorbed semantic operations remain visible through provenance mappings.

The selected semantic DAG may remain diagnostic metadata, but it is not
PrecomputePlan executable content.

Version the new installed representation. Normalize supported legacy publications
at the backend boundary; do not reinterpret an old field under an unchanged
schema version.

## 4. Validate, install and visualize

At compilation and installation, verify definition, materialization, schema,
encoding, grouping, time partition, coverage requirements and generation across
both plans. Then perform backend-local resource checks.

Stage the catalog and two plans as one snapshot and activate them atomically.
State readiness remains separate from installation. Until required coverage is
ready, QueryPlan uses its configured exact fallback or explicit unavailability.

Render PrecomputePlan and QueryPlan separately, joined by labeled state references.
Legacy full-DAG views must label maintenance-owned and query-owned projections.

Acceptance cases:

- build-summary/read-estimate executes in the correct plan;
- one query reads multiple summaries;
- two queries share one compatible producer without duplicate updates;
- supported derived state observes completion requirements;
- wrong schema, grouping, time partition or generation fails before activation;
- failed staging, restart and generation switching preserve consistency;
- old and new supported artifacts produce equivalent results and update counts;
- all cases run without ASAPCollector.

## 5. Migrate and retire

Release the backend with pinned neutral-library versions and rollback artifacts.
Migrate backend-local publications first. Retain versioned adapters for the
supported compatibility window.

Remove complete-DAG precompute execution and Collector adapter code only after
their replacements pass fixtures and end-to-end tests. Reusing state across plan
generations requires an explicit SDS compatibility decision independently of
binary rollback.

## Final evidence

Record tested revisions, supported state families, fixture results and dependency
checks. Trace at least one query from its selected semantic root through the
materialization boundary, PrecomputePlan writer, SDS state reference and QueryPlan
reader. Completion depends on the two-plan acceptance cases and zero backend
dependency on ASAPCollector, not on deferred distributed work.
