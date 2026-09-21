# PrecomputePlan and QueryPlan migration plan

Status: proposed delivery sequence. Audience: backend implementers.

Terminology: [Planner/backend glossary](planner-backend-glossary.md).

## Goal and scope

Replace complete semantic DAGs stored under PrecomputePlan with separate
PrecomputePlan and QueryPlan executable subgraphs connected by SDS state
references. Also remove the backend build/runtime dependency on ASAPCollector by
moving shared contracts and reconstruction code to neutral libraries.

CollectorPlan, TransmissionPlan, distributed activation, new transport behavior
and a general ASAPPlanner API redesign are deferred.

## Document map

1. [Migration at a glance](#migration-at-a-glance)
2. [Worked example](#worked-example)
3. [Stage 1: inventory and fixtures](#stage-1-inventory-and-fixtures)
4. [Stage 2: extract common code](#stage-2-extract-common-code)
5. [Stage 3: bind and split plans](#stage-3-bind-and-split-plans)
6. [Stage 4: validate and install](#stage-4-validate-and-install)
7. [Stage 5: migrate and retire](#stage-5-migrate-and-retire)
8. [Completion evidence](#completion-evidence)

## Migration at a glance

| Stage | Change | Exit gate |
| --- | --- | --- |
| 1. Inventory | Freeze current contracts and behavior as fixtures | Every supported path has a fixture or explicit unsupported result |
| 2. Extract | Move neutral contracts/codecs out of Collector | Backend dependencies and tests contain no ASAPCollector |
| 3. Split | Derive catalog, maintenance DAGs and query DAGs from one binding | Ownership and state references match selected semantics |
| 4. Install | Validate and atomically activate one plan version | Invalid snapshots fail without disturbing the active plan version |
| 5. Retire | Normalize old artifacts and remove superseded paths | Compatibility and end-to-end gates pass |

Do not combine payload-format changes with dependency extraction. Version the new
plan representation separately from any later wire/schema change.

## Worked example

The current artifact may store this complete DAG under PrecomputePlan:

```text
Input -> BuildKLL -> SummaryEstimate -> Result
```

The migration produces:

```yaml
plan_version: 42
summary_catalog:
  definition: {id: def-9, algorithm: kll, k: 200}

precompute_plan:
  nodes: [Input, BuildKLL, 'WriteState(slot-17)']
  write_binding: {state_slot_id: slot-17, definition_id: def-9, schema: kll-v1}

query_plan:
  nodes: ['ReadState(slot-17)', SummaryEstimate, Result]
  read_binding: {state_slot_id: slot-17, definition_id: def-9, expected_schema: kll-v1}

provenance:
  selected_dag: Input -> BuildKLL -> SummaryEstimate -> Result
```

During rollout, the backend normalizes a supported legacy artifact into this
internal form. Old and new forms must produce the same update count and query
result. After compatibility gates pass, the complete-DAG execution path can be
removed while its versioned reader remains for the supported window.

## Stage 1: inventory and fixtures

Inventory Planner output, plan/SDS types, state schemas, envelopes,
`asap_precompute_rs` imports, Cargo patches, build scripts and tests that invoke
Collector.

Capture fixtures for:

- full, delta and legacy bare-state decoding;
- summary reconstruction, maintenance updates and query readout;
- completion, restart and recovery;
- staging, activation, readiness and fallback.

Fixtures may originate from Collector but must run without a Collector checkout
or process. Record source revision and schema provenance; use semantic assertions
when randomized sketch bytes are unstable.

Preserve each selected deployment guarantee and its schedule/retention from
Planner selection. A backend that only supports batch construction from data at
rest must not infer incremental support from recurring query demand.

## Stage 2: extract common code

| Neutral responsibility | Excludes |
| --- | --- |
| Envelope metadata, shared IDs/schema references and validation | Planner optimization and runtime executors |
| Sketch schemas, encode/decode/reconstruction and supported state operations | Window scheduling, host adapters and backend storage |

Prefer existing sketch-library APIs. Move reusable DDSketch/KLL reconstruction
out of Collector wrappers and remove reconstruct-serialize-decode round trips.
Keep legacy readers and family-specific backend paths until replacements have
parity evidence.

Remove `asap-precompute-rs` and Collector-specific Cargo patches. Inspect
manifests, lockfiles, dependency graphs, scripts and required tests for direct or
transitive Collector dependencies.

## Stage 3: bind and split plans

Create compiler bindings for semantic nodes, summary definitions,
version-scoped state slots, schemas and state references. Derive the catalog and both plans
from those bindings using the
[materialization-boundary rules](asapplanner-integration.md#executable-subgraphs-and-materialization-boundaries):

- PrecomputePlan contains maintenance inputs/operators and state sinks.
- QueryPlan contains state reads, `SummaryEstimate`, exact residuals and results.
- Derived maintenance uses explicit completed-state references.
- Shared producers retain one identity and update path.
- Provenance records semantic operations absorbed into physical nodes.

Version the split representation. Do not reinterpret an old field under an
unchanged schema version.

Do not introduce a standalone catalog `Materialization` object. Keep definitions
in the catalog, format/partition/writer configuration in executable bindings,
and actual coverage/location/readiness in instance inventory. Normalize legacy
stored-output identities into state slots while preserving payload locators;
validate all consumers against the same writer configuration. The existing
`BackendNodeBinding::Materialization` remains a placement marker for stored output.

## Stage 4: validate and install

Validate definition, state slot, schema, encoding, grouping, time partition,
coverage and plan version across the catalog and both plans. Then perform local
resource checks.

Stage and activate the three artifacts as one snapshot. Readiness remains
separate: until coverage is ready, QueryPlan follows its configured fallback or
explicit unavailability. Failed staging preserves the previous plan version.

Render PrecomputePlan and QueryPlan separately, joined by state references.
Legacy projected views label maintenance-owned and query-owned nodes.

## Stage 5: migrate and retire

Release pinned neutral-library versions and rollback artifacts. Migrate
backend-local publications first and retain versioned adapters for the supported
compatibility window.

Remove complete-DAG precompute execution and Collector adapter code only after
fixtures and end-to-end tests pass. State reuse across plan versions requires an
explicit SDS compatibility decision independently of binary rollback.

## Completion evidence

Completion requires:

- summary construction runs only in PrecomputePlan and estimation only in
  QueryPlan;
- one query can read multiple summaries and two queries can share one producer;
- derived state observes completion and schema requirements;
- invalid bindings fail before activation;
- restart and plan version switching preserve consistency and fallback;
- legacy and split artifacts produce equivalent results and update counts;
- backend builds and required tests do not fetch, build or run ASAPCollector.

Record tested revisions, supported state families, fixture results and dependency
checks. Trace one query from its selected semantic root through the state
writer, SDS reference and QueryPlan reader.
