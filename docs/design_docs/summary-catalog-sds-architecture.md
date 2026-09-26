# Self-Describing Summary: Computation Definitions and Stored Results

Status: target design, not an implemented wire schema. Audience: developers
compiling, storing, recovering and reading summary state.

## 1. Problem and goals

Stored sketch bytes do not explain what was summarized. Even source, filter,
grouping and window are insufficient: KLL over `latency` and KLL over
`log(latency)` have different meanings despite sharing those fields. Reusing
one as the other changes the query result.

SDS must preserve the computation that gives a stored result its meaning, and
associate each record with its concrete data scope and format. It must do this
without inventing another expression language or requiring a live Planner process.

The design has two stored objects: `SummaryDefinition` describes parameterized
computation using Planner IR; `StoredSummary` contains one concrete result and
references that definition. One `SummaryStore` owns both.

Goals are semantic identity, recoverable definitions, explicit read eligibility
and shared state across compatible consumers. SDS does not perform planning,
execute expressions, choose materialization boundaries or schedule maintenance.

## 2. Architecture and ownership

```text
Planner selected computation
    ↓ export normalized, typed computation rooted at persisted output
SummaryDefinition
    ↑ definition_id
StoredSummary: concrete group/window/revision + format + payload
    ↑ installed stored-output binding
Precompute writer / query reader
```

| Owner | Responsibility |
| --- | --- |
| Planner | Canonical computation semantics, typed IR export and versioned normalization rules |
| Deployment compiler | Associate selected physical outputs with definitions and concrete storage bindings |
| SummaryStore | Persist immutable definitions and committed records; enforce their references and read contracts |
| Shared executor | Execute Planner-provided Physical DAGs; SDS descriptions do not become a second execution path |

This follows the [canonical planning/deployment boundary](https://github.com/ProjectASAP/ASAPPlanner/blob/e9390031fcecd7bc0d611127eddc5c6603a281e5/docs/design_docs/physical-planning-and-deployment.md).
The [integration design](asapplanner-integration.md) defines deployment binding.
Definitions accompany the installed plan and are persisted before records can
reference them. Recovery must work without Planner memory, temporary node IDs
or fetching a mutable branch from a repository.

## 3. SummaryDefinition: the meaning of a result

A definition contains a normalized, typed logical Post-ASAP computation fragment
rooted at the persisted output. It includes all dependencies needed to interpret
that output, including retained Pre-ASAP expressions. It excludes unrelated
query consumers and physical storage locations.

### Input semantics are necessary but not sufficient

Source, filter, grouping and window describe input-data semantics. The full
computation also defines value expressions, joins/transforms and their order,
item/weight expressions, operation parameters, types and output representation.
Operation-defined null, duplicate and numeric behavior comes from versioned
Planner contracts rather than independent SDS switches.

```text
Definition A                      Definition B

Scan(latency)                     Scan(latency)
      ↓                                 ↓
KLLBuild(k=200)                    Project(log(latency))
                                        ↓
                                  KLLBuild(k=200)
```

The two definitions must have different identities. A display string such as
`"log(latency)"` is not a sufficient semantic representation: the typed function,
arguments and their semantics must be resolved in the canonical computation.

The conceptual persisted envelope is:

```yaml
summary_definition:
  id: <versioned-semantic-fingerprint>
  planner_ir_version: <supported-semantic-format-version>
  canonicalization_version: <normalization-version>
  computation: <serialized-normalized-typed-Planner-fragment>
  output: <root-in-that-fragment>
  parameters: <typed-parameter-contract-exported-by-Planner>
```

This is an ownership illustration, not a new node schema. `computation` reuses
Planner IR, including schemas and summary parameters; SDS does not define its own
Scan/Project/Build variants or copy them into separate source/filter fields.
`parameters` represents Planner-owned placeholders such as an evaluation endpoint
or input interval. Time bounds, time-column interpretation, alignment and pane
requirements must be unambiguous in the exported contract. If required semantics
are not exportable, that definition is unsupported rather than partially recorded.

### Definition boundary and sharing

For persisted KLL state, the root stops at the state-producing computation.
p50 and p99 consumers therefore share that definition and its stored state.
For a persisted p99 value, the root includes the quantile readout and its parameter;
persisted p50 has a different definition.

Group-key expressions and types belong in the definition. Concrete group values
and interval endpoints normally belong in records. A literal filter restricting
the source to `service="api"` remains part of the definition; it cannot be removed
and treated as a harmless record parameter.

If an input is already materialized, its meaning must remain recoverable. Export
the semantic dependency closure, or immutable references to definitions installed
and persisted with that closure. A reference only to a deployment node or store
address is insufficient. Such references reuse the same definition model, not
another registry or computation language.

### What is outside the definition

Storage locations, plan versions, physical operator implementation choices,
encoding, scheduling, retention, costs and observed readiness are separate
contracts. A physical implementation may vary only while preserving the
selected semantics; state compatibility still needs explicit format validation.
Changing `k`, the value expression or the output operation changes the definition.
Moving the same state to another store does not.

## 4. Semantic identity

The definition ID fingerprints a versioned canonical encoding of the computation,
its output and semantic parameter contract. The encoding includes stable source
identities, types and operator/function semantics, not display names or temporary
Planner node numbers. Source identity must distinguish different logical datasets
with identical schemas, including any applicable namespace.

Canonicalization must preserve ordered operands, constants, types, dependencies
and relevant operation semantics. Equivalent exports differing only in temporary
node numbering or map iteration order should produce the same ID. It must not
reorder arithmetic or replace expressions merely because they look algebraically
equivalent under different null or floating-point behavior.

This is conservative identity, not general equivalence proof. Unless Planner's
versioned normalization establishes equivalence, different computations have
different definitions and cannot be substituted by SDS. A legal transformation
or merge across definitions must appear in Planner's selected computation.

Registration recomputes the fingerprint and validates the canonical content.
An existing ID with different content is rejected. The canonical bytes are
retained, so a digest is never the only surviving description of the semantics.
Do not hash ordinary JSON output or a debug rendering.

The exact canonical encoding, digest algorithm and version compatibility policy
remain implementation decisions that must be fixed and tested before persistent
IDs are introduced. Unknown semantic/normalization versions fail validation.
A Planner source-code revision may be recorded for provenance but is not a
substitute for a stable semantic format contract.

## 5. StoredSummary: one concrete result

A stored record instantiates a definition for a concrete group, window and data
revision, and contains the resulting bytes:

```yaml
stored_summary:
  key:
    plan_version: 42
    stored_output_id: latency-kll
    group_key: {service: api}
    window: {start_exclusive: '12:00', end_inclusive: '12:01'}
  definition_id: <fingerprint-of-latency-kll-definition>
  revision: <data-revision>
  coverage:
    start_exclusive: '12:00'
    end_inclusive: '12:01'
    complete: true
  format: {schema: kll-v1, encoding: kll-binary-v1}
  payload: <encoded-KLL-state>
```

The interval identifies intended scope; actual coverage/completeness must be
established by the producer's input contract, not inferred from endpoints alone.
Format metadata identifies supported bytes. Integrity and producer sequence
metadata accompany the record where required by the installed protocol.

The logical lookup key is `(plan_version, stored_output_id, group_key, window)`.
Revision is validated record metadata, not permission to combine snapshots.
Replacement of a record must expose metadata and payload atomically and protect
in-flight readers from observing mixed revisions. Supporting simultaneous
historical revisions requires an explicit versioned lookup/storage contract;
this design does not imply it through the four-part key.

A stored record contains neither a repeated expression DAG nor a definition
chosen at write time. Its authorized output binding determines the definition.
Both definition and record must survive restart.

## 6. Deployment references and storage

One store owns two logical tables:

| Table | Contents |
| --- | --- |
| `summary_definitions` | Definition ID → immutable canonical computation and its versioned contract |
| `stored_summaries` | Concrete lookup key → committed record metadata and payload |

A `StoredOutputReference` is part of an installed plan:

```yaml
reference:
  stored_output_id: latency-kll
  definition_id: <fingerprint-of-latency-kll-definition>
```

The enclosing plan supplies its version; the reader supplies group/time selection
and required revision/coverage. The output identity names the authorized producer,
while definition identity describes meaning. Equal definitions do not authorize
reading another plan's output or bypassing its freshness requirements.

Installation validates definitions, their dependency closure and matching
physical-boundary bindings as one bundle. Records become visible only when their
metadata and payload are committed together. Definitions cannot be reclaimed
while live records, installed plans or other retained definitions reference them.
These are logical consistency requirements within the existing store, not a
proposal for separate metadata/payload services or a third Materialization object.

## 7. Read eligibility

A reader performs two distinct checks:

1. **Semantic compatibility:** the installed input expects this definition and
   typed output. `KLL(latency)` cannot satisfy `KLL(log(latency))`. A different
   definition needs an explicit Planner-approved computation, not a store heuristic.
2. **Instance eligibility:** the record belongs to the authorized output/version,
   is committed, has the required group, interval, revision and completeness, and
   uses a supported schema/encoding with valid payload integrity.

For a five-minute query using one-minute panes, definition semantics describe
each pane's computation. The query Physical DAG describes merging eligible panes
for the five-minute result. The reader must establish complete, nonoverlapping
coverage and compatible revisions. It must not assume five arbitrary records
with the same definition cover the requested interval.

Readout parameters can differ across consumers of the same KLL state. The shared
executor performs the selected readouts; the store does not execute the definition
or search for substitute summaries. Failure follows the installed fallback or
unavailability policy. Installation alone does not establish future readiness.

## 8. Alternatives and tradeoffs

A flat source/filter/grouping/window definition is simple but loses value
expressions and arbitrary input computation. A separate SDS expression language
would restore that detail at the cost of duplicating Planner semantics. Reusing
Planner's canonical typed computation avoids both problems.

Embedding the full definition in every record simplifies standalone transfer but
repeats metadata. Persisting it once and referencing it keeps records small;
export and recovery must therefore include the definition closure. A payload
without its required definition is not a complete SDS artifact.

Using a physical or deployment graph as semantic identity would make equivalent
results depend on placement or implementation choices. Logical computation gives
semantic identity; physical format and runtime eligibility remain separate checks.

## 9. Validation and delivery

Acceptance tests must establish:

- `latency` versus `log(latency)`, different filters/types/weights/window semantics,
  and different algorithm parameters produce distinct definitions.
- Temporary node renumbering and serialization map order do not alter identity;
  ordered operands and semantic constants are preserved.
- p50/p99 share a persisted KLL definition, while finalized p50/p99 have distinct
  definitions.
- Registering altered content under an existing ID, unresolved dependencies and
  unsupported semantic versions fails explicitly.
- Installation and recovery resolve the complete definition without a live
  Planner process; reclamation preserves referenced definitions.
- Correct definitions with missing coverage, incompatible revisions or corrupt
  payloads remain unreadable; replacement snapshots are not double-counted.
- Writers cannot publish a different definition under an authorized output ID.

Implementation must first establish the Planner-owned export/canonicalization
contract. Legacy definitions lacking required expressions cannot be assigned a
new identity by guessing omitted semantics. They require reconstruction from an
authoritative plan or an explicit unsupported/rebuild outcome.

Plan-schema migration and persisted-payload decoding remain separate. Existing
bytes can be retained only with justified semantic identity and format
compatibility. The [migration plan](asapplanner-migration-plan.md) governs rollout;
these requirements are not claims of completed implementation or tests.
