# Self-Describing Summary: Semantic Definitions and Stored Results

Status: target design. Ad-hoc discovery is a future extension, not implemented
behavior claimed by this document.

## 1. Why SDS?

Stored summary bytes are not enough to determine what they mean.

For example:

```text
KLL(latency)
```

and

```text
KLL(log(latency))
```

may have the same source, grouping, window, and sketch format, but they cannot
be used interchangeably to answer queries.

SDS therefore separates:

```text
SummaryDefinition = what a summary means
StoredSummary     = one concrete result of that definition
```

This supports two use cases:

1. **Bound queries:** an installed QueryPlan reads the specific SDS output
   selected during planning.
2. **Future ad-hoc queries:** Planner can search existing SummaryDefinitions and
   determine whether an SDS can legally support a new query.

SDS describes stored computation. It does not plan queries, execute operators,
or choose materialization boundaries.

## 2. Architecture

```text
                 Planner
                    │
       canonical semantic description
                    ▼
            SummaryDefinition
                    ▲
                    │ definition_id
              StoredSummary
       group + window + payload
                    ▲
                    │ stored_output_id
          installed plan binding
             /             \
    PrecomputePlan       QueryPlan
       writes              reads
```

| Component | Responsibility |
| --- | --- |
| Planner | Defines computation semantics and decides whether an SDS can support a query |
| Deployment compiler | Binds Planner-selected physical outputs to deployed stored outputs |
| SummaryStore | Persists definitions and concrete summary results |
| Shared executor | Executes Planner-provided Physical DAGs |

The store knows **what exists**. Planner decides **what can be used**.

## 3. SummaryDefinition: what does this state mean?

A SummaryDefinition is a canonical semantic description of a persisted result.

It contains enough information to distinguish computations such as:

```text
KLL(latency)

vs.

Project(log(latency))
        ↓
KLL
```

It therefore includes relevant:

- source and filter semantics;
- value expressions and transformations;
- grouping and time semantics;
- summary algorithm and parameters;
- types and operation semantics.

Conceptually:

```yaml
summary_definition:
  id: <semantic-fingerprint>
  semantic_format_version: <version>
  semantics: <canonical-typed-description>
  output: <described-output>
```

The description reuses Planner-defined semantics, but it is **not an executable
plan** and does not need to serialize Planner's complete internal IR. It contains
only the semantic dependencies needed to distinguish and interpret the output.
Definitions and their required dependencies are persisted so recovery does not
require a live Planner process.

Physical placement, encoding, scheduling, retention, readiness, and plan version
are not part of semantic identity. Identity uses a versioned canonical semantic
encoding, not display strings or temporary node IDs. Its encoding and compatibility
rules must be established before persistence; internal Planner refactoring alone
must not force state migration. Unknown semantic versions fail validation.

### Definition boundary

The definition stops at the persisted output.

```text
KLL(latency) ──persist──> state
                            ├── p50
                            └── p99
```

p50 and p99 can therefore share one KLL SummaryDefinition.

If p99 itself is persisted:

```text
KLL(latency) → p99 ──persist──> value
```

then the readout becomes part of that definition.

## 4. StoredSummary: one concrete result

A StoredSummary instantiates a definition for a particular group and time range.
The examples illustrate the contract, not a finalized wire schema.

```yaml
stored_summary:
  key:
    plan_version: 42
    stored_output_id: latency-kll
    group_key: {service: api}
    window: {start_exclusive: '12:00', end_inclusive: '12:01'}
  definition_id: <KLL-latency-definition>
  revision: <input-revision>
  coverage: complete
  format: {schema: kll-v1, encoding: kll-binary-v1}
  payload: <bytes>
```

The record answers:

> Which concrete state is this, what data does it cover, and can it be read?

The SummaryDefinition answers:

> What does this state mean?

SummaryStore persists both:

```text
summary_definitions
    definition_id → SummaryDefinition

stored_summaries
    plan version + deployed output + group + window → StoredSummary
```

Metadata and payload become visible together. Completeness is established from
the producer's input contract, not inferred from interval endpoints alone.
A replacement snapshot replaces a record's revision; readers must not mix its
old metadata with new bytes or count both snapshots as separate inputs.

## 5. Semantic identity vs. deployed-output identity

SDS uses two identities because they answer different questions:

```text
definition_id
    = What does this state mean?

stored_output_id
    = Which authorized deployed output does this state belong to?
```

For example, within the same plan version:

```text
Definition D = KLL(latency, k=200)

                 D
              /     \
           hot      rebuild
```

Both outputs have identical semantics, but hot may be the active serving output
while rebuild is still being validated. Even adding plan version to definition
ID would not distinguish these two outputs.

Therefore:

```text
definition_id = D
stored_output_id = hot
```

must not silently read:

```text
definition_id = D
stored_output_id = rebuild
```

Equal semantics do not imply interchangeable deployed state.

StoredOutputReference binds the two within the enclosing plan version:

```yaml
reference:
  stored_output_id: hot
  definition_id: D
```

It is a plan binding, not another stored object or Materialization catalog.

## 6. Reading a bound SDS

An installed `QueryPlan` selects a deployed output and its expected semantics:

```yaml
reference:
  stored_output_id: latency-kll
  definition_id: D1
```

The query supplies a concrete group and requested time range. Within the installed
plan's namespace, `SummaryStore` locates state by:

```text
(plan_version, stored_output_id, group_key)
    → records ordered/indexed by window
```

For `(42, latency-kll, service=api)`, a query for `(12:00, 12:05]` performs a
range lookup over the available panes. `definition_id` does not select another
producer when this output is absent.

```text
plan version + stored output + group + window → locate concrete state
expected definition + revision + format + coverage → validate that state
```

This requires efficient prefix and window-range lookup; the design does not
prescribe a physical index such as a hash table or B-tree. The installed plan
also supplies any enclosing deployment namespace; equal plan-version numbers
in different deployments do not authorize cross-deployment reads.

The runtime checks two things.

**Semantic compatibility**

The record must have the definition selected by Planner. For a binding expecting
KLL over latency:

```text
KLL(latency)      ✓
KLL(log(latency)) ✗
```

**Instance eligibility**

The concrete record must be committed and have the required:

```text
authorized output / plan version
group
window / coverage
revision
schema / encoding
completeness
```

For example, a five-minute query may consume five compatible one-minute KLL panes:

```text
(12:00, 12:01] ─┐
(12:01, 12:02]  │
(12:02, 12:03]  ├─→ KLL Merge → p99
(12:03, 12:04]  │
(12:04, 12:05] ─┘
```

The runtime verifies complete non-overlapping coverage and compatible revisions.
It does not decide whether KLL merging is semantically legal; Planner already
made that decision. Missing or invalid state follows the installed fallback or
unavailability policy. Plan installation alone does not establish readiness.

## 7. Future: discovering SDS for an unregistered query

The same definitions can later support queries not known when the SDS was created.

Suppose the store already contains:

```text
D1 = KLL(latency)
D2 = KLL(log(latency))
```

and a new query arrives:

```text
p99(latency)
```

Planner can search available definitions:

```text
New query
   +
available SummaryDefinitions
        │
        ▼
Planner semantic matching
        │
        ▼
Can existing SDS support this computation?
        │
        ▼
KLL(latency) → Quantile(0.99)
        │
        ▼
Physical DAG
        │
        ▼
bind to an authorized, eligible stored_output_id
        │
        ▼
QueryPlan
```

Importantly:

```text
KLL(latency) ≠ p99(latency)
```

The SDS is **not equivalent** to the query. It is reusable because Planner knows
a legal computation, subject to the query's accuracy and input requirements:

```text
KLL(latency)
      ↓
Quantile(0.99)
```

Likewise, p99(log(latency)) may reuse KLL(log(latency)), but cannot directly
substitute KLL(latency). Any transformation requires a supported Planner rewrite
with justified domain, numeric and accuracy semantics.

### Who decides reuse?

```text
SummaryStore:
    What SDS definitions and instances exist?

Planner:
    Can they legally support all or part of this query?

Deployment compiler:
    Which authorized deployed output realizes the selected definition?

Runtime:
    Are the required concrete records currently eligible?
```

SummaryStore therefore does not implement a semantic decision engine such as:

```text
find_compatible(query)
```

Semantic compatibility, mergeability, grouping, window composition, accuracy,
and residual computation remain Planner decisions. Backend capability and
availability evidence can inform selection; a definition alone does not guarantee
an executable deployment. Availability must be checked again at execution time.
This extension does not require another catalog service or a new operator IR.

## 8. Key invariants

1. A SummaryDefinition describes semantics, not execution or deployment.
2. Different meanings must not share a definition ID; equivalence requires
   Planner's versioned normalization rather than a store heuristic.
3. Equal definition IDs do not make different deployed outputs interchangeable.
4. A writer cannot publish state with a definition different from its installed binding.
5. Runtime reads require both semantic compatibility and eligible concrete state.
6. Bound QueryPlans directly resolve their selected outputs; they do not search for alternatives.
7. Ad-hoc SDS discovery happens through Planner and produces a new bound QueryPlan.
8. SummaryStore reports available state; it never decides query rewrite legality.

```text
Planner        → what can compute the query
Deployment     → which output to use
SummaryStore   → what state actually exists
Executor       → run the selected computation
```

The [deployment design](asapplanner-integration.md) defines compilation and
execution ownership. The [migration plan](asapplanner-migration-plan.md) defines
implementation and acceptance gates. This document does not claim that semantic
fingerprinting or ad-hoc discovery has been implemented.
