# Query-to-Sketch Translation: How QL Maps to Sketch Execution

This document explains how a PromQL or SQL query is translated through the
control plane's five-layer architecture and ultimately mapped to sketch-based
distributed execution.

## 1. Five-Layer Architecture

The control plane is structured as a five-layer pipeline.  Each layer has a
clear input, output, and responsibility:

```
Query workloads
  │
  │  Layer 1 — Query Language
  │  (PromQL, SQL, DataFusion, ElasticDSL, ...)
  ▼
Language-specific AST
  │
  │  Layer 2 — Language Logical Plan
  │  (each language's own relational/query algebra)
  ▼
Language Logical Plan
  │
  │  Layer 3 — Sketch Logical Plan (Sketch Algebra)
  │  (language-independent, implementation-independent)
  ▼
Sketch Logical Plan
  │
  │  Layer 4 — Sketch Optimizer
  │  (rewrite rules on the sketch logical plan)
  ▼
Optimised Sketch Logical Plan
  │
  │  Layer 5 — Physical Execution Plan
  │  (concrete implementations for a specific deployment)
  ▼
Physical Plan (edge processors, backend sketchDB, backend original DB, object store)
```

### What each layer owns

| Layer | Input | Output | Responsibility |
|---|---|---|---|
| **1. Query Language** | query string | language AST | grammar, parsing |
| **2. Language Logical Plan** | AST | language-specific relational plan | language semantics (PromQL instant/range vectors, SQL frames, Elastic buckets) |
| **3. Sketch Logical Plan** | language plan | sketch algebra tree (`QueryExpr`) | **what** to compute: aggregation intent + accuracy requirement + window semantics — no sketch names, no implementation details |
| **4. Sketch Optimizer** | sketch plan + deployment constraints | optimised sketch plan | cost-aware rewrites: push-down, fusion, elimination, budget-driven deferral — considers physical deployment constraints (memory budgets, network topology, available backends) |
| **5. Physical Plan** | optimised plan + deployment config | executable plan | **how** to execute: edge processors (sketch build), backend sketchDB (merge + query), backend original DB (exact), object store (raw backup) |

### Key design principle

**Layers 1–3 are query-language-independent and workload-independent.**
They define *what* to compute without reference to any specific query language,
sketch implementation, or deployment topology.  A `Quantile { φ=0.99, accuracy=0.01 }`
intent is the same whether it came from PromQL, SQL, DataFusion, or ElasticDSL,
and whether the deployment is a single node or a 1000-agent fleet.

**Layer 4 is deployment-constraint-aware.**
The optimizer considers physical deployment constraints — memory budgets per stage,
network bandwidth, available backends — when applying cost-based rewrite rules
(e.g., deferring a sketch from Agent to Backend when the agent memory budget is
exceeded, or fusing TopK when the downstream merge is expensive).

**Layer 5 is deployment-specific.**
The physical planner commits to concrete implementations based on the specific
setup: edge processors, backend sketchDB, backend original DB, or object store.

### `AggIntent` — the Layer 3 aggregation vocabulary

| `AggIntent` variant | Meaning | Physical candidates (Layer 5) |
|---|---|---|
| `Quantile { quantiles, accuracy }` | "I need quantile estimates at these φ values within this error" | DDSketch, KLL, t-digest, PromSketch EHKLL |
| `Cardinality { accuracy }` | "I need a distinct-count estimate within this error" | HLL, UnivMon, PromSketch EHUniv |
| `Frequency { accuracy }` | "I need frequency estimates within this error" | CountSketch, CountMinSketch |
| `Extrema { min, max }` | "I need exact min/max" | ExactMinMax, DDSketch at φ=0/1 |
| `PerPartition { inner, keys }` | "Run inner once per distinct key tuple" | Hydra, per-key sketch instances |
| `Exact(Sum\|Count\|Avg\|Min\|Max)` | "No sketch benefit — exact computation" | Raw passthrough, DB-side |

The flow:
- **Layers 1–2** (parsers): "this query needs a quantile at φ=0.99" → `Aggregate { Quantile(0.99) }`
- **Layer 3** (lowering): `Aggregate` → `SketchAgg { AggIntent::Quantile }` (shared by all languages)
- **Layer 4** (optimizer): rewrites the plan considering deployment constraints
- **Layer 5** (physical planner): "for this deployment, DDSketch at the edge is cheapest" or "KLL at the backend sketchDB is better for this workload"

## 2. Sketch Logical Plan: `QueryExpr` (Layer 3)

`QueryExpr` (`algebra/expr.rs`) is the sketch algebra IR — a **logical plan** that
normalises all query languages into a common algebraic form.

| | AST (syntax tree) | Logical Plan (QueryExpr) |
|---|---|---|
| **Structure** | Mirrors the grammar | Mirrors relational algebra operators |
| **Semantics** | Preserves syntactic details | Preserves only operator semantics |
| **Sketch types** | N/A | Implementation-independent intents (`AggIntent`) |
| **Language** | Language-specific | Language-independent (shared by SQL, PromQL, etc.) |

QueryExpr has **25 operator variants** organized into categories:

**Relational core** — standard relational algebra:
- `Source` — base metric / table (leaf node)
- `Filter { pred, input }` — selection (σ)
- `Project { cols, input }` — projection (π)
- `Aggregate { keys, aggs, having, input }` — grouping + aggregation (γ)
- `Join { kind, pred, left, right }` — relational join (⋈)
- `SetOp { kind, all, left, right }` — UNION / INTERSECT / EXCEPT
- `Sort { keys, input }` — ORDER BY
- `Limit { n, offset, input }` — LIMIT / OFFSET

**Sketch-specific** — operators that express sketch computation intent:
- `SketchAgg { op: AggIntent, col, input }` — sketch aggregation intent (what, not how)
- `WindowedAgg { agg: AggIntent, window: WindowSpec, col, input }` — bundled window + sketch agg (window defines sketch lifecycle)
- `Partition { keys, input }` — GROUP BY distribution for distributed sketches
- `Dedup { col, input }` — deduplication (absorbed by cardinality sketches)
- `TopK { k, by, input }` — top-K heavy-hitter query
- `Merge { inputs }` — sketch merge (linearity: sketch(A∪B) = merge(sketch(A), sketch(B)))
- `JoinSketch { join_key, outer, inner }` — sketch-aware join push-down

**Time / streaming** — window operators:
- `Window { duration, slide, input }` — standalone time window (batching)

**PromQL-specific** — operators that preserve PromQL semantics:
- `HistogramQuantile { phi, input }` — `histogram_quantile(φ, …)`
- `PromQLSubquery { range, resolution, input }` — `expr[range:resolution]`
- `BinaryOp { op, lhs, rhs, vector_match }` — vector binary arithmetic with matching

**Structural** — subqueries and bindings:
- `Subquery`, `LetBinding`, `Ref`, `WindowFunc`

### Window operators: `Window` vs `WindowedAgg`

| Operator | Use | Why separate |
|---|---|---|
| `Window { duration, slide }` | Standalone time batching (no sketch) | Used when the sketch op is a separate `SketchAgg` child node |
| `WindowedAgg { agg, window, col }` | Bundled window + sketch aggregation | In sketch systems the window defines the sketch lifecycle (when to flush/reset). Bundling lets the physical planner choose the best implementation (edge tumbling flush vs backend sketchDB EH vs original DB time_bucket). |

`WindowSpec` supports five window kinds:

| `WindowKind` | Semantics | Example |
|---|---|---|
| `Tumbling { size }` | Fixed-size, non-overlapping | PromQL implicit, SQL `TUMBLE(ts, '5m')`, Elastic `fixed_interval` |
| `Sliding { size, slide }` | Fixed-size, overlapping | PromQL `[5m]` range vector, SQL `HOP(ts, '1m', '5m')` |
| `Unbounded` | All samples, no time dimension | SQL `GROUP BY key` without time |
| `Landmark` | From epoch to now (cumulative) | Running aggregates |
| `Session { gap }` | Gap-based, closes after inactivity | Elastic session windows |

## 3. Layers 1–3: Query Language → Language Plan → Sketch Algebra

### 3.1 Layer 1→2: Language AST → Language Logical Plan

Each parser takes a language-specific AST (from an external crate) and produces
a **language logical plan** using relational operators (`Aggregate`, `Window`,
`Filter`, `Sort`, `Limit`, etc.) with generic `AggFunc` variants — no sketch
names at this layer.

**PromQL** (`query_parser/promql.rs`):

```
PromQL AST (promql-parser crate)
  ↓ walk_qe(ast_node, ctx)
Language Logical Plan (Aggregate + AggFunc + Window)
```

The walker carries context downward: `partition` (GROUP BY keys), `topk` (K value),
`outer_count` (whether wrapped in `count(…)`).

| PromQL construct | Layer 2 output |
|---|---|
| `quantile_over_time(φ, m[5m])` | `Aggregate { Quantile(φ), input: Window { 5m, Filter(Source) } }` |
| `histogram_quantile(φ, rate(…))` | `HistogramQuantile { φ, Aggregate { Quantile(φ), Window(...) } }` |
| `count_over_time(m[5m])` | `Aggregate { Count, input: Window { 5m, Source } }` |
| `avg_over_time(m[5m])` | `Aggregate { Avg, input: Window { 5m, Source } }` |
| `topk(k, …) by (dims)` | `TopK { k, Partition { dims, inner } }` |
| `a + b` | `BinaryOp { Add, lhs, rhs, VectorMatch }` |
| `m[5m:1m]` | `PromQLSubquery { range: 5m, step: 1m, inner }` |

**SQL** (`query_parser/sql.rs`):

```
SQL AST (sqlparser crate)
  ↓ extract_select_qe(select, order_by, limit, offset)
Language Logical Plan (Aggregate + AggFunc + Sort + Limit)
```

The SQL parser builds the plan bottom-up from SELECT clauses:

| SQL construct | Layer 2 output |
|---|---|
| `FROM table` | `Source(table)` |
| `WHERE pred` | `Filter(ScalarExpr, Source)` |
| `JOIN … ON` | `Join(kind, pred, left, right)` |
| `GROUP BY keys` + agg functions | `Aggregate { keys, aggs: [AggItem { func }] }` |
| `TUMBLE(ts, INTERVAL '5m')` | `Aggregate { input: Window { 5m, Source } }` |
| `ORDER BY … DESC` | `Sort(keys, input)` |
| `LIMIT n` | `Limit(n, input)` |
| `UNION ALL` | `SetOp(Union, all, left, right)` |

### 3.2 Layer 2→3: Language Logical Plan → Sketch Algebra (lowering)

The shared `lower_to_sketch_algebra()` pass (`algebra/lower.rs`) converts
language-independent `Aggregate { AggFunc }` nodes into sketch algebra
`SketchAgg { AggIntent }` nodes. This is the same pass for both PromQL and SQL.

**Algorithm**:

```
lower_to_sketch_algebra(expr):
  Recursively walk the QueryExpr tree.
  For each single-agg Aggregate node:

  1. Map AggFunc → AggIntent (implementation-independent):
     Quantile(φ)   → AggIntent::Quantile { [φ], accuracy }
     CountDistinct  → AggIntent::Cardinality { accuracy }
     Count (w/ GROUP BY) → AggIntent::Frequency { accuracy }
     Avg            → AggIntent::Quantile { [0.5], accuracy }  (median proxy)
     Min            → AggIntent::Extrema { min: true }
     Max            → AggIntent::Extrema { max: true }
     StdDev         → AggIntent::Quantile { [0.25, 0.75], accuracy }  (IQR proxy)
     Sum/Rate/Delta → AggIntent::Exact(Sum)
     Count (no GROUP BY) → stays as Aggregate (no sketch benefit)

  2. If the Aggregate's input is a Window, fuse into WindowedAgg:
     Aggregate { AggFunc, input: Window { duration } }
       → WindowedAgg { AggIntent, WindowSpec { Tumbling(duration) }, input }

  3. If the Aggregate had GROUP BY keys, wrap with Partition:
     → Partition { keys, input: SketchAgg/WindowedAgg }

  Multi-agg Aggregates and HAVING clauses pass through unchanged.
```

| Layer 2 input | Layer 3 output |
|---|---|
| `Aggregate { Quantile(0.99), Window { 5m, Source } }` | `WindowedAgg { Quantile([0.99]), Tumbling(5m), Source }` |
| `Aggregate { CountDistinct, Source }` | `SketchAgg { Cardinality, Source }` |
| `Aggregate { Count, keys: [region], Source }` | `Partition { [region], SketchAgg { Frequency, Source } }` |
| `Aggregate { Avg, keys: [symbol], Source }` | `Partition { [symbol], SketchAgg { Quantile([0.5]), Source } }` |
| `Aggregate { Sum, Source }` | `SketchAgg { Exact(Sum), Source }` |
| `Aggregate { Count (no GROUP BY), Source }` | unchanged (no sketch benefit) |

**SQL function → AggFunc mapping**:

| SQL function | AggFunc | Sketch candidate |
|---|---|---|
| `COUNT(*)` with GROUP BY | `Count` | CountSketch / CountMinSketch |
| `COUNT(*)` without GROUP BY | `Count` | Exact (no sketch benefit) |
| `COUNT(DISTINCT col)` | `CountDistinct` | HLL |
| `SUM(col)` | `Sum` | Exact (not sketchable) |
| `AVG(col)` | `Avg` | DDSketch (p50 proxy) or Exact(Avg) |
| `MIN(col)` | `Min` | DDSketch (φ=0.0) or ExactMinMax |
| `MAX(col)` | `Max` | DDSketch (φ=1.0) or ExactMinMax |

Note: the parser emits `Aggregate { func: Avg }` — it does **not** emit sketch ops.
Sketch assignment happens later in the optimizer (R9 HydraConversion) and allocator.
The SQL parser only produces relational operators; the PromQL parser is more aggressive
and emits `SketchAgg` nodes directly because PromQL functions like `quantile_over_time`
have a 1-to-1 mapping to sketch types.

## 4. Concrete Example: PromQL (all 5 layers)

### Query
```promql
quantile_over_time(0.99, http_request_duration{env="prod"}[5m])
```

### Layer 1 — Language AST

The `promql-parser` crate parses the string into a PromQL AST:
`Call("quantile_over_time", [NumberLiteral(0.99), MatrixSelector("http_request_duration", {env="prod"}, 5m)])`

### Layer 2 — Language Logical Plan (parser output)

The PromQL parser emits **relational operators only** — `Aggregate { AggFunc }` + `Window`,
no sketch names:

```
Aggregate {
  keys: [],
  aggs: [AggItem { func: Quantile(0.99), col: SampleValue }],
  input: Window {
    duration: 5m,
    input: Filter {
      pred: Column("env") = Literal("prod"),
      input: Source("http_request_duration")
    }
  }
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

The shared `lower_to_sketch_algebra()` pass converts `Aggregate { Quantile }` to
`AggIntent::Quantile` and fuses with `Window` into `WindowedAgg`:

```
WindowedAgg {
  agg: Quantile { quantiles: [0.99], accuracy: 0.01 },
  window: WindowSpec { kind: Tumbling { size: 5m } },
  col: SampleValue,
  input: Filter {
    pred: Column("env") = Literal("prod"),
    input: Source("http_request_duration")
  }
}
```

Note: no sketch implementation names — just "I need a quantile at φ=0.99 with ≤1% error."

### Layer 4 — Optimizer

R1 (PredicatePushDown): filter is already below the window — no change. Tree is returned as-is.

### Layer 5 — Physical Plan

`physical::plan(expr, config)` produces a `PhysicalNode` tree. For this simple
query, all nodes are at the Agent — no Exchange boundaries:

```
OtelSketchBuild { DDSketch, OtelTumblingFlush(5m) }  [AgentCollector]
  └── Filter { env="prod" }                          [AgentCollector]
        └── OtlpScan                                 [AgentCollector]
```

Resolution: `Quantile([0.99], 0.01)` → `DDSketch { relative_accuracy: 0.01, quantiles: [0.99] }`,
`Tumbling(5m)` at AgentCollector → `OtelTumblingFlush { 5m }`.

### Execution

1. **Agent** receives raw samples → filters `env="prod"` → batches 5m windows → DDSketch → emit
2. **Backend** merges DDSketches from N agents
3. **Query time**: extract 0.99 quantile from merged DDSketch

---

## 5. Concrete Example: PromQL with Top-K (all 5 layers)

### Query
```promql
topk by (service) (10, count_over_time(requests{env="prod"}[1m]))
```

### Layer 1 — Language AST

The `promql-parser` crate parses this as:
`Aggregate(op="topk", param=10, modifier=By(["service"]), expr=Call("count_over_time", MatrixSelector("requests", {env="prod"}, 1m)))`

### Layer 2 — Language Logical Plan (parser output)

The PromQL parser emits relational operators.  The `by (service)` partition keys
are propagated into the inner `Aggregate`'s GROUP BY keys, so the lowering pass
can see `Count WITH GROUP BY` → `Frequency`:

```
TopK {
  k: 10,
  by: ["service"],
  input: Aggregate {
    keys: ["service"],
    aggs: [AggItem { func: Count, col: SampleValue }],
    input: Window {
      duration: 1m,
      input: Filter {
        pred: Column("env") = Literal("prod"),
        input: Source("requests")
      }
    }
  }
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

`lower_to_sketch_algebra()` converts `Aggregate { Count, keys: ["service"] }` →
`Partition { ["service"], WindowedAgg { Frequency } }`.  The `Window + Aggregate`
fuses into `WindowedAgg`:

```
TopK {
  k: 10,
  by: ["service"],
  input: Partition {
    keys: By(["service"]),
    input: WindowedAgg {
      agg: Frequency { accuracy: 0.001 },
      window: WindowSpec { kind: Tumbling { size: 1m } },
      col: SampleValue,
      input: Filter {
        pred: env = "prod",
        input: Source("requests")
      }
    }
  }
}
```

Note: `Frequency`, not `CountSketch` — implementation-independent. Both CountSketch
and CountMinSketch are valid candidates; the physical planner decides.

### Layer 4 — Optimizer

R1 (PredicatePushDown): filter already below window — no change.

### Layer 5 — Physical Plan

`physical::plan(expr, config)` produces a multi-stage `PhysicalNode` tree with
Exchange nodes at stage boundaries:

```
TopK { k: 10 }                                          [QueryEngine]
  └── Exchange { SketchBinary }                          [QueryEngine]
        └── HashAggregate { keys: ["service"] }          [BackendCollector]
              └── Exchange { Otlp }                      [BackendCollector]
                    └── OtelSketchBuild { CountSketch,   [AgentCollector]
                          OtelTumblingFlush(1m) }
                          └── Filter { env="prod" }      [AgentCollector]
                                └── OtlpScan             [AgentCollector]
```

Three stages, two Exchange boundaries:
- **Agent → Backend** (Otlp): sketch data flows from agent collectors to merge tier
- **Backend → QueryEngine** (SketchBinary): merged sketches flow to query engine for top-K

### Execution

1. **Agent** → filters → builds CountSketch per 1m window → emits via OTLP
2. **Backend** → merges CountSketches per service
3. **QueryEngine** → extracts top-10 services by frequency

---

## 6. Concrete Example: SQL (all 5 layers)

### Query
```sql
SELECT symbol, AVG(price) FROM trades GROUP BY symbol
```

### Layer 1 — Language AST

`sqlparser` produces: `Select { projection: [Identifier("symbol"), Function(AVG, "price")], from: [Table("trades")], group_by: [Identifier("symbol")] }`

### Layer 2 — Language Logical Plan

Both parsers emit the same kind of output — relational `Aggregate { AggFunc }`:

```
Aggregate {
  keys: ["symbol"],
  aggs: [AggItem { func: Avg, col: Named("price"), alias: "avg" }],
  having: None,
  input: Source("trades")
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

`lower_to_sketch_algebra()` converts `Avg` → `Quantile { [0.5], 0.01 }` (median proxy):

```
Partition {
  keys: By(["symbol"]),
  input: SketchAgg {
    op: Quantile { quantiles: [0.5], accuracy: 0.01 },
    col: Named("price"),
    input: Source("trades")
  }
}
```

However, `Avg` is **non-mergeable** (`avg(A∪B) ≠ merge(avg(A), avg(B))`).
The stage-split will route this to DB for exact computation.

### Layer 4 — Optimizer

No rewrites applicable.

### Layer 5 — Physical Plan

`physical::plan()` assigns the non-mergeable Aggregate to the Database:

```
DbQuery { GROUP BY ["symbol"] }          [Database]
  └── Exchange { RawSamples }            [Database]
        └── OtlpScan                     [AgentCollector]
```

The Agent passes raw samples through to the Database, which computes exact AVG.

---

## 7. Concrete Example: SQL with TUMBLE window (all 5 layers)

### Query
```sql
SELECT region, COUNT(DISTINCT user_id) AS cnt
FROM sessions
GROUP BY region, TUMBLE(ts, INTERVAL '5' MINUTE)
ORDER BY cnt DESC LIMIT 10
```

### Layer 1–2 — Parse to relational operators

The SQL parser detects `TUMBLE(ts, INTERVAL '5' MINUTE)` in GROUP BY and emits
a `Window` node. `COUNT(DISTINCT user_id)` becomes `AggFunc::CountDistinct`:

```
Limit {
  n: 10,
  input: Sort {
    keys: [{ col: "cnt", desc: true }],
    input: Aggregate {
      keys: ["region"],
      aggs: [AggItem { func: CountDistinct, col: Named("user_id"), alias: "cnt" }],
      input: Window {
        duration: 5m,
        input: Source("sessions")
      }
    }
  }
}
```

### Layer 3 — Sketch Logical Plan (after lowering)

`lower_to_sketch_algebra()` converts `CountDistinct` → `Cardinality { 0.01 }` and
fuses `Window + Aggregate` → `WindowedAgg`:

```
Limit {
  n: 10,
  input: Sort {
    input: Partition {
      keys: By(["region"]),
      input: WindowedAgg {
        agg: Cardinality { accuracy: 0.01 },
        window: WindowSpec { kind: Tumbling { size: 5m } },
        col: Named("user_id"),
        input: Source("sessions")
      }
    }
  }
}
```

### Layer 4 — Optimizer

**R5 (TopKFusion)**: `Limit(10, Sort(desc, ...))` → fused into `TopK { k: 10 }`

### Layer 5 — Physical Plan

`physical::plan()` produces a multi-stage tree:

```
TopK { k: 10 }                                        [QueryEngine]
  └── Exchange { SketchBinary }                        [QueryEngine]
        └── HashAggregate { keys: ["region"] }         [BackendCollector]
              └── Exchange { Otlp }                    [BackendCollector]
                    └── OtelSketchBuild { HLL,         [AgentCollector]
                          OtelTumblingFlush(5m) }
                          └── OtlpScan                 [AgentCollector]
```

Resolution: `Cardinality(0.01)` → `HLL { precision: 14 }`, `Tumbling(5m)` → `OtelTumblingFlush`.

### Execution

1. **Agent**: builds one HLL per region per 5m window → emits via OTLP
2. **Backend**: merges HLLs from N agents (HLL merge = set union)
3. **QueryEngine**: extracts cardinality per region → top 10

---

## 8. Optimizer: Formulation of the Sketch Placement Problem

### Optimization Goal

Given a set of query workloads Q = {q₁, q₂, …, qₙ} and a deployment with
pipeline stages S = {Agent, BackendCollector, BackendDB, OriginalDB, ObjectStore},
the optimizer solves:

```
minimize    TotalCost(P)
subject to  Accuracy(qᵢ, P) ≤ accuracy_sla(qᵢ)      ∀ qᵢ ∈ Q
            Latency(qᵢ, P) ≤ latency_sla(qᵢ)         ∀ qᵢ ∈ Q
            Throughput(qᵢ, P) ≥ throughput_sla(qᵢ)    ∀ qᵢ ∈ Q
            ResourceUsage(s, P) ≤ Budget(s)             ∀ s ∈ S
```

where P is the physical plan (sketch type assignment + stage placement + window
configuration for each query operator).

### Cost Model

The total cost decomposes into per-stage costs:

```
TotalCost(P) = Σ_s [ BandwidthCost(s) + MemoryCost(s) + CPUCost(s) + StorageCost(s) ]
```

Each term is the aggregate resource consumption across all queries assigned to
that stage:

| Cost component | Formula |
|---|---|
| `BandwidthCost(s)` | Σ_q transmission_bytes(sketch(q)) × flush_rate(q) |
| `MemoryCost(s)` | Σ_q memory_per_series(sketch(q)) × series_count(q) |
| `CPUCost(s)` | Σ_q cpu_per_insert(sketch(q)) × samples_per_sec(q) |
| `StorageCost(s)` | Σ_q transmission_bytes(sketch(q)) × retention(q) |

### Constraints

**Per-stage resource budgets** — each stage has memory, CPU, disk, and bandwidth limits:

```
∀ s ∈ S:
  Σ_q memory_per_series(sketch(q, s)) × series_count(q) ≤ s.memory_bytes
  Σ_q cpu_per_insert(sketch(q, s)) × samples_per_sec(q) ≤ s.cpu_budget
  Σ_q transmission_bytes(sketch(q, s)) × flush_rate(q)  ≤ s.bandwidth_budget
```

**Accuracy constraint** — sketch error must be within the query's SLA:

```
∀ qᵢ:
  error(sketch_type(qᵢ), sketch_params(qᵢ)) ≤ accuracy_sla(qᵢ)
```

For example: DDSketch with `relative_accuracy = 0.01` guarantees ≤1% relative error
on quantile queries. HLL with `precision = 14` guarantees ≤0.8% relative error
on cardinality.

**Functional constraint** — the sketch must support the query's aggregation intent:

```
∀ qᵢ:
  intent(qᵢ) ∈ sketch_capability(sketch_type(qᵢ)).supported_intents
```

For example: a `Cardinality` intent can only be served by a sketch with
`SupportedIntent::Cardinality` (HLL, UnivMon), not by DDSketch.

### Decision Variables

For each query operator `op` in the plan:

1. **Sketch type selection**: `sketch_type(op) ∈ candidates(intent(op))`
   - Quantile → {DDSketch, KLL}
   - Cardinality → {HLL}
   - Frequency → {CountSketch, CountMinSketch}

2. **Stage placement**: `stage(op) ∈ S`
   - Subject to `stage_budget(stage(op)).fits(sketch_capability(sketch_type(op)))`
   - Deferral chain: Agent → BackendCollector → BackendDB

3. **Window configuration**: `window(op) ∈ {Tumbling(d), Sliding(d, s), Unbounded}`
   - Subject to sketch capability: `sketch_capability(type).supports_sliding_window`

4. **Delta encoding**: `delta(op) ∈ {true, false}`
   - Subject to: `sketch_capability(type).supports_delta`
   - Reduces bandwidth at the cost of reconstruction at the receiver

### Cross-Query Optimization: What to Precompute

When multiple queries share overlapping time series or aggregation patterns, the
optimizer can amortise costs:

**Shared sketch reuse**: if q₁ = `quantile_over_time(0.99, m[5m])` and
q₂ = `quantile_over_time(0.5, m[5m])`, a single DDSketch serves both
(DDSketch can answer any quantile from one structure).

**Precomputation decision**: a query should be precomputed (sketch maintained
continuously) rather than computed on-demand when:

```
precompute(q) = true  iff  repeat_interval(q) < query_latency_sla(q)
```

i.e., the query fires more often than the system can recompute it from raw data.
Precomputed sketches are maintained at the Agent and merged at the Backend,
with the Precompute Engine answering queries against the merged state.

**Multi-query sketch sharing matrix**: for N queries over the same metric, the
optimizer builds a sharing matrix:

| | DDSketch | HLL | CountSketch |
|---|---|---|---|
| q₁: quantile(0.99) | ✓ serves | ✗ | ✗ |
| q₂: quantile(0.5) | ✓ **shared with q₁** | ✗ | ✗ |
| q₃: count_distinct | ✗ | ✓ serves | ✗ |
| q₄: topk(10) | ✗ | ✗ | ✓ serves |

One DDSketch instance serves both q₁ and q₂ → memory cost counted once, not twice.

### Current Implementation

The optimizer currently solves a simplified version:

1. **Per-query greedy**: each query is optimised independently (no cross-query sharing yet)
2. **Sketch selection**: `CostModelPlanner` scores all candidates per query, picks cheapest meeting accuracy SLA
3. **Stage placement**: `physical::decide_sketch_placement()` checks `StageBudget::fits(SketchCapability)` per stage in order: Agent → Backend → QueryEngine
4. **Precomputation**: `should_precompute(q)` checks `repeat_interval < latency_sla`

Future work:
- Global optimisation across queries (shared sketch instances)
- Joint sketch+stage+window optimisation (currently done greedily per dimension)
- Workload-adaptive re-optimisation (replan when query patterns change)

---

## 9. Sketch Directory: Which Sketch for Which Operation?

The sketch directory (`algebra/directory.rs`) maps aggregation types to candidate
sketch families.  The `CostModelPlanner` scores all candidates and picks the
cheapest that meets the accuracy SLA.

### Candidates per Aggregation Type

| Aggregation | Candidates (default first) | When non-default is chosen |
|---|---|---|
| Quantile | **DDSketch**, KLL | KLL when memory-constrained |
| Cardinality | **HLL** | Single candidate |
| Frequency | **CountSketch**, CountMinSketch | Based on cost model scoring |

### Sketch Type → OTel Collector Processor

| SketchType | Go processor | Key parameters |
|---|---|---|
| DDSketch | `ddsketch` | relative_accuracy, quantiles |
| KLL | `KLL` | k, quantiles |
| HLL | `HLL` | (fixed precision in Go code) |
| CountSketch | `countsketch` | epsilon, delta |
| CountMinSketch | `countmin` | rows, cols, metric_name |

### Stage Assignment Rules

| QueryExpr node | Default stage | Deferral trigger |
|---|---|---|
| Source, Filter, Window, SketchAgg | **Agent** | Memory budget exceeded → Backend |
| Partition, Merge, Dedup, Exact(Sum/Count/Min/Max) | **Backend** | Memory exceeded → Precompute |
| TopK, HistogramQuantile, BinaryOp, PromQLSubquery | **Precompute** | — |
| Exact(Avg) | **DB** | Non-mergeable — cannot distribute |

When an Agent sketch exceeds the memory budget, it is deferred to Backend.
If it also exceeds the Backend budget, it moves to Precompute.  Every deferral
is logged in `StagedPlan.deferral_log` for observability.
