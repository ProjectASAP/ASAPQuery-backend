# Developing query routing and summary readout

> Interface status: target public API. Summary execution and fallback exist;
> BackendPlan is still replacing legacy routing and local query-shape logic.

## 1. Code architecture

```text
protocol request -> QueryAdapter -> QueryService
                                      |
                           BackendPlanSnapshot
                              /              \
                         SummaryReader    ExactQueryClient
                              \              /
                               QueryResponse
```

The adapter owns protocol conversion. `QueryService` owns plan-aware route
selection. `SummaryReader` executes an already selected readout. The exact
client executes only explicit fallback routes.

## 2. Public interfaces and definitions

```rust
pub struct QueryRequest {
    pub tenant: String,
    pub language: QueryLanguage,
    pub expression: String,
    pub evaluation: EvaluationRange,
    pub requested_accuracy: AccuracyRequirement,
}

pub struct EvaluationRange {
    pub start: Timestamp,
    pub end: Timestamp,
    pub step: Option<Duration>,
}
```

```rust
pub trait QueryService {
    type Error;
    async fn execute(&self, request: QueryRequest)
        -> Result<QueryResponse, Self::Error>;
}

pub trait SummaryReader {
    type Error;
    fn read(
        &self,
        request: &QueryRequest,
        route: &SummaryRoute,
        plan: &BackendPlanSnapshot,
    ) -> Result<SummaryReadout, Self::Error>;
}

pub struct SummaryRoute {
    pub query_id: String,
    pub materialization_ids: Vec<String>,
    pub readout: ReadoutSpec,
    pub required_guarantee: AccuracyRequirement,
}

pub struct SummaryReadout {
    pub result: PrometheusResult,
    pub guarantee: ResultGuarantee,
    pub coverage: LogicalCoverage,
}

pub trait ExactQueryClient {
    type Error;
    async fn execute_exact(&self, request: &QueryRequest)
        -> Result<QueryResponse, Self::Error>;
}
```

Supporting public type definitions:

| Type | Definition |
| --- | --- |
| `QueryLanguage` | Language identifier; MVP value is PromQL. |
| `AccuracyRequirement` | Exact, epsilon, or epsilon-delta constraint requested for the result. |
| `ReadoutSpec` | Planner-selected operation and typed parameters applied to maintained state. |
| `PrometheusResult` | Matrix/vector/scalar/string result with labels, timestamps, values, warnings, and errors. |
| `ResultGuarantee` | Effective exact/approximate guarantee of the returned result. |
| `LogicalCoverage` | Requested and actually covered time intervals plus readiness timestamp. |
| `QueryError` | Typed parse, unsupported, inactive-plan, missing/stale/gapped state, or exact-backend failure. |

```rust
pub struct QueryResponse {
    pub result: PrometheusResult,
    pub source: QuerySource,
    pub guarantee: ResultGuarantee,
    pub coverage: LogicalCoverage,
    pub plan_id: Option<String>,
    pub materialization_ids: Vec<String>,
}

pub enum QuerySource {
    Summary,
    ExactFallback,
}
```

Why these interfaces exist: protocol code cannot bypass plan/readiness checks,
and callers can interpret whether an answer is summary-backed or exact with its
coverage and guarantee.

## 3. Adding and verifying functionality

### Add a readout/operator

1. Add the logical semantics and guarantee to ASAPPlanner.
2. Extend public backend readout capability and BackendPlan route types.
3. Implement it through `SummaryReader`; do not parse and choose a family again.
4. Return labels/timestamps/result type through `PrometheusResult`.
5. Compare with the exact backend over identical series and logical range.

### Add a protocol adapter

Convert protocol inputs to `QueryRequest` and `QueryResponse` back to protocol
output. Verify tenant, evaluation timestamps, labels, result type, errors, and
accuracy metadata round-trip unchanged.

### Add an exact backend

Implement `ExactQueryClient`, preserving the complete `QueryRequest`. Verify
remote failures remain errors and are not successful empty vectors.

### Interpret and verify output

- `QuerySource::Summary` requires active plan/materialization IDs and complete
  coverage.
- `QuerySource::ExactFallback` must satisfy exact semantics and carry no false
  summary guarantee.
- Missing/additional labels or timestamps are validation failures.
- Partial/stale/gapped summary state must not return a successful complete
  `QueryResponse`.
- A plan swap during execution must not mix identities in one response.
