# Adding a summary family

> Interface status: cross-repository developer workflow. ASAPQuery-backend
> implements runtime capabilities; ASAPPlanner and summary libraries own logical
> semantics and algorithm guarantees.

## 1. Code architecture

```text
ASAPPlanner public summary/readout types
                  |
                  v
PhysicalCompiler capability match
            /                 \
CollectorPlan                 BackendPlan
      |                           |
ASAPCollector                 SummaryDecoder
update + encode        ->     SummaryStore -> SummaryReader
```

A family is supported only when the same public semantic contract crosses all
components. A decoder or enum variant by itself is not pipeline support.

## 2. Public interfaces and definitions

The following public structures must describe the same family/version:

```rust
pub struct SummaryCapability {
    pub family: SummaryFamily,
    pub algorithm: SummaryAlgorithm,
    pub parameter_schema: ParameterSchema,
    pub encodings: Vec<SummaryEncoding>,
    pub operations: SummaryOperations,
    pub readouts: Vec<ReadoutCapability>,
    pub guarantee_kinds: Vec<GuaranteeKind>,
}

pub struct SummaryOperations {
    pub update: bool,
    pub merge: bool,
    pub subtract: bool,
    pub delete: bool,
    pub full_state: bool,
    pub delta_state: bool,
}
```

```rust
pub trait SummaryDecoder {
    type Error;
    fn decode(&self, request: OtlpMetricsRequest)
        -> Result<Vec<ReceivedSummary>, Self::Error>;
}

pub trait SummaryStore {
    type Error;
    fn register(&self, contract: ValidatedMaterialization)
        -> Result<MaterializationState, Self::Error>;
    fn apply(&self, update: ValidatedSummary)
        -> Result<StoreWriteResult, Self::Error>;
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
```

Definitions:

| Interface | Input | Output |
| --- | --- | --- |
| Planner mapping | PromQL workload and constraints | Selected logical summary producer/readout and guarantee |
| Capability | Family/algorithm/version | Supported parameters, encodings, operations, readouts, guarantees |
| Decoder | OTLP request | Untrusted `ReceivedSummary` values |
| Validator | Received summary + active plan | `ValidatedSummary` or structured error |
| Store | Validated materialization/update | Lifecycle/write result |
| Reader | Query + selected route + plan snapshot | Summary readout with coverage/guarantee |

Why these interfaces exist: every stage can compare exact typed semantics and
reject unsupported combinations instead of mapping a new family to a similar
legacy one.

## 3. Adding and verifying a family

### Step 1: define logical semantics outside this repository

Add the query mapping, readout, composability, and guarantee to ASAPPlanner.
Add update/merge/encoding behavior and mathematical guarantee to the owning
summary library. Record concrete PromQL examples.

### Step 2: advertise runtime capability

Add `SummaryCapability` values for collector and backend. Declare only the
parameter ranges, encodings, operations, and readouts actually implemented.
Verify the physical compiler rejects a candidate if either side lacks one
required capability.

### Step 3: ingest and store

Implement decode to `ReceivedSummary`, validation to `ValidatedSummary`, and
store application through public interfaces. Include family, algorithm,
canonical parameters, encoding version, grouping, and window in compatibility
identity.

### Step 4: execute readout

Implement `SummaryReader::read` for the Planner-selected readout. Preserve
labels, timestamps, result type, logical coverage, and guarantee. Do not choose
the family again from query text.

### Step 5: interpret and verify output

For a quantile family, an end-to-end example is:

```promql
quantile_over_time(0.95, request_duration_seconds[5m])
```

Verify:

- output series align with exact results by labels and timestamps;
- reported guarantee matches the selected parameterization;
- errors satisfy the predeclared SLA over identical input;
- full and delta state have equivalent query semantics when delta is claimed;
- corrupt, mismatched, stale, gapped, or unsupported input returns a structured
  failure and does not change queryable state; and
- `QueryResponse.source`, plan/materialization IDs, coverage, and freshness
  prove which implementation produced the answer.

Unit tests for serialization are necessary but do not establish cross-repository
support.
