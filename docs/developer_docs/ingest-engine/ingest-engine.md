# Developing OTLP summary ingestion

> Interface status: target public API. OTLP decoding, SID resolution, and
> summary handling exist; complete BackendPlan-gated validation is partial.

## 1. Code architecture

```text
OTLP request
    |
    v
SummaryDecoder -> SeriesIdentityResolver -> SummaryValidator
                                              |
                                              v
                                      SummaryStateApplier
                                              |
                                              v
                                         SummaryStore
```

Transport decoding is separate from semantic validation. No decoder is allowed
to append directly to storage or choose a summary family.

## 2. Public interfaces and definitions

```rust
pub trait SummaryDecoder {
    type Error;
    fn decode(&self, request: OtlpMetricsRequest)
        -> Result<Vec<ReceivedSummary>, Self::Error>;
}

pub struct ReceivedSummary {
    pub tenant: String,
    pub resource: AttributeSet,
    pub scope: InstrumentationScope,
    pub metric_name: String,
    pub attributes: AttributeSet,
    pub source_timestamp: Timestamp,
    pub envelope: SummaryEnvelope,
}
```

`SummaryEnvelope` contains plan/materialization/producer/window identity,
family/parameters/encoding, full-or-delta metadata, and payload bytes. Required
identity cannot be inferred from metric-name suffixes.

```rust
pub struct SummaryEnvelope {
    pub plan_id: String,
    pub plan_version: u64,
    pub materialization_id: String,
    pub producer_id: String,
    pub window: LogicalWindow,
    pub family: SummaryFamily,
    pub algorithm: SummaryAlgorithm,
    pub parameters: SummaryParameters,
    pub encoding: SummaryEncoding,
    pub frame: SummaryFrame,
    pub payload: Bytes,
}

pub enum SummaryFrame {
    Full { checkpoint_id: String },
    Delta {
        base_checkpoint_id: String,
        sequence: u64,
    },
}
```

```rust
pub trait SeriesIdentityResolver {
    type Error;
    fn resolve(&self, key: CanonicalMaterializedSeriesKey)
        -> Result<ResolvedSeries, Self::Error>;
}
```

`SeriesId`, `SeriesIdNamespace`, `CanonicalMaterializedSeriesKey`, and `ResolvedSeries` have
one public definition in
[Summary storage and series identity](../summary-store-engine/implementation.md#sid-definition).

```rust
pub trait SummaryValidator {
    type Error;
    fn validate(
        &self,
        received: ReceivedSummary,
        plan: &BackendPlanSnapshot,
        series: ResolvedSeries,
    ) -> Result<ValidatedSummary, Self::Error>;
}

pub trait SummaryStateApplier {
    type Error;
    fn apply(&self, summary: ValidatedSummary)
        -> Result<IngestResult, Self::Error>;
}

pub struct ValidatedSummary {
    pub received: ReceivedSummary,
    pub series: ResolvedSeries,
    pub materialization: ValidatedMaterialization,
}

pub struct ValidatedMaterialization {
    pub plan_id: String,
    pub plan_version: u64,
    pub materialization_id: String,
    pub compatibility_fingerprint: String,
}

pub struct IngestResult {
    pub disposition: IngestDisposition,
    pub plan_id: String,
    pub materialization_id: String,
    pub series_id: SeriesId,
    pub window: LogicalWindow,
    pub queryable_at: Option<Timestamp>,
}

pub enum IngestDisposition {
    AppliedFull,
    AppliedDelta,
    Duplicate,
    Rejected,
    AwaitingCheckpoint,
}
```

Supporting type definitions:

| Type | Definition |
| --- | --- |
| `OtlpMetricsRequest` | Decoded public OTLP ExportMetricsServiceRequest. |
| `AttributeSet` | Canonically typed OTel attributes with no identity-relevant loss. |
| `InstrumentationScope` | OTel scope name/version/schema identifying the producer library. |
| `LogicalWindow` | Start/end plus window identity used by plan, state, and query coverage. |
| `SummaryEncoding` | Versioned state representation shared by collector/backend capabilities. |

Why these interfaces exist: each stage can reject invalid data without changing
queryable state, and `IngestResult` gives the MVP harness unambiguous evidence.

## 3. Adding and verifying functionality

### Add an OTLP summary encoding

1. Extend public `SummaryEnvelope` encoding/version definitions.
2. Implement `SummaryDecoder` without applying state.
3. Add compatibility validation against BackendPlan/capabilities.
4. Implement full/delta application through `SummaryStateApplier`.
5. Verify corrupt bytes return an error and do not change storage.

### Add a delta-capable family

Define base/checkpoint, sequence scope, duplicate handling, gap behavior, and
recovery full state. Verify `AppliedDelta`, `Duplicate`, and
`AwaitingCheckpoint` are distinguishable outputs for reorder/gap tests.

### Add series identity behavior

Add canonical input fields to `CanonicalMaterializedSeriesKey`, never to `SeriesId.value`
alone. Verify label-order independence, tenant isolation, cached-ID conflict
recovery, and stable namespace reporting.

### Interpret and verify output

- `Applied*` means compatible state was committed.
- `Duplicate` means idempotent replay with no second mutation.
- `AwaitingCheckpoint` means a visible delta gap and non-queryable state.
- `queryable_at` is populated only when coverage/readiness is satisfied.
- Freshness uses `source_timestamp -> queryable_at`, not receive time.
