# Developing summary storage and series identity

> Interface status: target public API. Store/index and SID resolution exist;
> plan/materialization lifecycle convergence is partial.

## 1. Code architecture

```text
CanonicalMaterializedSeriesKey -> SeriesRegistry -> SeriesId
              ^                                      |
ValidatedMaterialization ----------------------------+
             |
             v
         SummaryStore
       write / coverage / read / retire
```

The series registry owns canonical **materialized-series** identity. Its key
combines the materialization definition with the canonical metric and concrete
retained label values. The summary store owns the windows and payload state
under the resulting SID. Plan, materialization, and SID identities remain
distinct but related.

## 2. Public interfaces and definitions

```rust
pub struct SeriesId {
    pub namespace: SeriesIdNamespace,
    pub value: u64,
}

pub struct SeriesIdNamespace {
    pub tenant: String,
    pub version: String,
}

pub struct CanonicalMaterializedSeriesKey {
    pub tenant: String,
    pub materialization_fingerprint: String,
    pub metric_name: String,
    pub identifying_labels: BTreeMap<String, String>,
}

pub struct ResolvedSeries {
    pub id: SeriesId,
    pub canonical_key: CanonicalMaterializedSeriesKey,
}

pub trait SeriesRegistry: Send + Sync {
    type Error;

    fn resolve(&self, key: CanonicalMaterializedSeriesKey)
        -> Result<ResolvedSeries, Self::Error>;

    fn lookup(&self, id: &SeriesId)
        -> Result<Option<ResolvedSeries>, Self::Error>;
}
```

### SID definition

The interfaces below are the target public namespace model. The current
implementation uses a backend-allocated `u64` keyed by `(canonical metric,
canonical stored labels, agg_kind_canonical)`. It therefore identifies a stored
summary or exact-aggregate series. The same target model can identify a raw
sample series under a distinct raw materialization kind, but current raw samples
are served through the configured archive/pass-through path rather than a
`SketchStore` raw payload variant. See the
[cross-repository identity design](../summary-series-id/resolver.md).

`SeriesId` (`sid`) is an opaque numeric identifier scoped by exactly one
`SeriesIdNamespace`. The namespace contains the tenant/isolation domain and a
version that changes whenever the authoritative registry is rebuilt without
preserving its previous assignments.

```text
(SeriesIdNamespace, SeriesId.value) <-> CanonicalMaterializedSeriesKey
```

Within one namespace this mapping is one-to-one:

- the same canonical key always resolves to the same SID;
- two different canonical keys never resolve to the same SID; and
- the same numeric value in two namespaces is not the same SID.

`identifying_labels` is ordered by label name before lookup or hashing, so input
label order does not affect identity. Summary family, parameters, aggregation
group, window, materialization ID, and plan ID are excluded because they
identify maintained state, not the source metric series.

`SeriesRegistry::resolve` is idempotent. A sender-provided SID is only a lookup
shortcut; it never overrides a conflicting canonical key.

```rust
pub struct MaterializationKey {
    pub plan_id: String,
    pub plan_version: u64,
    pub materialization_id: String,
    pub tenant: String,
    pub group: MaterializationGroup,
    pub window: LogicalWindow,
}

pub enum MaterializationState {
    Staged,
    Queryable,
    Gapped,
    Draining,
    Expired,
    Rejected,
}
```

```rust
pub trait SummaryStore: Send + Sync {
    type Error;

    fn register(&self, contract: ValidatedMaterialization)
        -> Result<MaterializationState, Self::Error>;

    fn apply(&self, update: ValidatedSummary)
        -> Result<StoreWriteResult, Self::Error>;

    fn coverage(&self, request: CoverageRequest)
        -> Result<CoverageResult, Self::Error>;

    fn read(&self, request: SummaryReadRequest)
        -> Result<SummaryReadResult, Self::Error>;

    fn retire(&self, materialization_id: &str, policy: RetirementPolicy)
        -> Result<MaterializationState, Self::Error>;
}

pub struct CoverageRequest {
    pub catalog: SummaryCatalog,
    pub query_plan: QueryPlan,
    pub materialization_ids: Vec<String>,
    pub range: EvaluationRange,
}

pub struct SummaryReadRequest {
    pub coverage: LogicalCoverage,
    pub route: SummaryRoute,
}

pub struct SummaryReadResult {
    pub states: Vec<SummaryState>,
    pub coverage: LogicalCoverage,
}

pub struct RetirementPolicy {
    pub drain_until: Timestamp,
    pub retain_for_rollback_until: Option<Timestamp>,
}
```

```rust
pub struct StoreWriteResult {
    pub key: MaterializationKey,
    pub state: MaterializationState,
    pub disposition: IngestDisposition,
}

pub enum CoverageResult {
    Complete(LogicalCoverage),
    Missing(Vec<LogicalWindow>),
    Stale { newest_source_timestamp: Timestamp },
    Gapped { producer: String, expected_sequence: u64 },
    Incompatible { reason: String },
}
```

Supporting types `ValidatedMaterialization`, `ValidatedSummary`, and
`IngestDisposition` are defined by
[OTLP summary ingestion](../ingest-engine/ingest-engine.md). `EvaluationRange`,
`SummaryRoute`, and `LogicalCoverage` are defined by
[Query routing and readout](../query-engine/query-engine.md).

Why these interfaces exist: callers receive typed completeness/failure rather
than interpreting an empty collection as “no data,” and storage cannot accept
unvalidated summary bytes.

## 3. Adding and verifying functionality

### Add a summary family to storage

1. Extend public materialization capability/contract types.
2. Define canonical parameters and representation compatibility.
3. Accept only `ValidatedSummary` through `SummaryStore::apply`.
4. Implement merge/read behavior through public result types.
5. Verify incompatible family/parameters/windows never merge.

### Add a storage backend

Implement `SummaryStore` with identical semantic outputs. Persistence or remote
transport must not change coverage, lifecycle, identity, or error behavior.
Verify restart restores metadata before returning `Complete` or `Queryable`.

### Add SID persistence/distribution

Implement `SeriesRegistry` while preserving deterministic canonical keys,
tenant isolation, idempotent resolve, namespace versioning, and conflict
detection. Verify cache loss/restart cannot bind an old SID to new labels.

### Interpret and verify output

- `Queryable` means the registered state may be considered for coverage; it is
  not proof that every requested window is complete.
- Only `CoverageResult::Complete` may proceed to summary readout.
- `Missing`, `Stale`, `Gapped`, and `Incompatible` must remain distinguishable.
- `StoreWriteResult` identifies exactly which plan/materialization/window was
  changed.
- Concurrent plan versions remain isolated through `MaterializationKey`.
