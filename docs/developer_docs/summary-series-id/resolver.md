# Developing summary-series identity

## Architecture

The Backend is the SID authority. `drivers/ingest/series_resolver.rs` maps:

```text
(canonical metric, attributes fingerprint, agg-kind canonical string) -> u64
```

The OTLP receiver handles attribute-bearing registration, ID-only lookup,
assignment responses, and unknown-ID feedback. `SketchStore` owns the reverse
metadata needed after resolution. Collector owns the assignment cache.

## Contract

SID identifies one stored summary/exact/raw materialization series, not
necessarily one original input time series. SID zero is unassigned. A supplied
ID never overrides conflicting labels/kind. Different family, parameters,
filter, or retained label values require different SIDs; window/full/delta does
not.

`FilePersistence` WAL recovery can preserve assignments across Backend restart.
Without it, unknown-ID feedback makes Collector evict and re-register.

## Safe changes

Canonicalization changes are cross-repository wire changes. Version them or
prove old/new equivalence with shared vectors. When adding tenant/distributed
namespaces, make namespace evidence explicit before allowing the same numeric
value in multiple authorities.

## Verification

Test label-order stability, kind/config separation, concurrent idempotent
resolve, conflict handling, ID-only unknown rejection, WAL replay/torn tail,
Collector eviction/relearning, and query label reconstruction. See the
[cross-repository design](https://github.com/ProjectASAP/ASAPCollector/blob/main/docs/design_docs/cross-cutting/summary-series-id.md).
