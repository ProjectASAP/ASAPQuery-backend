# Developing the summary store engine

## Architecture

`storage_engines/sketch_db` separates plain payload taxonomy, SID-keyed index,
lifecycle, persistence, backfill, accuracy, and query reconstruction.

```text
data/        AggKind, AggPayload, encoding and accuracy
index/       instance metadata and per-SID columnar epochs
lifecycle/   Active/Retired/Expired reconciliation and eviction
persistence/ parts, manifest, cache, flusher, recovery
query/       decode, delta apply, window merge, schema timeline
backfill/    job/source/worker contracts
```

## Instance metadata

`SketchInstanceMetadata` stores one descriptor per SID:

| Field | Meaning |
| --- | --- |
| `metric_name` | Canonical metric used by query matching |
| `group_by_keys` | Label names retained by edge/backend aggregation |
| `capability` | Quantile, cardinality, frequency, or top-k support |
| `agg_kind` | Sketch or exact-aggregation kind and compatible parameters |
| `accuracy` | Derived sketch bound; absent for exact state |
| `policy_fp` | Content fingerprint of the producing policy |
| lifecycle timestamps | First seen, retired, and expiry times |

Raw stored-series identity is valid in the system model, but current
`AggPayload` has Sketch and ExactAgg variants; raw samples use the configured
archive/pass-through engine.

## Extension rules

A new payload kind needs canonical identity, immutable per-SID metadata,
memory/disk serialization, recovery decoding, coverage semantics, lifecycle
behavior, metrics, and query readout. Unknown durable types are misses, never
guessed decodes. A storage backend must preserve typed missing/stale/gapped/
incompatible outcomes.

## Verification

Test instance registration, incompatible SID separation, concurrent per-SID
append, exact/overlap reads, delta carry-in, retirement, epoch rotation,
flush-before-evict, memory/disk union, TTL, corrupt/orphan recovery, and restart
discovery. The detailed target interfaces remain in
[summary store and series ID](storage-and-series-id.md).
