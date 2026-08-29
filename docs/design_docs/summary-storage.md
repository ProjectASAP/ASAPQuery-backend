# Summary storage

> Status: active implementation with known MVP limitations
>
> MVP relation: stores windowed summary and exact-aggregation state for the
> ASAP warm query tier.

Developer guide:
[Summary storage and series identity](../../data_plane/docs/developer_docs/summary-storage-and-series-identity.md).

## TL;DR

The backend implements summary storage as `SketchStore`, a two-level index:

```text
summary-series ID (sid) -> SketchInstanceMetadata
summary-series ID (sid) -> label dictionary + mutable epoch + sealed epochs
```

Ingestion resolves a SID, registers aggregation metadata on first sight, and
appends a payload under `(sid, label values, logical window)`. Query planning
and routing select compatible SIDs; storage reads overlapping windows from
memory and durable parts. Sketch bytes are decoded and full/delta frames are
reconstructed at readout. Exact aggregates are accumulator objects in memory
and serialized accumulator states on disk where a decoder exists.

The SID model can also identify a raw/pass-through series when its
materialization kind is raw. The current in-process `SketchStore` payload enum
implements sketch and exact-aggregate variants; raw samples currently use the
configured archive/pass-through path. SID names the stored series, not
necessarily the source time series and not necessarily a sketch.

## Scope and goals

This document explains the current implementation for developers and
architecture reviewers: write keys, memory layout, persistence, read behavior,
lifecycle, concurrency, and failure modes. It does not choose summaries, define
PromQL mappings, or define sketch mathematics.

Required behavior is:

- incompatible materializations never share state;
- missing windows are not interpreted as zero;
- full/delta lineage is reconstructed only from a valid base;
- a durable part becomes visible before its memory copy is evicted; and
- reconfiguration cannot make retired state writable again.

## End-to-end path

```text
ASAPCollector modified OTLP
        |
        v
decode and canonicalize metric, labels, materialization kind
        |
        v
SeriesIdResolver -> sid
        |
        v
SketchStore.register once
SketchStore.append per window
        |
        +---- mutable epoch ----> sealed epochs ----> durable parts
        |                                              |
PromQL -> route/policy -> compatible sid(s)             |
        |                                              |
        +------------ memory + disk union <------------+
                              |
                    reconstruct and merge
                              |
                 result + provenance + accuracy
```

Storage does not parse PromQL to select a family. The query layer selects an
instance using BackendPlan/routing state, policy fingerprint, capability,
metric, grouping, and requested time range.

## Storage key and metadata

### Summary-series identity

The current resolver allocates a non-zero `u64` SID for:

```text
(canonical_metric_name, attributes_fingerprint, materialization_kind)
```

`materialization_kind` is currently named `agg_kind_canonical` in code. Its live
`SketchStore` variants contain sketch family and parameters or exact-aggregation
type and parameters, plus the canonical spatial filter. A future raw variant
must remain distinct. For example:

```text
metric = request_duration_seconds
attrs  = region=us-east;service=checkout;
kind   = sketch:DDSketch:D:0.01:filter=
sid    = 42  # example allocation; not a content hash
```

DDSketch α=0.02, KLL k=200, or a different filter gets another SID even with
the same metric and labels. A raw materialization likewise needs its own kind,
so the same identity contract can name a raw sample series without conflating
it with the summary. See
[series identity](series-identity.md).

### Instance metadata

`SketchInstanceMetadata` stores one descriptor per SID:

| Field | Meaning |
| --- | --- |
| `metric_name` | Canonical metric used by query matching |
| `group_by_keys` | Label names retained by edge aggregation |
| `capability` | Quantile, cardinality, frequency, or top-k support |
| `agg_kind` | Sketch/exact/raw kind and compatible parameters |
| `accuracy` | Derived sketch bound; absent for exact/raw state |
| `policy_fp` | Content fingerprint of the policy that produced the SID |
| lifecycle timestamps | First seen, retired, and expiry time |

Registration without data is allowed. Such a ghost SID cannot satisfy
coverage and produces a warm-tier miss.

### Stored cell

Each append is conceptually:

```text
(sid, [window_start, window_end), label_values_id, payload)
```

Label names live in instance metadata. A per-SID intern table maps each repeated
label-value map to a compact `u32`. Payload is either encoded sketch state or an
exact accumulator. Multiple frames with the same SID, labels, and window are
preserved in insertion order because a full base and subsequent deltas may use
the same logical window.

## Write path

1. **Decode.** The OTLP receiver identifies the materialization container,
   validates required metadata, and strips only recognized Collector family
   suffixes from the metric name.
2. **Canonicalize.** Attribute pairs, parameters, and filters become stable
   strings independent of map iteration order.
3. **Resolve SID.** Attribute-bearing frames resolve or allocate; ID-only
   frames are accepted only for registered SIDs.
4. **Register.** First sight records grouping, capability, kind, accuracy, and
   policy metadata. A heap-bearing frequency frame may promote the same base
   SID from frequency-estimate to top-k capability; it never downgrades.
5. **Check lifecycle.** Only active SIDs accept writes. Retired/expired IDs do
   not silently resurrect old state.
6. **Append.** The store interns labels and pushes window, label ID, and payload
   into parallel columns. Append is amortized O(1).

Malformed, unknown, retired, or incompatible state is rejected rather than
stored under a guessed identity.

## Memory layout and concurrency

The outer SID map is read-mostly. Each SID owns a separate `RwLock` around its
columnar state, so unrelated series can ingest concurrently. An active
`MutableEpoch` contains:

- parallel window, label-ID, and payload vectors;
- a distinct-window set that drives rotation;
- minimum start and maximum end for O(1) range rejection; and
- a lazy exact-window offset map, invalidated by each append and rebuilt only
  when exact lookup needs it.

Range scans touch the window column first and dereference labels/payloads only
for matches. Epoch size is bounded by the seal cadence. This keeps the ingest
path simple instead of maintaining a balanced time index on every write.

## Sealing and durable parts

Persistence is optional. When disabled, restart loses the in-memory summary
state. When enabled:

1. the current epoch seals after a configured number of distinct windows;
2. a background flusher selects sealed epochs because they crossed the hot-time
   watermark or memory high watermark;
3. immutable snapshots are written to a new part;
4. `meta.bin`, `data.bin`, and `index.bin` are CRC-protected and fsynced;
5. the part is appended to the durable manifest;
6. SID metadata required to discover disk-only state is persisted; and
7. only then is the sealed memory epoch evicted.

```text
PERSISTENCE_ROOT/
├── parts_manifest.snapshot
├── parts_manifest.log
└── parts/000000000000002a/
    ├── meta.bin
    ├── data.bin
    └── index.bin
```

The manifest is authoritative. The v1 index stores SID under the historical
field name `agg_id`, plus start/end timestamps and data offsets. `data.bin`
stores serialized labels, payload type, encoding tag, and payload bytes. Query
reads use mmap and index offsets instead of linearly decoding every payload.

The default config constructor uses an 80% low watermark, 125% hard cap,
one-hour hot window, seven-day disk TTL, one-second flush cadence, and 20
distinct windows per sealed epoch. Deployments may override all values.

### Recovery

Startup replays the manifest, validates referenced parts, and removes corrupt
or orphaned parts. A part is never queryable before both its files and manifest
record are durable. Resolver WAL and persisted SID metadata are recovered so
disk-only state is discoverable without waiting for a new data point.

Unknown payload types or corrupt entries are skipped and surfaced as missing;
they are never reinterpreted. The exact-aggregate disk decoder currently
supports only explicitly registered accumulator types.

## Query path

### Instance selection

Routing selects candidate SIDs by policy/capability and semantic compatibility.
A metric-name match alone is insufficient. A SID whose family, parameters,
filter, grouping, lifecycle, or accuracy cannot serve the request is a miss.

### Time lookup

Sketch reads use half-open overlap:

```text
stored [ws, we) overlaps requested [qs, qe)
iff we > qs && ws < qe
```

Overlap is needed when a query boundary cuts a Collector pane. Exact-window
lookup uses the precise pair. Operator-specific output timestamps are applied
after retrieval.

### Memory/disk union

The store queries the mutable epoch, sealed epochs, and manifest parts whose
time bounds overlap the request. Results combine by SID, labels, and window. A
bounded part cache avoids repeatedly decoding active cold parts.

### Full and delta reconstruction

A full frame establishes state; deltas apply in arrival order. If a query
starts inside a delta chain, storage retrieves the latest preceding full frame
as carry-in. Carry-in initializes reconstruction but does not produce an
out-of-range result. Missing base, unknown encoding, or incompatible family is
an explicit miss/error.

### Coverage and merge

The reducer merges only compatible panes. A five-minute request over one-minute
panes requires the five declared panes. Missing series/groups/windows are not
zeros. The router may use configured exact fallback; otherwise the request
fails explicitly.

## Reconfiguration and retention

SID, policy fingerprint, and plan version are distinct. A policy can own many
SIDs because each group has different label values. Reconciliation retires SIDs
removed from current configuration. Retired state can remain readable during
its retention interval but rejects writes; expired state is dropped by the
schema-eviction service.

Age-based disk TTL and schema retirement are independent. Disk TTL should be
longer than retirement retention so coherent SID eviction happens before
generic old-part deletion. Queries spanning a reconfiguration boundary use the
schema timeline and combine segments only where the operator defines a safe
cross-segment rule.

## Concrete example

For DDSketch α=0.01 and one-minute panes:

```text
request_duration_seconds{service="checkout",region="us-east"}

attrs = "region=us-east;service=checkout;"
kind  = "sketch:DDSketch:D:0.01:filter="
sid   = 42
```

Three frames create:

```text
(42, [12:00,12:01), labels=7, ProtoFull, A)
(42, [12:01,12:02), labels=7, ProtoFull, B)
(42, [12:02,12:03), labels=7, ProtoFull, C)
```

A three-minute quantile request selects SID 42, unions memory/disk panes,
decodes A/B/C, merges them, and evaluates the quantile. If B is absent, it does
not claim complete three-minute coverage. For delta transmission, A may be a
full base and B/C ordered deltas; all three frames remain separately stored.

## Guarantees and limitations

Guaranteed:

- one SID has one compatible materialization kind for its lifetime;
- same-window frames are preserved in insertion order;
- retired/expired writes are rejected;
- full/delta needs a valid base;
- disk visibility precedes memory eviction; and
- storage does not invent accuracy or treat missing coverage as zero.

Current limitations:

- tenant/registry namespace is not explicit in the live SID key;
- the v1 disk format retains `agg_id` naming;
- not every exact accumulator has a durable decoder;
- raw samples are served by the configured archive/pass-through path rather
  than a live `SketchStore::AggPayload::Raw` variant;
- manifest overlap search is linear in live part count; and
- distributed replication/rebalancing and object-store summary parts are not
  implemented.

## Acceptance tests

Tests must cover SID registration, incompatible-kind separation, concurrent
per-SID append, overlap/exact lookup, same-window full+deltas, carry-in,
retirement barriers, epoch rotation, flush-before-evict, memory/disk union,
TTL, corrupt/orphan recovery, and restart discovery. End-to-end validation must
use Collector-produced OTLP and verify coverage, provenance, accuracy, and
freshness for the same declared workload.
