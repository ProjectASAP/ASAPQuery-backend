# Future storage and compression

> Status: mixed: local durable summary parts and backfill scaffolding exist;
> compaction, object-store summary parts, and standalone service mode are future
>
> MVP relation: not required for the basic in-memory summary pipeline, but local
> persistence already supports restart and bounded memory.

## TL;DR

The future storage system should treat raw samples, exact aggregates, and
approximate summaries as typed materialized series under one semantic contract:

```text
SID + materialization kind + labels + window + lineage + payload
```

Representation may change across memory, local disk, and object storage, but
identity, coverage, accuracy, and query semantics must not. Current
`SketchStore` already flushes sealed epochs to local immutable parts and unions
them with memory at query time. The next stages are safe compaction, complete
durable decoding, backfill publication, object-store parts, service separation,
and measured family-specific compression.

## Current baseline

Already implemented:

- per-SID mutable and sealed epochs;
- memory and hot-time watermarks;
- CRC-protected local part files and durable manifest;
- flush-before-evict ordering;
- mmap-backed disk reads and a bounded part cache;
- startup recovery of manifest/parts and SID metadata;
- disk TTL; and
- backfill job/reader/worker components for selected sources.

Not yet general guarantees:

- compaction of parts or summary windows;
- remote/object-store summary parts;
- every exact accumulator's disk decoder;
- distributed SID allocation and ownership;
- transactional live/backfill coverage publication;
- standalone storage service mode; or
- automatic representation selection from measured cost.

## Unified stored-series model

Future tiers must preserve the distinction the SID document defines:

| Kind | Payload | Exactness | Typical use |
| --- | --- | --- | --- |
| Raw | Timestamp/value samples | Exact source data | Arbitrary fallback and backfill |
| Exact aggregate | Sum/count/min/max/rate state | Exact for declared operator/window | Planned exact readout |
| Approximate summary | DDSketch/KLL/HLL/frequency state | Family-specific bound | Planned low-cost readout |

A raw series and its DDSketch summary receive different SIDs because their
materialization kinds differ. They can retain lineage linking both to the same
source metric, allowing the raw tier to validate or backfill the summary
without conflating state.

## Target tiering model

```text
Tier 0: mutable memory
  newest windows, append and query
       |
       v seal
Tier 1: immutable local parts
  mmap reads, bounded cache, restart recovery
       |
       v upload/compact
Tier 2: object-store parts
  long retention, sparse reads, shared backend access
       |
       +---- optional raw archive for exact fallback/backfill
```

Movement between tiers changes physical placement only. The SID,
materialization kind, label identity, logical window, encoding lineage, and
accuracy contract remain unchanged.

## Durable part evolution

The local v1 format prioritizes correctness: one part directory contains
metadata, opaque data, and a sorted time index. A future version should add:

- explicit SID naming instead of historical `agg_id` fields;
- tenant/namespace and materialization schema version;
- per-entry codec and payload format version;
- full/delta checkpoint lineage and sequence bounds;
- label-dictionary blocks shared across entries;
- footer statistics for pruning by SID, time, family, and labels;
- checksums per block to isolate corruption; and
- a compatibility reader for the previous version.

Format activation requires dual-read tests and a rollback period. Writers may
switch only after every serving version can read the new format. Unknown
versions fail closed.

## Semantic compaction

Compaction has two distinct operations.

### Physical compaction

Repack many small immutable parts without changing logical windows or payloads.
This reduces object/manifest count and read amplification. It is valid for raw,
exact, and summary series when it preserves entry order needed by delta chains.

### Summary compaction

Merge adjacent logical windows into a coarser summary. This is legal only when:

- family and representation declare an associative merge;
- SID/materialization kind, labels, parameters, filter, and accuracy agree;
- windows are contiguous and complete;
- delta chains are first reconstructed to valid states; and
- query routing knows which requested resolutions the compacted pane can serve.

Example:

```text
60 complete 1-minute DDSketch panes
  -> one 1-hour DDSketch pane with the same α
```

This can serve a compatible one-hour quantile query but cannot replace the
minute panes for queries needing minute-resolution output. Compaction therefore
needs a retention matrix, not a blanket “delete inputs after merge” rule.

Raw samples may use timestamp/value compression such as Gorilla-XOR, but that
is physical compression, not summary compaction. It retains exact samples and
must remain queryable through the exact archive engine.

## Full/delta checkpoints

Long delta chains reduce wire size but increase recovery and read cost. Future
storage should enforce checkpoint policy:

- write a full state every configured number of deltas or elapsed time;
- record base checkpoint and sequence range in part metadata;
- never compact across a missing sequence;
- retain a full base while any surviving delta references it; and
- measure reconstruction CPU/latency before increasing checkpoint distance.

Compaction can materialize a new full checkpoint from a verified chain, then
atomically publish it and retire the old chain.

## Backfill and refresh

Backfill constructs a summary/exact materialization from a retained raw source.
The intended flow is:

1. control plane specifies target materialization, source snapshot, range, and
   expected policy/SID lineage;
2. a reader fetches raw samples from the exact source;
3. the same accumulator semantics as live ingest build window states;
4. output lands in a staging SID/coverage generation isolated from live writes;
5. validation checks window coverage, labels, family/config, and accuracy; and
6. one atomic publication makes the generation queryable.

A retry for the same source snapshot and contract must be idempotent. Live
ingest wins for overlapping active windows unless the plan explicitly defines a
replacement. Partial backfill never advertises complete coverage.

Refresh repeats backfill for a new source snapshot or corrected semantics. It
creates a new generation; it does not overwrite queryable windows in place.

## Object storage

Remote summary parts should be immutable objects addressed by part ID/content
checksum. A small durable catalog maps time/SID ranges to objects. Publish is:

```text
upload object -> verify checksum -> commit catalog entry -> evict local copy
```

Deletion reverses visibility first: remove/tombstone catalog reference, wait
for readers, then delete object. Object listing is not the authoritative
catalog because it is too expensive and has weaker transaction semantics.

The cache key must include object version/checksum. Range requests should fetch
only needed index/data blocks. Remote errors remain distinguishable from
missing coverage.

## Standalone storage service

The in-process and remote forms must expose the same semantic operations:

```text
register materialization
append validated frame
check coverage
read compatible state
retire/expire series
report diagnostics
```

Transport adds authentication, tenant isolation, request IDs, deadlines,
backpressure, and retry tokens; it must not become a second planner or SID
authority. Writes require idempotency keys derived from SID/window/producer
sequence. Reads return typed missing/stale/gapped/incompatible outcomes rather
than empty arrays.

## Compression choices

Compression must be selected per payload class:

| Data | Candidate techniques | Constraint |
| --- | --- | --- |
| Labels | Dictionary/front coding | Exact round trip |
| Timestamps | Delta-of-delta | Exact window boundaries |
| Raw float values | Gorilla-XOR | Exact bit representation unless declared lossy |
| Sparse frequency state | Sparse indices, varints, entropy coding | Preserve counters/signs and merge semantics |
| Dense sketch arrays | Block compression | Preserve family/version bytes |
| Repeated full states | Delta frames + periodic checkpoint | Valid base and bounded chain |

General gzip/zstd can wrap parts, but block-level codecs are preferable when
queries need selective reads. A lossy codec creates a new materialization kind
and accuracy contract; it cannot be labeled exact or silently replace an
existing SID.

## Planner cost and profiling inputs

The backend may measure, but not independently decide, representation choice.
For each family/config/workload/hardware tuple it should report:

- update and merge CPU;
- encoded bytes and compression ratio;
- memory bytes per active series/window;
- flush throughput and amplification;
- cold read bytes, cache hit rate, and p50/p95/p99 latency;
- reconstruction cost versus delta-chain length; and
- backfill throughput and source bytes read.

Measurements must name commits, parameters, dataset, cardinality, query mix,
duration, and hardware. Planner consumes these as versioned cost inputs.

## Failure handling

| Failure | Required behavior |
| --- | --- |
| Crash during part write | Unreferenced partial part removed on recovery |
| Crash after part fsync before manifest | Orphan removed or adopted only after full validation |
| Corrupt block | Isolate affected coverage; never fabricate state |
| Missing delta | Mark chain gapped; use exact fallback if configured |
| Object store unavailable | Preserve hot service where possible; return distinct remote error |
| Compactor crash | Old parts remain authoritative until atomic publication |
| Backfill crash | Staging generation remains non-queryable and retryable |
| Version skew | Reject unsupported format/materialization version |

## Security and tenancy

Remote storage requires explicit tenant/namespace in every catalog key and
request. Authorization is checked before metadata lookup to avoid existence
leaks. Encryption keys and credentials are endpoint configuration, not plan
payloads. Quotas bound memory, disk, object bytes, request concurrency, and
backfill work per tenant.

## Rollout sequence

1. Complete durable decoders and recovery coverage for current materializations.
2. Add physical part compaction without semantic window changes.
3. Add explicit SID namespace/version and format v2 dual reads.
4. Add staged/atomic backfill publication.
5. Add object-store parts behind shadow reads and checksum comparison.
6. Introduce standalone service transport after in-process semantics are stable.
7. Enable semantic compaction family by family with resolution-retention rules.

Each stage has a kill switch and leaves the previous readable representation
intact until acceptance evidence passes.

## Acceptance criteria

Activation requires:

- byte/semantic round trips for every supported payload and format version;
- crash tests at every write/manifest publication boundary;
- memory/local/remote query equivalence;
- raw versus summary SID isolation;
- complete/gapped coverage tests for full/delta and backfill;
- compaction equivalence for supported operators and explicit rejection for
  unsupported ones;
- measured memory, write, read, and object amplification; and
- end-to-end Collector-to-query tests with restart, fallback, and retention.

No proposal becomes active architecture solely because a type or scaffold
exists. Its owning component, compatibility policy, rollback behavior, and
reproducible evidence must all be present.
