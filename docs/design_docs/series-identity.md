# Series identity across ASAPCollector and ASAPQuery-backend

> Status: active implementation; explicit tenant/version namespaces are future
>
> MVP relation: identifies stored summary, exact-aggregate, or raw series across
> Collector export, backend storage, and query readout.

Developer guide:
[Summary storage and series identity](../../data_plane/docs/developer_docs/summary-storage-and-series-identity.md).

## TL;DR

SID means **stored series ID**. Most ASAP warm-tier SIDs identify summary
series, but the same identity model can identify an exact aggregate or raw
sample series when that is the stored materialization. SID does not necessarily
mean the original source time series.

ASAPQuery-backend allocates the non-zero SID. On first export, ASAPCollector
sends canonical identity evidence with `series_id = 0`; the backend resolves or
allocates an ID and returns `SeriesAssignment`. Collector caches it and may omit
repeated labels. If the backend later reports the ID in `unknown_series_ids`,
Collector evicts it and sends labels again.

Current identity is:

```text
sid := backend allocation for
       (canonical metric, canonical stored labels, materialization kind)
```

Materialization kind distinguishes DDSketch, KLL, exact Sum, raw samples, and
different parameters/filters. The numeric value is not global and is stable
across restart only with resolver persistence.

## Why the handshake exists

For high-cardinality summaries, repeated labels can be larger than the sketch
delta. The handshake allows ID-only steady-state traffic without losing a
recovery path:

```text
first frame       sid=0 + labels  -> assign N
steady state      sid=N           -> registered lookup
backend mismatch  sid=N unknown   -> request cache eviction
recovery frame    sid=0 + labels  -> assign current ID
```

Labels/materialization metadata are authoritative. A sender-provided number is
only a shortcut and cannot overwrite a conflicting backend binding.

## What SID identifies

| Stored object | Example materialization kind | What one SID groups |
| --- | --- | --- |
| Summary series | DDSketch α=0.01 | One metric, retained labels, family/config/filter |
| Exact aggregate series | Sum grouped by service | One metric, retained labels, exact operator/config/filter |
| Raw sample series | Raw/pass-through | One metric and its retained raw-series labels |

Window, frame encoding, payload bytes, sequence, retention, and plan version do
not change SID. They are records or lifecycle around the stored series.

## Canonical components

### Metric

Collector sketch processors can add family suffixes such as `_kll` or `_hll`.
The backend strips only recognized family suffixes before resolution so planning
and query matching use the raw metric name.

### Labels

Collector builds stable keys from resource labels and configured point-label
grouping. Unretained dimensions are folded away before summary export. Backend
canonicalizes emitted point attributes as sorted `key=value;` pairs. Wire label
order therefore does not affect SID. Empty attributes are valid for a global
aggregation.

Reserved transport metadata must be removed before identity construction.

### Materialization kind

Current code calls this `agg_kind_canonical`. Examples are:

```text
sketch:DDSketch:D:0.01:filter=
sketch:Kll:K:200:filter=
exact_agg:sum::filter=
raw:filter=                         # target representation for raw storage
```

The raw spelling is a design-level example; the authoritative live encoding is
the materialization type emitted by the plan/runtime. Parameter maps and
filters are canonicalized. Family/config/filter differences create different
SIDs. Full versus delta encoding does not.

## Collector-to-backend example

Assume Collector receives:

```text
resource: service.name="checkout", cloud.region="us-east"
metric:   request_duration_seconds
labels:   method="POST", status="200", pod="checkout-7f9c"
plan:     group by [method,status], DDSketch α=0.01, 60s windows
```

### 1. Collector aggregation identity

Collector routes samples using resource identity plus grouped point labels:

```text
resource segment: cloud.region=us-east;service.name=checkout;
group segment:    method=POST;status=200;
```

`pod` is removed by grouping and must not return in the emitted summary series.
The maintained Go `SeriesKey`/`AttributesKey` helpers are authoritative for the
Collector-local byte layout.

### 2. First export

Conceptually, the modified-OTLP frame is:

```text
metric.name = request_duration_seconds_ddsketch
attributes  = {method="POST", status="200"}
series_id   = 0
window      = [12:00,12:01)
encoding    = PROTO_FULL
config      = DDSketch(relative_accuracy=0.01)
payload     = <bytes>
```

### 3. Backend resolution

Backend derives:

```text
metric = request_duration_seconds
attrs  = method=POST;status=200;
kind   = sketch:DDSketch:D:0.01:filter=
```

Suppose it allocates SID 42. It registers SID metadata, appends the first
window, and returns conceptually:

```text
SeriesAssignment {
  metric_name: "request_duration_seconds_ddsketch",
  attributes_fingerprint: "method=POST;status=200;",
  series_id: 42
}
```

Resource/scope descriptor fields also exist on `SeriesAssignment`; modified
OTLP hops that use them must preserve identical canonical encodings.

### 4. Steady state

After caching the assignment, Collector may emit:

```text
metric.name = request_duration_seconds_ddsketch
attributes  = {}
series_id   = 42
window      = [12:01,12:02)
```

Backend accepts this only if SID 42 is registered and gets metric, grouping,
kind, and accuracy from metadata. It does not infer them from the number.

### 5. Query

```promql
quantile_over_time(
  0.95,
  request_duration_seconds{method="POST",status="200"}[2m]
)
```

Routing selects the compatible DDSketch policy and SID 42. Storage reads the
two windows, and result labels come from stored label metadata. SID stays an
internal key; users see PromQL labels, provenance, and accuracy.

## Raw-series example

If the plan routes raw samples instead of a sketch, the stored identity keeps
all labels required to distinguish the raw Prometheus series:

```text
http_requests_total{
  service="checkout",pod="checkout-7f9c",method="POST",status="200"
}
```

Its SID may identify that raw sample series because materialization kind is raw
and `pod` is retained. A summary plan grouping across pods would exclude `pod`
and receive another SID. The two SIDs can share source metric lineage without
sharing stored state.

## Same or different SID?

| Change | Result | Reason |
| --- | --- | --- |
| Reverse label order | Same | Canonical order |
| New window | Same | Window is below SID |
| Full to delta frame | Same | Encoding is per frame |
| DDSketch α=0.01 to α=0.02 | Different | Config changes materialization |
| DDSketch to KLL | Different | Family changes |
| Summary to raw | Different | Stored object semantics change |
| Retained `status=200` to `500` | Different | Stored labels change |
| Change a label removed by group-by | Same summary SID | It is not in emitted summary identity |
| Different spatial filter | Different | Filter is part of kind |
| Same number on another backend | Unrelated | Allocation domains differ |

## Wire state machine

| Frame | Backend action |
| --- | --- |
| `sid=0`, identity evidence present | Resolve/mint, register, append, return assignment |
| `sid!=0`, evidence present | Resolve from evidence; use canonical SID; report stale supplied SID |
| `sid!=0`, no evidence | Accept only if registered; otherwise drop and return `unknown_series_ids` |
| `sid=0`, empty labels | Resolve only when other metadata defines a valid global materialization; otherwise reject |

If supplied SID 42 plus labels resolve to SID 57, backend appends under 57,
returns 57's assignment, and reports 42 as stale. Numeric input cannot rebind a
canonical identity.

## Restart recovery

With resolver persistence, each fresh binding is fsynced to a WAL as:

```text
(sid, metric, attributes_fingerprint, materialization_kind)
```

Startup replays it and advances the allocator. A torn final record is truncated
to the last durable record. Storage metadata is recovered separately so
disk-only SIDs are queryable.

Without persistence, an ID-only frame after restart is unknown. Backend returns
the SID in `unknown_series_ids`; the patched exporter calls `EvictByID`; the
next export restores labels with SID 0 and receives a fresh assignment. The
unknown frame is dropped rather than guessed.

## Related identities

| Identity | Purpose |
| --- | --- |
| SID | One stored summary/exact/raw series |
| Policy fingerprint | Content identity of aggregation policy |
| Plan ID/version | Control-plane decision and rollout |
| Label-values ID | Per-SID compact label map in storage |

One policy can map to many SIDs for different groups. One source series can
feed several summary/raw materializations. Plan compatibility and lifecycle
checks remain necessary even after SID lookup.

## Target namespace model and limitations

Long term, allocation scope should be explicit:

```text
SeriesId = (tenant, registry_version, numeric_value)
```

Current wire/runtime carries only `u64`; tenant isolation, registry version,
distributed allocation, sharding, and namespace negotiation are future work.
SIDs are not deterministic across independent backends. Resolver WAL is
append-only and has no compaction. Resource/scope descriptor consistency also
depends on all modified-OTLP hops sharing canonicalization.

## Acceptance tests

Cross-repository tests must prove first-export assignment, ID-only steady state,
label-order stability, different materialization separation, raw-versus-summary
separation, unknown-ID eviction, conflict recovery, WAL/no-WAL restart, and
query label preservation. Producer/consumer conformance should receive an
independent review because both ends can otherwise encode the same mistake.
