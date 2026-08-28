# Series identity

> Status: active
>
> MVP relation: provides stable identity for ingestion, grouping, and result
> labels across collector and backend boundaries.

Developer guide:
[Summary storage and series identity](../../data_plane/docs/developer_docs/summary-storage-and-series-identity.md).

## TL;DR

A series ID (`sid`) names one canonical metric series within a tenant and
identity namespace. The backend registry assigns or validates this mapping;
collectors may cache it, but payload labels remain the recovery evidence needed
to detect stale or unknown IDs.

## Identity contract

The canonical series key consists of:

- tenant or isolation domain;
- metric name; and
- a deterministically ordered set of identifying labels.

Two observations with the same canonical key resolve to the same `sid` within
one namespace. Different canonical keys must not share a `sid`. Aggregation
group labels and summary parameters are not silently folded into series
identity; they belong to the materialization/group contract.

For example, these are different series:

```text
http_requests_total{job="api",region="us-east"}
http_requests_total{job="api",region="eu-west"}
```

but reordering the two labels does not create a third identity.

## Relationship to plan identity

`sid`, materialization identity, and plan identity serve different purposes. A
series can participate in several materializations and plan versions. Reusing a
`sid` does not authorize reuse of summary state with different family,
grouping, parameters, or windows.

## Resolution and caching

The backend registry is authoritative for the namespace. A collector may cache
resolved IDs to reduce coordination, provided it also carries enough canonical
identity evidence for the backend to validate or recover the mapping.

Resolution is idempotent: retrying the same canonical key returns the same
mapping. Registering an ID without receiving state is allowed and must not make
a query appear complete.

## Recovery

When the backend does not recognize a sender-provided `sid`, or finds that it
maps to different labels, it rejects the numeric shortcut and resolves from the
canonical key. A stale cache cannot overwrite an existing authoritative
mapping.

Backend restart behavior depends on registry durability:

- with durable identity state, mappings are restored before dependent payloads
  become queryable;
- without durable state, collectors re-resolve from canonical labels under a
  new namespace/version.

In both cases ambiguity fails closed.

## Distributed backend

Distributed allocation, shard ownership, rebalancing, and high availability are
future deployment concerns. Any scheme must preserve deterministic lookup,
namespace/version evidence, and conflict detection. Numeric partitioning alone
must not weaken the canonical-key contract.

## Non-goals

This document does not prescribe RPC messages, integer width, database tables,
cache files, sharding algorithms, or a migration sequence from older `agg_id`
names.
