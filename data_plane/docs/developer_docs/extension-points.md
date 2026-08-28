# Data-plane extension boundaries

> Status: active
>
> MVP relation: Prometheus HTTP and the configured exact fallback are required;
> additional protocols and fallback systems are future extensions.

## TL;DR

The data plane separates network transport, request/response adaptation,
plan-aware execution, and exact fallback. An extension implements one boundary
without duplicating planning or bypassing BackendPlan validation.

## Protocol server

A protocol server owns network concerns: endpoints, authentication context,
request limits, cancellation, and transport errors. It hands a request to a
protocol adapter and returns the adapter's response.

It does not parse Planner IR, select a summary, access summary storage directly,
or decide when fallback is allowed.

## Protocol adapter

An adapter converts a protocol request into the data plane's canonical query
request and converts the canonical result back into the protocol response.
Prometheus label and timestamp semantics must survive both conversions.

An adapter may report that a language feature cannot be represented, but it
must not approximate or rewrite an unsupported query on its own.

## Fallback client

A fallback client executes the canonical query against the exact backend named
by BackendPlan. It preserves the logical evaluation time, range, tenant, and
error response.

Fallback is invoked by plan-aware routing. A fallback client must not turn a
remote error into an empty successful result.

## Adding an extension

An extension is complete when it demonstrates:

- request and response semantic round trips;
- cancellation, timeout, and error propagation;
- tenant and authentication context preservation;
- plan-aware routing rather than direct store access;
- no silent fallback or approximation; and
- integration coverage with one successful and one failing request.

Implementation locations and trait signatures are intentionally left to the
code and API documentation, where they can evolve without changing this design.
