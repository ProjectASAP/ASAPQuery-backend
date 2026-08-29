# Developing the ingest and precompute engine

## Architecture

```text
drivers/ingest/otel.rs
  -> drivers/ingest/series_resolver.rs
  -> precompute_engine/{series_router,worker,window_manager}
  -> precompute_engine/accumulator_factory.rs
  -> precompute_engine/output_sink.rs
  -> storage_engines/sketch_db
```

Modified-OTLP summary decoding and raw-sample routing meet at the storage sink,
but remain separate paths before it. `IngestState` supplies the installed
configuration, `SeriesIdResolver`, and `SketchStore`.

## Interfaces

- `OtlpReceiverConfig` defines gRPC/HTTP ports and receiver behavior.
- `SeriesIdResolver::resolve(metric, attrs_fp, agg_kind)` is the sole live SID
  mint authority.
- `PrecomputeEngineConfig` defines workers, buffers, lateness, flush cadence,
  cleanup, and lock strategy.
- `AccumulatorFactory` maps a validated aggregation specification to an
  accumulator.
- `SketchStoreSink` writes completed outputs to the SID-keyed store.

The detailed modified-OTLP contract remains in
[OTLP summary ingestion](ingest-engine-otlp.md).

## Extension workflow

For a new family, add wire decoding, canonical `AggKind`, accumulator support if
Backend precompute is allowed, store payload decoding/readout, capability
matching, and cross-language vectors. For a new input protocol, normalize into
the same observation/materialized-frame contract rather than bypassing SID,
window, lifecycle, or backpressure validation.

## Verification

Run focused receiver, SID resolution, precompute worker/window, accumulator, and
store tests. Add an end-to-end case proving CollectorPlan/BackendPlan select one
placement and that the other path does not duplicate aggregation.
