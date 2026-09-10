# Maintenance replay and retention

The production maintenance adapter accepts an already computed source summary
and executes homogeneous `SummaryMerge` dependencies. An additional `SummaryAgg`
requires evaluation of its declared update expression and target family; it is
rejected until that evaluator exists. Copying or merging its input would silently
ignore those semantics (for example, count over a sum state is not that sum).
The scheduler's ability to traverse a DAG does not imply every operator is
implemented by the production adapter.

The maintenance sink retains in-process publication receipts for the active
physical plan. Receipt identity includes the plan generation, target summary
definition, output window, and a SHA-256 digest of the source definition,
group, and serialized input state. Query-local DAG node IDs are not publication
identity: separate query DAGs may use the same node numbers.

An accepted write releases its cached derived payload. Its compact receipt
remains until the materialization's event-time retry horizon expires. The
horizon is the configured retained-state count times the slide interval, with
a minimum of one complete materialized window. With no retained-state count,
the horizon is one complete window. A later output advances that definition's
event-time frontier. An output ending at or before the frontier minus the
horizon is rejected; it must not be reinserted as a new write after receipt
eviction. Sparse or out-of-order data within the horizon remains eligible.

When the sink observes a changed active plan generation, it clears old receipts
and cached failures. Previously captured work then fails closed when it attempts
to commit or publish. This uses installed plan identity and event time, not host
wall-clock time, so finite-input replay does not expire state merely because
the dataset is old.

These receipts do not provide durable exactly-once publication after restart.
Storage must still make uncertain writes idempotent. A failed write retains
its derived state for retry within the same horizon; a successful write is
acknowledged only after the downstream sink accepts it. Retention expiration
may discard failed work once its output is outside the supported horizon.
