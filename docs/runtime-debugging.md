# Runtime debugging logs

Set `RUST_LOG=info,control_plane=debug,data_plane=debug` on the controller and backend when following a physical plan through the system. The controller writes to stdout. The backend writes to stdout and `<output_dir>/query_engine.log`.

Filter both streams by `plan_id` and `plan_version`. The controller reports compilation, collector preflight and publication, backend staging, and activation. The backend reports staging validation, activation, and summary catalog installation. At `debug`, the query engine also reports the selected installed query DAG and completion using `query_id` and the active plan identifiers. The completion event includes remote evaluation and RPC counts. Query text and request bodies are deliberately absent from these events.

For precompute and storage work, use the same backend log with `data_plane::precompute_engine=debug,data_plane::storage_engines=debug` in `RUST_LOG`. Existing worker, sink, persistence, and recovery events identify the relevant processing stage. An error before backend staging points to plan compilation or publication; an error after activation with a matching plan identifier points to runtime query or storage behavior.
