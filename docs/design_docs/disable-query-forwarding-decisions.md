# Disable query forwarding: decisions

Q: What does the mode prohibit?
A: No Prometheus, VictoriaMetrics, ClickHouse, or exact-subquery query requests. Health and runtime metadata remain allowed.

Q: How is it enabled?
A: CLI-only `--disable-query-forwarding`.

Q: What conflicts are errors?
A: Combining it with `--forward-unsupported-queries`, `--profile asapquery`, `--victoriametrics-http-port`, or `--clickhouse-http-port` is a startup error.

Q: What happens when a plan needs an external exact subquery?
A: Fail closed with the normal protocol-specific unsupported/capability-miss response; never return an empty or partial result.

Q: How is the behavior represented in code?
A: A shared forwarding-policy enum gates adapter fallback and planned exact subqueries.

Q: What is the default?
A: No flag means existing forwarding behavior is unchanged.

Q: How is it tested?
A: A production-process request-capture test covers instant and range paths; CLI validation tests cover conflicting settings.

Q: What observability is required?
A: Startup INFO announces the mode; blocked HTTP fallbacks and planned exact subqueries emit DEBUG logs and increment a backend/path-labeled counter. Existing protocol response shapes remain unchanged.
