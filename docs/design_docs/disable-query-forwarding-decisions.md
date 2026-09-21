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
A: A centralized forwarding policy with typed errors/enums, not scattered booleans or ad-hoc error strings.

Q: What is the default?
A: No flag means existing forwarding behavior is unchanged.

Q: How is it tested?
A: Request-capturing mocks cover instant and range paths, proving zero external query requests.

Q: What observability is required?
A: Startup INFO announces the mode; blocked attempts emit DEBUG logs and increment a backend/path-labeled counter. Existing protocol response shapes remain unchanged.
