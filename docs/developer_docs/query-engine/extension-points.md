# Developing protocol and fallback extensions

> Interface status: public extension boundary. Concrete trait names may migrate
> toward the canonical interfaces below; private server helpers are not API.

## 1. Code architecture

```text
network request -> ProtocolServer -> ProtocolAdapter -> QueryService
                                                        |
                                                ExactQueryClient
```

- `ProtocolServer` owns transport, authentication context, limits, timeout, and
  cancellation.
- `ProtocolAdapter` converts protocol-specific data to/from canonical query
  structures.
- `QueryService` performs plan-aware execution.
- `ExactQueryClient` is called only for an explicit fallback route.

## 2. Public interfaces and definitions

```rust
pub trait ProtocolAdapter: Send + Sync {
    type Request;
    type Response;
    type Error;

    fn decode(&self, request: Self::Request)
        -> Result<QueryRequest, Self::Error>;

    fn encode(&self, response: QueryResponse)
        -> Result<Self::Response, Self::Error>;

    fn encode_error(&self, error: QueryError) -> Self::Response;
}
```

```rust
pub trait ProtocolServer {
    type Error;
    async fn serve<S>(&self, service: Arc<S>) -> Result<(), Self::Error>
    where
        S: QueryService<Error = QueryError> + Send + Sync + 'static;
}
```

```rust
pub trait ExactQueryClient: Send + Sync {
    type Error;
    async fn execute_exact(&self, request: &QueryRequest)
        -> Result<QueryResponse, Self::Error>;

    fn capabilities(&self) -> ExactBackendCapabilities;
}

pub struct ExactBackendCapabilities {
    pub backend_id: String,
    pub query_languages: Vec<QueryLanguage>,
    pub supports_instant: bool,
    pub supports_range: bool,
    pub maximum_range: Option<Duration>,
}
```

`QueryRequest` and `QueryResponse` are defined in
[Query routing and readout](routing-and-readout.md). They preserve tenant,
query language/expression, logical evaluation range, requested accuracy,
result labels/timestamps/type, source, guarantee, and coverage.

Why these interfaces exist: transport/protocol extensions cannot bypass
BackendPlan routing or directly access summary storage, and fallback backends
cannot silently reinterpret a request.

## 3. Adding and verifying functionality

### Add a protocol adapter

1. Implement `ProtocolAdapter` for its request/response types.
2. Map every supported evaluation-time/range and tenant field.
3. Preserve Prometheus label/timestamp/result/error semantics where applicable.
4. Verify decode→canonical→encode round trips for success and error cases.

### Add a protocol server

1. Implement `ProtocolServer` and inject only the public `QueryService`.
2. Propagate cancellation, timeout, authentication, and request limits.
3. Never call `SummaryStore` or an exact client directly.
4. Verify cancelled requests stop downstream work and transport errors map
   through `encode_error`.

### Add an exact fallback backend

1. Implement `ExactQueryClient` and declare `ExactBackendCapabilities`.
2. Forward the canonical logical range and tenant unchanged.
3. Return exact `QueryResponse` or a visible error.
4. Verify unsupported capability and remote failure do not return an empty
   successful result.

### Interpret and verify output

- Adapter output is canonical input, not a routing decision.
- Server success means the response was transported, not that it was
  summary-backed.
- Inspect `QueryResponse.source` to distinguish summary and exact fallback.
- End-to-end tests must include one supported request, one explicit fallback,
  one malformed request, and one backend failure.
