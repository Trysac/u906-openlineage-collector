# Security notes

## Threat Model

### Defended

- **Untrusted Input**: The validator enforces strictly typed schemas for OpenLineage 2-0-2 objects. Recursion depth is capped at 64, and body size is limited to `max_body_bytes` to mitigate DoS via large JSON payloads.
- **Leaked Secrets**: API keys (if used in `HttpEmitter`) are passed as headers and are not logged or captured in `LineageError` messages. 
- **Code Safety**: The library uses no external dependencies. The codebase is restricted by strict Mypy types and Ruff linting rules.

### Not Defended

- **Authorization**: The WSGI application does not implement authentication; any caller reaching the endpoint can ingest events or query the graph.
- **Persistence**: Data is held in memory and is lost on restart; no secure storage mechanisms are implemented.

## Limits

Validation rejects payloads exceeding 64 levels of nested JSON or the configured `max_body_bytes` (default 1MB).