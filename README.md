# openlineage-collector

Emit and receive OpenLineage run events, validate them against the 2-0-2 spec and query upstream and downstream lineage of datasets and jobs.

## The Problem

Pipelines built from scripts and cron jobs have no record of which job read and wrote which dataset, so a bad table cannot be traced to its sources or its consumers. Lineage servers such as Marquez require a database and web stack, which may be too heavy for smaller deployments or local CI test suites.

## Usage Example

```python
from datetime import datetime, timezone
from openlineage_collector import LineageGraph, make_run_event

graph = LineageGraph()
event = make_run_event(
    "COMPLETE",
    run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
    job=("etl", "load_orders"),
    inputs=[("pg", "raw.orders")],
    outputs=[("pg", "mart.orders")],
    event_time=datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc),
    producer="https://example.test/etl",
)
graph.ingest(event)
assert graph.upstream("dataset:pg/mart.orders") == ["dataset:pg/raw.orders", "job:etl/load_orders"]
```

## Public API Summary

- `make_run_event(...)`: Constructs a valid OpenLineage 2-0-2 dict.
- `validate_event(obj)`: Validates input against the 2-0-2 spec; raises `LineageError` on failure.
- `LineageGraph(max_events=100000)`: In-memory graph. Methods: `ingest`, `upstream`, `downstream`, `runs`, `stats`.
- `make_wsgi_app(graph, max_body_bytes=1048576)`: Returns a WSGI callable for ingestion and queries.
- `HttpEmitter(url, transport, api_key)`: Sends events to an HTTP endpoint.

### Errors, Limits, and Options

- **Error Codes**: `invalid_event`, `unsupported_event`, `graph_full`, `unknown_node`, `transport_error`, `input_too_large`, `invalid_depth`.
- **Limits**: Max recursion depth of 64 for validation; configurable `max_body_bytes` (default 1MB); max 100,000 events per graph instance; BFS query depth 1-100.

## Why not Marquez or other lineage servers?

This library provides a zero-dependency, in-process lineage receiver and graph specifically suited for lightweight CI test environments and small-scale automation where deploying a full database-backed server is not viable.

See [Security details](docs/SECURITY.md) for vulnerability reporting and threat modeling.