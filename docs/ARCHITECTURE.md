# Architecture

## Overview

`trysac-openlineage-collector` provides in-memory lineage tracking and OpenLineage 2-0-2 event processing without requiring external databases or server infrastructure.

```
+-----------------------+
|       Producers       |
| (HttpEmitter / tasks) |
+-----------+-----------+
            |
            v  HTTP POST /api/v1/lineage
+-----------------------+
|       WSGI App        |
|    (make_wsgi_app)    |
+-----------+-----------+
            |
            v  ingest(event)
+-----------------------+
|     LineageGraph      | <--- upstream() / downstream()
|  In-memory BFS Graph  |
+-----------------------+
```

## Components

1. **Event Builder & Validator (`make_run_event`, `validate_event`)**
   - Implements OpenLineage 2-0-2 RunEvent rules.
   - Enforces RFC 3339 date-time formats, UUID v4-formatted runIds, and URI validation.
   - Reports specific failing document paths on invalid events.

2. **In-Memory Lineage Graph (`LineageGraph`)**
   - Tracks job runs using terminal state machines (`START`/`RUNNING` -> `COMPLETE`/`FAIL`/`ABORT`).
   - Edges are created only when a run completes successfully.
   - BFS upstream and downstream queries support cycle detection and configurable depth (1-100).
   - Bounded event count ensures memory safety in long-running test suites.

3. **WSGI Endpoint (`make_wsgi_app`)**
   - Compliant WSGI callable handling `POST /api/v1/lineage` and `GET /api/v1/lineage/(upstream|downstream)`.
   - Enforces payload size limits (default 1MB).

4. **HTTP Emitter (`HttpEmitter`)**
   - Encapsulates HTTP transport with pluggable `Transport` protocol (defaults to `UrllibTransport`).
   - Safely injects bearer authentication headers without exposing tokens in logs or error messages.
