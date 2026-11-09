# Use stdlib-only in-memory DAG for OpenLineage 2-0-2 lineage tracking

Status: accepted (generated, pending human review)

## Context

Marquez and other lineage servers require external relational databases and web stacks, making them heavy for CI and embedded script testing. A lightweight, dependency-free lineage collector was needed that strictly respects the OpenLineage 2-0-2 spec and provides upstream/downstream BFS queries.

## Decision

Implement openlineage_collector with Python standard library only (json, re, urllib, collections). Hand-craft validation conforming to OpenLineage 2-0-2 RunEvent schema with document-order error paths, enforce Run state machines (terminal states finalize edges; subsequent events ignored), and provide a WSGI app and HttpEmitter.

## Consequences

Zero runtime dependencies ensures high portability across Python 3.10-3.13. Large datasets are bounded by max_events in memory. Conformance against the official OpenLineage JSON schema draft is tested in CI using jsonschema.
