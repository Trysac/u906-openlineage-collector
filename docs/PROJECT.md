# Project record

## Objective

Emit and receive OpenLineage run events, validate them against the 2-0-2 spec and query upstream and downstream lineage of datasets and jobs.

## Scope

- Implementation of OpenLineage 2-0-2 serialization and validation.
- In-memory lineage graph for job and dataset relationship tracking.
- WSGI interface for ingestion and querying.
- HTTP Emitter with pluggable transport.

## Out-of-Scope

- Facet schema management.
- Persistent storage (database backend).
- Column-level lineage tracking.
- Authentication/Authorization on the ingestion side (HTTP receiver).
- Kafka or other non-HTTP transports.

## Effort Metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 761700 / 21729 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.1834 |

## Factory run u906 (measured from API usage)

| Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|
| generation | google/gemini-3.8-flash | yes | 5750 | 18270 | 0 | 0 | 0.0728 |
| fix-1 | google/gemini-3.1-flash-lite | no | 72382 | 1358 | 0 | 131162 | 0.0234 |
| audit | google/gemini-3.1-flash-lite | no | 58874 | 400 | 0 | 64640 | 0.0169 |
| threat-model | google/gemini-3.1-flash-lite | no | 131430 | 273 | 0 | 175812 | 0.0377 |
| docs | google/gemini-3.1-flash-lite | no | 121650 | 1428 | 0 | 0 | 0.0326 |

Total: $0.1834. Prices: vertex line, as measured by its adapter.
