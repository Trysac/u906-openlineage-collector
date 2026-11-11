# Testing

## Structure

Tests are located in `tests/test_openlineage_collector.py` and mirror the public module structure. Shared fixtures reside in `tests/conftest.py`.

## Coverage

- The test suite covers unit and integration scenarios.
- Every public method (`ingest`, `upstream`, `downstream`, `validate_event`, etc.) is exercised.
- Bug fixes are introduced with regression tests.

## Execution

```bash
uv run pytest --cov=openlineage_collector
```

- Tests are deterministic, require no network, and depend on no external services.
- Conformance testing against the OpenLineage schema is performed in CI when the schema file is present.