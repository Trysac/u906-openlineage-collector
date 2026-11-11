"""Unit, integration, and conformance tests for openlineage_collector."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from openlineage_collector import (
    HttpEmitter,
    LineageError,
    LineageGraph,
    make_run_event,
    make_wsgi_app,
    validate_event,
)

UTC = timezone.utc


class FakeTransport:
    """Deterministic in-memory Transport oracle."""

    def __init__(self, status: int = 201) -> None:
        self.status = status
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, body: bytes, headers: dict[str, str]) -> int:
        self.calls.append({"url": url, "body": body, "headers": headers})
        return self.status


def test_acceptance_make_run_event_and_validate() -> None:
    event = make_run_event(
        "START",
        run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
        job=("etl", "load_orders"),
        inputs=[("pg", "raw.orders")],
        outputs=[("pg", "mart.orders")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    assert (
        event["schemaURL"] == "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
    )
    assert event["eventTime"] == "2026-01-05T10:00:00Z"
    validate_event(event)


def test_acceptance_validate_event_errors() -> None:
    valid_base = make_run_event(
        "START",
        run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
        job=("etl", "load_orders"),
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )

    # path 'run.runId' for runId 'abc'
    ev_bad_run_id = dict(valid_base)
    ev_bad_run_id["run"] = {"runId": "abc"}
    with pytest.raises(LineageError) as exc_info:
        validate_event(ev_bad_run_id)
    assert exc_info.value.code == "invalid_event"
    assert exc_info.value.path == "run.runId"

    # path 'eventType' for 'DONE'
    ev_bad_event_type = dict(valid_base)
    ev_bad_event_type["eventType"] = "DONE"
    with pytest.raises(LineageError) as exc_info:
        validate_event(ev_bad_event_type)
    assert exc_info.value.code == "invalid_event"
    assert exc_info.value.path == "eventType"

    # path 'eventTime' for '2026-01-05 10:00'
    ev_bad_event_time = dict(valid_base)
    ev_bad_event_time["eventTime"] = "2026-01-05 10:00"
    with pytest.raises(LineageError) as exc_info:
        validate_event(ev_bad_event_time)
    assert exc_info.value.code == "invalid_event"
    assert exc_info.value.path == "eventTime"

    # path 'job.name' when the job name is missing
    ev_missing_job_name = dict(valid_base)
    ev_missing_job_name["job"] = {"namespace": "etl"}
    with pytest.raises(LineageError) as exc_info:
        validate_event(ev_missing_job_name)
    assert exc_info.value.code == "invalid_event"
    assert exc_info.value.path == "job.name"


def test_acceptance_graph_lineage_resolution() -> None:
    graph = LineageGraph()
    # Ingest START then COMPLETE for job etl.load_orders (raw.orders to mart.orders)
    run1_id = "11111111-1111-1111-1111-111111111111"
    e1_start = make_run_event(
        "START",
        run_id=run1_id,
        job=("etl", "load_orders"),
        inputs=[("pg", "raw.orders")],
        outputs=[("pg", "mart.orders")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    e1_complete = make_run_event(
        "COMPLETE",
        run_id=run1_id,
        job=("etl", "load_orders"),
        inputs=[("pg", "raw.orders")],
        outputs=[("pg", "mart.orders")],
        event_time=datetime(2026, 1, 5, 10, 5, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e1_start)
    graph.ingest(e1_complete)

    # COMPLETE for job etl.report (mart.orders to bi.revenue)
    run2_id = "22222222-2222-2222-2222-222222222222"
    e2_complete = make_run_event(
        "COMPLETE",
        run_id=run2_id,
        job=("etl", "report"),
        inputs=[("pg", "mart.orders")],
        outputs=[("pg", "bi.revenue")],
        event_time=datetime(2026, 1, 5, 10, 10, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e2_complete)

    assert graph.upstream("dataset:pg/bi.revenue") == [
        "dataset:pg/mart.orders",
        "dataset:pg/raw.orders",
        "job:etl/load_orders",
        "job:etl/report",
    ]
    assert graph.downstream("dataset:pg/raw.orders", depth=1) == ["job:etl/load_orders"]


def test_acceptance_fail_abort_and_terminal_semantics() -> None:
    graph = LineageGraph()
    run_fail_id = "33333333-3333-3333-3333-333333333333"
    e_start = make_run_event(
        "START",
        run_id=run_fail_id,
        job=("etl", "fail_job"),
        inputs=[("pg", "src")],
        outputs=[("pg", "dst")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    e_fail = make_run_event(
        "FAIL",
        run_id=run_fail_id,
        job=("etl", "fail_job"),
        inputs=[("pg", "src")],
        outputs=[("pg", "dst")],
        event_time=datetime(2026, 1, 5, 10, 5, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e_start)
    graph.ingest(e_fail)
    # A run that ends with FAIL or ABORT contributes no edges
    assert graph.downstream("dataset:pg/src") == []
    assert graph.upstream("dataset:pg/dst") == []

    # Ingest RUNNING event arriving after COMPLETE
    run_ok_id = "44444444-4444-4444-4444-444444444444"
    e_comp = make_run_event(
        "COMPLETE",
        run_id=run_ok_id,
        job=("etl", "ok_job"),
        inputs=[("pg", "in1")],
        outputs=[("pg", "out1")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e_comp)
    initial_events_count = graph.stats()["events"]

    # RUNNING arriving after COMPLETE
    e_late_running = make_run_event(
        "RUNNING",
        run_id=run_ok_id,
        job=("etl", "ok_job"),
        inputs=[("pg", "in1")],
        outputs=[("pg", "out1")],
        event_time=datetime(2026, 1, 5, 10, 1, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e_late_running)
    assert graph.stats()["ignored_after_terminal"] == 1
    assert graph.stats()["events"] == initial_events_count

    # Ingesting the same event twice leaves the graph unchanged
    graph.ingest(e_comp)
    assert graph.stats()["events"] == initial_events_count


def _invoke_wsgi(
    app: Any,
    method: str,
    path: str,
    body: bytes = b"",
    query_string: str = "",
    extra_headers: dict[str, str] | None = None,
) -> tuple[str, dict[str, str], bytes]:
    resp_status = ""
    resp_headers: dict[str, str] = {}

    def start_response(status: str, headers: list[tuple[str, str]]) -> None:
        nonlocal resp_status, resp_headers
        resp_status = status
        resp_headers = dict(headers)

    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": query_string,
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": io.BytesIO(body),
    }
    if extra_headers:
        for k, v in extra_headers.items():
            environ["HTTP_" + k.upper().replace("-", "_")] = v

    chunks = app(environ, start_response)
    body_out = b"".join(chunks)
    return resp_status, resp_headers, body_out


def test_acceptance_wsgi_app() -> None:
    graph = LineageGraph()
    app = make_wsgi_app(graph, max_body_bytes=1_048_576)

    # Populate graph with load_orders and report jobs
    run1_id = "11111111-1111-1111-1111-111111111111"
    e1 = make_run_event(
        "COMPLETE",
        run_id=run1_id,
        job=("etl", "load_orders"),
        inputs=[("pg", "raw.orders")],
        outputs=[("pg", "mart.orders")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    run2_id = "22222222-2222-2222-2222-222222222222"
    e2 = make_run_event(
        "COMPLETE",
        run_id=run2_id,
        job=("etl", "report"),
        inputs=[("pg", "mart.orders")],
        outputs=[("pg", "bi.revenue")],
        event_time=datetime(2026, 1, 5, 10, 10, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e1)
    graph.ingest(e2)

    # POST /api/v1/lineage with a valid event with status '201 Created'
    e_post = make_run_event(
        "START",
        run_id="55555555-5555-5555-5555-555555555555",
        job=("etl", "new_job"),
        event_time=datetime(2026, 1, 5, 11, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    status, headers, body = _invoke_wsgi(
        app, "POST", "/api/v1/lineage", body=json.dumps(e_post).encode("utf-8")
    )
    assert status == "201 Created"
    assert headers["Content-Type"] == "application/json"

    # an invalid one with '400 Bad Request' and body {'code': 'invalid_event', 'path': 'run.runId'}
    invalid_ev = dict(e_post)
    invalid_ev["run"] = {"runId": "invalid-uuid"}
    status, headers, body = _invoke_wsgi(
        app, "POST", "/api/v1/lineage", body=json.dumps(invalid_ev).encode("utf-8")
    )
    assert status == "400 Bad Request"
    assert json.loads(body) == {"code": "invalid_event", "path": "run.runId"}

    # a body of 1_048_577 bytes with '413 Request Entity Too Large'
    status, _, _ = _invoke_wsgi(app, "POST", "/api/v1/lineage", body=b"x" * 1_048_577)
    assert status == "413 Request Entity Too Large"

    # GET /api/v1/lineage/upstream?node=dataset:pg/bi.revenue with the JSON list above
    status, headers, body = _invoke_wsgi(
        app, "GET", "/api/v1/lineage/upstream", query_string="node=dataset:pg/bi.revenue"
    )
    assert status == "200 OK"
    assert json.loads(body) == [
        "dataset:pg/mart.orders",
        "dataset:pg/raw.orders",
        "job:etl/load_orders",
        "job:etl/report",
    ]


def test_acceptance_http_emitter() -> None:
    fake = FakeTransport(status=200)
    emitter = HttpEmitter("https://lineage.example.test/api/v1/lineage", transport=fake)
    event = make_run_event(
        "START",
        run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
        job=("etl", "load_orders"),
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    emitter.emit(event)
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["url"] == "https://lineage.example.test/api/v1/lineage"
    assert call["headers"]["Content-Type"] == "application/json"
    assert json.loads(call["body"]) == event

    # a 500 reply raises code 'transport_error'
    fake_err = FakeTransport(status=500)
    emitter_err = HttpEmitter("https://lineage.example.test/api/v1/lineage", transport=fake_err)
    with pytest.raises(LineageError) as exc_info:
        emitter_err.emit(event)
    assert exc_info.value.code == "transport_error"


def test_acceptance_capacity_and_on_event_payload() -> None:
    recorded_events: list[dict[str, Any]] = []
    graph = LineageGraph(max_events=2, on_event=lambda p: recorded_events.append(p))

    e1 = make_run_event(
        "START",
        run_id="11111111-1111-1111-1111-111111111111",
        job=("etl", "job1"),
        inputs=[("pg", "tbl1")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    e2 = make_run_event(
        "START",
        run_id="22222222-2222-2222-2222-222222222222",
        job=("etl", "job2"),
        outputs=[("pg", "tbl2")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    e3 = make_run_event(
        "START",
        run_id="33333333-3333-3333-3333-333333333333",
        job=("etl", "job3"),
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e1)
    graph.ingest(e2)
    with pytest.raises(LineageError) as exc_info:
        graph.ingest(e3)
    assert exc_info.value.code == "graph_full"

    # on_event payloads contain the event type and node ids only, never facets
    assert len(recorded_events) == 2
    for p in recorded_events:
        assert "facets" not in p
        assert set(p.keys()) == {"kind", "event_type", "nodes"}
        assert p["kind"] == "run_event"


def test_parent_facet_and_microsecond_datetime() -> None:
    dt = datetime(2026, 2, 3, 14, 30, 15, 123456, tzinfo=UTC)
    event = make_run_event(
        "START",
        run_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        job=("analytics", "daily_summary"),
        event_time=dt,
        producer="https://orchestrator.local",
        parent=("orchestrator", "pipeline", "99999999-9999-9999-9999-999999999999"),
    )
    validate_event(event)
    assert "parent" in event["run"]["facets"]
    assert event["eventTime"].startswith("2026-02-03T14:30:15.123456")


def test_validation_edge_cases() -> None:
    with pytest.raises(LineageError) as exc:
        validate_event([1, 2, 3])
    assert exc.value.code == "invalid_event"

    # Unsupported event
    with pytest.raises(LineageError) as exc:
        validate_event(
            {"schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/DatasetEvent"}
        )
    assert exc.value.code == "unsupported_event"

    # Missing eventTime
    with pytest.raises(LineageError) as exc:
        validate_event({"run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"}})
    assert exc.value.code == "invalid_event"
    assert exc.value.path == "eventTime"

    # Missing run
    with pytest.raises(LineageError) as exc:
        validate_event({"eventTime": "2026-01-05T10:00:00Z"})
    assert exc.value.path == "run"

    # Missing run.runId
    with pytest.raises(LineageError) as exc:
        validate_event({"eventTime": "2026-01-05T10:00:00Z", "run": {}})
    assert exc.value.path == "run.runId"

    # Non-dict facets
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc", "facets": []},
            }
        )
    assert exc.value.path == "run.facets"

    # Missing job
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
            }
        )
    assert exc.value.path == "job"

    # Missing job.namespace
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"name": "job1"},
            }
        )
    assert exc.value.path == "job.namespace"

    # Bad job.facets
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1", "facets": "not-a-dict"},
            }
        )
    assert exc.value.path == "job.facets"

    # Bad inputs type
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
                "inputs": "not-a-list",
            }
        )
    assert exc.value.path == "inputs"

    # Bad inputs element
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
                "inputs": ["not-dict"],
            }
        )
    assert exc.value.path == "inputs[0]"

    # Bad outputs namespace
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
                "outputs": [{"name": "tbl"}],
            }
        )
    assert exc.value.path == "outputs[0].namespace"

    # Bad outputs name
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
                "outputs": [{"namespace": "ns"}],
            }
        )
    assert exc.value.path == "outputs[0].name"

    # Bad outputs facets
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
                "outputs": [{"namespace": "ns", "name": "tbl", "facets": 123}],
            }
        )
    assert exc.value.path == "outputs[0].facets"

    # Missing producer
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
            }
        )
    assert exc.value.path == "producer"

    # Missing schemaURL
    with pytest.raises(LineageError) as exc:
        validate_event(
            {
                "eventTime": "2026-01-05T10:00:00Z",
                "run": {"runId": "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"},
                "job": {"namespace": "ns", "name": "job1"},
                "producer": "http://example.com",
            }
        )
    assert exc.value.path == "schemaURL"

    # Deeply nested recursion limit
    deep_doc: dict[str, Any] = {}
    ptr = deep_doc
    for _ in range(70):
        ptr["sub"] = {}
        ptr = ptr["sub"]
    with pytest.raises(LineageError) as exc:
        validate_event(deep_doc)
    assert exc.value.code == "input_too_large"


def test_graph_depth_and_runs() -> None:
    graph = LineageGraph()
    e = make_run_event(
        "START",
        run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
        job=("etl", "job1"),
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    graph.ingest(e)

    # Depth check
    with pytest.raises(LineageError) as exc:
        graph.upstream("job:etl/job1", depth=0)
    assert exc.value.code == "invalid_depth"
    with pytest.raises(LineageError) as exc:
        graph.upstream("job:etl/job1", depth=101)
    assert exc.value.code == "invalid_depth"

    # Unknown node
    with pytest.raises(LineageError) as exc:
        graph.upstream("job:unknown/job", depth=1)
    assert exc.value.code == "unknown_node"

    # Runs query
    runs = graph.runs("job:etl/job1")
    assert len(runs) == 1
    assert runs[0]["runId"] == "3f5e9c1a-1b2c-4d5e-8f90-123456789abc"

    with pytest.raises(LineageError) as exc:
        graph.runs("job:nonexistent/job")
    assert exc.value.code == "unknown_node"


def test_emitter_auth_and_error_handling() -> None:
    fake = FakeTransport(status=200)
    emitter = HttpEmitter(
        "https://lineage.example.test/api/v1/lineage", transport=fake, api_key="secret-token-123"
    )
    event = make_run_event(
        "START",
        run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
        job=("etl", "load_orders"),
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    emitter.emit(event)
    assert fake.calls[0]["headers"]["Authorization"] == "Bearer secret-token-123"

    # Ensure errors do not leak api_key
    class FailingTransport:
        def post(self, url: str, body: bytes, headers: dict[str, str]) -> int:
            raise RuntimeError("network dead")

    emitter_fail = HttpEmitter(
        "https://lineage.example.test/api/v1/lineage",
        transport=FailingTransport(),
        api_key="secret-token-123",
    )
    with pytest.raises(LineageError) as exc:
        emitter_fail.emit(event)
    assert exc.value.code == "transport_error"
    assert "secret-token-123" not in str(exc.value)


def test_wsgi_app_routing_and_errors() -> None:
    graph = LineageGraph()
    app = make_wsgi_app(graph)

    # 404 Route
    status, _, _ = _invoke_wsgi(app, "GET", "/unknown/path")
    assert status == "404 Not Found"

    # POST malformed json
    status, _, body = _invoke_wsgi(app, "POST", "/api/v1/lineage", body=b"not valid json")
    assert status == "400 Bad Request"
    assert json.loads(body)["code"] == "invalid_event"

    # GET downstream missing node
    status, _, body = _invoke_wsgi(app, "GET", "/api/v1/lineage/downstream")
    assert status == "400 Bad Request"

    # GET downstream bad depth
    status, _, body = _invoke_wsgi(
        app, "GET", "/api/v1/lineage/downstream", query_string="node=x&depth=abc"
    )
    assert status == "400 Bad Request"
    assert json.loads(body)["code"] == "invalid_depth"

    # GET downstream unknown node -> 404
    status, _, body = _invoke_wsgi(app, "GET", "/api/v1/lineage/downstream", query_string="node=x")
    assert status == "404 Not Found"
    assert json.loads(body)["code"] == "unknown_node"


@pytest.mark.conformance
def test_openlineage_conformance_schema() -> None:
    schema_path = Path("tests/conformance/OpenLineage.json")
    if not schema_path.exists():
        pytest.skip("OpenLineage.json fixture not present (run ci/conformance-setup.sh)")

    try:
        import jsonschema  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("jsonschema not installed")

    schema_doc = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = jsonschema.Draft202012Validator(
        schema_doc, format_checker=jsonschema.Draft202012Validator.FORMAT_CHECKER
    )

    # 1. Validate make_run_event output against official schema
    event = make_run_event(
        "START",
        run_id="3f5e9c1a-1b2c-4d5e-8f90-123456789abc",
        job=("etl", "load_orders"),
        inputs=[("pg", "raw.orders")],
        outputs=[("pg", "mart.orders")],
        event_time=datetime(2026, 1, 5, 10, 0, tzinfo=UTC),
        producer="https://example.test/etl",
    )
    validator.validate(event)

    # 2. Check 5 invalid fixtures fail schema validation
    bad_fixtures = [
        # Missing eventTime
        {
            "schemaURL": event["schemaURL"],
            "producer": event["producer"],
            "run": event["run"],
            "job": event["job"],
        },
        # Bad eventType
        {**event, "eventType": "INVALID_EVENT_TYPE"},
        # Bad runId uuid format
        {**event, "run": {"runId": "not-a-uuid"}},
        # Missing job name
        {**event, "job": {"namespace": "default"}},
        # Bad producer uri
        {**event, "producer": "not-a-uri"},
    ]
    for bad_event in bad_fixtures:
        assert not validator.is_valid(bad_event)
