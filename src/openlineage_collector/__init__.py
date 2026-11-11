"""OpenLineage 2-0-2 collector, validator, lineage graph, and emitter."""

from __future__ import annotations

import json
import logging
import re
from collections import deque
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

__all__ = [
    "HttpEmitter",
    "LineageError",
    "LineageGraph",
    "Transport",
    "UrllibTransport",
    "__version__",
    "make_run_event",
    "make_wsgi_app",
    "validate_event",
]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

ErrorCode = Literal[
    "invalid_event",
    "unsupported_event",
    "graph_full",
    "unknown_node",
    "transport_error",
    "input_too_large",
    "invalid_depth",
]

RUN_EVENT_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
MAX_RECURSION_DEPTH = 64
DEFAULT_MAX_BODY_BYTES = 1_048_576

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_RFC3339_RE = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|[+-][0-9]{2}:[0-9]{2})$"
)
_URI_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*:")

VALID_EVENT_TYPES = {"START", "RUNNING", "COMPLETE", "ABORT", "FAIL", "OTHER"}
TERMINAL_EVENT_TYPES = {"COMPLETE", "FAIL", "ABORT"}


class LineageError(Exception):
    """Base typed error for openlineage_collector operations."""

    def __init__(
        self, code: ErrorCode, path: str | None = None, message: str | None = None
    ) -> None:
        self.code: ErrorCode = code
        self.path: str | None = path
        desc = (
            message or f"Lineage error [{code}] at path '{path}'"
            if path
            else (message or f"Lineage error [{code}]")
        )
        # Enforce no secrets and max 200 chars
        desc_trimmed = desc[:200]
        super().__init__(desc_trimmed)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {"code": self.code}
        if self.path is not None:
            data["path"] = self.path
        return data


def _format_rfc3339(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc_dt = dt.astimezone(timezone.utc)
    if utc_dt.microsecond:
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_run_event(
    event_type: str,
    *,
    run_id: str,
    job: tuple[str, str],
    inputs: Sequence[tuple[str, str]] = (),
    outputs: Sequence[tuple[str, str]] = (),
    event_time: datetime,
    producer: str,
    parent: tuple[str, str, str] | None = None,
) -> dict[str, Any]:
    """Construct an OpenLineage 2-0-2 RunEvent dict."""
    event_time_str = _format_rfc3339(event_time)
    run_facets: dict[str, Any] = {}
    if parent is not None:
        parent_job_ns, parent_job_name, parent_run_id = parent
        run_facets["parent"] = {
            "_producer": producer,
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-1/ParentRunFacet.json",
            "job": {"namespace": parent_job_ns, "name": parent_job_name},
            "run": {"runId": parent_run_id},
        }

    input_entries: list[dict[str, str]] = [{"namespace": ns, "name": name} for ns, name in inputs]
    output_entries: list[dict[str, str]] = [{"namespace": ns, "name": name} for ns, name in outputs]

    event: dict[str, Any] = {
        "eventType": event_type,
        "eventTime": event_time_str,
        "run": {
            "runId": run_id,
            "facets": run_facets,
        },
        "job": {
            "namespace": job[0],
            "name": job[1],
            "facets": {},
        },
        "inputs": input_entries,
        "outputs": output_entries,
        "producer": producer,
        "schemaURL": RUN_EVENT_SCHEMA_URL,
    }
    return event


def _check_depth_and_size(obj: object, current_depth: int = 1) -> None:
    if current_depth > MAX_RECURSION_DEPTH:
        raise LineageError("input_too_large", message="Exceeded maximum nesting depth of 64")
    if isinstance(obj, dict):
        for v in obj.values():
            _check_depth_and_size(v, current_depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _check_depth_and_size(item, current_depth + 1)


def _is_valid_uri(val: object) -> bool:
    if not isinstance(val, str) or not val.strip():
        return False
    return bool(_URI_SCHEME_RE.match(val))


def validate_event(obj: object) -> None:
    """Validate an OpenLineage RunEvent against the 2-0-2 specification.

    Reports the first failing path in document order.
    """
    _check_depth_and_size(obj)
    if not isinstance(obj, dict):
        raise LineageError("invalid_event", path="", message="Event must be a JSON object")

    # Check unsupported event types (DatasetEvent, JobEvent)
    schema_url_candidate = obj.get("schemaURL")
    if isinstance(schema_url_candidate, str) and (
        "DatasetEvent" in schema_url_candidate or "JobEvent" in schema_url_candidate
    ):
        raise LineageError(
            "unsupported_event", path="schemaURL", message="Only RunEvent is supported"
        )

    # Required from BaseEvent: eventTime, producer, schemaURL
    # Document order checking:
    # 1. eventType (optional, but if present must be string in VALID_EVENT_TYPES)
    if "eventType" in obj:
        et = obj["eventType"]
        if not isinstance(et, str) or et not in VALID_EVENT_TYPES:
            raise LineageError("invalid_event", path="eventType")

    # 2. eventTime (required, RFC 3339)
    if "eventTime" not in obj:
        raise LineageError("invalid_event", path="eventTime")
    event_time = obj["eventTime"]
    if not isinstance(event_time, str) or not _RFC3339_RE.match(event_time):
        raise LineageError("invalid_event", path="eventTime")
    try:
        # Ensure it parses as standard iso/rfc3339 date-time
        iso_clean = event_time.replace("Z", "+00:00")
        datetime.fromisoformat(iso_clean)
    except Exception as err:
        raise LineageError("invalid_event", path="eventTime") from err

    # 3. run (required object, run.runId required UUID)
    if "run" not in obj or not isinstance(obj["run"], dict):
        raise LineageError("invalid_event", path="run")
    run_obj = obj["run"]
    if "runId" not in run_obj:
        raise LineageError("invalid_event", path="run.runId")
    run_id = run_obj["runId"]
    if not isinstance(run_id, str) or not _UUID_RE.match(run_id):
        raise LineageError("invalid_event", path="run.runId")
    if "facets" in run_obj and not isinstance(run_obj["facets"], dict):
        raise LineageError("invalid_event", path="run.facets")

    # 4. job (required object, job.namespace and job.name required strings)
    if "job" not in obj or not isinstance(obj["job"], dict):
        raise LineageError("invalid_event", path="job")
    job_obj = obj["job"]
    if (
        "namespace" not in job_obj
        or not isinstance(job_obj["namespace"], str)
        or not job_obj["namespace"]
    ):
        raise LineageError("invalid_event", path="job.namespace")
    if "name" not in job_obj or not isinstance(job_obj["name"], str) or not job_obj["name"]:
        raise LineageError("invalid_event", path="job.name")
    if "facets" in job_obj and not isinstance(job_obj["facets"], dict):
        raise LineageError("invalid_event", path="job.facets")

    # 5. inputs (optional array of objects with namespace, name)
    if "inputs" in obj:
        inputs = obj["inputs"]
        if not isinstance(inputs, list):
            raise LineageError("invalid_event", path="inputs")
        for idx, item in enumerate(inputs):
            p = f"inputs[{idx}]"
            if not isinstance(item, dict):
                raise LineageError("invalid_event", path=p)
            if (
                "namespace" not in item
                or not isinstance(item["namespace"], str)
                or not item["namespace"]
            ):
                raise LineageError("invalid_event", path=f"{p}.namespace")
            if "name" not in item or not isinstance(item["name"], str) or not item["name"]:
                raise LineageError("invalid_event", path=f"{p}.name")
            if "facets" in item and not isinstance(item["facets"], dict):
                raise LineageError("invalid_event", path=f"{p}.facets")

    # 6. outputs (optional array of objects with namespace, name)
    if "outputs" in obj:
        outputs = obj["outputs"]
        if not isinstance(outputs, list):
            raise LineageError("invalid_event", path="outputs")
        for idx, item in enumerate(outputs):
            p = f"outputs[{idx}]"
            if not isinstance(item, dict):
                raise LineageError("invalid_event", path=p)
            if (
                "namespace" not in item
                or not isinstance(item["namespace"], str)
                or not item["namespace"]
            ):
                raise LineageError("invalid_event", path=f"{p}.namespace")
            if "name" not in item or not isinstance(item["name"], str) or not item["name"]:
                raise LineageError("invalid_event", path=f"{p}.name")
            if "facets" in item and not isinstance(item["facets"], dict):
                raise LineageError("invalid_event", path=f"{p}.facets")

    # 7. producer (required URI)
    if "producer" not in obj:
        raise LineageError("invalid_event", path="producer")
    if not _is_valid_uri(obj["producer"]):
        raise LineageError("invalid_event", path="producer")

    # 8. schemaURL (required URI)
    if "schemaURL" not in obj:
        raise LineageError("invalid_event", path="schemaURL")
    if not _is_valid_uri(obj["schemaURL"]):
        raise LineageError("invalid_event", path="schemaURL")


class LineageGraph:
    """In-memory lineage graph maintaining jobs and datasets."""

    def __init__(
        self,
        *,
        max_events: int = 100_000,
        on_event: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._max_events = max_events
        self._on_event = on_event
        self._events_count = 0
        self._seen_events: set[tuple[str, str | None, str]] = set()
        self._run_terminal_state: dict[str, str] = {}  # run_id -> terminal status
        self._run_events: dict[str, list[dict[str, Any]]] = {}  # run_id -> events list
        self._run_job: dict[str, str] = {}  # run_id -> job_node
        self._job_runs: dict[str, list[dict[str, Any]]] = {}  # job_node -> list of run dicts
        self._ignored_after_terminal = 0

        # Adjacency lists:
        # In OpenLineage: inputs flow Dataset -> Job; outputs flow Job -> Dataset
        # Downstream: source -> targets
        # Upstream: target -> sources
        self._outgoing: dict[str, set[str]] = {}
        self._incoming: dict[str, set[str]] = {}
        self._all_nodes: set[str] = set()

    def _add_node(self, node: str) -> None:
        self._all_nodes.add(node)
        if node not in self._outgoing:
            self._outgoing[node] = set()
        if node not in self._incoming:
            self._incoming[node] = set()

    def _add_edge(self, u: str, v: str) -> None:
        self._add_node(u)
        self._add_node(v)
        self._outgoing[u].add(v)
        self._incoming[v].add(u)

    def ingest(self, event: dict[str, Any]) -> None:
        """Validate and ingest a RunEvent into the lineage graph."""
        validate_event(event)
        run_id = event["run"]["runId"]
        event_type = event.get("eventType")
        event_time = event["eventTime"]
        job_node = f"job:{event['job']['namespace']}/{event['job']['name']}"

        event_dedup_key = (run_id, event_type, event_time)
        if event_dedup_key in self._seen_events:
            return

        if run_id in self._run_terminal_state:
            self._ignored_after_terminal += 1
            return

        if self._events_count >= self._max_events:
            raise LineageError("graph_full", message="LineageGraph event capacity reached")

        self._seen_events.add(event_dedup_key)
        self._events_count += 1

        self._add_node(job_node)
        if run_id not in self._run_events:
            self._run_events[run_id] = []
            self._run_job[run_id] = job_node
        self._run_events[run_id].append(event)

        # Keep job run references
        if job_node not in self._job_runs:
            self._job_runs[job_node] = []
        # Update or append run status in job_runs
        run_entry = next((r for r in self._job_runs[job_node] if r["runId"] == run_id), None)
        if run_entry is None:
            run_entry = {"runId": run_id, "status": event_type or "UNKNOWN", "events": 1}
            self._job_runs[job_node].append(run_entry)
        else:
            run_entry["events"] += 1
            if event_type is not None:
                run_entry["status"] = event_type

        # Register input/output dataset nodes so they exist in graph
        for inp in event.get("inputs", []):
            self._add_node(f"dataset:{inp['namespace']}/{inp['name']}")
        for outp in event.get("outputs", []):
            self._add_node(f"dataset:{outp['namespace']}/{outp['name']}")

        if event_type in TERMINAL_EVENT_TYPES:
            self._run_terminal_state[run_id] = event_type
            if event_type == "COMPLETE":
                # Consolidate all inputs and outputs across all events of this run
                all_inputs: set[str] = set()
                all_outputs: set[str] = set()
                for ev in self._run_events[run_id]:
                    for inp in ev.get("inputs", []):
                        all_inputs.add(f"dataset:{inp['namespace']}/{inp['name']}")
                    for outp in ev.get("outputs", []):
                        all_outputs.add(f"dataset:{outp['namespace']}/{outp['name']}")

                # Edges: dataset -> job (inputs), job -> dataset (outputs)
                for ds_in in all_inputs:
                    self._add_edge(ds_in, job_node)
                for ds_out in all_outputs:
                    self._add_edge(job_node, ds_out)

        if self._on_event is not None:
            nodes_involved = [job_node]
            for inp in event.get("inputs", []):
                nodes_involved.append(f"dataset:{inp['namespace']}/{inp['name']}")
            for outp in event.get("outputs", []):
                nodes_involved.append(f"dataset:{outp['namespace']}/{outp['name']}")
            self._on_event(
                {
                    "kind": "run_event",
                    "event_type": event_type,
                    "nodes": sorted(set(nodes_involved)),
                }
            )

    def _bfs(self, start_node: str, adj: dict[str, set[str]], depth: int) -> list[str]:
        if not (1 <= depth <= 100):
            raise LineageError("invalid_depth", message="Depth must be between 1 and 100")
        if start_node not in self._all_nodes:
            raise LineageError("unknown_node", message=f"Unknown node: {start_node}")

        visited: set[str] = set()
        queue: deque[tuple[str, int]] = deque([(start_node, 0)])
        results: set[str] = set()

        while queue:
            curr, d = queue.popleft()
            if d >= depth:
                continue
            for nxt in adj.get(curr, ()):
                if nxt not in visited:
                    visited.add(nxt)
                    results.add(nxt)
                    queue.append((nxt, d + 1))
        return sorted(results)

    def upstream(self, node: str, *, depth: int = 10) -> list[str]:
        """Return nodes upstream of ``node`` within ``depth`` BFS steps."""
        return self._bfs(node, self._incoming, depth)

    def downstream(self, node: str, *, depth: int = 10) -> list[str]:
        """Return nodes downstream of ``node`` within ``depth`` BFS steps."""
        return self._bfs(node, self._outgoing, depth)

    def runs(self, job: str) -> list[dict[str, Any]]:
        """Return the run summaries recorded for a job node."""
        if job not in self._all_nodes and job not in self._job_runs:
            raise LineageError("unknown_node", message=f"Unknown job: {job}")
        return list(self._job_runs.get(job, []))

    def stats(self) -> dict[str, int]:
        """Return statistics on stored events and graph size."""
        return {
            "events": self._events_count,
            "nodes": len(self._all_nodes),
            "ignored_after_terminal": self._ignored_after_terminal,
        }


def make_wsgi_app(
    graph: LineageGraph, *, max_body_bytes: int = DEFAULT_MAX_BODY_BYTES
) -> Callable[..., Any]:
    """Return a WSGI application serving OpenLineage ingestion and lineage queries."""

    def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
        method = environ.get("REQUEST_METHOD", "GET")
        path_info = environ.get("PATH_INFO", "")

        def respond(status: str, payload: Any) -> list[bytes]:
            body = json.dumps(payload).encode("utf-8")
            headers = [
                ("Content-Type", "application/json"),
                ("Content-Length", str(len(body))),
            ]
            start_response(status, headers)
            return [body]

        if method == "POST" and path_info == "/api/v1/lineage":
            content_length_str = environ.get("CONTENT_LENGTH", "")
            try:
                content_length = int(content_length_str) if content_length_str else 0
            except ValueError:
                content_length = 0

            if content_length > max_body_bytes:
                return respond("413 Request Entity Too Large", {"code": "input_too_large"})

            wsgi_input = environ.get("wsgi.input")
            raw_body = wsgi_input.read(content_length) if wsgi_input else b""
            if len(raw_body) > max_body_bytes:
                return respond("413 Request Entity Too Large", {"code": "input_too_large"})

            try:
                parsed = json.loads(raw_body.decode("utf-8"))
            except Exception:
                return respond(
                    "400 Bad Request", {"code": "invalid_event", "message": "Malformed JSON"}
                )

            try:
                graph.ingest(parsed)
                return respond("201 Created", {"status": "created"})
            except LineageError as err:
                status_code = "400 Bad Request"
                if err.code == "graph_full":
                    status_code = "507 Insufficient Storage"
                elif err.code == "input_too_large":
                    status_code = "413 Request Entity Too Large"
                return respond(status_code, err.to_dict())

        if method == "GET" and path_info in (
            "/api/v1/lineage/upstream",
            "/api/v1/lineage/downstream",
        ):
            query_string = environ.get("QUERY_STRING", "")
            from urllib.parse import parse_qs

            params = parse_qs(query_string)
            nodes = params.get("node", [])
            if not nodes:
                return respond(
                    "400 Bad Request", {"code": "invalid_event", "message": "Missing node param"}
                )
            node = nodes[0]
            depth = 10
            if "depth" in params:
                try:
                    depth = int(params["depth"][0])
                except ValueError:
                    return respond("400 Bad Request", {"code": "invalid_depth"})

            try:
                if path_info.endswith("/upstream"):
                    res = graph.upstream(node, depth=depth)
                else:
                    res = graph.downstream(node, depth=depth)
                return respond("200 OK", res)
            except LineageError as err:
                status = "400 Bad Request"
                if err.code == "unknown_node":
                    status = "404 Not Found"
                return respond(status, err.to_dict())

        return respond("404 Not Found", {"code": "not_found"})

    return app


class Transport(Protocol):
    """Transport protocol for HTTP emission."""

    def post(self, url: str, body: bytes, headers: dict[str, str]) -> int:
        """Send body to url with headers and return integer HTTP status."""
        ...


class UrllibTransport:
    """Standard-library HTTP transport."""

    def __init__(self, timeout: float = 10.0) -> None:
        self._timeout = timeout

    def post(self, url: str, body: bytes, headers: dict[str, str]) -> int:
        from http.client import HTTPConnection, HTTPSConnection
        from urllib.parse import urlparse

        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise LineageError("transport_error", message=f"Unsupported scheme {parsed.scheme}")

        conn_cls = HTTPSConnection if parsed.scheme == "https" else HTTPConnection
        conn = conn_cls(parsed.netloc, timeout=self._timeout)
        path = parsed.path
        if parsed.query:
            path += "?" + parsed.query
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            return int(resp.status)
        except Exception as err:
            raise LineageError("transport_error", message="Network communication failed") from err
        finally:
            conn.close()


class HttpEmitter:
    """Emits OpenLineage run events to an HTTP endpoint."""

    def __init__(
        self, url: str, *, transport: Transport | None = None, api_key: str | None = None
    ) -> None:
        self._url = url
        self._transport = transport if transport is not None else UrllibTransport()
        self._api_key = api_key

    def emit(self, event: dict[str, Any]) -> None:
        """Serialize and transmit event to the configured collector endpoint."""
        validate_event(event)
        body = json.dumps(event).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
        }
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"

        try:
            status = self._transport.post(self._url, body, headers)
        except LineageError:
            raise
        except Exception as err:
            raise LineageError("transport_error", message="Transport post failed") from err

        if not (200 <= status < 300):
            raise LineageError(
                "transport_error", message=f"HTTP emission failed with status {status}"
            )
