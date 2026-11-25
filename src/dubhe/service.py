import gzip
import io
import secrets
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from google.protobuf.message import DecodeError
from google.rpc import code_pb2, status_pb2
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)
from opentelemetry.proto.trace.v1.trace_pb2 import Span
from pymysql import MySQLError
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from .domain import TraceScope, encode_id
from .storage import SpanRecord, TraceStore, decode_id

_PROTOBUF = "application/x-protobuf"
_MAX_BODY_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class Workload:
    scope: TraceScope
    producer_id: str

    def __post_init__(self):
        encode_id(self.producer_id)


@dataclass(frozen=True)
class TraceReader:
    scope: TraceScope
    agent_ids: frozenset[str]

    def __post_init__(self):
        for agent_id in self.agent_ids:
            encode_id(agent_id)


class TracingAuth(Protocol):
    def authenticate_workload(self, authorization: str) -> Workload | None: ...

    def authenticate_reader(self, authorization: str) -> TraceReader | None: ...


class BearerAuth:
    """Reference credential resolver using server-controlled identity and grants."""

    def __init__(
        self, *, workloads: Mapping[str, Workload], readers: Mapping[str, TraceReader]
    ):
        self._workloads = dict(workloads)
        self._readers = dict(readers)

    def authenticate_workload(self, authorization: str) -> Workload | None:
        return _bearer_identity(authorization, self._workloads)

    def authenticate_reader(self, authorization: str) -> TraceReader | None:
        return _bearer_identity(authorization, self._readers)


def _bearer_identity(authorization: str, identities: Mapping):
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    for expected, identity in identities.items():
        if secrets.compare_digest(token.encode(), expected.encode()):
            return identity
    return None


class _RequestTooLarge(ValueError):
    pass


async def _read_body(request: Request, encoding: str) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > _MAX_BODY_BYTES:
            raise _RequestTooLarge
        body.extend(chunk)
    if encoding == "gzip":
        with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
            body = stream.read(_MAX_BODY_BYTES + 1)
        if len(body) > _MAX_BODY_BYTES:
            raise _RequestTooLarge
    return bytes(body)


def _normalize(span: Span) -> SpanRecord:
    if (
        len(span.trace_id) != 16
        or not any(span.trace_id)
        or len(span.span_id) != 8
        or not any(span.span_id)
        or span.parent_span_id
        and (len(span.parent_span_id) != 8 or not any(span.parent_span_id))
        or not 0 < span.start_time_unix_nano <= span.end_time_unix_nano
        or span.status.code not in {0, 1, 2}
    ):
        raise ValueError("Invalid span context, timestamps or status")
    attributes = {}
    allowed = {"dubhe.span.kind", "dubhe.agent.id", "dubhe.run.outcome"}
    for item in span.attributes:
        if item.key not in allowed:
            continue
        if item.key in attributes or item.value.WhichOneof("value") != "string_value":
            raise ValueError("Invalid known span attribute")
        attributes[item.key] = item.value.string_value
    kind = attributes.get("dubhe.span.kind")
    outcome = attributes.get("dubhe.run.outcome")
    if kind not in {"run", "llm"} or (
        outcome is not None
        and (kind != "run" or outcome not in {"completed", "failed", "cancelled"})
    ):
        raise ValueError("Invalid span kind or run outcome")
    return SpanRecord(
        trace_id=span.trace_id,
        span_id=span.span_id,
        parent_span_id=span.parent_span_id or None,
        agent_id=encode_id(attributes.get("dubhe.agent.id", "")),
        kind=kind,
        start_ns=span.start_time_unix_nano,
        end_ns=span.end_time_unix_nano,
        status=span.status.code,
        run_outcome=outcome,
    )


def _otlp_error(
    http_status: int, rpc_status: int, message: str, headers: dict | None = None
) -> Response:
    payload = status_pb2.Status(code=rpc_status, message=message)
    return Response(
        payload.SerializeToString(),
        status_code=http_status,
        media_type=_PROTOBUF,
        headers=headers,
    )


def _scope(request: Request) -> TraceScope:
    identifiers = {}
    for field in ("tenant_id", "environment_id", "project_id"):
        values = request.query_params.getlist(field)
        if len(values) != 1:
            raise ValueError("Expected one value for each scope field")
        identifiers[field] = values[0]
    return TraceScope(**identifiers)


def _unavailable() -> JSONResponse:
    return JSONResponse({"detail": "Not available"}, status_code=404)


def create_app(
    store: TraceStore, auth: TracingAuth, *, query_store: TraceStore | None = None
) -> Starlette:
    reads = query_store if query_store is not None else store

    async def ingest(request: Request) -> Response:
        workload = auth.authenticate_workload(request.headers.get("authorization", ""))
        if workload is None:
            return _otlp_error(
                401,
                code_pb2.UNAUTHENTICATED,
                "Workload authentication required",
                {"WWW-Authenticate": "Bearer"},
            )
        content_type = (
            request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        )
        encoding = request.headers.get("content-encoding", "identity").strip().lower()
        if content_type != _PROTOBUF or encoding not in {"identity", "gzip"}:
            return _otlp_error(
                415, code_pb2.INVALID_ARGUMENT, "Unsupported OTLP encoding"
            )
        try:
            body = await _read_body(request, encoding)
            batch = ExportTraceServiceRequest.FromString(body)
        except _RequestTooLarge:
            return _otlp_error(
                413, code_pb2.RESOURCE_EXHAUSTED, "OTLP request exceeds the size limit"
            )
        except (DecodeError, OSError, EOFError, zlib.error):
            return _otlp_error(
                400, code_pb2.INVALID_ARGUMENT, "Invalid OTLP trace request"
            )
        records = []
        rejected = 0
        for resource_spans in batch.resource_spans:
            for scope_spans in resource_spans.scope_spans:
                for span in scope_spans.spans:
                    try:
                        records.append(_normalize(span))
                    except ValueError:
                        rejected += 1
        try:
            if records:
                rejected += await run_in_threadpool(
                    store.ingest, workload.scope, workload.producer_id, records
                )
        except MySQLError:
            return _otlp_error(503, code_pb2.UNAVAILABLE, "Tracing storage unavailable")
        response = ExportTraceServiceResponse()
        if rejected:
            response.partial_success.rejected_spans = rejected
            response.partial_success.error_message = (
                "One or more spans failed validation or authorization"
            )
        return Response(response.SerializeToString(), media_type=_PROTOBUF)

    def authorized_reader(request: Request) -> TraceReader | None:
        reader = auth.authenticate_reader(request.headers.get("authorization", ""))
        try:
            return (
                reader
                if reader is not None and reader.scope == _scope(request)
                else None
            )
        except ValueError:
            return None

    async def get_trace(request: Request) -> JSONResponse:
        reader = authorized_reader(request)
        if reader is None:
            return _unavailable()
        try:
            trace_id = decode_id(request.path_params["trace_id"], 16)
            trace = await run_in_threadpool(
                reads.get_trace, reader.scope, reader.agent_ids, trace_id
            )
        except ValueError:
            return _unavailable()
        except MySQLError:
            return JSONResponse(
                {"detail": "Tracing storage unavailable"}, status_code=503
            )
        return JSONResponse(trace) if trace is not None else _unavailable()

    async def get_node(request: Request) -> JSONResponse:
        reader = authorized_reader(request)
        kind, _, raw_id = request.path_params["node_id"].partition(":")
        if reader is None or kind != "span":
            return _unavailable()
        try:
            span_id = decode_id(raw_id, 8)
            expected = request.query_params.getlist("expected_trace_id")
            if len(expected) > 1:
                return _unavailable()
            trace_id = decode_id(expected[0], 16) if expected else None
            node = await run_in_threadpool(
                reads.get_span, reader.scope, reader.agent_ids, span_id, trace_id
            )
        except ValueError:
            return _unavailable()
        except MySQLError:
            return JSONResponse(
                {"detail": "Tracing storage unavailable"}, status_code=503
            )
        return JSONResponse(node) if node is not None else _unavailable()

    return Starlette(
        routes=[
            Route("/v1/traces", ingest, methods=["POST"]),
            Route("/v1/observability/traces/{trace_id}", get_trace),
            Route("/v1/observability/nodes/{node_id}", get_node),
        ]
    )
