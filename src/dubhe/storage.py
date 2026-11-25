import asyncio
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.resources import files

import pymysql
from pymysql.constants import CR, ER
from pymysql.cursors import DictCursor

from .domain import TraceScope, TransientRegistrationError, VerifiedRun, encode_id

_RETRYABLE_REGISTRATION_ERRORS = {
    CR.CR_CONN_HOST_ERROR,
    CR.CR_SERVER_GONE_ERROR,
    CR.CR_SERVER_LOST,
    ER.CON_COUNT_ERROR,
    ER.LOCK_DEADLOCK,
    ER.LOCK_WAIT_TIMEOUT,
    ER.SERVER_SHUTDOWN,
}


@dataclass(frozen=True)
class SpanRecord:
    trace_id: bytes
    span_id: bytes
    parent_span_id: bytes | None
    agent_id: bytes
    kind: str
    start_ns: int
    end_ns: int
    status: int
    run_outcome: str | None

    def columns(self, producer_id: bytes) -> dict:
        return {
            "trace_id": self.trace_id,
            "parent_span_id": self.parent_span_id,
            "producer_id": producer_id,
            "agent_id": self.agent_id,
            "kind": self.kind,
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "status": self.status,
            "metadata": {}
            if self.run_outcome is None
            else {"run_outcome": self.run_outcome},
        }


def decode_id(value: str, size: int) -> bytes:
    decoded = bytes.fromhex(value)
    if len(value) != size * 2 or len(decoded) != size or not any(decoded):
        raise ValueError("Invalid OpenTelemetry ID")
    return decoded


def _scope_key(scope: TraceScope) -> tuple[bytes, bytes, bytes]:
    return (
        encode_id(scope.tenant_id),
        encode_id(scope.environment_id),
        encode_id(scope.project_id),
    )


def _node(row: dict) -> dict:
    return {
        "node_id": f"span:{row['span_id'].hex()}",
        "trace_id": row["trace_id"].hex(),
        "span_id": row["span_id"].hex(),
        "parent_span_id": row["parent_span_id"].hex()
        if row["parent_span_id"]
        else None,
        "producer_id": row["producer_id"].decode(),
        "agent_id": row["agent_id"].decode(),
        "kind": row["kind"],
        "start_ns": row["start_ns"],
        "end_ns": row["end_ns"],
        "status": row["status"],
        "metadata": json.loads(row["metadata"]),
    }


class TraceStore:
    """Scoped tracing persistence and reads backed by one MySQL database."""

    def __init__(
        self, *, host: str, port: int, user: str, password: str, database: str
    ):
        self._connection_options = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    def _connect(self):
        return pymysql.connect(
            **self._connection_options,
            charset="utf8mb4",
            binary_prefix=True,
            autocommit=False,
            connect_timeout=2,
            read_timeout=3,
            write_timeout=3,
            cursorclass=DictCursor,
        )

    def initialize_schema(self) -> None:
        schema = files("dubhe").joinpath("schema.sql").read_text(encoding="utf-8")
        with self._connect() as conn, conn.cursor() as cursor:
            for statement in schema.split(";"):
                if statement.strip():
                    cursor.execute(statement)
            conn.commit()

    async def register_run(self, run: VerifiedRun) -> None:
        """Register the host's verified binding using a private integration boundary."""
        try:
            await asyncio.to_thread(self._register_run, run)
        except pymysql.OperationalError as error:
            if error.args and error.args[0] in _RETRYABLE_REGISTRATION_ERRORS:
                raise TransientRegistrationError(
                    "MySQL tracing registration is temporarily unavailable"
                ) from error
            raise

    def _register_run(self, run: VerifiedRun) -> None:
        scope = _scope_key(run.turn.scope)
        session_id = encode_id(run.turn.session_id)
        turn_id = encode_id(run.turn.turn_id)
        agent_id = encode_id(run.turn.root_agent_id)
        trace_id = decode_id(run.trace_id, 16)
        root = decode_id(run.root_span_id, 8)
        producer = encode_id(run.root_producer_id)
        elapsed = run.turn.session_started_at - datetime(1970, 1, 1, tzinfo=UTC)
        session_started = (
            (elapsed.days * 86400 + elapsed.seconds) * 1_000_000 + elapsed.microseconds
        ) * 1000
        if not 0 <= session_started <= run.registered_at_ns < run.accept_until_ns:
            raise ValueError("Inconsistent trusted run timestamps")

        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                """INSERT INTO tracing_sessions
                   (tenant_id, environment_id, project_id, session_id, root_agent_id, started_at_ns)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE session_id = session_id""",
                (*scope, session_id, agent_id, session_started),
            )
            cursor.execute(
                """SELECT root_agent_id, started_at_ns FROM tracing_sessions
                   WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND session_id = %s
                   FOR UPDATE""",
                (*scope, session_id),
            )
            if cursor.fetchone() != {
                "root_agent_id": agent_id,
                "started_at_ns": session_started,
            }:
                raise ValueError(
                    "Trusted session registration conflicts with an existing session"
                )
            cursor.execute(
                """INSERT INTO tracing_traces (
                       tenant_id, environment_id, project_id, trace_id, session_id, turn_id,
                       root_span_id, root_producer_id, root_agent_id, registered_at_ns, accept_until_ns
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE trace_id = trace_id""",
                (
                    *scope,
                    trace_id,
                    session_id,
                    turn_id,
                    root,
                    producer,
                    agent_id,
                    run.registered_at_ns,
                    run.accept_until_ns,
                ),
            )
            cursor.execute(
                """SELECT session_id, turn_id, root_span_id, root_producer_id, root_agent_id,
                          registered_at_ns, accept_until_ns FROM tracing_traces
                   WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND trace_id = %s
                   FOR UPDATE""",
                (*scope, trace_id),
            )
            if cursor.fetchone() != {
                "session_id": session_id,
                "turn_id": turn_id,
                "root_span_id": root,
                "root_producer_id": producer,
                "root_agent_id": agent_id,
                "registered_at_ns": run.registered_at_ns,
                "accept_until_ns": run.accept_until_ns,
            }:
                raise ValueError(
                    "Trusted run registration conflicts with an existing trace"
                )
            conn.commit()

    def ingest(
        self, scope: TraceScope, producer_id: str, spans: list[SpanRecord]
    ) -> int:
        """Commit accepted records together and return the number permanently rejected."""
        scope_key = _scope_key(scope)
        producer = encode_id(producer_id)
        now = time.time_ns()
        rejected = 0
        with self._connect() as conn, conn.cursor() as cursor:
            bindings = {}
            for span in spans:
                if span.trace_id not in bindings:
                    cursor.execute(
                        """SELECT root_span_id, root_agent_id, root_producer_id, accept_until_ns
                           FROM tracing_traces
                           WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND trace_id = %s
                           FOR UPDATE""",
                        (*scope_key, span.trace_id),
                    )
                    bindings[span.trace_id] = cursor.fetchone()
                binding = bindings[span.trace_id]
                if not _allowed_span(span, binding, producer, now):
                    rejected += 1
                    continue
                expected = span.columns(producer)
                cursor.execute(
                    """INSERT INTO tracing_spans (
                           tenant_id, environment_id, project_id, span_id, trace_id, parent_span_id,
                           producer_id, agent_id, kind, start_ns, end_ns, status, metadata
                       ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE span_id = span_id""",
                    (
                        *scope_key,
                        span.span_id,
                        span.trace_id,
                        span.parent_span_id,
                        producer,
                        span.agent_id,
                        span.kind,
                        span.start_ns,
                        span.end_ns,
                        span.status,
                        json.dumps(expected["metadata"]),
                    ),
                )
                cursor.execute(
                    """SELECT trace_id, parent_span_id, producer_id, agent_id, kind, start_ns, end_ns, status, metadata
                       FROM tracing_spans
                       WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND span_id = %s
                       FOR UPDATE""",
                    (*scope_key, span.span_id),
                )
                stored = cursor.fetchone()
                stored["metadata"] = json.loads(stored["metadata"])
                if stored != expected:
                    rejected += 1
                    continue
                if span.kind == "run":
                    cursor.execute(
                        """UPDATE tracing_traces SET start_ns = %s, end_ns = %s, run_state = %s
                           WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND trace_id = %s""",
                        (
                            span.start_ns,
                            span.end_ns,
                            span.run_outcome,
                            *scope_key,
                            span.trace_id,
                        ),
                    )
            conn.commit()
        return rejected

    def get_trace(
        self, scope: TraceScope, agent_ids: frozenset[str], trace_id: bytes
    ) -> dict | None:
        if not agent_ids:
            return None
        scope_key = _scope_key(scope)
        agents = tuple(map(encode_id, agent_ids))
        agent_placeholders = ", ".join(["%s"] * len(agents))
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute("START TRANSACTION READ ONLY")
            cursor.execute(
                f"""SELECT session_id, turn_id, root_agent_id, start_ns, end_ns, run_state FROM tracing_traces
                    WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND trace_id = %s
                      AND root_agent_id IN ({agent_placeholders})""",
                (*scope_key, trace_id, *agents),
            )
            trace = cursor.fetchone()
            if trace is None:
                return None
            cursor.execute(
                f"""SELECT span_id, trace_id, parent_span_id, producer_id, agent_id, kind, start_ns, end_ns, status,
                           metadata FROM tracing_spans
                    WHERE tenant_id = %s AND environment_id = %s AND project_id = %s AND trace_id = %s
                      AND agent_id IN ({agent_placeholders})
                    ORDER BY start_ns, span_id""",
                (*scope_key, trace_id, *agents),
            )
            nodes = [_node(row) for row in cursor.fetchall()]
            return {
                **scope.query_params(),
                **trace,
                "trace_id": trace_id.hex(),
                "session_id": trace["session_id"].decode(),
                "turn_id": trace["turn_id"].decode(),
                "root_agent_id": trace["root_agent_id"].decode(),
                "nodes": nodes,
            }

    def get_span(
        self,
        scope: TraceScope,
        agent_ids: frozenset[str],
        span_id: bytes,
        expected_trace_id: bytes | None,
    ) -> dict | None:
        if not agent_ids:
            return None
        agents = tuple(map(encode_id, agent_ids))
        agent_placeholders = ", ".join(["%s"] * len(agents))
        with self._connect() as conn, conn.cursor() as cursor:
            cursor.execute(
                f"""SELECT s.span_id, s.trace_id, s.parent_span_id, s.producer_id, s.agent_id, s.kind,
                           s.start_ns, s.end_ns, s.status, s.metadata
                    FROM tracing_spans AS s JOIN tracing_traces AS t
                      ON s.tenant_id = t.tenant_id AND s.environment_id = t.environment_id
                         AND s.project_id = t.project_id AND s.trace_id = t.trace_id
                    WHERE s.tenant_id = %s AND s.environment_id = %s AND s.project_id = %s AND s.span_id = %s
                      AND t.root_agent_id IN ({agent_placeholders}) AND s.agent_id IN ({agent_placeholders})""",
                (*_scope_key(scope), span_id, *agents, *agents),
            )
            span = cursor.fetchone()
            if span is None or (
                expected_trace_id is not None and span["trace_id"] != expected_trace_id
            ):
                return None
            return {**scope.query_params(), **_node(span)}


def _allowed_span(
    span: SpanRecord, binding: dict | None, producer: bytes, now: int
) -> bool:
    if (
        binding is None
        or binding["accept_until_ns"] < now
        or binding["root_producer_id"] != producer
        or binding["root_agent_id"] != span.agent_id
    ):
        return False
    root_id = binding["root_span_id"]
    if span.kind == "run":
        return span.span_id == root_id and span.parent_span_id is None
    return span.span_id != root_id and span.parent_span_id == root_id
