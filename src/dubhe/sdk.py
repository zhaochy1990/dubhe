import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from threading import Lock

from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.trace import (
    Span,
    format_span_id,
    format_trace_id,
    set_span_in_context,
)

from .domain import (
    TraceScope,
    TransientRegistrationError,
    VerifiedRun,
    VerifiedTurn,
    encode_id,
)

logger = logging.getLogger(__name__)


class TraceAlreadyBoundError(RuntimeError):
    """The trusted host has already bound a Trace to this turn."""

    code = "TRACE_ALREADY_BOUND"


@dataclass(frozen=True)
class TracingOptions:
    scope: TraceScope
    producer_id: str
    otlp_trace_endpoint: str
    otlp_headers: Mapping[str, str] = field(repr=False)
    prepare_run: Callable[[VerifiedRun], Awaitable[bool]]
    register_run: Callable[[VerifiedRun], Awaitable[None]]
    max_buffered_spans: int = 2048


@dataclass
class _Registration:
    pending: list[ReadableSpan] = field(default_factory=list)
    accepted: bool | None = None
    finished: bool = False


class _RegistrationGate(SpanProcessor):
    """Withhold ended spans until the trusted registration commits."""

    def __init__(self, batch: BatchSpanProcessor, limit: int):
        self._batch = batch
        self._limit = limit
        self._lock = Lock()
        self._registrations: dict[int, _Registration] = {}
        self._pending_count = 0

    def add(self, trace_id: int) -> None:
        with self._lock:
            self._registrations[trace_id] = _Registration()

    def on_end(self, span: ReadableSpan) -> None:
        assert span.context is not None
        with self._lock:
            registration = self._registrations.get(span.context.trace_id)
            if (
                registration is None
                or registration.finished
                or registration.accepted is False
            ):
                return
            if registration.accepted:
                self._batch.on_end(span)
            elif self._pending_count < self._limit:
                registration.pending.append(span)
                self._pending_count += 1
            else:
                logger.warning(
                    "Dropped a span while the tracing registration buffer was full"
                )

    def resolve(self, trace_id: int, accepted: bool) -> None:
        with self._lock:
            registration = self._registrations.get(trace_id)
            if registration is None:
                return
            registration.accepted = accepted
            if accepted:
                for span in registration.pending:
                    self._batch.on_end(span)
            self._pending_count -= len(registration.pending)
            registration.pending.clear()
            if registration.finished:
                del self._registrations[trace_id]

    def close(self, trace_id: int) -> None:
        with self._lock:
            registration = self._registrations.get(trace_id)
            if registration is None:
                return
            registration.finished = True
            if registration.accepted is not None:
                del self._registrations[trace_id]

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        return self._batch.force_flush(timeout_millis)

    def shutdown(self) -> None:
        with self._lock:
            self._registrations.clear()
            self._pending_count = 0
        self._batch.shutdown()


class TracingSdk:
    def __init__(self, options: TracingOptions):
        encode_id(options.producer_id)
        if options.max_buffered_spans < 1:
            raise ValueError("max_buffered_spans must be positive")
        self._options = options
        exporter = OTLPSpanExporter(
            endpoint=options.otlp_trace_endpoint, headers=options.otlp_headers
        )
        batch = BatchSpanProcessor(
            exporter,
            max_queue_size=options.max_buffered_spans,
            max_export_batch_size=min(512, options.max_buffered_spans),
        )
        self._gate = _RegistrationGate(batch, options.max_buffered_spans)
        self._provider = TracerProvider(sampler=ALWAYS_ON, shutdown_on_exit=False)
        self._provider.add_span_processor(self._gate)
        self._tracer = self._provider.get_tracer("dubhe")
        self._registration_tasks: set[asyncio.Task[None]] = set()
        self._closed = False
        self._shutdown_task: asyncio.Task | None = None

    def _start_span(self, kind: str, agent_id: str, context: Context) -> Span:
        return self._tracer.start_span(
            kind,
            context=context,
            attributes={"dubhe.span.kind": kind, "dubhe.agent.id": agent_id},
        )

    def _start_model_span(self, parent: Span, agent_id: str) -> Span:
        return self._start_span("llm", agent_id, set_span_in_context(parent))

    async def _prepare_turn(self, turn: VerifiedTurn) -> Span:
        if self._closed:
            raise RuntimeError("Tracing SDK is shut down")
        if turn.scope != self._options.scope:
            raise ValueError("Verified turn is outside the SDK scope")
        root = self._start_span("run", turn.root_agent_id, Context())
        context = root.get_span_context()
        registered_at = time.time_ns()
        run = VerifiedRun(
            turn=turn,
            trace_id=format_trace_id(context.trace_id),
            root_span_id=format_span_id(context.span_id),
            root_producer_id=self._options.producer_id,
            registered_at_ns=registered_at,
            accept_until_ns=registered_at + 48 * 60 * 60 * 1_000_000_000,
        )
        try:
            if not await self._options.prepare_run(run):
                raise TraceAlreadyBoundError("A Trace is already bound to this turn")
            if self._closed:
                raise RuntimeError("Tracing SDK is shut down")
        except BaseException:
            root.end()
            raise
        self._gate.add(context.trace_id)
        registration = asyncio.create_task(self._register(context.trace_id, run))
        self._registration_tasks.add(registration)
        registration.add_done_callback(self._registration_tasks.discard)
        return root

    async def _register(self, trace_id: int, run: VerifiedRun) -> None:
        accepted = False
        remaining = (run.accept_until_ns - time.time_ns()) / 1_000_000_000
        deadline = asyncio.timeout(remaining)
        delay = 0.1
        try:
            async with deadline:
                while time.time_ns() < run.accept_until_ns:
                    try:
                        await self._options.register_run(run)
                    except TransientRegistrationError:
                        await asyncio.sleep(delay)
                        delay = min(delay * 2, 5)
                    else:
                        accepted = time.time_ns() < run.accept_until_ns
                        break
        except Exception:  # noqa: BLE001 - Host adapters may raise transport-specific errors.
            if not deadline.expired():
                logger.warning("Trusted tracing registration failed")
                return
        finally:
            self._gate.resolve(trace_id, accepted)
        if not accepted:
            logger.warning("Trusted tracing registration expired")

    def _finish_turn(self, root: Span) -> None:
        self._gate.close(root.get_span_context().trace_id)

    async def flush(self, *, timeout_ms: int = 30000) -> bool:
        """Drain spans already ended within the timeout. Returns whether draining finished."""
        deadline = time.monotonic() + timeout_ms / 1000
        if self._registration_tasks:
            _, pending = await asyncio.wait(
                tuple(self._registration_tasks),
                timeout=max(0, deadline - time.monotonic()),
            )
            if pending:
                return False
        remaining = int((deadline - time.monotonic()) * 1000)
        if remaining <= 0:
            return False
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(self._provider.force_flush, remaining),
                timeout=remaining / 1000,
            )
        except TimeoutError:
            return False

    async def shutdown(self, *, timeout_ms: int = 30000) -> bool:
        """Stop preparing turns and drain the SDK within the timeout."""
        if self._shutdown_task is None:
            self._closed = True
            self._shutdown_task = asyncio.create_task(self._shutdown(timeout_ms))
        try:
            return await asyncio.wait_for(
                asyncio.shield(self._shutdown_task), timeout=timeout_ms / 1000
            )
        except TimeoutError:
            return False

    async def _shutdown(self, timeout_ms: int) -> bool:
        drained = await self.flush(timeout_ms=timeout_ms)
        registrations = tuple(self._registration_tasks)
        for task in registrations:
            task.cancel()
        await asyncio.gather(*registrations, return_exceptions=True)
        await asyncio.to_thread(self._provider.shutdown)
        return drained


def create_tracing_sdk(options: TracingOptions) -> TracingSdk:
    return TracingSdk(options)
