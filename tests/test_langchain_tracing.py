import asyncio
import os
import socket
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import uvicorn
from langchain.agents import create_agent
from langchain_core.callbacks import AsyncCallbackHandler
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage
from langgraph.checkpoint.memory import InMemorySaver

from dubhe import (
    TraceScope,
    TracingOptions,
    TransientRegistrationError,
    VerifiedTurn,
    create_tracing_sdk,
)
from dubhe.langchain import LangChainTracing
from dubhe.service import BearerAuth, TraceReader, Workload, create_app
from dubhe.storage import TraceStore

OTHER_SCOPE_IDS = {
    "tenant_id": "tenant-beta",
    "environment_id": "Staging",
    "project_id": "billing",
}


class ExistingCallback(AsyncCallbackHandler):
    def __init__(self):
        self.invocations = []

    async def on_chat_model_start(self, serialized, messages, *, metadata, **kwargs):
        self.invocations.append(metadata["integration_marker"])


class FailingChatModel(FakeMessagesListChatModel):
    error: BaseException

    async def _agenerate(self, *args, **kwargs):
        raise self.error


class BlockingChatModel(FakeMessagesListChatModel):
    started: asyncio.Event

    async def _agenerate(self, *args, **kwargs):
        self.started.set()
        await asyncio.Future()


@pytest.fixture(scope="module")
def tracing_service():
    scope = TraceScope("tenant-alpha", "staging", "support")
    agent_grants = frozenset({"support-agent"})
    readers = {"reader-token": TraceReader(scope, agent_grants)}
    readers.update(
        {
            f"other-{field}-reader": TraceReader(
                replace(scope, **{field: value}), agent_grants
            )
            for field, value in OTHER_SCOPE_IDS.items()
        }
    )
    workloads = {
        "workload-token": Workload(scope, "agent-worker"),
        "other-scope-workload": Workload(
            replace(scope, tenant_id="tenant-beta"), "agent-worker"
        ),
    }
    auth = BearerAuth(workloads=workloads, readers=readers)
    store = TraceStore(
        host=os.getenv("DUBHE_TEST_MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("DUBHE_TEST_MYSQL_PORT", "3306")),
        user=os.getenv("DUBHE_TEST_MYSQL_USER", "dubhe"),
        password=os.getenv("DUBHE_TEST_MYSQL_PASSWORD", "dubhe"),
        database=os.getenv("DUBHE_TEST_MYSQL_DATABASE", "dubhe"),
    )
    store.initialize_schema()
    app = create_app(store, auth)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        endpoint = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(app, log_level="warning"))
        worker = threading.Thread(
            target=server.run, kwargs={"sockets": [listener]}, daemon=True
        )
        worker.start()
        try:
            deadline = time.monotonic() + 5
            while not server.started:
                if not worker.is_alive() or time.monotonic() >= deadline:
                    pytest.fail("Tracing HTTP service failed to start")
                time.sleep(0.01)
            yield scope, store, endpoint
        finally:
            server.should_exit = True
            worker.join(timeout=5)


async def invoke_turn(tracing_service, *, model=None, invoke=None):
    scope, store, endpoint = tracing_service
    binding_by_turn = {}
    registration_attempts = []
    allow_commit = asyncio.Event()

    async def prepare_run(run):
        key = (run.turn.scope, run.turn.session_id, run.turn.turn_id)
        if key in binding_by_turn:
            return False
        binding_by_turn[key] = run
        return True

    async def register_run(run):
        registration_attempts.append(run)
        if len(registration_attempts) == 1:
            raise TransientRegistrationError("Registration is temporarily unavailable")
        await allow_commit.wait()
        await store.register_run(run)

    sdk = create_tracing_sdk(
        TracingOptions(
            scope=scope,
            producer_id="agent-worker",
            otlp_trace_endpoint=f"{endpoint}/v1/traces",
            otlp_headers={"Authorization": "Bearer workload-token"},
            prepare_run=prepare_run,
            register_run=register_run,
        )
    )
    tracer = LangChainTracing(sdk)
    if model is None:
        model = FakeMessagesListChatModel(
            responses=[AIMessage(content="Your balance is 42.")]
        )

    agent = create_agent(model, tools=[], checkpointer=InMemorySaver())
    callback = ExistingCallback()
    thread_id = uuid4().hex
    prior_config = {
        "callbacks": [callback],
        "configurable": {"thread_id": thread_id},
        "metadata": {"integration_marker": "kept"},
    }
    turn = VerifiedTurn(
        scope,
        f"session-{thread_id}",
        "turn-1",
        "support-agent",
        datetime.now(UTC),
    )
    try:
        config = await asyncio.wait_for(
            tracer.prepare_config(turn, config=prior_config), timeout=5
        )
        try:
            invocation = agent.ainvoke(
                {"messages": [{"role": "user", "content": "What is my balance?"}]},
                config=config,
            )
            result = await invocation if invoke is None else await invoke(invocation)
        finally:
            tracer.finish(config)
            tracer.finish(config)
        checkpoint = await agent.aget_state({"configurable": {"thread_id": thread_id}})
        binding = binding_by_turn[scope, turn.session_id, turn.turn_id]
        async with httpx.AsyncClient(base_url=endpoint) as client:
            before_commit = await client.get(
                f"/v1/observability/traces/{binding.trace_id}",
                params=scope.query_params(),
                headers={"Authorization": "Bearer reader-token"},
            )
        assert before_commit.status_code == 404, before_commit.text
        allow_commit.set()
        assert await sdk.flush(timeout_ms=5_000)
        assert registration_attempts == [binding, binding]
    finally:
        allow_commit.set()
        await sdk.shutdown(timeout_ms=5_000)
    return (
        result,
        checkpoint,
        callback,
        prior_config,
        binding,
    )


@pytest.fixture(scope="module")
def traced_turn(tracing_service):
    return asyncio.run(invoke_turn(tracing_service))


def read_trace(tracing_service, binding):
    scope, _, endpoint = tracing_service
    response = httpx.get(
        f"{endpoint}/v1/observability/traces/{binding.trace_id}",
        params=scope.query_params(),
        headers={"Authorization": "Bearer reader-token"},
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_native_ainvoke_preserves_callbacks_checkpoint_and_trace_when_registration_retries(
    tracing_service, traced_turn
):
    scope, _, endpoint = tracing_service
    result, checkpoint, callback, prior_config, binding = traced_turn

    assert result["messages"][-1].content == "Your balance is 42."
    assert callback.invocations == ["kept"]
    assert prior_config["callbacks"] == [callback]
    assert checkpoint.values["messages"][-1].content == "Your balance is 42."
    assert len(checkpoint.values["messages"]) == 2

    trace = read_trace(tracing_service, binding)
    assert trace["trace_id"] == binding.trace_id
    assert trace["session_id"] == binding.turn.session_id
    assert trace["turn_id"] == "turn-1"
    assert trace["root_agent_id"] == "support-agent"
    assert trace["run_state"] == "completed"
    assert len(trace["nodes"]) == 2
    nodes = {node["kind"]: node for node in trace["nodes"]}
    assert nodes.keys() == {"run", "llm"}
    assert nodes["run"]["span_id"] == binding.root_span_id
    assert nodes["run"]["parent_span_id"] is None
    assert nodes["llm"]["parent_span_id"] == binding.root_span_id
    assert nodes["llm"]["agent_id"] == "support-agent"

    detail = httpx.get(
        f"{endpoint}/v1/observability/nodes/{nodes['llm']['node_id']}",
        params={**scope.query_params(), "expected_trace_id": binding.trace_id},
        headers={"Authorization": "Bearer reader-token"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["parent_span_id"] == binding.root_span_id


def test_native_ainvoke_preserves_original_exception_and_records_failed_run(
    tracing_service,
):
    error = ValueError("Model call failed")
    model = FailingChatModel(responses=[], error=error)

    async def expect_model_failure(invocation):
        with pytest.raises(ValueError) as raised:
            await invocation
        assert raised.value is error

    _, _, _, _, binding = asyncio.run(
        invoke_turn(tracing_service, model=model, invoke=expect_model_failure)
    )

    trace = read_trace(tracing_service, binding)
    assert trace["run_state"] == "failed"
    roots = [node for node in trace["nodes"] if node["kind"] == "run"]
    assert [root["span_id"] for root in roots] == [binding.root_span_id]
    assert roots[0]["metadata"] == {"run_outcome": "failed"}
    assert roots[0]["status"] == 2  # OTLP error status.


def test_native_ainvoke_leaves_unconfirmed_cancellation_outcome_unset(tracing_service):
    model = BlockingChatModel(responses=[], started=asyncio.Event())

    async def cancel_invocation(invocation):
        task = asyncio.create_task(invocation)
        try:
            await asyncio.wait_for(model.started.wait(), timeout=5)
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

    _, _, _, _, binding = asyncio.run(
        invoke_turn(tracing_service, model=model, invoke=cancel_invocation)
    )

    trace = read_trace(tracing_service, binding)
    assert trace["run_state"] is None
    roots = [node for node in trace["nodes"] if node["kind"] == "run"]
    assert [root["span_id"] for root in roots] == [binding.root_span_id]
    assert roots[0]["metadata"] == {}
    assert roots[0]["status"] == 0  # OpenTelemetry unset status.


@pytest.mark.parametrize("field", OTHER_SCOPE_IDS)
def test_trace_and_span_are_unavailable_from_each_wrong_scope(
    tracing_service, traced_turn, field
):
    scope, _, endpoint = tracing_service
    binding = traced_turn[-1]
    real_scope = scope.query_params()
    wrong_scope = {**real_scope, field: OTHER_SCOPE_IDS[field]}
    with httpx.Client(
        base_url=endpoint, headers={"Authorization": "Bearer reader-token"}
    ) as client:
        unavailable = client.get(
            f"/v1/observability/traces/{uuid4().hex}", params=real_scope
        )
        assert unavailable.status_code == 404
        for route in (
            f"/v1/observability/traces/{binding.trace_id}",
            f"/v1/observability/nodes/span:{binding.root_span_id}",
        ):
            for params, token in (
                (wrong_scope, "reader-token"),
                (real_scope, f"other-{field}-reader"),
                (wrong_scope, f"other-{field}-reader"),
                (real_scope, "other-scope-workload"),
            ):
                response = client.get(
                    route, params=params, headers={"Authorization": f"Bearer {token}"}
                )
                assert response.status_code == unavailable.status_code
                assert response.json() == unavailable.json()
