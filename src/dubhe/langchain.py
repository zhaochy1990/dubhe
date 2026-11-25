from threading import Lock
from uuid import UUID

from langchain_core.callbacks import AsyncCallbackHandler, BaseCallbackManager
from langchain_core.runnables import RunnableConfig
from opentelemetry.trace import Span, StatusCode, set_span_in_context

from .domain import VerifiedTurn
from .sdk import TracingSdk


class LangChainTracing:
    """Prepare an ordinary native LangChain invocation bound to a verified turn."""

    def __init__(self, sdk: TracingSdk):
        self._sdk = sdk

    async def prepare_config(
        self, turn: VerifiedTurn, config: RunnableConfig | None = None
    ) -> RunnableConfig:
        prepared: RunnableConfig = {**(config or {})}
        callbacks = prepared.get("callbacks")
        handler = _TurnCallback(
            self, self._sdk, await self._sdk._prepare_turn(turn), turn.root_agent_id
        )
        if isinstance(callbacks, BaseCallbackManager):
            manager = callbacks.copy()
            manager.add_handler(handler, inherit=True)
            prepared["callbacks"] = manager
        else:
            prepared["callbacks"] = [*(callbacks or []), handler]
        return prepared

    def finish(self, config: RunnableConfig) -> None:
        callbacks = config.get("callbacks")
        handlers = (
            callbacks.handlers
            if isinstance(callbacks, BaseCallbackManager)
            else callbacks or ()
        )
        for handler in handlers:
            if isinstance(handler, _TurnCallback) and handler.owner is self:
                handler.finish()


class _TurnCallback(AsyncCallbackHandler):
    run_inline = True

    def __init__(
        self, owner: LangChainTracing, sdk: TracingSdk, root: Span, agent_id: str
    ):
        self.owner = owner
        self._sdk = sdk
        self._root = root
        self._agent_id = agent_id
        self._root_run_id: UUID | None = None
        self._root_ended = False
        self._finished = False
        self._parents: dict[UUID, Span] = {}
        self._models: dict[UUID, Span] = {}
        self._lock = Lock()

    def _start(
        self, run_id: UUID, parent_run_id: UUID | None, *, model: bool = False
    ) -> None:
        with self._lock:
            if self._finished or run_id in self._parents:
                return
            if self._root_run_id is None:
                self._root_run_id = run_id
                parent = self._root
            else:
                parent = self._parents.get(parent_run_id)
                if parent is None:
                    return
            if model:
                parent = self._sdk._tracer.start_span(
                    "llm",
                    context=set_span_in_context(parent),
                    attributes={
                        "dubhe.span.kind": "llm",
                        "dubhe.agent.id": self._agent_id,
                    },
                )
                self._models[run_id] = parent
            self._parents[run_id] = parent

    def _end(self, run_id: UUID, *, failed: bool) -> None:
        with self._lock:
            if self._finished:
                return
            self._parents.pop(run_id, None)
            status = StatusCode.ERROR if failed else StatusCode.OK
            model = self._models.pop(run_id, None)
            if model is not None:
                model.set_status(status)
                model.end()
            if run_id == self._root_run_id and not self._root_ended:
                self._root.set_status(status)
                self._root.set_attribute(
                    "dubhe.run.outcome", "failed" if failed else "completed"
                )
                self._root.end()
                self._root_ended = True

    def finish(self) -> None:
        with self._lock:
            if self._finished:
                return
            self._finished = True
            for model in self._models.values():
                model.end()
            self._models.clear()
            self._parents.clear()
            if self._root_run_id is None:
                # An invocation never started. Discard the candidate after closing the export gate.
                self._sdk._finish_turn(self._root)
                self._root.end()
            else:
                if not self._root_ended:
                    self._root.end()
                self._sdk._finish_turn(self._root)

    async def on_chain_start(
        self, serialized, inputs, *, run_id, parent_run_id=None, **kwargs
    ):
        self._start(run_id, parent_run_id)

    async def on_chain_end(self, outputs, *, run_id, **kwargs):
        self._end(run_id, failed=False)

    async def on_chain_error(self, error, *, run_id, **kwargs):
        self._end(run_id, failed=True)

    async def on_chat_model_start(
        self, serialized, messages, *, run_id, parent_run_id=None, **kwargs
    ):
        self._start(run_id, parent_run_id, model=True)

    async def on_llm_start(
        self, serialized, prompts, *, run_id, parent_run_id=None, **kwargs
    ):
        self._start(run_id, parent_run_id, model=True)

    async def on_llm_end(self, response, *, run_id, **kwargs):
        self._end(run_id, failed=False)

    async def on_llm_error(self, error, *, run_id, **kwargs):
        self._end(run_id, failed=True)
