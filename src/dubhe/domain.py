from dataclasses import asdict, dataclass
from datetime import datetime


def encode_id(value: str) -> bytes:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("An opaque ID must be a non-empty string without null bytes")
    encoded = value.encode("utf-8")
    if len(encoded) > 128:
        raise ValueError("An opaque ID may occupy at most 128 UTF-8 bytes")
    return encoded


@dataclass(frozen=True)
class TraceScope:
    tenant_id: str
    environment_id: str
    project_id: str

    def __post_init__(self):
        for identifier in self.query_params().values():
            encode_id(identifier)

    def query_params(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True)
class VerifiedTurn:
    scope: TraceScope
    session_id: str
    turn_id: str
    root_agent_id: str
    session_started_at: datetime

    def __post_init__(self):
        for identifier in (self.session_id, self.turn_id, self.root_agent_id):
            encode_id(identifier)
        if self.session_started_at.utcoffset() is None:
            raise ValueError("Session start requires a timezone")


@dataclass(frozen=True)
class VerifiedRun:
    turn: VerifiedTurn
    trace_id: str
    root_span_id: str
    root_producer_id: str
    registered_at_ns: int
    accept_until_ns: int
