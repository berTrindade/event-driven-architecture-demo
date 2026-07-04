from dataclasses import dataclass, field, fields
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional
import os
import uuid

EVENT_PREFIX = "event."
REQUEST_PREFIX = "request."


def event_subject(topic: str) -> str:
    """NATS subject for fire-and-forget events."""
    return f"{EVENT_PREFIX}{topic}"


def request_subject(topic: str) -> str:
    """NATS subject for request-response."""
    return f"{REQUEST_PREFIX}{topic}"


@dataclass
class EventPayload:
    topic: str
    data: Dict[str, Any] = field(default_factory=dict)
    eventId: Optional[str] = field(default_factory=lambda: str(uuid.uuid4()))
    userId: Optional[str] = None
    groupId: Optional[str] = None
    timestamp: Optional[str] = None
    # Carries W3C trace context so a trace can span services. See README.
    traceContext: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        message = {
            "topic": self.topic,
            "eventId": self.eventId,
            "userId": self.userId,
            "groupId": self.groupId,
            "data": self.data,
        }
        if self.traceContext:
            message["traceContext"] = self.traceContext
        return message


@dataclass
class EventMessage:
    topic: str
    data: Dict[str, Any] = field(default_factory=dict)
    timestamp: Optional[str] = None
    eventId: str = field(default_factory=lambda: str(uuid.uuid4()))
    userId: Optional[str] = None
    groupId: Optional[str] = None
    correlationId: Optional[str] = None
    source: Optional[str] = None
    traceContext: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, raw: Dict[str, Any]) -> "EventMessage":
        """Build from a wire dict, dropping unknown fields (forward-compatible)."""
        known = {member.name for member in fields(cls)}
        return cls(**{key: value for key, value in raw.items() if key in known})


@dataclass
class EventBusConfig:
    source: str
    endpoint_url: Optional[str] = None
    default_timeout: float = 30.0

    def __post_init__(self):
        if self.endpoint_url is None:
            self.endpoint_url = os.getenv("NATS_URL", "nats://localhost:4222")


def build_event_message(
    payload: EventPayload, source: str, correlation_id: Optional[str] = None
) -> Dict[str, Any]:
    """Wrap a payload with the fields the bus layer owns (source, timestamp, ids)."""
    message = payload.to_dict()
    message.update(
        {
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "correlationId": correlation_id,
            "eventId": message.get("eventId") or str(uuid.uuid4()),
        }
    )
    return message


EventHandler = Callable[[EventMessage], Awaitable[None]]
