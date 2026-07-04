"""
Core NATS EventBus - event-driven boilerplate.

Mirrors the QuitBuddy AI System event bus (architecture by Mitsu Hadeishi):
simple, ephemeral Core NATS with queue groups for service-level load
balancing. No JetStream, no persistence, no durable consumers.

ARCHITECTURE PRINCIPLES
1. Service-level load balancing: service_name maps to a NATS queue group, so
   multiple copies of a service share the load and each event is handled once.
2. Ephemeral messaging: no persistence; events reach whoever is subscribed now.
3. Native request-response: Core NATS reply subjects handle correlation.
4. One connection per process: use EventBus.get_instance().
"""

import asyncio
import inspect
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

from .types import (
    EventBusConfig,
    EventHandler,
    EventMessage,
    EventPayload,
    build_event_message,
    event_subject,
    request_subject,
)


async def _maybe_await(result):
    """Allow handlers to be either sync or async."""
    if inspect.isawaitable(result):
        return await result
    return result


class EventBus:
    _instance: Optional["EventBus"] = None
    _instance_lock = asyncio.Lock()

    def __init__(self, config: EventBusConfig):
        if EventBus._instance is not None:
            raise RuntimeError(
                "Singleton violation: EventBus already exists. "
                "Use EventBus.get_instance() instead of constructing it directly."
            )
        self.config = config
        self.nc: Optional[Any] = None
        self._subscriptions: List[Any] = []
        self.logger = logging.getLogger(__name__)

    @classmethod
    async def get_instance(cls, config: Optional[EventBusConfig] = None) -> "EventBus":
        async with cls._instance_lock:
            if cls._instance is None:
                if config is None:
                    raise ValueError("config is required to create the EventBus")
                cls._instance = cls(config)
                await cls._instance.connect()
            return cls._instance

    @classmethod
    async def reset_instance(cls) -> None:
        async with cls._instance_lock:
            if cls._instance is not None:
                await cls._instance.close()
                cls._instance = None

    async def connect(self) -> None:
        if self.nc and not self.nc.is_closed:
            return
        import nats

        self.nc = await nats.connect(
            servers=[self.config.endpoint_url],
            name=self.config.source,
            reconnect_time_wait=2,
            max_reconnect_attempts=3,
            connect_timeout=10,
        )
        self.logger.info("Core NATS connected: %s", self.config.endpoint_url)

    async def close(self) -> None:
        for subscription in self._subscriptions:
            try:
                await subscription.unsubscribe()
            except Exception:
                self.logger.exception("Error unsubscribing")
        self._subscriptions.clear()
        if self.nc and not self.nc.is_closed:
            await self.nc.close()

    async def _ensure_connected(self) -> None:
        if not self.nc or self.nc.is_closed:
            await self.connect()

    def is_healthy(self) -> bool:
        return self.nc is not None and not self.nc.is_closed

    # ---- fire-and-forget --------------------------------------------------
    async def send(self, payload: EventPayload) -> None:
        await self._ensure_connected()
        message = build_event_message(payload, self.config.source)
        subject = event_subject(message["topic"])
        await self.nc.publish(subject, json.dumps(message).encode("utf-8"))
        self.logger.debug("sent %s", subject)

    async def publish(self, payload: EventPayload) -> None:
        """Alias for send()."""
        await self.send(payload)

    async def subscribe(
        self, service_name: str, topic_pattern: str, handler: EventHandler
    ) -> None:
        """Subscribe with load balancing: service_name becomes the queue group."""
        await self._ensure_connected()
        subject = event_subject(topic_pattern)

        async def on_message(msg):
            try:
                raw = json.loads(msg.data.decode("utf-8"))
                raw.setdefault("topic", topic_pattern)
                await _maybe_await(handler(EventMessage.from_dict(raw)))
            except Exception:
                self.logger.exception("handler error for %s", topic_pattern)

        subscription = await self.nc.subscribe(
            subject, queue=service_name, cb=on_message
        )
        self._subscriptions.append(subscription)
        self.logger.info("subscribed %s (queue=%s)", subject, service_name)

    # ---- request-response -------------------------------------------------
    async def request(
        self, payload: EventPayload, timeout: Optional[float] = None
    ) -> EventMessage:
        await self._ensure_connected()
        request_timeout = (
            timeout if timeout is not None else self.config.default_timeout
        )
        message = build_event_message(payload, self.config.source, str(uuid.uuid4()))
        subject = request_subject(message["topic"])
        try:
            response = await self.nc.request(
                subject, json.dumps(message).encode("utf-8"), timeout=request_timeout
            )
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"request timed out after {request_timeout}s for {payload.topic}"
            )
        return EventMessage.from_dict(json.loads(response.data.decode("utf-8")))

    async def on_request(
        self,
        topic_pattern: str,
        handler: Callable[
            [EventMessage], Union[Dict[str, Any], Awaitable[Dict[str, Any]]]
        ],
    ) -> None:
        """Register a request handler. Handler returns a dict {'topic', 'data'}."""
        await self._ensure_connected()
        subject = request_subject(topic_pattern)
        # All pods serving this request type share one queue group so a request
        # is answered exactly once.
        queue_group = f"request-handler-{topic_pattern}"

        async def on_request_message(msg):
            request_message = EventMessage.from_dict(
                json.loads(msg.data.decode("utf-8"))
            )
            correlation_id = request_message.correlationId or str(uuid.uuid4())
            try:
                result = await _maybe_await(handler(request_message)) or {}
                response = {
                    "source": self.config.source,
                    "correlationId": correlation_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "topic": result.get("topic", "response"),
                    "userId": request_message.userId,
                    "groupId": request_message.groupId,
                    "eventId": str(uuid.uuid4()),
                    "data": result.get("data", {}),
                }
            except Exception as exc:
                self.logger.exception("request handler error for %s", topic_pattern)
                response = {
                    "source": self.config.source,
                    "correlationId": correlation_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "topic": "error.error",
                    "eventId": str(uuid.uuid4()),
                    "data": {"error": str(exc)},
                }
            if msg.reply:
                await self.nc.publish(
                    msg.reply, json.dumps(response).encode("utf-8")
                )
            else:
                self.logger.warning("no reply subject for request %s", topic_pattern)

        subscription = await self.nc.subscribe(
            subject, queue=queue_group, cb=on_request_message
        )
        self._subscriptions.append(subscription)
        self.logger.info("request handler for %s (queue=%s)", subject, queue_group)
