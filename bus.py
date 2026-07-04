"""A tiny event bus over NATS: connect, publish, subscribe. That's all the demo needs.

An event on the wire is just JSON on a subject named `event.<topic>`. A real
system would add reconnection, an event id and timestamp, tracing, and delivery
guarantees (see the README's "Making it production-grade"). Here we stay readable.
"""

import json
import os

import nats

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")

_nc = None  # one NATS connection per process


async def connect(name: str) -> None:
    """Open this process's connection to NATS. `name` shows up in NATS monitoring."""
    global _nc
    _nc = await nats.connect(NATS_URL, name=name)


async def publish(topic: str, data: dict) -> None:
    """Emit an event. The publisher doesn't know or care who is listening."""
    event = {"topic": topic, "data": data}
    await _nc.publish(f"event.{topic}", json.dumps(event).encode())


async def subscribe(topic: str, handler) -> None:
    """Call handler(data) for every event published on `topic`."""

    async def on_message(msg):
        event = json.loads(msg.data.decode())
        await handler(event["data"])

    await _nc.subscribe(f"event.{topic}", cb=on_message)
