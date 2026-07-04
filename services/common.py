"""Shared plumbing: settings and the Core NATS bus. That's all the core demo needs."""

import os

from eventbus import EventBus, EventBusConfig

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")


async def get_bus(source: str) -> EventBus:
    """The Core NATS bus for this process. `source` labels who is publishing."""
    return await EventBus.get_instance(EventBusConfig(source=source, endpoint_url=NATS_URL))
