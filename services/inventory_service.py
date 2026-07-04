"""inventory-service - another choreographed consumer.

It reacts to the same orders.placed event, independently of payment-service,
and emits inventory.reserved. Two services, one event, no coordination.

Run: python -m services.inventory_service
"""

import asyncio
import logging

from eventbus import EventPayload

from .common import get_bus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory-service")


async def main():
    bus = await get_bus("inventory-service")

    async def handler(message):
        data = message.data
        logger.info("reserved stock for order %s", data.get("order_id"))
        await bus.send(
            EventPayload(
                topic="inventory.reserved",
                data={"order_id": data.get("order_id"), "item": data.get("item")},
            )
        )

    await bus.subscribe("inventory-service", "orders.placed", handler)
    logger.info("listening on orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
