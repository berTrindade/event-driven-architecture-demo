"""inventory-service - a choreographed consumer (PATTERN 1).

Reacts to orders.placed independently of payment-service - both fan out from
the same event with no orchestrator between them. Reserving stock always
succeeds here, so this is the happy path alongside payment's failure path.
PATTERN 3: mark_processed() makes redelivery a no-op.

Run: python -m services.inventory_service
"""

import asyncio
import logging

from eventbus import EventPayload

from .common import get_bus, get_pool, mark_processed, process_with_dlq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inventory-service")

CONSUMER = "inventory-service"


async def main():
    pool = await get_pool()
    bus = await get_bus(CONSUMER)

    async def handler(message):
        if not await mark_processed(pool, CONSUMER, message.eventId):
            logger.info("skipping already-processed event %s", message.eventId)
            return

        data = message.data

        async def work():
            logger.info("reserved stock for order %s", data.get("order_id"))
            await bus.send(
                EventPayload(
                    topic="inventory.reserved",
                    data={
                        "order_id": data.get("order_id"),
                        "item": data.get("item"),
                    },
                )
            )

        await process_with_dlq(pool, bus, CONSUMER, message, work)

    await bus.subscribe(CONSUMER, "orders.placed", handler)
    logger.info("inventory-service listening on orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
