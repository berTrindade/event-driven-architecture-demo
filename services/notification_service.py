"""notification-service - a downstream choreographed consumer (PATTERN 1).

Reacts to payments.captured, not orders.placed, showing an event chain:
orders.placed -> payments.captured -> (this) send confirmation email. It emits
no further event; it is a leaf. PATTERN 3: idempotent via mark_processed().

Run: python -m services.notification_service
"""

import asyncio
import logging

from .common import get_bus, get_pool, mark_processed, process_with_dlq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification-service")

CONSUMER = "notification-service"


async def main():
    pool = await get_pool()
    bus = await get_bus(CONSUMER)

    async def handler(message):
        if not await mark_processed(pool, CONSUMER, message.eventId):
            logger.info("skipping already-processed event %s", message.eventId)
            return

        order_id = message.data.get("order_id")

        async def work():
            # A real service would send an email/SMS here. The payment event
            # carries no customer, so we just key the notification on order_id.
            logger.info("email sent for order %s", order_id)

        await process_with_dlq(pool, bus, CONSUMER, message, work)

    await bus.subscribe(CONSUMER, "payments.captured", handler)
    logger.info("notification-service listening on payments.captured")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
