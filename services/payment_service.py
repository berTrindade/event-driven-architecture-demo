"""payment-service - a choreographed consumer.

PATTERN 1 (choreography): it reacts to orders.placed on its own. Nothing
orchestrates it; it decides what to do and emits its own event.
PATTERN 3 (idempotency): mark_processed() dedupes redelivered events.
PATTERN 4 (retries + DLQ): an item named "POISON" always fails, so after the
retries it lands in dead_letters - the demo's failure path.

Run: python -m services.payment_service
"""

import asyncio
import logging

from eventbus import EventPayload

from .common import get_bus, get_pool, mark_processed, process_with_dlq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("payment-service")

CONSUMER = "payment-service"


async def main():
    pool = await get_pool()
    bus = await get_bus(CONSUMER)

    async def handler(message):
        # Idempotent skip: if we've already handled this eventId, do nothing.
        if not await mark_processed(pool, CONSUMER, message.eventId):
            logger.info("skipping already-processed event %s", message.eventId)
            return

        data = message.data

        async def work():
            # Simulate a payment charge. A POISON item always declines - this is
            # the poison-message path that exercises retries + dead-letter.
            if data.get("item") == "POISON":
                raise RuntimeError("payment declined")
            logger.info("charged order %s", data.get("order_id"))
            await bus.send(
                EventPayload(
                    topic="payments.captured",
                    data={
                        "order_id": data.get("order_id"),
                        "amount_cents": data.get("amount_cents"),
                    },
                )
            )

        await process_with_dlq(pool, bus, CONSUMER, message, work)

    await bus.subscribe(CONSUMER, "orders.placed", handler)
    logger.info("payment-service listening on orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
