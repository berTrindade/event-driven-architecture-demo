"""payment-service - a choreographed consumer.

It reacts to orders.placed on its own and emits its own fact, payments.captured.
Nothing orchestrates it. That is choreography: each service owns its decision.

Run: python -m services.payment_service
"""

import asyncio
import logging

from eventbus import EventPayload

from .common import get_bus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("payment-service")


async def main():
    bus = await get_bus("payment-service")

    async def handler(message):
        data = message.data
        logger.info("charged order %s", data.get("order_id"))
        await bus.send(
            EventPayload(
                topic="payments.captured",
                data={"order_id": data.get("order_id"), "amount_cents": data.get("amount_cents")},
            )
        )

    await bus.subscribe("payment-service", "orders.placed", handler)
    logger.info("listening on orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
