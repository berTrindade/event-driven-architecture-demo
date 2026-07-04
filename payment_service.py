"""payment-service - reacts to orders.placed, then emits its own fact.

Nothing tells it to run. It consumes orders.placed, does its bit, and publishes
payments.captured. That is choreography.

Run: python payment_service.py
"""

import asyncio
import logging

import bus

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payment-service")


async def on_order_placed(data):
    log.info("charging order %s", data["order_id"])
    await bus.publish(
        "payments.captured",
        {"order_id": data["order_id"], "amount_cents": data["amount_cents"]},
    )


async def main():
    await bus.start("payment-service")
    log.info("listening for orders.placed")
    await bus.consume("orders.placed", "payment-service", on_order_placed)


if __name__ == "__main__":
    asyncio.run(main())
