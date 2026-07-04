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


async def on_event(topic, data):
    log.info("charging order %s", data["order_id"])
    await bus.publish(
        "payments.captured",
        {"order_id": data["order_id"], "amount_cents": data["amount_cents"]},
    )


async def main():
    await bus.start("payment-service")
    await bus.consume("payment-service", ["orders.placed"], on_event)
    log.info("listening for orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
