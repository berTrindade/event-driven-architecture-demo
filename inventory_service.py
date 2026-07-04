"""inventory-service - reacts to the same orders.placed event, independently.

Two services, one event, no coordination between them. It reserves stock and
emits inventory.reserved.

Run: python inventory_service.py
"""

import asyncio
import logging

import bus

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("inventory-service")


async def on_event(topic, data):
    log.info("reserving stock for order %s", data["order_id"])
    await bus.publish(
        "inventory.reserved",
        {"order_id": data["order_id"], "item": data["item"]},
    )


async def main():
    await bus.start("inventory-service")
    await bus.consume("inventory-service", ["orders.placed"], on_event)
    log.info("listening for orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
