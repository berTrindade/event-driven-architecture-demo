"""payment-service - reacts to orders.placed, then emits its own fact.

Nothing tells it to run. It consumes orders.placed, charges, and publishes
payments.captured - carrying the same correlation_id forward. That is choreography.

It is idempotent: delivery is at-least-once, so it records each event id it has
handled (in Postgres) and skips duplicates - a redelivered event never double-charges.

An item named "POISON" always fails, so you can watch a message land in the
dead-letter queue.

Run: python payment_service.py
"""

import asyncio
import logging
import os

import asyncpg

import bus

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("payment-service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")

pool = None


async def on_event(event):
    # Idempotency guard: claim this event id once. If it's already recorded,
    # this INSERT returns nothing and we skip - so a duplicate can't double-charge.
    first_time = await pool.fetchval(
        """
        INSERT INTO processed_events (consumer, event_id) VALUES ('payment-service', $1)
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        event.id,
    )
    if first_time is None:
        log.info("duplicate event %s, skipping", event.id)
        return

    data = event.data
    if data.get("item") == "POISON":
        raise RuntimeError("payment declined")  # -> dead-letter queue

    log.info("charging order %s [corr=%s]", data["order_id"], event.correlation_id)
    await bus.publish(
        "payments.captured",
        {"order_id": data["order_id"], "amount_cents": data["amount_cents"]},
        correlation_id=event.correlation_id,
    )


async def main():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await bus.start("payment-service")
    await bus.consume("payment-service", ["orders.placed"], on_event)
    log.info("listening for orders.placed")
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
