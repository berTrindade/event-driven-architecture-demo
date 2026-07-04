"""outbox-relay - the other half of PATTERN 2 (transactional outbox).

Polls the outbox for unpublished rows, publishes each to NATS, then marks it
published. This is what makes publishing reliable: order-service only had to
commit a DB row, and this relay turns committed rows into events at-least-once.

It is a separate process on purpose - you can scale it independently, and
FOR UPDATE SKIP LOCKED lets several relays run without publishing a row twice.

Run: python -m services.outbox_relay
"""

import asyncio
import json
import logging

from eventbus import EventPayload

from .common import get_bus, get_pool

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("outbox-relay")

POLL_SECONDS = 0.5


async def relay_once(pool, bus) -> int:
    """Publish one batch of pending outbox rows. Returns how many were sent."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT id, topic, payload
                FROM outbox
                WHERE published_at IS NULL
                ORDER BY id
                LIMIT 100
                FOR UPDATE SKIP LOCKED
                """
            )
            for row in rows:
                payload = json.loads(row["payload"])
                await bus.send(
                    EventPayload(
                        topic=row["topic"],
                        data=payload["data"],
                        eventId=payload["eventId"],
                    )
                )
                await conn.execute(
                    "UPDATE outbox SET published_at = now() WHERE id = $1",
                    row["id"],
                )
            return len(rows)


async def main():
    pool = await get_pool()
    bus = await get_bus("outbox-relay")
    logger.info("outbox-relay started, polling every %ss", POLL_SECONDS)
    while True:
        try:
            sent = await relay_once(pool, bus)
            if sent:
                logger.info("published %d event(s)", sent)
        except Exception:
            logger.exception("relay loop error")
        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
