"""Shared plumbing for the demo services: settings, a Postgres pool, the bus,
the idempotency guard, and the retry + dead-letter wrapper.

Kept deliberately small - it is the only place that imports both asyncpg and
the eventbus package, so the pattern logic in each service stays readable.
"""

import asyncio
import json
import logging
import os

import asyncpg

from eventbus import EventBus, EventBusConfig, EventPayload

logger = logging.getLogger(__name__)

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    """Process-wide asyncpg pool, created on first use."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    return _pool


async def get_bus(source: str) -> EventBus:
    """The Core NATS bus for this process. `source` labels who is publishing."""
    return await EventBus.get_instance(EventBusConfig(source=source, endpoint_url=NATS_URL))


async def mark_processed(pool: asyncpg.Pool, consumer: str, event_id) -> bool:
    """PATTERN 3: idempotency guard.

    Returns True the first time this (consumer, event_id) pair is seen, False on
    any repeat. Consumers call this before doing work and bail out on False, so a
    redelivered event is handled at-most-once per consumer.
    """
    row = await pool.fetchval(
        """
        INSERT INTO processed_events (consumer, event_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        consumer,
        event_id,
    )
    return row is not None


async def process_with_dlq(pool, bus, consumer, message, work, attempts: int = 3):
    """PATTERN 4: retries + dead-letter.

    Run `work` (an async callable). On exception, retry up to `attempts` times
    with a short backoff. If it still fails, park the event in dead_letters and
    announce it on dead-letter.<topic> so the failure is visible and replayable
    rather than silently dropped.
    """
    for attempt in range(1, attempts + 1):
        try:
            await work()
            return
        except Exception as exc:
            logger.warning(
                "[%s] attempt %d/%d failed for %s: %s",
                consumer,
                attempt,
                attempts,
                message.topic,
                exc,
            )
            if attempt < attempts:
                await asyncio.sleep(0.2 * attempt)
                continue

            # Final failure: dead-letter it.
            await pool.execute(
                """
                INSERT INTO dead_letters (topic, event_id, payload, error)
                VALUES ($1, $2, $3, $4)
                """,
                message.topic,
                message.eventId,
                json.dumps(message.data),
                str(exc),
            )
            await bus.send(
                EventPayload(
                    topic=f"dead-letter.{message.topic}",
                    data={"original": message.data, "error": str(exc)},
                    eventId=message.eventId,
                )
            )
            logger.error(
                "[%s] dead-lettered %s (eventId=%s): %s",
                consumer,
                message.topic,
                message.eventId,
                exc,
            )
