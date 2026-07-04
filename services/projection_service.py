"""projection-service - the query side / read model (PATTERN 5, CQRS-lite).

Subscribes to the three domain events and folds each into the order_status
table using the pure apply_event() rules. It never issues commands; it only
builds a queryable view. The HTTP API reads that view - so writes (order-service)
and reads (here) are fully separated.

Run: uvicorn services.projection_service:app --host 0.0.0.0 --port 8001
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .common import get_bus, get_pool
from .projection_logic import apply_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("projection-service")

CONSUMER = "projection-service"
PROJECTED_TOPICS = ["orders.placed", "payments.captured", "inventory.reserved"]


async def project(pool, order_id, topic):
    """Fold one event into the read model.

    Read-modify-write inside a single transaction with a row lock, so two events
    for the same order (e.g. payment and inventory arriving together) can't clobber
    each other. apply_event() decides the new state.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT status, payment, inventory FROM order_status WHERE order_id = $1 FOR UPDATE",
                order_id,
            )
            current = dict(row) if row else {}
            new_state = apply_event(current, topic)
            await conn.execute(
                """
                INSERT INTO order_status (order_id, status, payment, inventory, updated_at)
                VALUES ($1, $2, $3, $4, now())
                ON CONFLICT (order_id) DO UPDATE
                SET status = EXCLUDED.status,
                    payment = EXCLUDED.payment,
                    inventory = EXCLUDED.inventory,
                    updated_at = now()
                """,
                order_id,
                new_state.get("status"),
                new_state.get("payment"),
                new_state.get("inventory"),
            )
    logger.info("projected %s onto order %s -> %s", topic, order_id, new_state.get("status"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await get_pool()
    bus = await get_bus(CONSUMER)

    def make_handler(topic):
        async def handler(message):
            order_id = message.data.get("order_id")
            if order_id:
                await project(pool, order_id, topic)

        return handler

    for topic in PROJECTED_TOPICS:
        await bus.subscribe(CONSUMER, topic, make_handler(topic))
    logger.info("projection-service subscribed to %s", PROJECTED_TOPICS)
    yield


app = FastAPI(title="projection-service", lifespan=lifespan)


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT order_id, status, payment, inventory, updated_at FROM order_status WHERE order_id = $1",
        order_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="order not found in read model")
    return dict(row)


@app.get("/orders")
async def list_orders():
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT order_id, status, payment, inventory, updated_at FROM order_status ORDER BY updated_at DESC"
    )
    return [dict(row) for row in rows]


@app.get("/health")
async def health():
    return {"ok": True}
