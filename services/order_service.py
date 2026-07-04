"""order-service - the command side (write model).

PATTERN 2: transactional outbox.
Placing an order writes the `orders` row AND an `outbox` row in ONE Postgres
transaction. This service never touches NATS - that is the whole point. If the
process dies right after commit, the event is still safely in the outbox and
outbox_relay.py will publish it. If the transaction rolls back, neither the
order nor the event exists. No dual-write, no lost events.
"""

import json
import uuid

from fastapi import FastAPI
from pydantic import BaseModel

from .common import get_pool

app = FastAPI(title="order-service")


class NewOrder(BaseModel):
    customer: str
    item: str
    amount_cents: int


@app.post("/orders", status_code=201)
async def place_order(order: NewOrder):
    order_id = uuid.uuid4()
    payload = {
        "topic": "orders.placed",
        "eventId": str(order_id),
        "data": {
            "order_id": str(order_id),
            "customer": order.customer,
            "item": order.item,
            "amount_cents": order.amount_cents,
        },
    }

    pool = await get_pool()
    async with pool.acquire() as conn:
        # One transaction: the order and its outbox row commit together or not
        # at all. This is the transactional-outbox guarantee.
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO orders (id, customer, item, amount_cents) VALUES ($1, $2, $3, $4)",
                order_id,
                order.customer,
                order.item,
                order.amount_cents,
            )
            await conn.execute(
                "INSERT INTO outbox (topic, payload) VALUES ($1, $2)",
                "orders.placed",
                # asyncpg needs an explicit JSON encode for a jsonb column.
                json.dumps(payload),
            )

    return {"order_id": str(order_id)}


@app.get("/health")
async def health():
    return {"ok": True}
