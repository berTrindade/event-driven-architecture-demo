"""order-service - the entry point.

It publishes one fact, `orders.placed`, and calls no one. It doesn't know that
payment, inventory, or the projection exist. That decoupling is the whole idea:
add or remove a consumer without ever touching this service.

Run: uvicorn services.order_service:app --host 0.0.0.0 --port 8000
"""

import uuid

from fastapi import FastAPI
from pydantic import BaseModel

from eventbus import EventPayload

from .common import get_bus

app = FastAPI(title="order-service")


class NewOrder(BaseModel):
    customer: str
    item: str
    amount_cents: int


@app.post("/orders", status_code=201)
async def place_order(order: NewOrder):
    order_id = str(uuid.uuid4())
    bus = await get_bus("order-service")
    await bus.send(
        EventPayload(
            topic="orders.placed",
            eventId=order_id,
            data={
                "order_id": order_id,
                "customer": order.customer,
                "item": order.item,
                "amount_cents": order.amount_cents,
            },
        )
    )
    return {"order_id": order_id}


@app.get("/health")
async def health():
    return {"ok": True}
