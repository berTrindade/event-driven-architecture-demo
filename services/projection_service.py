"""projection-service - the read model (CQRS-lite).

Subscribes to the three domain events and folds each into a per-order status
using the pure apply_event() rules, then serves that view over HTTP. It issues
no commands - it only derives queryable state from events.

The store is an in-memory dict to keep the demo to one moving part. In
production this is a real database you can rebuild by replaying the events.

Run: uvicorn services.projection_service:app --host 0.0.0.0 --port 8001
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .common import get_bus
from .projection_logic import apply_event

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("projection-service")

CONSUMER = "projection-service"
TOPICS = ["orders.placed", "payments.captured", "inventory.reserved"]

# order_id -> status view, built purely from events.
orders: dict[str, dict] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    bus = await get_bus(CONSUMER)

    def make_handler(topic):
        async def handler(message):
            order_id = message.data.get("order_id")
            if not order_id:
                return
            # read-modify-write with no await in between, so it's atomic on the loop
            orders[order_id] = {**apply_event(orders.get(order_id, {}), topic), "order_id": order_id}
            logger.info("projected %s onto %s -> %s", topic, order_id, orders[order_id].get("status"))

        return handler

    for topic in TOPICS:
        await bus.subscribe(CONSUMER, topic, make_handler(topic))
    logger.info("subscribed to %s", TOPICS)
    yield


app = FastAPI(title="projection-service", lifespan=lifespan)


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    if order_id not in orders:
        raise HTTPException(status_code=404, detail="order not in read model yet")
    return orders[order_id]


@app.get("/orders")
async def list_orders():
    return list(orders.values())


@app.get("/health")
async def health():
    return {"ok": True}
