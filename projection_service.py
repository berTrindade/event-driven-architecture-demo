"""projection-service - the read model (CQRS-lite).

Consumes the three events (one queue bound to all three routing keys) and folds
each into a per-order status using the pure apply_event() rules, then serves
that view over HTTP. It only reads events and builds state - it issues no commands.

The store is an in-memory dict to keep the demo to one moving part. In
production it's a real database you could rebuild by replaying the events.

Run: uvicorn projection_service:app --host 0.0.0.0 --port 8001
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

import bus
from projection_logic import apply_event

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("projection-service")

TOPICS = ["orders.placed", "payments.captured", "inventory.reserved"]

# order_id -> status view, built purely from events.
orders = {}


async def on_event(topic, data):
    order_id = data["order_id"]
    orders[order_id] = {**apply_event(orders.get(order_id, {}), topic), "order_id": order_id}
    log.info("%s -> order %s is %s", topic, order_id, orders[order_id].get("status"))


@asynccontextmanager
async def lifespan(app):
    await bus.start("projection-service")
    await bus.consume("projection-service", TOPICS, on_event)
    log.info("projecting %s", TOPICS)
    yield
    await bus.stop()


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
