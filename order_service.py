"""order-service - publishes one fact and calls no one.

Placing an order emits orders.placed. This service imports none of the other
services and doesn't know they exist. That decoupling is the whole point.

It mints a fresh correlation_id per order - the id that ties every event in that
order's flow together, so you can trace one order across all the services.

Run: uvicorn order_service:app --host 0.0.0.0 --port 8000
"""

import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import bus


@asynccontextmanager
async def lifespan(app):
    await bus.start("order-service")
    yield
    await bus.stop()


app = FastAPI(title="order-service", lifespan=lifespan)

# Let the browser dashboard (a different port) place test orders.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class NewOrder(BaseModel):
    customer: str
    item: str
    amount_cents: int


@app.post("/orders", status_code=201)
async def place_order(order: NewOrder):
    order_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())
    await bus.publish(
        "orders.placed",
        {
            "order_id": order_id,
            "customer": order.customer,
            "item": order.item,
            "amount_cents": order.amount_cents,
        },
        correlation_id=correlation_id,
    )
    return {"order_id": order_id, "correlation_id": correlation_id}


@app.get("/health")
async def health():
    return {"ok": True}
