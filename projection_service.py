"""projection-service - the read model (CQRS-lite), stored in PostgreSQL.

Consumes the three events (one queue bound to all three types) and folds each
into the order_status table using the pure apply_event() rules, then serves that
view over HTTP. It only reads events and builds state - it issues no commands.

Run: uvicorn projection_service:app --host 0.0.0.0 --port 8001
"""

import logging
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import bus
from projection_logic import apply_event

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("projection-service")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://demo:demo@localhost:5432/demo")
EVENT_TYPES = ["orders.placed", "payments.captured", "inventory.reserved"]

pool = None


async def on_event(event):
    order_id = event.data["order_id"]
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT status, payment, inventory FROM order_status WHERE order_id = $1 FOR UPDATE",
                order_id,
            )
            new_state = apply_event(dict(row) if row else {}, event.type)
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
    log.info("%s -> order %s is %s [corr=%s]", event.type, order_id, new_state.get("status"), event.correlation_id)


@asynccontextmanager
async def lifespan(app):
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    await bus.start("projection-service")
    await bus.consume("projection-service", EVENT_TYPES, on_event)
    log.info("projecting %s", EVENT_TYPES)
    yield
    await bus.stop()
    await pool.close()


app = FastAPI(title="projection-service", lifespan=lifespan)

# Let the browser dashboard (a different port) read the order status.
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    row = await pool.fetchrow(
        "SELECT order_id, status, payment, inventory, updated_at FROM order_status WHERE order_id = $1",
        order_id,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="order not in read model yet")
    return dict(row)


@app.get("/orders")
async def list_orders():
    rows = await pool.fetch(
        "SELECT order_id, status, payment, inventory, updated_at FROM order_status ORDER BY updated_at DESC"
    )
    return [dict(row) for row in rows]


@app.get("/health")
async def health():
    return {"ok": True}
