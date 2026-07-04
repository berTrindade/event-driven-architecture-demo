# Event-driven architecture - reference demo

A runnable, laptop-sized example of a **proper** event-driven architecture, built to demo the patterns people usually get wrong. One order event fans out to independent services over NATS, backed by Postgres. No cloud account needed.

```text
                      ┌──────────────┐   HTTP POST /orders
                      │ order-service│◀────────────────────── you
                      └──────┬───────┘
              one DB tx:     │ orders + outbox row        (PATTERN 2: outbox)
                      ┌──────▼───────┐
                      │   Postgres   │
                      └──────┬───────┘
              polls unpublished rows
                      ┌──────▼───────┐
                      │ outbox-relay │──publish──▶ NATS ─┐
                      └──────────────┘                    │
                                                          │  event.orders.placed
                     ┌────────────────────────────────────┼───────────────────┐
                     ▼                                     ▼                    ▼
             ┌───────────────┐                   ┌─────────────────┐   ┌───────────────┐
             │payment-service│                   │inventory-service│   │  projection   │
             └───────┬───────┘                   └────────┬────────┘   │ (read model)  │
        payments.captured                     inventory.reserved       └───────┬───────┘
                     └──▶ notification-service          │                       │ GET /orders/{id}
                                                        └───────────────────────▶  order_status
```

## The 5 patterns (and where to look)

| # | Pattern | Where | Why it matters |
|---|---------|-------|----------------|
| 1 | **Choreography** | [payment_service.py](services/payment_service.py), [inventory_service.py](services/inventory_service.py) | Services react on their own and emit their own events. No orchestrator to become a bottleneck or single point of change. |
| 2 | **Transactional outbox** | [order_service.py](services/order_service.py) + [outbox_relay.py](services/outbox_relay.py) | The order row and the event commit in one DB transaction, so you never lose an event or publish one for an order that rolled back. The relay turns committed rows into events. |
| 3 | **Idempotent consumers** | `mark_processed` in [common.py](services/common.py) | Delivery is at-least-once. Dedupe by `eventId` so a redelivered event isn't charged twice. |
| 4 | **Retries + dead-letter** | `process_with_dlq` in [common.py](services/common.py) | A failing event is retried, then parked in `dead_letters` and announced on `dead-letter.<topic>` - visible and replayable, not silently dropped. |
| 5 | **Read model / projection** | [projection_logic.py](services/projection_logic.py) + [projection_service.py](services/projection_service.py) | Events are the source of truth. The query side folds them into a queryable `order_status` view (CQRS-lite), fully separate from the write side. |

## Run it

Needs Docker.

```bash
make up      # build + start NATS, Postgres, and all services
make demo    # place an order, watch it reach CONFIRMED, then trigger a dead-letter
make logs    # follow every service as events flow
make down    # stop and wipe volumes
```

Read model in the browser: <http://localhost:8001/orders>. NATS monitoring: <http://localhost:8222>.

## What to show your audience

1. `make demo` places a normal order. Tail `make logs` and point out the cascade: order-service commits, outbox-relay publishes, payment and inventory react **independently**, projection folds all three into `CONFIRMED`.
2. Show the outbox: the order-service code never imports NATS. It only writes a DB row. Reliability comes from the relay, not from a fragile "write DB then publish" dual write.
3. Trigger the poison order (`item: "POISON"`). It retries, dead-letters, and never confirms. Show the `dead_letters` table.
4. Point at `order_status`: it's derived purely from events, so you could rebuild it by replaying them.

## Teaching caveats (say these out loud)

- **Core NATS is ephemeral.** If a consumer is down when an event fires, it misses it. That keeps the demo simple, but for real durability use **NATS JetStream** or **AWS EventBridge + SQS**. The patterns above don't change.
- **The outbox guarantees publish, not end-to-end delivery.** Pair it with a durable broker for the full guarantee.
- **Idempotency here marks-then-works** (at-most-once per consumer). If you need at-least-once processing, mark only after the work succeeds and make the work itself idempotent.

## Layout

```text
eventbus/        reusable Core NATS client (pub/sub + request-response)
services/        order, outbox-relay, payment, inventory, notification, projection
db/schema.sql    orders, outbox, processed_events, order_status, dead_letters
tests/           offline self-checks (no broker needed)
docker-compose.yml / Dockerfile / Makefile / scripts/demo.sh
```

## Offline tests

```bash
python -m tests.test_logic      # projection reducer -> ok
python -m tests.test_eventbus   # wire format + subjects -> ok
```
