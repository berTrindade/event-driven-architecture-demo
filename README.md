# Event-driven architecture - reference demo

A runnable, laptop-sized example of a **proper** event-driven architecture. One order event fans out to independent services over NATS. The whole thing is NATS plus four tiny Python services - no database, no cloud account.

```text
        HTTP POST /orders
you ───────────────────▶ order-service ──publish orders.placed──▶ NATS ─┐
                         (calls no one)                                  │
              ┌──────────────────────────────────────────────────────────┼──────────────┐
              ▼                             ▼                              ▼
      payment-service              inventory-service                  projection
              │                             │                        (read model)
     payments.captured            inventory.reserved                      │  GET /orders/{id}
              └──────────────┬──────────────┘                             ▼
                             └────────────────────────────────────▶  order status
```

## The idea

That's the whole lesson, and it's the part people get wrong:

- **Events are facts, in the past tense** - `orders.placed`, not "create order".
- **The producer doesn't know its consumers** - order-service publishes to a topic and calls no one. Add or remove a consumer without touching it.
- **Consumers react independently and asynchronously** - payment and inventory both handle the same event, with no orchestrator between them.

The projection then folds those events into a queryable order status, which shows the other half of the idea: **events are the source of truth**, and read models are derived from them.

## The four services

| Service | Does | Demonstrates |
| --- | --- | --- |
| [order-service](services/order_service.py) | `POST /orders` → publishes `orders.placed` | producer that knows no consumers |
| [payment-service](services/payment_service.py) | reacts → publishes `payments.captured` | choreography |
| [inventory-service](services/inventory_service.py) | reacts → publishes `inventory.reserved` | independent fan-out |
| [projection-service](services/projection_service.py) | folds all three → `order status` view | read model / CQRS-lite |

The folding rules are pure and unit-tested: [projection_logic.py](services/projection_logic.py).

## Run it

Needs Docker.

```bash
make up      # build + start NATS and the four services
make demo    # place an order, watch it reach CONFIRMED
make logs    # follow every service as events flow
make down    # stop everything
```

Read model in the browser: <http://localhost:8001/orders>. NATS monitoring: <http://localhost:8222>.

## What to show your audience

1. `make demo` places one order. In `make logs`, point out that payment and inventory both wake up from the **same** event, independently, and the projection reaches `CONFIRMED` once both have reacted.
2. Open [order_service.py](services/order_service.py): it imports no other service and calls no one. It only publishes a fact. That's the decoupling.
3. Kill inventory-service, place another order, and note payment still runs and the projection stays partial. Restart it and the flow completes for new orders. Consumers are independent.

## Making it production-grade

This demo keeps to the core idea on purpose. A real system layers on concerns that each deserve their own explanation:

- **Durable transport** - Core NATS is ephemeral, so a consumer that's down misses events. Use **NATS JetStream** or **AWS EventBridge + SQS** for persistence and redelivery.
- **Transactional outbox** - if the producer also writes its own database, commit the row and the event together so you never lose or orphan an event.
- **Idempotent consumers** - with at-least-once delivery, dedupe by event id so a redelivered event isn't processed twice.
- **Retries + dead-letter** - retry a failing event, then park it somewhere visible instead of dropping it.
- **Durable read model** - swap the in-memory dict for a database you can rebuild by replaying events.

## Layout

```text
eventbus/        reusable Core NATS client (pub/sub + request-response)
services/        order, payment, inventory, projection + the pure projection reducer
tests/           offline self-checks (no broker needed)
docker-compose.yml / Dockerfile / Makefile / scripts/demo.sh
```

## Offline tests

```bash
python -m tests.test_logic      # projection reducer -> ok
python -m tests.test_eventbus   # wire format + subjects -> ok
```
