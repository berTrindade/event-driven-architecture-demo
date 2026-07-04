# Event-driven architecture - reference demo

A runnable, laptop-sized example of a **proper** event-driven architecture, small enough to read top to bottom. One order event fans out to independent services over NATS. The whole thing is NATS plus four tiny Python services - no database, no cloud account.

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

## Core concepts

Every event-driven system has these, no matter how simple or complex. Four things and two properties:

**Things**

1. **Event** - an immutable record of something that already happened, in the past tense (`orders.placed`). The unit of communication.
2. **Producer** - emits an event and addresses no specific recipient.
3. **Channel / broker** - the indirection events travel through. NATS here; could be Kafka, SNS/SQS, or an in-process bus. Producers publish to the channel, never to a consumer.
4. **Consumer** - subscribes to and reacts to the events it cares about.

**Properties**

5. **Decoupling** - producers and consumers depend only on the channel and the event's shape, never on each other. Lose this and you just have RPC.
6. **Asynchrony** - emitting is fire-and-forget, not a call that waits for a return value.

In this demo: event = `orders.placed`, producer = [order_service.py](order_service.py), channel = NATS via [bus.py](bus.py), consumers = payment + inventory + projection.

Everything else - persistence, ordering, delivery guarantees, idempotency, retries/DLQ, schema versioning, event sourcing, CQRS, sagas - is hardening or a variant you add when the problem needs it, not part of the core. See [Making it production-grade](#making-it-production-grade).

### Where this sits in the "event-driven" landscape

Martin Fowler's [*What do you mean by "Event-Driven"?*](https://martinfowler.com/articles/201701-event-driven.html) warns the term is overloaded across four patterns people conflate. To be precise about what this demo is (and isn't):

- **Event notification + event-carried state transfer** - `orders.placed` both signals the change and carries the order data, so consumers act without calling back.
- **CQRS-lite** - the projection is a read model kept separate from the write side.
- **Not event sourcing** - the event log isn't the source of truth here. That's a separate pattern you'd add for full replay/audit.

## The idea, in code

- **Events are facts, past tense** - `orders.placed`, not "create order".
- **The producer knows no consumers** - [order_service.py](order_service.py) publishes to a topic and imports none of the other services. Add or remove a consumer without touching it.
- **Consumers react independently** - payment and inventory both handle the same event, with no orchestrator between them.
- **The read model is derived** - the projection folds events into a queryable status you could rebuild by replaying them.

The bus is deliberately tiny: [bus.py](bus.py) is about 20 lines - `connect`, `publish`, `subscribe`. An event on the wire is just `{"topic": ..., "data": ...}`.

## The four services

| Service | Does | Shows |
| --- | --- | --- |
| [order_service.py](order_service.py) | `POST /orders` → publishes `orders.placed` | producer that knows no consumers |
| [payment_service.py](payment_service.py) | reacts → publishes `payments.captured` | choreography |
| [inventory_service.py](inventory_service.py) | reacts → publishes `inventory.reserved` | independent fan-out |
| [projection_service.py](projection_service.py) | folds all three → order status view | read model / CQRS-lite |

Folding rules are pure and unit-tested: [projection_logic.py](projection_logic.py).

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
2. Open [order_service.py](order_service.py): it imports none of the other services and calls no one. It only publishes a fact. That's the decoupling.
3. Stop inventory-service, place another order, and note payment still runs while the projection stays partial. Start it again and new orders complete. Consumers are independent.

## Making it production-grade

This demo keeps to the core on purpose. A real system layers on concerns that each deserve their own lesson:

- **Durable transport** - Core NATS is ephemeral, so a consumer that's down misses events. Use **NATS JetStream** or **AWS EventBridge + SQS** for persistence and redelivery.
- **A real client** - `bus.py` is bare. Production clients add reconnection, an event id + timestamp, request-response, and tracing.
- **Idempotent consumers** - with at-least-once delivery, dedupe by event id so a redelivered event isn't processed twice.
- **Retries + dead-letter** - retry a failing event, then park it somewhere visible instead of dropping it.
- **Durable read model** - swap the in-memory dict for a database you can rebuild by replaying events.

## Layout

```text
bus.py                 tiny NATS event bus: connect / publish / subscribe
order_service.py       publishes orders.placed
payment_service.py     reacts, publishes payments.captured
inventory_service.py   reacts, publishes inventory.reserved
projection_service.py  builds the read model, serves it over HTTP
projection_logic.py    pure folding rules (unit-tested)
test_logic.py          offline self-check (no broker needed)
docker-compose.yml / Dockerfile / Makefile / scripts/demo.sh
```

## Offline test

```bash
python test_logic.py    # projection reducer -> ok
```
