# Event-driven architecture - reference demo

A runnable, laptop-sized example of a **proper** event-driven architecture, small enough to read top to bottom. One order event fans out to independent services over **RabbitMQ** - the classic message-broker backbone. The whole thing is RabbitMQ plus four tiny Python services, self-hosted, **no cloud account**.

```text
        HTTP POST /orders
you ───────────────────▶ order-service ──publish orders.placed──▶ RabbitMQ ─┐
                         (calls no one)                        (topic exchange)│
          ┌───────────────────────────────────────────────────────────────────┼──────────────┐
          ▼                             ▼                                       ▼
  payment-service              inventory-service                           projection
    (own queue)                   (own queue)                              (own queue)
          │                             │                                       │
 payments.captured            inventory.reserved                               │  GET /orders/{id}
          └──────────────┬──────────────┘                                      ▼
                         └─────────────────────────────────────────────▶  order status
```

## Core concepts

Every event-driven system has these, no matter how simple or complex. Four things and two properties:

1. **Event** - an immutable record of something that already happened, in the past tense (`orders.placed`). The unit of communication.
2. **Producer** - emits an event and addresses no specific recipient.
3. **Channel / broker** - the indirection events travel through. RabbitMQ here; could be Kafka, NATS, or an in-process bus. Producers publish to the channel, never to a consumer.
4. **Consumer** - subscribes to and reacts to the events it cares about.
5. **Decoupling** *(a property, not a component)* - producers and consumers depend only on the channel and the event's shape, never on each other. Lose this and you just have RPC.
6. **Asynchrony** *(a property)* - emitting is fire-and-forget, not a call that waits for a return value.

In this demo: event = `orders.placed`, producer = [order_service.py](order_service.py), channel = RabbitMQ via [bus.py](bus.py), consumers = payment + inventory + projection.

Everything else - schema versioning, idempotency, retries/dead-letter, event sourcing, CQRS, sagas - is hardening or a variant you add when the problem needs it, not part of the core. See [Making it production-grade](#making-it-production-grade).

### Where this sits in the "event-driven" landscape

Martin Fowler's [*What do you mean by "Event-Driven"?*](https://martinfowler.com/articles/201701-event-driven.html) warns the term is overloaded across four patterns people conflate. To be precise about what this demo is (and isn't):

- **Event notification + event-carried state transfer** - `orders.placed` both signals the change and carries the order data, so consumers act without calling back.
- **CQRS-lite** - the projection is a read model kept separate from the write side.
- **Not event sourcing** - RabbitMQ is a broker, not a log. Messages are consumed off queues, so there's no built-in "replay from the beginning". Full event sourcing needs a durable log (Kafka) or an event store.

## Why RabbitMQ

RabbitMQ is the classic, battle-tested message broker. A **topic exchange** routes each event by its routing key; **each service binds its own durable queue**, so all three services get their own copy of the events they care about (fan-out). It's self-hosted here as a single container, with a management UI to watch it - no cloud. One honest caveat: a broker holds messages in queues and hands them out, unlike a log - see the note above about replay.

## The idea, in code

- **Events are facts, past tense** - `orders.placed`, not "create order".
- **The producer knows no consumers** - [order_service.py](order_service.py) publishes to the exchange and imports none of the other services. Add or remove a consumer without touching it.
- **Consumers react independently** - payment and inventory each bind their **own queue** to `orders.placed`, so both receive every event (fan-out). Share one queue and RabbitMQ load-balances instead (competing consumers).
- **The read model is derived** - the projection folds events into a queryable status.

The bus is deliberately tiny: [bus.py](bus.py) wraps `aio-pika` into `start` / `publish` / `consume`. An event on the wire is just `{"topic": ..., "data": ...}`, published to the `events` topic exchange with the topic as the routing key.

## The four services

| Service | Does | Shows |
| --- | --- | --- |
| [order_service.py](order_service.py) | `POST /orders` → publishes `orders.placed` | producer that knows no consumers |
| [payment_service.py](payment_service.py) | reacts → publishes `payments.captured` | choreography |
| [inventory_service.py](inventory_service.py) | reacts → publishes `inventory.reserved` | independent fan-out |
| [projection_service.py](projection_service.py) | folds all three → order status view | read model / CQRS-lite |

Folding rules are pure and unit-tested: [projection_logic.py](projection_logic.py).

## Run it

Needs Docker. RabbitMQ takes ~10s to become healthy on first `up`.

```bash
make up      # build + start RabbitMQ and the four services
make demo    # place an order, watch it reach CONFIRMED
make logs    # follow every service as events flow
make down    # stop everything
```

Read model in the browser: <http://localhost:8001/orders>. Watch the broker itself (exchanges, queues, message rates) in the **management UI**: <http://localhost:15672> (guest / guest).

## What to show your audience

1. `make demo` places one order. In `make logs`, point out that payment and inventory both wake up from the **same** event, independently, and the projection reaches `CONFIRMED` once both have reacted.
2. Open [order_service.py](order_service.py): it imports none of the other services and calls no one. It only publishes a fact. That's the decoupling.
3. Stop inventory-service, place another order, and note payment still runs while the projection stays partial. Its durable queue keeps the message; when you start it again, it drains the queue and catches up. Consumers are independent, and in-flight messages aren't lost.

## Making it production-grade

This demo keeps to the core on purpose. A real system layers on concerns that each deserve their own lesson (all self-hostable, no cloud account):

- **Reliable publishing** - turn on **publisher confirms** so the producer knows the broker accepted the message (messages are already marked persistent here).
- **Dead-letter exchange (DLX)** - route messages that fail or expire to a dead-letter queue instead of dropping them.
- **Idempotent consumers** - delivery is at-least-once, so dedupe by event id to avoid double-processing on redelivery.
- **Schemas** - wrap events in **CloudEvents** and enforce a schema so producers and consumers evolve independently.
- **High availability** - use **quorum queues** across a RabbitMQ cluster.
- **Durable read model + observability** - swap the in-memory dict for a real database, and add OpenTelemetry trace context across events.

## Layout

```text
bus.py                 tiny RabbitMQ event bus (aio-pika): start / publish / consume
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
