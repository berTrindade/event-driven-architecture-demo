# Event-driven architecture - demo

A tiny, runnable example of event-driven architecture. When you place an order, that fact is published as an **event**, and separate services react to it on their own. No service calls another directly. It runs on your laptop with Docker, no cloud.

```text
        place an order (HTTP)
you ──────────────────▶ order-service ──"orders.placed"──▶ RabbitMQ ─┐
                        (publishes, calls no one)                     │
              ┌───────────────────────────────────────────────────────┼──────────────┐
              ▼                             ▼                           ▼
      payment-service              inventory-service              projection-service
              │                             │                    (keeps an order-status
     "payments.captured"          "inventory.reserved"            view in Postgres)
              └──────────────┬──────────────┘                           │
                             └──────────────────────────────────▶  GET /orders
```

## The idea

- **Events are facts.** Something happened, named in the past tense: `orders.placed`.
- **The producer doesn't know who's listening.** order-service just publishes the event. It never calls payment or inventory.
- **Consumers react on their own.** Payment and inventory both handle the same event, independently. You can add or remove a consumer without touching anyone else.

That last point - services staying independent - is the whole reason to go event-driven.

## The pieces

- **RabbitMQ** - the broker every event flows through.
- **[order-service](order_service.py)** - takes an order over HTTP, publishes `orders.placed`.
- **[payment-service](payment_service.py)** - reacts, publishes `payments.captured`.
- **[inventory-service](inventory_service.py)** - reacts, publishes `inventory.reserved`.
- **[projection-service](projection_service.py)** - listens to all three and keeps an order-status view in Postgres, served over HTTP.

## Run it

Needs Docker.

```bash
make up      # start RabbitMQ, Postgres, and the four services
make demo    # place an order and watch it reach CONFIRMED
make logs    # watch the events flow between services
make down    # stop everything
```

- See the result: <http://localhost:8001/orders>
- Watch the broker (queues, messages): <http://localhost:15672> (guest / guest)

## Core concepts

Every event-driven system has these, however simple or complex:

1. **Event** - a record of something that happened.
2. **Producer** - publishes events, addresses no one in particular.
3. **Broker** - carries events from producers to consumers (RabbitMQ here).
4. **Consumer** - reacts to the events it cares about.

Plus two rules that make it "event-driven": producers and consumers are **decoupled** (they only know the broker, not each other), and it's **asynchronous** (publish and move on, don't wait for a reply).

## Going further

This demo stays minimal on purpose. A real system adds things like delivery guarantees, idempotent consumers, retries with a dead-letter queue, event schemas, and tracing - each a topic on its own.

## Tests

```bash
python test_logic.py    # checks the order-status rules, no broker needed
```
