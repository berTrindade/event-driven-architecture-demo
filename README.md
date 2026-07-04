# Event-driven architecture - reference demo

A runnable, laptop-sized example of a **proper** event-driven architecture, small enough to read top to bottom. One order event fans out to independent services over **Kafka** - the usual default for event-driven systems. The whole thing is Kafka plus four tiny Python services, self-hosted, **no cloud account**.

```text
        HTTP POST /orders
you ───────────────────▶ order-service ──publish orders.placed──▶ Kafka ─┐
                         (calls no one)                                   │
              ┌───────────────────────────────────────────────────────────┼──────────────┐
              ▼                             ▼                               ▼
      payment-service              inventory-service                   projection
              │                             │                         (read model)
     payments.captured            inventory.reserved                       │  GET /orders/{id}
              └──────────────┬──────────────┘                              ▼
                             └─────────────────────────────────────▶  order status
```

## Core concepts

Every event-driven system has these, no matter how simple or complex. Four things and two properties:

1. **Event** - an immutable record of something that already happened, in the past tense (`orders.placed`). The unit of communication.
2. **Producer** - emits an event and addresses no specific recipient.
3. **Channel / broker** - the indirection events travel through. Kafka here; could be RabbitMQ, NATS, or an in-process bus. Producers publish to the channel, never to a consumer.
4. **Consumer** - subscribes to and reacts to the events it cares about.
5. **Decoupling** *(a property, not a component)* - producers and consumers depend only on the channel and the event's shape, never on each other. Lose this and you just have RPC.
6. **Asynchrony** *(a property)* - emitting is fire-and-forget, not a call that waits for a return value.

In this demo: event = `orders.placed`, producer = [order_service.py](order_service.py), channel = Kafka via [bus.py](bus.py), consumers = payment + inventory + projection.

Everything else - schema versioning, ordering keys, idempotency, retries/DLQ, event sourcing, CQRS, sagas - is hardening or a variant you add when the problem needs it, not part of the core. See [Making it production-grade](#making-it-production-grade).

### Where this sits in the "event-driven" landscape

Martin Fowler's [*What do you mean by "Event-Driven"?*](https://martinfowler.com/articles/201701-event-driven.html) warns the term is overloaded across four patterns people conflate. To be precise about what this demo is (and isn't):

- **Event notification + event-carried state transfer** - `orders.placed` both signals the change and carries the order data, so consumers act without calling back.
- **CQRS-lite** - the projection is a read model kept separate from the write side.
- **Not event sourcing** - the Kafka log *could* be the source of truth, but here the read model is rebuilt from events rather than being the system's authority. Full event sourcing is a separate step.

## Why Kafka

Kafka is what people usually reach for in event-driven systems: durable and replayable (events are a log, not fire-and-forget), with consumer groups for independent scaling. It's self-hosted here in **KRaft mode** - a single container, no ZooKeeper, no cloud. Swap in **Redpanda** (Kafka-API compatible, even lighter) by changing one image if you prefer.

## The idea, in code

- **Events are facts, past tense** - `orders.placed`, not "create order".
- **The producer knows no consumers** - [order_service.py](order_service.py) publishes to a topic and imports none of the other services. Add or remove a consumer without touching it.
- **Consumers react independently** - payment and inventory each use their **own consumer group**, so both receive every `orders.placed` event (fan-out). Share a group and Kafka load-balances instead.
- **The read model is derived** - the projection folds events into a queryable status you could rebuild by replaying the topic.

The bus is deliberately tiny: [bus.py](bus.py) wraps `aiokafka` into `start` / `publish` / `consume`. An event on the wire is just `{"topic": ..., "data": ...}` on a Kafka topic named `event.<name>`.

## The four services

| Service | Does | Shows |
| --- | --- | --- |
| [order_service.py](order_service.py) | `POST /orders` → publishes `orders.placed` | producer that knows no consumers |
| [payment_service.py](payment_service.py) | reacts → publishes `payments.captured` | choreography |
| [inventory_service.py](inventory_service.py) | reacts → publishes `inventory.reserved` | independent fan-out |
| [projection_service.py](projection_service.py) | folds all three → order status view | read model / CQRS-lite |

Folding rules are pure and unit-tested: [projection_logic.py](projection_logic.py).

## Run it

Needs Docker. Kafka takes ~15s to become healthy on first `up`.

```bash
make up      # build + start Kafka and the four services
make demo    # place an order, watch it reach CONFIRMED
make logs    # follow every service as events flow
make down    # stop everything
```

Read model in the browser: <http://localhost:8001/orders>. Watch raw events on the log:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic event.orders.placed --from-beginning
```

## What to show your audience

1. `make demo` places one order. In `make logs`, point out that payment and inventory both wake up from the **same** event, independently, and the projection reaches `CONFIRMED` once both have reacted.
2. Open [order_service.py](order_service.py): it imports none of the other services and calls no one. It only publishes a fact. That's the decoupling.
3. Stop inventory-service, place another order, and note payment still runs while the projection stays partial. Start it again - because Kafka retains the log, the restarted consumer catches up. Consumers are independent, and events aren't lost.

## Making it production-grade

This demo keeps to the core on purpose. A real system layers on concerns that each deserve their own lesson (all self-hostable, no cloud account):

- **Ordering** - use a partition key (e.g. `order_id`) so a single order's events stay ordered.
- **Schemas** - wrap events in **CloudEvents** and enforce a **schema registry** (Avro/Protobuf) so producers and consumers can evolve independently.
- **Idempotent consumers** - Kafka is at-least-once, so dedupe by event id to avoid double-processing on redelivery.
- **Retries + dead-letter** - on repeated failure, route an event to a `dead-letter.<topic>` instead of blocking the partition.
- **Durable read model** - swap the in-memory dict for a database you can rebuild by replaying the topic.
- **Monitoring** - add a Kafka UI (e.g. `kafka-ui`, `redpanda-console`) and OpenTelemetry trace context across events.

## Layout

```text
bus.py                 tiny Kafka event bus (aiokafka): start / publish / consume
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
