"""A tiny event bus over RabbitMQ (aio-pika): a topic exchange, a queue per service.

Each event is a small envelope - type, id, time, correlation_id, data - as JSON.
Producers publish to the topic exchange with the event type as the routing key;
each service binds its own durable queue (fan-out). A message the handler can't
process is routed to a shared dead-letter queue instead of being lost or looping.
"""

import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import aio_pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = "events"
DLX_NAME = "events.dlx"  # dead-letter exchange
DEAD_LETTER_QUEUE = "dead-letter"

_connection = None
_channel = None
_exchange = None


@dataclass
class Event:
    type: str
    id: str
    time: str
    correlation_id: str
    data: dict


async def start(name: str) -> None:
    """Connect, open a channel, and declare the exchange + dead-letter queue."""
    global _connection, _channel, _exchange
    _connection = await aio_pika.connect_robust(
        RABBITMQ_URL, client_properties={"connection_name": name}
    )
    _channel = await _connection.channel()
    _exchange = await _channel.declare_exchange(
        EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
    )
    # Dead-letter exchange + queue: failed messages collect here, ready to inspect.
    await _channel.declare_exchange(DLX_NAME, aio_pika.ExchangeType.FANOUT, durable=True)
    dead_letters = await _channel.declare_queue(DEAD_LETTER_QUEUE, durable=True)
    await dead_letters.bind(DLX_NAME)


async def stop() -> None:
    if _connection is not None:
        await _connection.close()


async def publish(event_type: str, data: dict, correlation_id: str) -> None:
    """Emit an event. correlation_id ties every event in one flow together."""
    envelope = {
        "type": event_type,
        "id": str(uuid.uuid4()),
        "time": datetime.now(timezone.utc).isoformat(),
        "correlation_id": correlation_id,
        "data": data,
    }
    message = aio_pika.Message(
        body=json.dumps(envelope).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await _exchange.publish(message, routing_key=event_type)


async def consume(queue_name: str, event_types: list, handler) -> None:
    """Bind a durable queue to `event_types` and call handler(event) per message.

    The queue points its dead-lettering at the DLX, so if the handler raises we
    reject the message (requeue=False) and RabbitMQ moves it to the dead-letter
    queue rather than redelivering it forever.
    """
    queue = await _channel.declare_queue(
        queue_name,
        durable=True,
        arguments={"x-dead-letter-exchange": DLX_NAME},
    )
    for event_type in event_types:
        await queue.bind(EXCHANGE_NAME, routing_key=event_type)

    async def on_message(message):
        async with message.process(requeue=False):
            raw = json.loads(message.body.decode())
            await handler(Event(**raw))

    await queue.consume(on_message)
