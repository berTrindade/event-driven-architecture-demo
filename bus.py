"""A tiny event bus over RabbitMQ (via aio-pika): a topic exchange, a queue per service.

RabbitMQ is the classic message-broker backbone. Producers publish to a topic
exchange with a routing key; each service binds its own durable queue, so every
service gets its own copy of the events it cares about (fan-out). An event is
just JSON. See the README's "Making it production-grade" for the hardening knobs.
"""

import json
import os

import aio_pika

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
EXCHANGE_NAME = "events"

_connection = None
_channel = None
_exchange = None


async def start(name: str) -> None:
    """Connect, open a channel, and declare the shared topic exchange."""
    global _connection, _channel, _exchange
    _connection = await aio_pika.connect_robust(
        RABBITMQ_URL, client_properties={"connection_name": name}
    )
    _channel = await _connection.channel()
    _exchange = await _channel.declare_exchange(
        EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC, durable=True
    )


async def stop() -> None:
    if _connection is not None:
        await _connection.close()


async def publish(topic: str, data: dict) -> None:
    """Emit an event. The publisher routes by topic and knows no consumers."""
    event = {"topic": topic, "data": data}
    message = aio_pika.Message(
        body=json.dumps(event).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )
    await _exchange.publish(message, routing_key=topic)


async def consume(queue_name: str, topics: list, handler) -> None:
    """Bind a durable queue to `topics` and call handler(topic, data) per message.

    A distinct queue per service means each service gets its own copy of every
    matching event (fan-out). Sharing one queue would load-balance instead.
    """
    queue = await _channel.declare_queue(queue_name, durable=True)
    for topic in topics:
        await queue.bind(EXCHANGE_NAME, routing_key=topic)

    async def on_message(message):
        async with message.process():
            event = json.loads(message.body.decode())
            await handler(message.routing_key, event["data"])

    await queue.consume(on_message)
