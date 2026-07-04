"""A tiny event bus over Kafka (via aiokafka): one producer, per-service consumers.

Kafka is the usual default for event-driven systems - durable, replayable, with
consumer groups for scaling. An event is just JSON on a topic named event.<name>.
A real setup adds schemas, partitioning keys, and tuning (see the README's
"Making it production-grade"); here we stay readable.
"""

import json
import os

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

_producer = None  # one Kafka producer per process


async def start(name: str) -> None:
    """Open this process's Kafka producer. Call once before publishing."""
    global _producer
    _producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP, client_id=name)
    await _producer.start()


async def stop() -> None:
    if _producer is not None:
        await _producer.stop()


async def publish(topic: str, data: dict) -> None:
    """Emit an event. The publisher doesn't know or care who consumes it."""
    event = {"topic": topic, "data": data}
    await _producer.send_and_wait(f"event.{topic}", json.dumps(event).encode())


async def consume(topic: str, group: str, handler) -> None:
    """Run forever: call handler(data) for each event on `topic`.

    `group` is the Kafka consumer group. A distinct group per service means every
    service sees every event (fan-out). Sharing one group would load-balance instead.
    """
    consumer = AIOKafkaConsumer(
        f"event.{topic}",
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    try:
        async for message in consumer:
            event = json.loads(message.value.decode())
            await handler(event["data"])
    finally:
        await consumer.stop()
