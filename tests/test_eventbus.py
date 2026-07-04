"""Offline self-checks for the wire format and subject derivation (no NATS needed)."""

from eventbus.types import (
    EventMessage,
    EventPayload,
    build_event_message,
    event_subject,
    request_subject,
)


def test_subjects():
    assert event_subject("orders.placed") == "event.orders.placed"
    assert request_subject("catalog.price") == "request.catalog.price"


def test_build_event_message_adds_envelope():
    payload = EventPayload(topic="orders.placed", data={"order_id": "abc"})
    message = build_event_message(payload, source="svc", correlation_id="corr-1")
    assert message["topic"] == "orders.placed"
    assert message["data"] == {"order_id": "abc"}
    assert message["source"] == "svc"
    assert message["correlationId"] == "corr-1"
    assert message["eventId"]
    assert message["timestamp"]


def test_from_dict_ignores_unknown_fields():
    message = EventMessage.from_dict(
        {"topic": "t", "data": {}, "unknown": "drop me", "source": "svc"}
    )
    assert message.topic == "t"
    assert message.source == "svc"
    assert not hasattr(message, "unknown")


if __name__ == "__main__":
    test_subjects()
    test_build_event_message_adds_envelope()
    test_from_dict_ignores_unknown_fields()
    print("ok")
