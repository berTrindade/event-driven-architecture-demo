"""Offline self-checks for the projection reducer (no DB, no NATS needed)."""

from services.projection_logic import apply_event


def test_placed():
    assert apply_event({}, "orders.placed")["status"] == "PLACED"


def test_not_confirmed_with_only_payment():
    state = apply_event({}, "orders.placed")
    state = apply_event(state, "payments.captured")
    assert state["status"] == "PLACED"


def test_confirmed_once_both_present():
    state = apply_event({}, "orders.placed")
    state = apply_event(state, "payments.captured")
    state = apply_event(state, "inventory.reserved")
    assert state["status"] == "CONFIRMED"


def test_arrival_order_does_not_matter():
    # inventory before payment still confirms
    state = apply_event({}, "inventory.reserved")
    state = apply_event(state, "payments.captured")
    assert state["status"] == "CONFIRMED"


def test_apply_event_does_not_mutate_input():
    original = {"status": "PLACED"}
    apply_event(original, "payments.captured")
    assert original == {"status": "PLACED"}


if __name__ == "__main__":
    test_placed()
    test_not_confirmed_with_only_payment()
    test_confirmed_once_both_present()
    test_arrival_order_does_not_matter()
    test_apply_event_does_not_mutate_input()
    print("ok")
