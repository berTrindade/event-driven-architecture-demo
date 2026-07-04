"""PATTERN 5: read-model / projection logic (CQRS-lite).

Pure functions only - no DB, no NATS - so the folding rules can be unit-tested
offline (see tests/test_logic.py). projection_service.py wires this to Postgres.

The read model is built by folding independent events onto an order's status.
Because the folding is commutative for payment/inventory, the order the events
arrive in does not matter: once both are present the order is CONFIRMED.
"""


def apply_event(state: dict, topic: str) -> dict:
    """Return a NEW state dict with `topic` folded in. Never mutates `state`."""
    new_state = dict(state)

    if topic == "orders.placed":
        new_state["status"] = "PLACED"
    elif topic == "payments.captured":
        new_state["payment"] = "CAPTURED"
    elif topic == "inventory.reserved":
        new_state["inventory"] = "RESERVED"

    # Derived rule: an order is confirmed once payment and inventory both land,
    # regardless of which event arrived first.
    if new_state.get("payment") == "CAPTURED" and new_state.get("inventory") == "RESERVED":
        new_state["status"] = "CONFIRMED"

    return new_state
