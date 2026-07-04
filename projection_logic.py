"""Read-model logic (CQRS-lite): fold events onto an order's status.

Pure functions - no bus, no store - so the rules can be unit-tested offline
(see test_logic.py). projection_service.py wires this to the in-memory store.
"""


def apply_event(state: dict, topic: str) -> dict:
    """Return a NEW state with `topic` folded in. Never mutates `state`."""
    new_state = dict(state)

    if topic == "orders.placed":
        new_state["status"] = "PLACED"
    elif topic == "payments.captured":
        new_state["payment"] = "CAPTURED"
    elif topic == "inventory.reserved":
        new_state["inventory"] = "RESERVED"

    # Once payment and inventory both arrive, the order is confirmed - no matter
    # which one came first.
    if new_state.get("payment") == "CAPTURED" and new_state.get("inventory") == "RESERVED":
        new_state["status"] = "CONFIRMED"

    return new_state
