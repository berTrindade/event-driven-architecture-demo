-- Read model: one row per order, built purely from events by projection-service.
CREATE TABLE IF NOT EXISTS order_status (
    order_id   TEXT PRIMARY KEY,
    status     TEXT,
    payment    TEXT,
    inventory  TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency ledger: each consumer records the event ids it has already handled,
-- so a redelivered event is processed at most once (used by payment-service).
CREATE TABLE IF NOT EXISTS processed_events (
    consumer TEXT NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
