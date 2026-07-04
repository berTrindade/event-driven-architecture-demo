-- Read model: one row per order, built purely from events by projection-service.
CREATE TABLE IF NOT EXISTS order_status (
    order_id   TEXT PRIMARY KEY,
    status     TEXT,
    payment    TEXT,
    inventory  TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
