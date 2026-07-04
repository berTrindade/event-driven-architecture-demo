-- Schema for the event-driven order-processing demo.
-- Loaded once by Postgres on first start via /docker-entrypoint-initdb.d.

-- Written by order-service. The source of truth for an order.
CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY,
    customer    TEXT NOT NULL,
    item        TEXT NOT NULL,
    amount_cents INT  NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- PATTERN 2: transactional outbox.
-- order-service writes the order row AND the outbox row in ONE transaction.
-- A separate relay (outbox_relay.py) publishes unpublished rows to NATS, so
-- the app never has to talk to NATS inside its request path.
CREATE TABLE IF NOT EXISTS outbox (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ NULL
);
-- Partial index: the relay only ever queries rows still waiting to publish.
CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
    ON outbox (id) WHERE published_at IS NULL;

-- PATTERN 3: idempotent consumers.
-- Each consumer records the eventIds it has handled. A second delivery of the
-- same eventId is a no-op (Core NATS / any broker can deliver more than once).
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     TEXT NOT NULL,
    event_id     UUID NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- PATTERN 5: read model / projection (CQRS-lite).
-- Built purely by folding events. Queried by the dashboard; never written to
-- by the command side directly.
CREATE TABLE IF NOT EXISTS order_status (
    order_id   UUID PRIMARY KEY,
    status     TEXT NOT NULL,
    payment    TEXT NULL,
    inventory  TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- PATTERN 4: dead-letter store.
-- When a consumer exhausts its retries the failing event is parked here for
-- inspection / replay instead of being lost or blocking the stream.
CREATE TABLE IF NOT EXISTS dead_letters (
    id         BIGSERIAL PRIMARY KEY,
    topic      TEXT NOT NULL,
    event_id   UUID NULL,
    payload    JSONB NOT NULL,
    error      TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
