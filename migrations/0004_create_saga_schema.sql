-- The saga orchestrator's own schema. Owned by searchsync_app
-- (the migration-running role) so it can CREATE PUBLICATION, same pattern
-- migrations/0003 used for outbox_events; searchsync_saga_app (created in
-- infrastructure/postgres/01_service_roles.sh) is granted only the narrow
-- privileges its application code actually needs.
CREATE SCHEMA saga;

-- One row per order's fulfilment saga. saga_id is deliberately the order's
-- own id, not a separately minted uuid - this stage runs exactly one saga
-- per order, so a second identity would only be a redundant mapping table.
-- version is the optimistic-concurrency guard every transition checks
-- (see internal/saga/repository.go's guarded UPDATE): a transition only
-- applies if it targets the version the row is actually at, so a stale or
-- duplicate trigger can never double-apply.
CREATE TABLE saga.sagas (
  order_id UUID PRIMARY KEY,
  state TEXT NOT NULL CHECK (state IN (
    'PENDING', 'STOCK_RESERVED', 'CONFIRMED', 'REJECTED',
    'COMPENSATING', 'CANCELLED', 'NEEDS_ATTENTION'
  )),
  version INT NOT NULL DEFAULT 0,
  sku TEXT NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
  -- Persisted deadlines the poller (internal/saga/poller.go) checks
  -- against now() - never an in-memory timer. NULL means "no deadline
  -- currently pending" (e.g. before STOCK_RESERVED, or after a terminal
  -- state).
  payment_deadline TIMESTAMPTZ,
  compensation_deadline TIMESTAMPTZ,
  compensation_attempts INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Incoming-message dedup: the same mechanism as processed_events, one
-- level up. A unique-violation here means "already applied this exact
-- message," handled as a no-op rather than an error.
CREATE TABLE saga.processed_messages (
  message_id UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outgoing commands/notifications, same shape as public.outbox_events
-- (matches the Debezium Outbox Event Router's defaults) so
-- infrastructure/debezium/saga-connector.json needs no field-name
-- overrides.
CREATE TABLE saga.outbox_events (
  id UUID PRIMARY KEY,
  aggregatetype TEXT NOT NULL,
  aggregateid TEXT NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

GRANT USAGE ON SCHEMA saga TO searchsync_saga_app;
GRANT SELECT, INSERT, UPDATE ON saga.sagas TO searchsync_saga_app;
GRANT SELECT, INSERT ON saga.processed_messages TO searchsync_saga_app;
GRANT SELECT, INSERT ON saga.outbox_events TO searchsync_saga_app;

-- Debezium's initial snapshot reads outbox_events directly, same as every
-- prior stage's outbox table.
GRANT USAGE ON SCHEMA saga TO searchsync_repl;
GRANT SELECT ON saga.outbox_events TO searchsync_repl;

CREATE PUBLICATION searchsync_saga_publication FOR TABLE saga.outbox_events;
