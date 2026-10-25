-- The simulated Payments service's own schema. Simulation only -
-- no real card data, ever (see docs/GO_ENGINEERING_STANDARDS.md and the
-- implementation doc's hard constraint).
CREATE SCHEMA payments;

-- One authorization attempt per order, ever. The UNIQUE constraint on
-- order_id (via PRIMARY KEY) is what makes a duplicate AuthorizePayment
-- command idempotent: a retry finds the existing row and re-emits the same
-- outcome instead of deciding a fresh (and potentially different, if the
-- dev mode changed in between) outcome for the same order.
CREATE TABLE payments.authorizations (
  order_id UUID PRIMARY KEY,
  status TEXT NOT NULL CHECK (status IN ('AUTHORIZED', 'DECLINED')),
  mode_used TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments.processed_commands (
  message_id UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payments.outbox_events (
  id UUID PRIMARY KEY,
  aggregatetype TEXT NOT NULL,
  aggregateid TEXT NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Deterministic outcome mode, persisted (not an in-memory flag) so a
-- restarted payments-sim process doesn't silently revert to the default
-- mid-experiment. A single row; POST /dev/payments/mode upserts it. In
-- "timeout" mode the handler commits processed_commands (the command was
-- received) but deliberately produces no outbox event at all - see
-- internal/payments/handler.go - simulating a payment processor that never
-- responds, which is what the saga's deadline poller exists to detect.
CREATE TABLE payments.mode_config (
  id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  mode TEXT NOT NULL DEFAULT 'success' CHECK (mode IN ('success', 'decline', 'timeout'))
);
INSERT INTO payments.mode_config (id, mode) VALUES (1, 'success');

GRANT USAGE ON SCHEMA payments TO searchsync_payments_app;
GRANT SELECT, INSERT ON payments.authorizations TO searchsync_payments_app;
GRANT SELECT, INSERT ON payments.processed_commands TO searchsync_payments_app;
GRANT SELECT, INSERT ON payments.outbox_events TO searchsync_payments_app;
GRANT SELECT, UPDATE ON payments.mode_config TO searchsync_payments_app;

GRANT USAGE ON SCHEMA payments TO searchsync_repl;
GRANT SELECT ON payments.outbox_events TO searchsync_repl;

CREATE PUBLICATION searchsync_payments_publication FOR TABLE payments.outbox_events;
