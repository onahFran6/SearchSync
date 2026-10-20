-- Stage 4: extend orders in place (per the implementation prompt's hard
-- constraint - no forked copy of the orders package) rather than creating
-- a second orders-like table.

-- The saga's state machine states ARE orders.status's lifecycle now: every
-- SagaTransitioned notification orders' consumer applies
-- (internal/orders/sagaconsumer.go) writes one of these values directly.
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check CHECK (status IN (
  'PENDING', 'STOCK_RESERVED', 'CONFIRMED', 'REJECTED',
  'COMPENSATING', 'CANCELLED', 'NEEDS_ATTENTION'
));

-- A saga needs something to reserve. This stage's "simple single-SKU
-- case" (per the implementation doc) means one SKU and one quantity per
-- order, not a line-items table. DEFAULT 'DEMO-001'/1 only matters for any
-- order rows created before this migration ran in a long-lived local
-- database; fresh checkouts never hit the default.
ALTER TABLE orders ADD COLUMN product_sku TEXT NOT NULL DEFAULT 'DEMO-001';
ALTER TABLE orders ADD COLUMN quantity INT NOT NULL DEFAULT 1 CHECK (quantity > 0);
ALTER TABLE orders ALTER COLUMN product_sku DROP DEFAULT;
ALTER TABLE orders ALTER COLUMN quantity DROP DEFAULT;

-- Append-only projection of every SagaTransitioned notification this
-- order's saga has produced, built the same way order_notifications was
-- in Stage 2 (consume an event, write a local projection, dedup by the
-- event's own id) - this is what GET /orders/:id/timeline reads. event_id
-- is the SagaTransitioned message's MessageID: the unique constraint is
-- the entire "redelivery doesn't duplicate a timeline row" guarantee, the
-- same structural dedup as processed_events rather than inferred logic.
CREATE TABLE saga_transitions (
  event_id UUID PRIMARY KEY,
  order_id UUID NOT NULL REFERENCES orders (id),
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  event_version INT NOT NULL,
  transitioned_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX saga_transitions_order_id_idx ON saga_transitions (order_id, event_version);
