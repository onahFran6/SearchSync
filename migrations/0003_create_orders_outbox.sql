-- Orders table: authoritative in PostgreSQL, same integer-cents convention
-- as products.
CREATE TABLE orders (
  id UUID PRIMARY KEY,
  customer_id UUID NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('PENDING', 'CONFIRMED', 'CANCELLED')),
  total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Outbox for business events raised alongside an order write, inserted in
-- the same PostgreSQL transaction as the order row. Column names match the
-- Debezium Outbox Event Router's defaults (table.field.event.id=id,
-- table.field.event.key=aggregateid, route.by.field=aggregatetype) so the
-- connector config needs no field-name overrides. Rows are immutable after
-- insert; retention/cleanup is handled separately once delivery and WAL
-- retention guarantees are confirmed (see docs/evidence/stage-02).
CREATE TABLE outbox_events (
  id UUID PRIMARY KEY,
  aggregatetype TEXT NOT NULL,
  aggregateid TEXT NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Downstream consumer dedup: at-least-once delivery from Kafka is expected,
-- so the unique constraint on event_id (not inference from payload
-- contents) is what makes reprocessing a no-op.
CREATE TABLE processed_events (
  event_id UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Minimal notification projection built from OrderCreated events.
CREATE TABLE order_notifications (
  order_id UUID PRIMARY KEY,
  customer_id UUID NOT NULL,
  total_cents BIGINT NOT NULL CHECK (total_cents >= 0),
  notified_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium's initial snapshot reads outbox_events directly, same as
-- products; the replication role needs plain SELECT on it.
GRANT SELECT ON outbox_events TO searchsync_repl;

-- A second, dedicated publication for the outbox connector (separate from
-- searchsync_products_publication) so the two connectors' slots and
-- publications stay independent. Table ownership is required to create a
-- publication, hence the schema owner (not searchsync_repl) runs this.
CREATE PUBLICATION searchsync_outbox_publication FOR TABLE outbox_events;
