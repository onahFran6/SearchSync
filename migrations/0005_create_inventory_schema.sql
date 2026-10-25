-- Inventory's own schema. See migrations/0004 for the pattern
-- this follows.
CREATE SCHEMA inventory;

-- On-hand count per SKU. The conditional UPDATE Inventory's reservation
-- handler runs (available = available - $qty WHERE sku = $1 AND available
-- >= $qty) is what makes concurrent reservations for the same SKU safe:
-- Postgres row-level locking on that UPDATE serializes concurrent writers
-- to the same row under any isolation level, including the default READ
-- COMMITTED, because the second writer's WHERE clause is re-evaluated
-- against the first writer's already-committed result once its lock
-- releases - no SERIALIZABLE or explicit advisory lock needed. See
-- internal/inventory/service.go.
CREATE TABLE inventory.stock (
  sku TEXT PRIMARY KEY,
  available INT NOT NULL CHECK (available >= 0)
);

-- One reservation per order, ever. The UNIQUE constraint on order_id (via
-- PRIMARY KEY) is the entire "never reserve stock twice for the same
-- order" guarantee: a duplicate ReserveInventory command (Experiment #4)
-- finds an existing row here and re-acknowledges without touching
-- inventory.stock a second time, rather than relying on inferred logic.
CREATE TABLE inventory.reservations (
  order_id UUID PRIMARY KEY,
  sku TEXT NOT NULL,
  quantity INT NOT NULL CHECK (quantity > 0),
  status TEXT NOT NULL CHECK (status IN ('RESERVED', 'RELEASED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ
);

-- Incoming-command dedup, same mechanism as saga.processed_messages.
CREATE TABLE inventory.processed_commands (
  message_id UUID PRIMARY KEY,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory.outbox_events (
  id UUID PRIMARY KEY,
  aggregatetype TEXT NOT NULL,
  aggregateid TEXT NOT NULL,
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Local-only failure injection for Experiment #6 (release transiently
-- fails, then recovers or exhausts compensation retries). A single row:
-- every ReleaseInventory handled while remaining > 0 returns a transient
-- error and decrements remaining, without touching stock or reservations.
-- POST /dev/inventory/release-failures sets remaining; see
-- internal/inventory/handler.go.
CREATE TABLE inventory.dev_release_failures (
  id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  remaining INT NOT NULL DEFAULT 0 CHECK (remaining >= 0)
);
INSERT INTO inventory.dev_release_failures (id, remaining) VALUES (1, 0);

GRANT USAGE ON SCHEMA inventory TO searchsync_inventory_app;
GRANT SELECT, UPDATE ON inventory.stock TO searchsync_inventory_app;
GRANT SELECT, INSERT, UPDATE ON inventory.reservations TO searchsync_inventory_app;
GRANT SELECT, INSERT ON inventory.processed_commands TO searchsync_inventory_app;
GRANT SELECT, INSERT ON inventory.outbox_events TO searchsync_inventory_app;
GRANT SELECT, UPDATE ON inventory.dev_release_failures TO searchsync_inventory_app;

GRANT USAGE ON SCHEMA inventory TO searchsync_repl;
GRANT SELECT ON inventory.outbox_events TO searchsync_repl;

CREATE PUBLICATION searchsync_inventory_publication FOR TABLE inventory.outbox_events;
