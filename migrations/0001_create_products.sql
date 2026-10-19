-- Products table: authoritative in PostgreSQL. Integer minor currency units
-- (price_cents) are used instead of floating-point money.
CREATE TABLE products (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  sku TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  category TEXT NOT NULL,
  price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Default REPLICA IDENTITY (primary key) is sufficient: the indexer only
-- needs the id to upsert/delete in Elasticsearch, not full prior row values.
-- Switch to REPLICA IDENTITY FULL only if a future stage needs before-images
-- of non-key columns, and note the resulting increase in WAL volume.

-- Debezium's initial snapshot reads this table directly (outside logical
-- decoding), so the replication role needs ordinary SELECT in addition to
-- the REPLICATION attribute granted at container init.
GRANT SELECT ON products TO searchsync_repl;

-- PostgreSQL only allows adding a table to a publication if the acting role
-- owns the table, so the schema owner (not the minimally privileged
-- replication role) creates it here. The connector references this
-- publication by name with publication.autocreate.mode=disabled.
CREATE PUBLICATION searchsync_products_publication FOR TABLE products;
