-- Carry W3C traceparent through the outbox -> Debezium -> Kafka
-- path. Nullable because not every write happens inside a traced request
-- (e.g. a poller-driven compensation command has no incoming HTTP span),
-- and because this column must stay backward compatible with any row
-- written before this migration ran.
--
-- The four tables below are every outbox_events table in this database
-- (public, saga, inventory, payments schemas) - the same four connectors
-- that already route their outbox rows through Debezium's EventRouter SMT.
-- The raw products.outbox_events table does not exist (the products CDC
-- path reads the products table directly, no outbox wrapper), so there is
-- nothing to add a column to there - that path's tracing gap is
-- deliberate and documented, not an oversight.

ALTER TABLE outbox_events ADD COLUMN trace_parent TEXT;
ALTER TABLE saga.outbox_events ADD COLUMN trace_parent TEXT;
ALTER TABLE inventory.outbox_events ADD COLUMN trace_parent TEXT;
ALTER TABLE payments.outbox_events ADD COLUMN trace_parent TEXT;
