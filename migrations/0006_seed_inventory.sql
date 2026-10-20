-- Seed stock for the same SKUs migrations/0002 seeded into products, so a
-- Stage 4 order can reference a SKU that also exists in the Stage 1
-- product catalog. DEMO-003 starts at zero on purpose, to make Experiment
-- #2 (out-of-stock -> REJECTED) reproducible without first draining stock
-- by hand.
INSERT INTO inventory.stock (sku, available) VALUES
  ('DEMO-001', 5),
  ('DEMO-002', 10),
  ('DEMO-003', 0);
