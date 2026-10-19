-- Deterministic seed data used by the Stage 1 experiments (snapshot,
-- out-of-band mutation, etc). SKUs are stable identifiers across reruns.
INSERT INTO products (sku, name, description, category, price_cents) VALUES
  ('DEMO-001', 'MacBook Air 13"', 'Apple MacBook Air, M-series', 'laptops', 99900),
  ('DEMO-002', 'Mechanical Keyboard', '75% hot-swappable mechanical keyboard', 'accessories', 8900),
  ('DEMO-003', 'USB-C Dock', '10-in-1 USB-C docking station', 'accessories', 5400);
