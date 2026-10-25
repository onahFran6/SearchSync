#!/usr/bin/env bash
# Generates continuous, varied traffic against the live SearchSync stack so
# the Grafana dashboard ("SearchSync Overview",
# http://localhost:3000) shows real, moving panels instead of flat zeros -
# there are no real users on this stack, so this stands in for them.
#
# Two things run in parallel until you Ctrl-C:
#   1. Product create/update traffic via cmd/loadgen, looped (loadgen itself
#      is one-shot) - drives events_processed_total, consumer lag, event
#      processing duration, and search-visibility latency.
#   2. Order traffic cycling through every saga terminal outcome - CONFIRMED
#      (payments mode "success"), CANCELLED (mode "decline", stock released),
#      REJECTED (DEMO-003 seeds at zero stock), and periodically
#      NEEDS_ATTENTION (decline + enough injected inventory release
#      failures to exhaust the saga's compensation-retry budget) - so the
#      "Saga outcomes" panel shows more than one color.
#
# DEMO-001/DEMO-002 stock is topped up to a large number up front so a long
# demo run never runs dry and silently starts rejecting "success"-mode
# orders; DEMO-003 is left at its seeded zero on purpose, for REJECTED.
set -euo pipefail
cd "$(dirname "$0")/.."

PGPASSWORD="${POSTGRES_PASSWORD:-changeme}"
export PGPASSWORD
psql -h "${POSTGRES_HOST:-localhost}" -p "${POSTGRES_PORT:-5433}" -U "${POSTGRES_USER:-searchsync_app}" -d "${POSTGRES_DB:-searchsync}" \
  -c "UPDATE inventory.stock SET available = 1000000 WHERE sku IN ('DEMO-001','DEMO-002');" >/dev/null

pids=()
cleanup() {
  echo
  echo "stopping demo traffic..."
  kill "${pids[@]}" 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

# --- product create/update traffic ---
# Rate deliberately exceeds a single indexer instance's throughput ceiling
# (observed ~150-400/s in benchmarking) so consumer_lag_records actually
# climbs instead of sitting at 0 - the whole point of this invocation, not
# just "some traffic."
(
  while true; do
    go run ./cmd/loadgen -mode=benchmark -rate=300 -duration=60s -concurrency=60 -update-fraction=0.3 >/dev/null 2>&1 || true
  done
) &
pids+=($!)

# --- order/saga traffic, cycling through outcomes ---
(
  customer="4a6c6e2a-1e4b-4f0a-9c3a-7b2a2e9c1234"
  i=0
  while true; do
    i=$((i + 1))
    case $((i % 10)) in
      0)
        # every 10th order: force NEEDS_ATTENTION (decline + exhaust
        # inventory's release-failure injector, reset immediately after)
        curl -s -X POST http://localhost:8085/dev/payments/mode -d '{"mode":"decline"}' >/dev/null
        curl -s -X POST http://localhost:8084/dev/inventory/release-failures -d '{"count":10}' >/dev/null
        curl -s -X POST http://localhost:8082/orders -H "Idempotency-Key: $(uuidgen)" \
          -d "{\"customer_id\":\"$customer\",\"total_cents\":8900,\"product_sku\":\"DEMO-002\",\"quantity\":1}" >/dev/null
        sleep 1
        curl -s -X POST http://localhost:8084/dev/inventory/release-failures -d '{"count":0}' >/dev/null
        ;;
      1 | 4 | 7)
        # REJECTED: DEMO-003 has zero seeded stock
        curl -s -X POST http://localhost:8085/dev/payments/mode -d '{"mode":"success"}' >/dev/null
        curl -s -X POST http://localhost:8082/orders -H "Idempotency-Key: $(uuidgen)" \
          -d "{\"customer_id\":\"$customer\",\"total_cents\":5400,\"product_sku\":\"DEMO-003\",\"quantity\":1}" >/dev/null
        ;;
      2 | 5 | 8)
        # CANCELLED: payment declined after reservation
        curl -s -X POST http://localhost:8085/dev/payments/mode -d '{"mode":"decline"}' >/dev/null
        curl -s -X POST http://localhost:8082/orders -H "Idempotency-Key: $(uuidgen)" \
          -d "{\"customer_id\":\"$customer\",\"total_cents\":9900,\"product_sku\":\"DEMO-001\",\"quantity\":1}" >/dev/null
        ;;
      *)
        # CONFIRMED: the happy path, most common outcome
        curl -s -X POST http://localhost:8085/dev/payments/mode -d '{"mode":"success"}' >/dev/null
        curl -s -X POST http://localhost:8082/orders -H "Idempotency-Key: $(uuidgen)" \
          -d "{\"customer_id\":\"$customer\",\"total_cents\":8900,\"product_sku\":\"DEMO-002\",\"quantity\":1}" >/dev/null
        ;;
    esac
    sleep 3
  done
) &
pids+=($!)

echo "Generating demo traffic (products + orders cycling through CONFIRMED/CANCELLED/REJECTED/NEEDS_ATTENTION)."
echo "Open http://localhost:3000 (admin/changeme) -> 'SearchSync Overview' to watch it live."
echo "Press Ctrl-C to stop."
wait
