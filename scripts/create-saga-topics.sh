#!/usr/bin/env bash
# Pre-creates the ten command/event/notification topics the saga's
# Debezium outbox connectors route into (searchsync-saga-connector,
# searchsync-inventory-connector, searchsync-payments-connector) - one
# topic per message type, same granularity as
# searchsync.outbox.event.orders. Same reason as
# scripts/create-dlq-topics.sh: this Compose Kafka does not auto-create
# topics, and a connector's first route to a not-yet-existing topic fails
# the same way a consumer's first DLQ publish would. Safe to rerun.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

for topic in \
  "${RESERVE_INVENTORY_TOPIC:-searchsync.outbox.event.reserve_inventory}" \
  "${RELEASE_INVENTORY_TOPIC:-searchsync.outbox.event.release_inventory}" \
  "${AUTHORIZE_PAYMENT_TOPIC:-searchsync.outbox.event.authorize_payment}" \
  "${SAGA_TRANSITIONED_TOPIC:-searchsync.outbox.event.saga_transitioned}" \
  "${INVENTORY_RESERVED_TOPIC:-searchsync.outbox.event.inventory_reserved}" \
  "${INVENTORY_REJECTED_TOPIC:-searchsync.outbox.event.inventory_rejected}" \
  "${INVENTORY_RELEASED_TOPIC:-searchsync.outbox.event.inventory_released}" \
  "${INVENTORY_RELEASE_FAILED_TOPIC:-searchsync.outbox.event.inventory_release_failed}" \
  "${PAYMENT_AUTHORIZED_TOPIC:-searchsync.outbox.event.payment_authorized}" \
  "${PAYMENT_DECLINED_TOPIC:-searchsync.outbox.event.payment_declined}"; do
  echo "Creating topic $topic (if not already present)"
  docker exec searchsync-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done
