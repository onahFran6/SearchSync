#!/usr/bin/env bash
# Creates the dead-letter topics both consumers publish to. Kafka
# auto-topic-creation is off in this Compose setup (discovered while
# testing Stage 3's DLQ path: a produce to a not-yet-existing DLQ topic
# fails UNKNOWN_TOPIC_OR_PARTITION, which ConsumerLoop correctly treats as
# "cannot durably quarantine" and refuses to advance the offset for -
# correct fail-closed behavior, but not what anyone wants from routine
# setup). Safe to rerun: kafka-topics.sh --create is a no-op if the topic
# already exists with --if-not-exists.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

for topic in "${PRODUCTS_DLQ_TOPIC:-searchsync.dlq.products}" "${ORDER_EVENTS_DLQ_TOPIC:-searchsync.dlq.orders}"; do
  echo "Creating DLQ topic $topic (if not already present)"
  docker exec searchsync-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done
