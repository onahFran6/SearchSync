#!/usr/bin/env bash
# Registers (or updates) the Debezium PostgreSQL connector via the Kafka
# Connect REST API. The connector's database.password must match
# POSTGRES_REPLICATION_PASSWORD in .env.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="infrastructure/debezium/products-connector.json"
NAME="$(python3 -c "import json; print(json.load(open('$CONFIG_FILE'))['name'])")"

if curl -sf "$CONNECT_URL/connectors/$NAME" >/dev/null 2>&1; then
  echo "Updating existing connector $NAME"
  curl -sf -X PUT "$CONNECT_URL/connectors/$NAME/config" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c "import json; print(json.dumps(json.load(open('$CONFIG_FILE'))['config']))")"
else
  echo "Creating connector $NAME"
  curl -sf -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$CONFIG_FILE"
fi
echo
curl -s "$CONNECT_URL/connectors/$NAME/status"
echo
