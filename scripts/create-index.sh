#!/usr/bin/env bash
# Creates the versioned backing index (products_v1) with its explicit mapping
# and points the "products" alias at it. Application code (API and indexer)
# always reads/writes the alias, never the versioned index name directly, so
# a future stage can reindex into products_v2 and flip the alias with zero
# downtime.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
INDEX="${PRODUCTS_INDEX:-products}_v1"
ALIAS="${PRODUCTS_INDEX:-products}"

if curl -sf "$ES_URL/$INDEX" >/dev/null 2>&1; then
  echo "Index $INDEX already exists, skipping creation"
else
  echo "Creating index $INDEX"
  curl -sf -X PUT "$ES_URL/$INDEX" \
    -H "Content-Type: application/json" \
    -d @infrastructure/elasticsearch/products-mapping.json
  echo
fi

echo "Pointing alias $ALIAS -> $INDEX"
curl -sf -X POST "$ES_URL/_aliases" \
  -H "Content-Type: application/json" \
  -d "{\"actions\":[{\"add\":{\"index\":\"$INDEX\",\"alias\":\"$ALIAS\",\"is_write_index\":true}}]}"
echo
