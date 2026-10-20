#!/usr/bin/env bash
# Bulk-loads a fresh Elasticsearch index directly from PostgreSQL (the
# source of truth), for Elasticsearch recovery scenarios where Kafka
# retention or the Debezium replication slot can no longer answer "what is
# the current full state" on its own: a corrupted index, a mapping change,
# or simply wanting a known-clean index to cut over to.
#
# This is deliberately separate from Kafka replay (cmd/replay): a snapshot
# read of Postgres, not a redelivery of Kafka history. It does NOT touch
# the "products" alias — see docs/ for the full reindex runbook, which
# covers closing the gap with cmd/replay and the alias-switch step this
# script intentionally leaves to a human.
#
# Usage: scripts/reindex-from-postgres.sh <target-index-name>
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

TARGET_INDEX="${1:?usage: scripts/reindex-from-postgres.sh <target-index-name>}"
ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"

psql_run() {
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host "$POSTGRES_HOST" \
    --port "$POSTGRES_PORT" \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    "$@"
}

if curl -sf "$ES_URL/$TARGET_INDEX" >/dev/null 2>&1; then
  echo "Index $TARGET_INDEX already exists, skipping creation"
else
  echo "Creating index $TARGET_INDEX"
  curl -sf -X PUT "$ES_URL/$TARGET_INDEX" \
    -H "Content-Type: application/json" \
    -d @infrastructure/elasticsearch/products-mapping.json
  echo
fi

# Recorded for the operator to feed into `cmd/replay -from-time` afterward,
# closing the gap between this snapshot and the moment the alias switches.
# Taken before the SELECT runs: any write that commits concurrently with
# the snapshot read is safe to double-apply during the gap-closing replay
# (Elasticsearch external versioning makes that a no-op), but must never be
# missed, so this errs toward overlap, not a gap.
START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Snapshot start (use as cmd/replay -from-time to close the gap): $START_TS"

echo "Bulk-loading products into $TARGET_INDEX from PostgreSQL..."
# Captured via command substitution, not piped straight into curl: psql
# only interpolates :'variable' references when reading a script
# (stdin/file), not from a -c command-line string, and keeping the heredoc
# capture separate from the curl invocation avoids ambiguity between
# heredoc bodies and line-continued pipeline commands.
BULK_BODY="$(psql_run --tuples-only --no-align --quiet --variable index="$TARGET_INDEX" <<'SQL'
  SELECT
    json_build_object('index', json_build_object('_index', :'index', '_id', id::text))
    || E'\n' ||
    json_build_object('sku', sku, 'name', name, 'description', description,
                       'category', category, 'price_cents', price_cents)
  FROM products;
SQL
)"

echo "$BULK_BODY" | curl -sf -X POST "$ES_URL/$TARGET_INDEX/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @- \
  | python3 -c "
import json, sys
resp = json.load(sys.stdin)
errors = [item for item in resp.get('items', []) if item.get('index', {}).get('error')]
print(f\"bulk load: {len(resp.get('items', []))} actions, {len(errors)} errors\")
for e in errors[:5]:
    print(e)
sys.exit(1 if errors else 0)
"

curl -sf -X POST "$ES_URL/$TARGET_INDEX/_refresh" >/dev/null

PG_COUNT="$(psql_run --tuples-only --no-align -c 'SELECT count(*) FROM products')"
ES_COUNT="$(curl -sf "$ES_URL/$TARGET_INDEX/_count" | python3 -c 'import json,sys; print(json.load(sys.stdin)["count"])')"
echo "PostgreSQL products count: $PG_COUNT"
echo "$TARGET_INDEX document count: $ES_COUNT"
if [ "$PG_COUNT" != "$ES_COUNT" ]; then
  echo "WARNING: counts do not match, investigate before switching the alias" >&2
  exit 1
fi
echo "Counts match. Next: close the gap with cmd/replay, verify, then switch the alias (see docs runbook)."
