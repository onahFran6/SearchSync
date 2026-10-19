#!/usr/bin/env bash
# Applies every migrations/*.sql file in lexical order against the
# PostgreSQL instance described by .env. Safe to rerun; migrations are
# expected to be idempotent-by-naming (new files only), not auto-tracked.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

for f in migrations/*.sql; do
  echo "Applying $f"
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host "$POSTGRES_HOST" \
    --port "$POSTGRES_PORT" \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --file "$f" \
    --set ON_ERROR_STOP=1
done
