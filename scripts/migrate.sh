#!/usr/bin/env bash
# Applies migrations/*.sql files in lexical order against the PostgreSQL
# instance described by .env, skipping files already recorded in
# schema_migrations. Safe to rerun at any point (fresh database or one with
# earlier stages already applied): only new files run.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

psql_run() {
  PGPASSWORD="$POSTGRES_PASSWORD" psql \
    --host "$POSTGRES_HOST" \
    --port "$POSTGRES_PORT" \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set ON_ERROR_STOP=1 \
    "$@"
}

psql_run -c "CREATE TABLE IF NOT EXISTS schema_migrations (
  filename TEXT PRIMARY KEY,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)" >/dev/null

for f in migrations/*.sql; do
  name="$(basename "$f")"
  already_applied="$(psql_run --tuples-only --no-align -c \
    "SELECT 1 FROM schema_migrations WHERE filename = '$name'")"
  if [ "$already_applied" = "1" ]; then
    echo "Skipping $f (already applied)"
    continue
  fi

  echo "Applying $f"
  psql_run --file "$f"
  psql_run -c "INSERT INTO schema_migrations (filename) VALUES ('$name')" >/dev/null
done
