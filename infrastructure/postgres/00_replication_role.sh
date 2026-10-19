#!/usr/bin/env bash
# Dedicated role for Debezium logical replication, scoped to the minimum
# privileges needed to read WAL changes via pgoutput. Runs once on first
# container init, as part of docker-entrypoint-initdb.d.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE ROLE ${POSTGRES_REPLICATION_USER} WITH REPLICATION LOGIN PASSWORD '${POSTGRES_REPLICATION_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_REPLICATION_USER};
SQL
