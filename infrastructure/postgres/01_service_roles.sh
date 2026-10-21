#!/usr/bin/env bash
# Stage 4: one dedicated, non-superuser login role per new service
# (saga/inventory/payments), each scoped by GRANT to only its own schema
# once that schema's migration runs (migrations/0004-0006). Runs once on
# first container init, same as 00_replication_role.sh and for the same
# reason: these are login credentials, so they're created here from .env
# values, never hardcoded into a committed migrations/*.sql file.
#
# See docs/clarification/stage-04-ownership-decision.md for why these three
# roles exist instead of reusing searchsync_app (the Postgres superuser
# this Compose setup's POSTGRES_USER bootstraps) for the new services.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-SQL
    CREATE ROLE ${SAGA_DB_USER} WITH LOGIN PASSWORD '${SAGA_DB_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${SAGA_DB_USER};

    CREATE ROLE ${INVENTORY_DB_USER} WITH LOGIN PASSWORD '${INVENTORY_DB_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${INVENTORY_DB_USER};

    CREATE ROLE ${PAYMENTS_DB_USER} WITH LOGIN PASSWORD '${PAYMENTS_DB_PASSWORD}';
    GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO ${PAYMENTS_DB_USER};
SQL
