# SearchSync

Go-based, implementation-first distributed systems lab: PostgreSQL commits captured by Debezium CDC, published to Kafka, and indexed into Elasticsearch for search. See [docs/README.md](docs/README.md) for the full six-stage series and [docs/01_CDC_SEARCH_IMPLEMENTATION.md](docs/01_CDC_SEARCH_IMPLEMENTATION.md) for the current (Stage 1) implementation plan.

## Status

Stage 1 in progress: PostgreSQL -> Debezium -> Kafka -> Elasticsearch product search.

## Prerequisites

- Docker Engine + Compose plugin
- Go 1.24+ (toolchain in use: `go version`)
- `curl`

## Setup

```bash
cp .env.example .env   # fill in local values; never commit .env
docker compose -f infrastructure/compose.yaml up -d
docker compose -f infrastructure/compose.yaml ps

./scripts/migrate.sh             # create the products table and seed data
./scripts/register-connector.sh  # register the Debezium PostgreSQL connector
./scripts/create-index.sh        # create the Elasticsearch index + alias
```

PostgreSQL is published to the host on **5433**, not 5432, to avoid colliding with any other local PostgreSQL instance; `database.port` inside the Docker network (used by the Debezium connector config) is still the container's 5432. Kafka (9092), Kafka Connect (8083), and Elasticsearch (9200) use their default ports. The Go API listens on **8081** (`API_ADDR`), not 8080, to avoid colliding with other local dev servers.

## Running the services

```bash
go run ./cmd/api
go run ./cmd/indexer
```

## Quality gate

```bash
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
./scripts/smoke-test.sh   # requires the full stack + API + indexer already running
```

## Reliability notes

**Eventual consistency.** Elasticsearch is a derived, rebuildable read model, never the system of record. A write accepted by the API (or made directly in PostgreSQL via `psql`) is immediately durable and visible through `GET /products/:id`, but `/search` only reflects it after it has traveled PostgreSQL WAL -> Debezium -> Kafka -> the indexer -> Elasticsearch. In local testing this pipeline typically applies within roughly half a second to a few seconds (see `docs/evidence/stage-01/`); there is no hard upper bound, and a stopped indexer or connector can widen the gap indefinitely until it resumes. Do not read your own write back through `/search` and expect it immediately; poll, or read from PostgreSQL if you need the authoritative value right away.

**Security assumptions (local development only).** Elasticsearch runs with `xpack.security.enabled=false` (no auth, no TLS) and all four infrastructure ports are published to `127.0.0.1` only, never `0.0.0.0`. The Debezium replication role (`searchsync_repl`) is scoped to `REPLICATION`, `CONNECT`, and `SELECT` on `products` only; it cannot create publications itself (see `migrations/0001_create_products.sql`), own other tables, or read other schemas. Real credentials never live in `.env.example` or any committed file; `.env` is git-ignored. None of this is production-hardened: there is no network segmentation, no TLS between services, and no secrets manager. Treat every default password in `.env.example` as public.

**WAL slot retention risk.** The Debezium connector holds a PostgreSQL logical replication slot (`searchsync_products_slot`) open. PostgreSQL retains WAL segments for an open slot until its consumer (Kafka Connect) acknowledges them, regardless of disk pressure. If the connector is stopped or falls behind for an extended period, retained WAL will grow and can eventually fill the PostgreSQL data volume, which can in turn block writes entirely. Monitor replication slot lag (`SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots;`) in anything beyond local development, and drop the slot (`SELECT pg_drop_replication_slot('searchsync_products_slot');`) if the connector is being decommissioned rather than restarted.
