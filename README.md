# SearchSync

Go-based, implementation-first distributed systems lab: PostgreSQL commits captured by Debezium CDC, published to Kafka, and indexed into Elasticsearch for search. See [docs/README.md](docs/README.md) for the full six-stage series and [docs/01_CDC_SEARCH_IMPLEMENTATION.md](docs/01_CDC_SEARCH_IMPLEMENTATION.md) for the current (Stage 1) implementation plan.

## Status

Stage 1 complete: PostgreSQL -> Debezium -> Kafka -> Elasticsearch product search.
Stage 2 complete: orders + transactional outbox -> a second Debezium connector with the Outbox Event Router -> a dedicated domain-event topic -> a deduplicating notifications consumer. See [docs/02_TRANSACTIONAL_OUTBOX_IMPLEMENTATION.md](docs/02_TRANSACTIONAL_OUTBOX_IMPLEMENTATION.md).
Stage 3 in progress: bounded retries with backoff, dead-letter topics, Elasticsearch stale-replay protection, and bounded Kafka replay tooling for both existing consumers. See [docs/03_CONSUMER_RELIABILITY_IMPLEMENTATION.md](docs/03_CONSUMER_RELIABILITY_IMPLEMENTATION.md).

## Prerequisites

- Docker Engine + Compose plugin
- Go 1.24+ (toolchain in use: `go version`)
- `curl`

## Setup

```bash
cp .env.example .env   # fill in local values; never commit .env
docker compose -f infrastructure/compose.yaml up -d
docker compose -f infrastructure/compose.yaml ps

./scripts/migrate.sh                                              # all pending migrations, in order; safe to rerun
./scripts/register-connector.sh                                   # products connector (default config)
./scripts/register-connector.sh infrastructure/debezium/outbox-connector.json  # orders outbox connector
./scripts/create-index.sh        # create the Elasticsearch index + alias
./scripts/create-dlq-topics.sh   # create the two dead-letter topics (Kafka does not auto-create topics here)
```

`scripts/migrate.sh` tracks applied files in a `schema_migrations` table, so rerunning it against an already-migrated database only applies new files.

PostgreSQL is published to the host on **5433**, not 5432, to avoid colliding with any other local PostgreSQL instance; `database.port` inside the Docker network (used by the Debezium connector configs) is still the container's 5432. Kafka (9092), Kafka Connect (8083), and Elasticsearch (9200) use their default ports. The product API listens on **8081** (`API_ADDR`) and the orders API on **8082** (`ORDERS_API_ADDR`), both off 8080 to avoid colliding with other local dev servers.

## Running the services

```bash
go run ./cmd/api
go run ./cmd/indexer
go run ./cmd/orders           # POST /orders - transactional order + outbox insert
go run ./cmd/notifications    # consumes the orders outbox topic, dedupes by event_id
```

## Replay and recovery tooling (Stage 3)

```bash
# Bounded, dry-run-by-default Kafka replay through the exact same handler
# the live consumer uses (processed_events / ES external versioning make
# it safe to reapply already-processed events):
go run ./cmd/replay -target=notifications -from-offset=0 -to-offset=10 -partition=0            # dry-run
go run ./cmd/replay -target=notifications -from-offset=0 -to-offset=10 -partition=0 -dry-run=false
go run ./cmd/replay -target=indexer -from-time=2026-01-01T00:00:00Z -dry-run=false -products-index=products

# Elasticsearch recovery distinct from Kafka replay: fresh index + snapshot
# from Postgres + alias switch. See docs/ for the full runbook.
scripts/reindex-from-postgres.sh products_v2
```

## Quality gate

```bash
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
go test -tags=integration ./internal/orders/...   # requires live Postgres; proves order+outbox atomicity
go test -tags=integration ./internal/indexer/...        # requires live Kafka+Postgres+Elasticsearch; proves crash/restart duplicate-safety
go test -tags=integration ./internal/notifications/...  # same, for the notifications projection
./scripts/smoke-test.sh   # requires the full stack + API + indexer already running
```

## Reliability notes (Stage 3: retries, DLQ, replay)

**Retry, DLQ, and offset-commit discipline.** Both consumers now run through a shared `internal/platform.ConsumerLoop`: a transient failure (network error, Elasticsearch 5xx/429, a Postgres connection-class error) is retried up to 5 times with exponential backoff and jitter; a permanent one (malformed payload, an Elasticsearch 4xx, a Postgres constraint violation) skips straight to the consumer's dead-letter topic (`searchsync.dlq.products`, `searchsync.dlq.orders`). An offset is committed only after the record succeeds **or** is durably placed on the DLQ (a synchronously-acknowledged Kafka produce) — if the DLQ itself can't be reached, the consumer stops rather than silently dropping the record. See `docs/evidence/stage-03/00-failure-matrix.md` for the full classification and the per-key-ordering trade-off this implies.

**Elasticsearch stale-replay protection.** Every Elasticsearch write uses `version_type=external` with Debezium's `source.lsn` as the version, so a replayed or out-of-order older event can never overwrite a newer one already applied — Elasticsearch's own 409 version conflict is treated as a safe no-op.

**DLQ entries are not deduplicated across redeliveries of the same poison record** — unlike business-state idempotency, a record that fails to decode has no event ID to dedupe by. Resolve DLQ entries by their `original-topic`/`original-partition`/`original-offset` headers, not by assuming one entry per bad record. See `docs/evidence/stage-03/03-malformed-event-dlq.md`.

**Fail-stop on broker loss.** A Kafka connectivity failure (not a per-record processing failure) surfaces at the poll loop itself and exits the process; recovery is restarting it (a process supervisor's job in production, not something retried in-process).



**Eventual consistency.** Elasticsearch is a derived, rebuildable read model, never the system of record. A write accepted by the API (or made directly in PostgreSQL via `psql`) is immediately durable and visible through `GET /products/:id`, but `/search` only reflects it after it has traveled PostgreSQL WAL -> Debezium -> Kafka -> the indexer -> Elasticsearch. In local testing this pipeline typically applies within roughly half a second to a few seconds (see `docs/evidence/stage-01/`); there is no hard upper bound, and a stopped indexer or connector can widen the gap indefinitely until it resumes. Do not read your own write back through `/search` and expect it immediately; poll, or read from PostgreSQL if you need the authoritative value right away.

**Security assumptions (local development only).** Elasticsearch runs with `xpack.security.enabled=false` (no auth, no TLS) and all four infrastructure ports are published to `127.0.0.1` only, never `0.0.0.0`. The Debezium replication role (`searchsync_repl`) is scoped to `REPLICATION`, `CONNECT`, and `SELECT` on `products` only; it cannot create publications itself (see `migrations/0001_create_products.sql`), own other tables, or read other schemas. Real credentials never live in `.env.example` or any committed file; `.env` is git-ignored. None of this is production-hardened: there is no network segmentation, no TLS between services, and no secrets manager. Treat every default password in `.env.example` as public.

**WAL slot retention risk.** The Debezium connectors hold PostgreSQL logical replication slots open: `searchsync_products_slot` and `searchsync_outbox_slot`. PostgreSQL retains WAL segments for an open slot until its consumer (Kafka Connect) acknowledges them, regardless of disk pressure. If either connector is stopped or falls behind for an extended period, retained WAL will grow and can eventually fill the PostgreSQL data volume, which can in turn block writes entirely. `docs/evidence/stage-02/05-kafka-outage.md` demonstrates the slot correctly retaining a backlog across a Connect outage and draining it on restart; that same retention is the risk if the outage is prolonged or the connector is never restarted. Monitor replication slot lag (`SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots;`) in anything beyond local development, and drop a slot (`SELECT pg_drop_replication_slot('<slot_name>');`) if its connector is being decommissioned rather than restarted.

**Outbox retention and cleanup.** `outbox_events` rows are never updated or deleted by the application; they are immutable records written once by `Service.CreateOrder` and read once by Debezium's snapshot/streaming. This stage intentionally does **not** implement a cleanup job: deleting a row before Debezium has captured it would silently drop the event (the Outbox Event Router only processes insert/snapshot records, not deletes), and there is no cheap way from the application side to know a row has been durably delivered to every consumer. A safe cleanup strategy needs either a time-based retention window long enough to be confident Debezium is caught up (checked against replication slot lag, not wall-clock guesswork) or an explicit "delivered" signal the connector doesn't currently provide; both are deferred to a later stage rather than implemented speculatively here.
