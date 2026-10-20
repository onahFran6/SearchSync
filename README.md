# SearchSync

Go-based, implementation-first distributed systems lab: PostgreSQL commits captured by Debezium CDC, published to Kafka, and indexed into Elasticsearch for search. See [docs/README.md](docs/README.md) for the full six-stage series and [docs/01_CDC_SEARCH_IMPLEMENTATION.md](docs/01_CDC_SEARCH_IMPLEMENTATION.md) for the current (Stage 1) implementation plan.

## Status

Stage 1 complete: PostgreSQL -> Debezium -> Kafka -> Elasticsearch product search.
Stage 2 complete: orders + transactional outbox -> a second Debezium connector with the Outbox Event Router -> a dedicated domain-event topic -> a deduplicating notifications consumer. See [docs/02_TRANSACTIONAL_OUTBOX_IMPLEMENTATION.md](docs/02_TRANSACTIONAL_OUTBOX_IMPLEMENTATION.md).
Stage 3 complete: bounded retries with backoff, dead-letter topics, Elasticsearch stale-replay protection, and bounded Kafka replay tooling for both existing consumers. See [docs/03_CONSUMER_RELIABILITY_IMPLEMENTATION.md](docs/03_CONSUMER_RELIABILITY_IMPLEMENTATION.md).
Stage 4 complete: Orders, Inventory, and a simulated Payments service coordinated by a persisted, orchestrated saga - no cross-service database transaction anywhere, idempotent commands/events, a conditional-update inventory reservation safe under concurrency, and a durable Postgres-backed deadline poller for payment timeouts and compensation retries. See [docs/04_SAGA_IMPLEMENTATION.md](docs/04_SAGA_IMPLEMENTATION.md) and [docs/clarification/stage-04-low-level-design.md](docs/clarification/stage-04-low-level-design.md).

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
./scripts/register-connector.sh infrastructure/debezium/outbox-connector.json      # orders outbox connector
./scripts/register-connector.sh infrastructure/debezium/saga-connector.json        # saga coordinator's outbox connector
./scripts/register-connector.sh infrastructure/debezium/inventory-connector.json   # Inventory's outbox connector
./scripts/register-connector.sh infrastructure/debezium/payments-connector.json    # Payments-sim's outbox connector
./scripts/create-index.sh        # create the Elasticsearch index + alias
./scripts/create-dlq-topics.sh   # create every consumer's dead-letter topic (Kafka does not auto-create topics here)
./scripts/create-saga-topics.sh  # create the ten saga command/event/notification topics (same reason)
```

`scripts/migrate.sh` tracks applied files in a `schema_migrations` table, so rerunning it against an already-migrated database only applies new files.

PostgreSQL is published to the host on **5433**, not 5432, to avoid colliding with any other local PostgreSQL instance; `database.port` inside the Docker network (used by the Debezium connector configs) is still the container's 5432. Kafka (9092), Kafka Connect (8083), and Elasticsearch (9200) use their default ports. The product API listens on **8081** (`API_ADDR`), the orders API on **8082** (`ORDERS_API_ADDR`), Inventory's local-only dev endpoint on **8084** (`INVENTORY_API_ADDR`), and Payments-sim's local-only dev endpoint on **8085** (`PAYMENTS_API_ADDR`) - all off 8080 to avoid colliding with other local dev servers. The saga coordinator exposes no HTTP endpoint (pure consumer + poller, same shape as `cmd/notifications`/`cmd/indexer`).

## Running the services

```bash
go run ./cmd/api
go run ./cmd/indexer
go run ./cmd/orders            # POST /orders, GET /orders/:id, GET /orders/:id/timeline; also runs the saga-notification consumer
go run ./cmd/notifications     # consumes the orders outbox topic, dedupes by event_id
go run ./cmd/inventory         # consumes ReserveInventory/ReleaseInventory commands; also runs POST /dev/inventory/release-failures
go run ./cmd/payments-sim      # consumes AuthorizePayment commands; also runs POST /dev/payments/mode
go run ./cmd/saga-coordinator  # orchestrates the saga: event-driven consumer + persisted-deadline poller
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
go test -tags=integration ./internal/inventory/...      # requires live Postgres; proves the concurrent-reservation race and duplicate-command idempotency
go test -tags=integration ./internal/saga/...            # requires live Postgres; proves compensation-retry/escalation SQL
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

## Reliability notes (Stage 4: the order-fulfilment saga)

**Service ownership boundaries.** Orders stays in `public` (unchanged from Stage 1-2). Three new PostgreSQL schemas - `saga`, `inventory`, `payments` - each own their own tables, including their own `outbox_events`, and each have a dedicated, non-superuser login role granted only on their own schema (`searchsync_saga_app`, `searchsync_inventory_app`, `searchsync_payments_app`, created by `infrastructure/postgres/01_service_roles.sh`). No cross-service PostgreSQL transaction exists anywhere in this stage - every multi-step workflow crosses a service boundary via Kafka, never via a shared database connection. See `docs/clarification/stage-04-ownership-decision.md` for why schemas rather than separate databases, and the one deliberate asymmetry (Stage 1's bootstrap role, `searchsync_app`, is a Postgres superuser by virtue of this Compose setup's `POSTGRES_USER` mechanism, and that pre-existing property is not revoked by this stage).

**Idempotency and identity.** Every command and event carries `messageId`/`correlationId`/`causationId`/`sagaId`/`orderId`/`eventVersion` (`internal/saga.Envelope`). A unique constraint on `inventory.reservations.order_id` is what actually prevents reserving stock twice for one order, not application logic checking first - the same structural-dedup convention Stage 2 established for `processed_events`, reused here for `processed_commands`/`processed_messages` across every new service.

**Concurrent inventory reservation.** `internal/inventory.Service.TryReserveStock` uses one conditional `UPDATE inventory.stock SET available = available - $qty WHERE sku = $1 AND available >= $qty` inside a local transaction, committed atomically with the reservation row and its outbox event. Postgres's row-level locking on that `UPDATE` makes two concurrent reservations for the same SKU safe under any isolation level, including the default `READ COMMITTED` - no `SERIALIZABLE`, no advisory lock, no saga-level coordination needed. Proven live with two concurrent HTTP requests for the last unit of stock (`docs/evidence/stage-04/07-concurrent-last-unit.md`) and deterministically with ten concurrent goroutines (`internal/inventory/reservation_integration_test.go`).

**Durable timeouts, never an in-memory timer.** `internal/saga.Service.RunPoller` queries `saga.sagas` for passed `payment_deadline`/`compensation_deadline` values every `PollInterval` - the deadline itself was persisted in the same transaction as the state transition that set it, so a crashed-and-restarted (even `kill -9`'d) saga coordinator resumes exactly where it left off, proven live in `docs/evidence/stage-04/05-restart-mid-saga.md`.

**Compensation is retried, then escalated, never silently abandoned.** A payment decline or timeout dispatches `ReleaseInventory`; if no resolving event arrives within `CompensationRetryInterval`, the poller retries (a fresh command, fresh message id) up to `MaxCompensationAttempts` times before transitioning to the terminal `NEEDS_ATTENTION` state - visible via `GET /orders/:id` and `GET /orders/:id/timeline`, never an order stuck forever in `COMPENSATING` with no record of why. See `docs/evidence/stage-04/06-compensation-retry-and-exhaustion.md`.

**Payments are simulated only.** `cmd/payments-sim` never handles real card data; its outcome is entirely controlled by the local-only `POST /dev/payments/mode` endpoint (`success`/`decline`/`timeout`). In `timeout` mode it deliberately produces no event at all - the saga's deadline poller, not a third event type, is what detects a payment that never responds.

**A real bug found and fixed during this stage's development.** `internal/saga.Repository.AttemptCompensationRetry`'s original SQL had `CASE WHEN ... THEN NULL ELSE $3 END` assigning into a `timestamptz` column; Postgres could not infer a type for the bare `NULL` branch and resolved the whole expression as `text`, failing on every call regardless of which branch would have executed. Found live while proving Experiment #5 (restart mid-saga), fixed with an explicit `NULL::timestamptz` cast, and regression-tested directly against Postgres. Full writeup: `docs/evidence/stage-04/05-restart-mid-saga.md`.
