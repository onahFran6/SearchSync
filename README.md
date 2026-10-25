# SearchSync

A Go-based, hands-on distributed systems lab: a product catalog kept in sync with
Elasticsearch via PostgreSQL change-data-capture, and an order-fulfilment workflow coordinated
across Orders, Inventory, and a simulated Payments service with an orchestrated saga. See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full system design.

![SearchSync architecture diagram](docs/images/architecture.svg)

## How a request flows

### A - Catalog write to search

The API never touches Elasticsearch. A manual SQL edit reaches search the same way an API call does.

1. Client creates or edits a product through the Product API.
2. API commits the row to the `products` table. This is the only write.
3. Debezium reads the committed change from the WAL through a logical replication slot.
4. It publishes a row-change event to the product changes topic.
5. The Indexer consumes it on the shared runtime (dedupe, retry, DLQ).
6. Indexer upserts or deletes the document in Elasticsearch.
7. Search requests are served from the index, which is eventually consistent with PostgreSQL.



### B - Placing an order

The order and its event commit together, so an event can't be lost or published for an order that doesn't exist.

1. Client places an order with the Orders API.
2. One transaction inserts into `orders` and `outbox_events`.
3. Debezium's Outbox Event Router reads the outbox row from the WAL.
4. It publishes a typed domain event such as `OrderPlaced`. Notifications also consumes it.
5. Saga Coordinator consumes the event and starts the workflow.
6. Coordinator commands Inventory to reserve stock and waits for the outcome event.
7. Coordinator commands Payments to authorize and waits for approved or declined.
8. Coordinator moves the order to its next state: confirmed, or cancelled after compensation.



### When something fails

Failures are handled by the coordinator and the shared runtime, never by services calling each other.

1. Payment declines or never answers: coordinator commands Inventory to release the reserved stock.
2. The release is retried within a bounded budget. If it still fails, the saga lands in a state a human can see and act on.
3. Transient errors in any consumer retry with backoff. Malformed or contract-breaking messages go to a DLQ topic so the partition keeps moving.
4. The replay tool reprocesses a bounded range of a topic through the same handler code the live consumer uses.
5. Redelivered messages are dropped by message ID, so at-least-once delivery never doubles a side effect.



### Rules the whole system keeps

**One write per request.** Every request writes to a single PostgreSQL transaction. Kafka and Elasticsearch are filled from the WAL, never by application code.

**Message identity.** Every command and event carries `message_id`, a `correlation_id` fixed for the whole saga, and a `causation_id` pointing at the message that triggered it.

**Structural idempotency.** Unique constraints at each write boundary allow one reservation per order and one authorization attempt per order, so retries and out-of-order delivery are safe.

**Data ownership.** Each service reads and writes only its own schema. Services coordinate only through Kafka. The replication role can read outbox tables and nothing else.

**One coordinator.** Only the Saga Coordinator reads every saga event, decides the next step, and issues commands. Inventory and Payments never talk to each other.

**Traces cross the broker.** Trace context travels from the HTTP request into the outbox row, through Kafka headers, and is resumed by the consumer, so one trace spans the whole order.

## Capabilities

- **Real-time search from CDC.** PostgreSQL is the only system of truth; a Debezium connector
streams committed row changes to Kafka, and a Go consumer applies them to Elasticsearch. No
code path ever writes to Elasticsearch and PostgreSQL at once.
- **Transactional outbox.** Orders and their domain events are written in a single PostgreSQL
transaction, then published to Kafka through a second Debezium connector running the Outbox
Event Router - an order can never be saved without its event eventually publishing, and the
event can never exist without a corresponding saved order.
- **Orchestrated saga.** A Saga Coordinator drives order fulfilment across Orders, Inventory,
and a simulated Payments service: reserving stock, authorizing payment, confirming or
compensating (releasing stock) on failure, with durable deadlines and bounded compensation
retries that escalate to a human-visible state rather than retrying forever.
- **Consumer reliability.** Every consumer runs on a shared retry/dead-letter engine: bounded
retries with backoff for transient failures, immediate quarantine for permanent ones,
structural deduplication for at-least-once delivery, and a standalone replay tool that
reprocesses a bounded range of a topic through the same handler code the live consumer uses.
- **Observability.** Every service exposes Prometheus metrics and participates in distributed
tracing, including across the outbox -> Debezium -> Kafka hop, with Grafana dashboards and
alerting on consumer lag, dead-letter backlog, and replication-slot retention.



## Prerequisites

- Docker Engine + Compose plugin
- Go 1.26+ (toolchain in use: `go version`)
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

`scripts/migrate.sh` tracks applied files in a `schema_migrations` table, so rerunning it
against an already-migrated database only applies new files.

PostgreSQL is published to the host on **5433**, not 5432, to avoid colliding with any other
local PostgreSQL instance; `database.port` inside the Docker network (used by the Debezium
connector configs) is still the container's 5432. Kafka (9092), Kafka Connect (8083), and
Elasticsearch (9200) use their default ports. The product API listens on **8081** (`API_ADDR`),
the orders API on **8082** (`ORDERS_API_ADDR`), Inventory's local-only dev endpoint on **8084**
(`INVENTORY_API_ADDR`), and Payments-sim's local-only dev endpoint on **8085**
(`PAYMENTS_API_ADDR`) - all off 8080 to avoid colliding with other local dev servers. The saga
coordinator exposes no HTTP endpoint (pure consumer + poller, same shape as
`cmd/notifications`/`cmd/indexer`).

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

To stop every one of the above, including `go run`'s separately-named child binary (`pkill -f "go run ./cmd/X"` only kills the wrapper, not the process actually holding the port/consumer
group):

```bash
./scripts/stop-services.sh
```



## Replay and recovery tooling

```bash
# Bounded, dry-run-by-default Kafka replay through the exact same handler
# the live consumer uses (processed_events / ES external versioning make
# it safe to reapply already-processed events):
go run ./cmd/replay -target=notifications -from-offset=0 -to-offset=10 -partition=0            # dry-run
go run ./cmd/replay -target=notifications -from-offset=0 -to-offset=10 -partition=0 -dry-run=false
go run ./cmd/replay -target=indexer -from-time=2026-01-01T00:00:00Z -dry-run=false -products-index=products

# Elasticsearch recovery distinct from Kafka replay: fresh index + snapshot
# from Postgres + alias switch.
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



## Consumer reliability: retries, DLQ, and replay

**Retry, DLQ, and offset-commit discipline.** Every consumer runs through a shared
`internal/platform.ConsumerLoop`: a transient failure (network error, Elasticsearch 5xx/429, a
Postgres connection-class error) is retried up to 5 times with exponential backoff and jitter; a
permanent one (malformed payload, an Elasticsearch 4xx, a Postgres constraint violation) skips
straight to the consumer's dead-letter topic (`searchsync.dlq.products`,
`searchsync.dlq.orders`, ...). An offset is committed only after the record succeeds **or** is
durably placed on the DLQ (a synchronously-acknowledged Kafka produce) - if the DLQ itself can't
be reached, the consumer stops rather than silently dropping the record.

**Elasticsearch stale-replay protection.** Every Elasticsearch write uses `version_type=external`
with Debezium's `source.lsn` as the version, so a replayed or out-of-order older event can never
overwrite a newer one already applied - Elasticsearch's own 409 version conflict is treated as a
safe no-op.

**DLQ entries are not deduplicated across redeliveries of the same poison record** - unlike
business-state idempotency, a record that fails to decode has no event ID to dedupe by. Resolve
DLQ entries by their `original-topic`/`original-partition`/`original-offset` headers, not by
assuming one entry per bad record.

**Fail-stop on broker loss.** A Kafka connectivity failure (not a per-record processing failure)
surfaces at the poll loop itself and exits the process; recovery is restarting it (a process
supervisor's job in production, not something retried in-process).

**Eventual consistency.** Elasticsearch is a derived, rebuildable read model, never the system of
record. A write accepted by the API (or made directly in PostgreSQL via `psql`) is immediately
durable and visible through `GET /products/:id`, but `/search` only reflects it after it has
traveled PostgreSQL WAL -> Debezium -> Kafka -> the indexer -> Elasticsearch. In local testing
this pipeline typically applies within roughly half a second to a few seconds; there is no hard
upper bound, and a stopped indexer or connector can widen the gap indefinitely until it resumes.
Do not read your own write back through `/search` and expect it immediately; poll, or read from
PostgreSQL if you need the authoritative value right away.

**Security assumptions (local development only).** Elasticsearch runs with
`xpack.security.enabled=false` (no auth, no TLS) and all infrastructure ports are published to
`127.0.0.1` only, never `0.0.0.0`. The Debezium replication role (`searchsync_repl`) is scoped to
`REPLICATION`, `CONNECT`, and `SELECT` on each schema's outbox table only; it cannot create
publications itself, own other tables, or read other schemas. Real credentials never live in
`.env.example` or any committed file; `.env` is git-ignored. None of this is production-hardened:
there is no network segmentation, no TLS between services, and no secrets manager. Treat every
default password in `.env.example` as public.

**WAL slot retention risk.** The Debezium connectors hold PostgreSQL logical replication slots
open (`searchsync_products_slot`, `searchsync_outbox_slot`, and one per additional
outbox-owning schema). PostgreSQL retains WAL segments for an open slot until its consumer (Kafka
Connect) acknowledges them, regardless of disk pressure. If a connector is stopped or falls
behind for an extended period, retained WAL will grow and can eventually fill the PostgreSQL
data volume, which can in turn block writes entirely. Monitor replication slot lag (`SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) FROM pg_replication_slots;`) in
anything beyond local development, and drop a slot (`SELECT pg_drop_replication_slot('<slot_name>');`) if its connector is being decommissioned rather than
restarted.

**Outbox retention and cleanup.** `outbox_events` rows are never updated or deleted by the
application; they are immutable records written once by `Service.CreateOrder` and read once by
Debezium's snapshot/streaming. There is intentionally no cleanup job: deleting a row before
Debezium has captured it would silently drop the event (the Outbox Event Router only processes
insert/snapshot records, not deletes), and there is no cheap way from the application side to
know a row has been durably delivered to every consumer. A safe cleanup strategy needs either a
time-based retention window long enough to be confident Debezium is caught up (checked against
replication slot lag, not wall-clock guesswork) or an explicit "delivered" signal the connector
doesn't currently provide.

## Order fulfilment saga

**Service ownership boundaries.** Orders stays in the `public` schema. Three further PostgreSQL
schemas - `saga`, `inventory`, `payments` - each own their own tables, including their own
`outbox_events`, and each have a dedicated, non-superuser login role granted only on their own
schema (`searchsync_saga_app`, `searchsync_inventory_app`, `searchsync_payments_app`, created by
`infrastructure/postgres/01_service_roles.sh`). No cross-service PostgreSQL transaction exists
anywhere in this system - every multi-step workflow crosses a service boundary via Kafka, never
via a shared database connection.

**Idempotency and identity.** Every command and event carries
`messageId`/`correlationId`/`causationId`/`sagaId`/`orderId`/`eventVersion`
(`internal/saga.Envelope`). A unique constraint on `inventory.reservations.order_id` is what
actually prevents reserving stock twice for one order, not application logic checking first -
the same structural-dedup convention established for `processed_events`, reused here for
`processed_commands`/`processed_messages` across every service.

**Concurrent inventory reservation.** `internal/inventory.Service.TryReserveStock` uses one
conditional `UPDATE inventory.stock SET available = available - $qty WHERE sku = $1 AND available >= $qty` inside a local transaction, committed atomically with the reservation row and
its outbox event. Postgres's row-level locking on that `UPDATE` makes two concurrent
reservations for the same SKU safe under any isolation level, including the default `READ COMMITTED` - no `SERIALIZABLE`, no advisory lock, no saga-level coordination needed. Proven
deterministically with ten concurrent goroutines in
`internal/inventory/reservation_integration_test.go`.

**Durable timeouts, never an in-memory timer.** `internal/saga.Service.RunPoller` queries
`saga.sagas` for passed `payment_deadline`/`compensation_deadline` values every `PollInterval` -
the deadline itself was persisted in the same transaction as the state transition that set it,
so a crashed-and-restarted (even `kill -9`'d) saga coordinator resumes exactly where it left off.

**Compensation is retried, then escalated, never silently abandoned.** A payment decline or
timeout dispatches `ReleaseInventory`; if no resolving event arrives within
`CompensationRetryInterval`, the poller retries (a fresh command, fresh message id) up to
`MaxCompensationAttempts` times before transitioning to the terminal `NEEDS_ATTENTION` state -
visible via `GET /orders/:id` and `GET /orders/:id/timeline`, never an order stuck forever in
`COMPENSATING` with no record of why.

**Payments are simulated only.** `cmd/payments-sim` never handles real card data; its outcome is
entirely controlled by the local-only `POST /dev/payments/mode` endpoint
(`success`/`decline`/`timeout`). In `timeout` mode it deliberately produces no event at all - the
saga's deadline poller, not a third event type, is what detects a payment that never responds.