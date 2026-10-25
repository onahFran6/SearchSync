# Go Engineering Standards

## Scope and design choices

All custom application components are written in **Go**: the API, the Kafka
indexer/consumers, Orders, Inventory, the simulated Payments service, the Saga coordinator,
replay tooling, and the load generator. Infrastructure is PostgreSQL + Debezium/Kafka Connect +
Kafka + Elasticsearch, run locally via Docker Compose.

A single Go module holds multiple executables rather than one repository per service. Use the
current supported Go toolchain and pinned module dependencies. No application runtime requires
Node.js, npm, TypeScript, or an ORM.

## Repository layout

```text
searchsync/
├── go.mod
├── go.sum
├── cmd/
│   ├── api/main.go
│   ├── indexer/main.go
│   ├── notifications/main.go
│   ├── orders/main.go
│   ├── inventory/main.go
│   ├── payments-sim/main.go
│   ├── saga-coordinator/main.go
│   ├── replay/main.go
│   └── loadgen/main.go
├── internal/
│   ├── products/
│   ├── search/
│   ├── orders/
│   ├── outbox/
│   ├── saga/
│   ├── inventory/
│   ├── payments/
│   ├── cdc/
│   ├── platform/
│   └── observability/
├── migrations/
├── infrastructure/
│   ├── compose.yaml
│   ├── postgres/
│   ├── debezium/
│   ├── prometheus/
│   └── grafana/
├── scripts/
└── docs/
```

`internal/` keeps application packages out of reach of external consumers. Packages are
organized around business responsibilities (`orders`, `inventory`, `saga`, ...) rather than a
single catch-all `utils` package. Add a new executable only when a real responsibility needs its
own process, not speculatively.

## Dependencies

| Concern | Default | Notes |
|---|---|---|
| HTTP | standard `net/http` | Go 1.22+ method-pattern routing |
| PostgreSQL | `github.com/jackc/pgx/v5/pgxpool` | transactions via `pgx.Tx` and contexts |
| Kafka | `github.com/twmb/franz-go` | explicit offset/lifecycle management |
| Elasticsearch | `github.com/elastic/go-elasticsearch/v8` | match client major with cluster major |
| Structured logs | `log/slog` | correlation IDs; never log sensitive payloads |
| Metrics | Prometheus Go client | bounded label cardinality |
| Tracing | OpenTelemetry Go SDK | propagate trace context via Kafka headers |
| Tests | `testing` + Compose-backed integration tests | don't assume mocks prove delivery semantics |

## Core commands

```bash
go fmt ./...
go vet ./...
go test ./...
go test -race ./...
go run ./cmd/api
go run ./cmd/indexer
```

## Non-negotiable correctness rules

1. Pass `context.Context` through all external I/O; impose HTTP/DB/ES timeouts. Handle
   `SIGTERM` cleanly.
2. Treat PostgreSQL as the source of truth; never index into search inside the same
   transaction as a CRUD write.
3. Use a single PostgreSQL transaction for any write that must be atomic with its outbox
   event. A message published to Kafka is **not** automatically part of that transaction.
4. Preserve per-key ordering in Kafka. Goroutines must not reorder messages from the same
   partition. Don't commit offsets for unfinished work.
5. Design every consumer side effect for at-least-once delivery: stable IDs, transactional
   deduplication, and version checks wherever an older event might replay after a newer one.
6. Add structured logs carrying topic, partition, offset, and correlation/event IDs; avoid
   high-cardinality metric labels and PII in events.
7. Pair table-driven unit tests with real integration tests that exercise crash/restart
   behavior - the race detector catches Go memory races, not distributed-systems correctness.
8. Pin service, Kafka Connect/connector, PostgreSQL, and Elasticsearch versions together; never
   claim compatibility without testing it.
9. Benchmarks must record machine limits, resource settings, workload shape, versions, and
   latency distributions - not just a headline throughput number.
