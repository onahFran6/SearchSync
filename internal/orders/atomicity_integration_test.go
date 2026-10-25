//go:build integration

package orders

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onahFran6/SearchSync/internal/outbox"
)

// TestCreateOrder_RollsBackOnOutboxConflict proves the hard constraint that
// the order insert and the outbox insert share one transaction, so if the
// outbox insert fails, the order insert must not be committed either. It
// forces the failure by
// pre-seeding an outbox_events row whose id collides with the one
// CreateOrder will deterministically derive for a chosen idempotency key.
//
// Run against the Compose Postgres: go test -tags=integration ./internal/orders/...
func TestCreateOrder_RollsBackOnOutboxConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresDSNFromEnv())
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("postgres not reachable, skipping integration test: %v", err)
	}
	defer pool.Close()

	idempotencyKey := "integration-test-conflict-" + uuid.New().String()
	conflictingEventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(idempotencyKey)).String()

	// Pre-seed the row CreateOrder's outbox insert will collide with. Uses a
	// non-"orders" aggregatetype so that if a live outbox connector is
	// streaming WAL while this test runs, the Outbox Event Router routes
	// this malformed fixture row to its own throwaway topic instead of the
	// real searchsync.outbox.event.orders topic, where it would otherwise
	// reach the notifications consumer as an unparseable event.
	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregatetype, aggregateid, type, payload)
		VALUES ($1, 'orders_test_fixture', 'preexisting', 'OrderCreated', '{}')`, conflictingEventID)
	if err != nil {
		t.Fatalf("seed conflicting outbox row: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, conflictingEventID)
	}()

	customerID := uuid.New().String()
	var ordersBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE customer_id = $1`, customerID).Scan(&ordersBefore); err != nil {
		t.Fatalf("count orders before: %v", err)
	}

	service := NewService(pool, NewRepository(), outbox.NewRepository())
	_, err = service.CreateOrder(ctx, CreateRequest{CustomerID: customerID, TotalCents: 1500, ProductSKU: "DEMO-001", Quantity: 1}, idempotencyKey)
	if err == nil {
		t.Fatal("expected CreateOrder to fail when the outbox insert conflicts, got nil error")
	}

	var ordersAfter int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE customer_id = $1`, customerID).Scan(&ordersAfter); err != nil {
		t.Fatalf("count orders after: %v", err)
	}
	if ordersAfter != ordersBefore {
		t.Fatalf("order insert was not rolled back: before=%d after=%d", ordersBefore, ordersAfter)
	}
}

func postgresDSNFromEnv() string {
	get := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get("POSTGRES_USER", "searchsync_app"),
		get("POSTGRES_PASSWORD", "changeme"),
		get("POSTGRES_HOST", "localhost"),
		get("POSTGRES_PORT", "5433"),
		get("POSTGRES_DB", "searchsync"))
}
