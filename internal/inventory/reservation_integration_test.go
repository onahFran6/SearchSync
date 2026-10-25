//go:build integration

package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/saga"
)

// adminPool seeds/cleans up test fixtures using searchsync_app (this
// Compose setup's bootstrap superuser). appPool, used for
// the actual code under test below, connects as the real, narrowly
// scoped searchsync_inventory_app role Inventory runs as in production -
// deliberately not the same connection as fixture setup, so these tests
// prove TryReserveStock/HandleReserveInventory work under the actual
// deployed privilege set, not under elevated test-only access.
func adminPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := envDSN("POSTGRES_USER", "searchsync_app", "POSTGRES_PASSWORD", "changeme")
	pool, err := platform.NewPostgresPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect postgres as admin: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func appPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := envDSN("INVENTORY_DB_USER", "searchsync_inventory_app", "INVENTORY_DB_PASSWORD", "changeme")
	pool, err := platform.NewPostgresPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect postgres as inventory app role: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func envDSN(userKey, userDefault, passKey, passDefault string) string {
	get := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get(userKey, userDefault), get(passKey, passDefault),
		get("POSTGRES_HOST", "localhost"), get("POSTGRES_PORT", "5433"), get("POSTGRES_DB", "searchsync"))
}

// TestTryReserveStock_ConcurrentOrdersCompeteForLastUnit is Experiment #7:
// two concurrent orders reserving the same SKU when only one unit of
// stock remains. The conditional UPDATE in TryReserveStock
// (migrations/0005's comment explains why this is safe under any
// isolation level) must let exactly one of them succeed, never both and
// never neither.
func TestTryReserveStock_ConcurrentOrdersCompeteForLastUnit(t *testing.T) {
	admin := adminPool(t)
	app := appPool(t)
	ctx := context.Background()
	repo := NewRepository()

	sku := "test-last-unit-" + uuid.New().String()
	if _, err := admin.Exec(ctx, `INSERT INTO inventory.stock (sku, available) VALUES ($1, 1)`, sku); err != nil {
		t.Fatalf("seed stock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, `DELETE FROM inventory.stock WHERE sku = $1`, sku)
	})

	const attempts = 10
	var wg sync.WaitGroup
	results := make([]bool, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx, err := app.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				t.Errorf("begin tx %d: %v", i, err)
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			ok, err := repo.TryReserveStock(ctx, tx, sku, 1)
			if err != nil {
				t.Errorf("try reserve stock %d: %v", i, err)
				return
			}
			if ok {
				if err := tx.Commit(ctx); err != nil {
					t.Errorf("commit %d: %v", i, err)
					return
				}
			}
			results[i] = ok
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, ok := range results {
		if ok {
			successCount++
		}
	}
	if successCount != 1 {
		t.Fatalf("expected exactly 1 of %d concurrent reservations to succeed for 1 unit of stock, got %d", attempts, successCount)
	}

	var available int
	if err := admin.QueryRow(ctx, `SELECT available FROM inventory.stock WHERE sku = $1`, sku).Scan(&available); err != nil {
		t.Fatalf("read final stock: %v", err)
	}
	if available != 0 {
		t.Fatalf("expected 0 units remaining after exactly one reservation succeeded, got %d", available)
	}
}

// TestHandleReserveInventory_DuplicateCommandReservesOnce is Experiment
// #4 at the Inventory level: the same order_id receiving two
// ReserveInventory commands with different message ids (simulating the
// saga retrying after never seeing the first ack) must decrement stock
// exactly once.
func TestHandleReserveInventory_DuplicateCommandReservesOnce(t *testing.T) {
	admin := adminPool(t)
	app := appPool(t)
	ctx := context.Background()
	service := NewService(app, NewRepository())

	sku := "test-dup-reserve-" + uuid.New().String()
	orderID := uuid.New().String()
	if _, err := admin.Exec(ctx, `INSERT INTO inventory.stock (sku, available) VALUES ($1, 5)`, sku); err != nil {
		t.Fatalf("seed stock: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, `DELETE FROM inventory.stock WHERE sku = $1`, sku)
		_, _ = admin.Exec(ctx, `DELETE FROM inventory.reservations WHERE order_id = $1`, orderID)
		_, _ = admin.Exec(ctx, `DELETE FROM inventory.outbox_events WHERE aggregateid = $1`, orderID)
	})

	send := func(messageID string) {
		cmd := saga.ReserveInventory{
			Envelope: saga.Envelope{MessageID: messageID, OrderID: orderID, SagaID: orderID, CorrelationID: orderID},
			SKU:      sku, Quantity: 2,
		}
		payload, err := json.Marshal(cmd)
		if err != nil {
			t.Fatalf("marshal command: %v", err)
		}
		if err := service.HandleReserveInventory(ctx, payload); err != nil {
			t.Fatalf("handle reserve inventory: %v", err)
		}
	}

	send(uuid.New().String())
	send(uuid.New().String()) // different message id, same order id

	var available int
	if err := admin.QueryRow(ctx, `SELECT available FROM inventory.stock WHERE sku = $1`, sku).Scan(&available); err != nil {
		t.Fatalf("read final stock: %v", err)
	}
	if available != 3 {
		t.Fatalf("expected stock decremented by 2 exactly once (5 -> 3), got %d", available)
	}
}
