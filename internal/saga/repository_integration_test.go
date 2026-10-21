//go:build integration

package saga

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/platform"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	get := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get("SAGA_DB_USER", "searchsync_saga_app"), get("SAGA_DB_PASSWORD", "changeme"),
		get("POSTGRES_HOST", "localhost"), get("POSTGRES_PORT", "5433"), get("POSTGRES_DB", "searchsync"))
	pool, err := platform.NewPostgresPool(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestAttemptCompensationRetry_RetriesThenEscalates is a regression test
// for a real bug found live while running Stage 4's Experiment #5
// (restart mid-saga): the original SQL's `CASE WHEN ... THEN NULL ELSE $3
// END` left Postgres unable to infer a type for the untyped NULL branch
// and resolved the whole CASE expression as `text`, which then failed to
// assign into the `timestamptz` compensation_deadline column on every
// single call - the retry mechanism never actually retried, it only ever
// errored. The fix casts the NULL branch explicitly (`NULL::timestamptz`).
// This test drives both branches directly against live Postgres so a
// regression here fails loudly instead of only showing up as repeated
// "handle compensation retry" error logs during a live run.
func TestAttemptCompensationRetry_RetriesThenEscalates(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	repo := NewRepository()

	orderID := uuid.New().String()
	if err := repo.InsertSaga(ctx, pool, Row{OrderID: orderID, State: StatePending, SKU: "test-sku", Quantity: 1, AmountCents: 100}); err != nil {
		t.Fatalf("insert saga: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM saga.sagas WHERE order_id = $1`, orderID)
	})

	if _, ok, err := repo.GuardedTransition(ctx, pool, orderID, StatePending, StateStockReserved); err != nil || !ok {
		t.Fatalf("transition to STOCK_RESERVED: ok=%v err=%v", ok, err)
	}
	if _, ok, err := repo.GuardedTransition(ctx, pool, orderID, StateStockReserved, StateCompensating); err != nil || !ok {
		t.Fatalf("transition to COMPENSATING: ok=%v err=%v", ok, err)
	}
	past := time.Now().Add(-time.Second)
	if err := repo.SetCompensation(ctx, pool, orderID, past, 1); err != nil {
		t.Fatalf("set compensation: %v", err)
	}

	// First retry: attempts 1 -> 2, below maxAttempts=3, must NOT escalate
	// and must leave a fresh, non-null deadline.
	row, escalated, found, err := repo.AttemptCompensationRetry(ctx, pool, orderID, time.Now(), time.Now().Add(5*time.Second), 3)
	if err != nil {
		t.Fatalf("first retry: %v", err)
	}
	if !found {
		t.Fatal("expected first retry to find the row")
	}
	if escalated {
		t.Fatalf("expected first retry (attempt 2 of 3) not to escalate, got state %s", row.State)
	}
	if row.CompensationAttempts != 2 {
		t.Fatalf("expected compensation_attempts = 2, got %d", row.CompensationAttempts)
	}
	if row.CompensationDeadline == nil {
		t.Fatal("expected a fresh non-nil compensation deadline after a non-escalating retry")
	}

	// Second retry: attempts 2 -> 3, meets maxAttempts=3, must escalate to
	// NEEDS_ATTENTION with deadline cleared.
	past2 := time.Now().Add(-time.Second)
	if err := repo.SetCompensation(ctx, pool, orderID, past2, row.CompensationAttempts); err != nil {
		t.Fatalf("reset deadline for second retry: %v", err)
	}
	row2, escalated2, found2, err := repo.AttemptCompensationRetry(ctx, pool, orderID, time.Now(), time.Now().Add(5*time.Second), 3)
	if err != nil {
		t.Fatalf("second retry: %v", err)
	}
	if !found2 {
		t.Fatal("expected second retry to find the row")
	}
	if !escalated2 {
		t.Fatalf("expected second retry (attempt 3 of 3) to escalate, got state %s", row2.State)
	}
	if row2.State != StateNeedsAttention {
		t.Fatalf("expected NEEDS_ATTENTION, got %s", row2.State)
	}
	if row2.CompensationDeadline != nil {
		t.Fatal("expected compensation deadline cleared after escalation")
	}
}
