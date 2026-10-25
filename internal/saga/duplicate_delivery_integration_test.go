//go:build integration

package saga

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

// TestHandleOrderCreated_DuplicateDeliveryIsNoop is a regression test for a
// real bug found live while bringing up this package's instrumentation: once an
// INSERT inside a transaction hits a unique-constraint violation, Postgres
// aborts that transaction at the protocol level - every statement after it,
// including COMMIT, fails until ROLLBACK, no exceptions. The dedup path
// here (and in internal/inventory, internal/payments) caught the
// unique-violation, decided "this message was already applied, nothing to
// do", and then called tx.Commit(ctx) anyway - which itself failed with
// pgx.ErrTxCommitRollback ("commit unexpectedly resulted in rollback").
// ConsumerLoop classified that as transient, burned its whole retry budget,
// and quarantined an ordinary duplicate redelivery to the DLQ instead of
// silently no-op'ing it - exactly the case this dedup logic exists to
// handle. The fix: return nil directly and let the deferred tx.Rollback
// discard the already-aborted transaction. This test redelivers the exact
// same OrderCreated event twice and asserts the second delivery returns nil
// (not an error) and never creates a second saga row.
func TestHandleOrderCreated_DuplicateDeliveryIsNoop(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	service := NewService(pool, NewRepository(), DefaultConfig, nil)

	orderID := uuid.New().String()
	eventID := uuid.New().String()
	payload, err := json.Marshal(map[string]any{
		"eventId":    eventID,
		"orderId":    orderID,
		"totalCents": 1000,
		"productSku": "TEST-SKU",
		"quantity":   1,
	})
	if err != nil {
		t.Fatalf("marshal order created event: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM saga.sagas WHERE order_id = $1`, orderID)
		_, _ = pool.Exec(ctx, `DELETE FROM saga.processed_messages WHERE message_id = $1`, eventID)
	})

	if err := service.HandleOrderCreated(ctx, payload); err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	// Redelivery of the exact same message id - this is the path that used
	// to fail with "commit unexpectedly resulted in rollback".
	if err := service.HandleOrderCreated(ctx, payload); err != nil {
		t.Fatalf("duplicate delivery must be a silent no-op, got error: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM saga.sagas WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("count saga rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 saga row after duplicate delivery, got %d", count)
	}
}
