package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/orders"
)

const pgUniqueViolationCode = "23505"

type Handler struct {
	pool *pgxpool.Pool
}

func NewHandler(pool *pgxpool.Pool) *Handler {
	return &Handler{pool: pool}
}

// HandleMessage applies one OrderCreated domain event. It is the Debezium
// outbox router's expanded payload (table.expand.json.payload=true), not a
// raw row-change envelope, so it decodes directly into orders.OrderCreated.
//
// processed_events(event_id) and the order_notifications projection are
// written in one local transaction; a unique-constraint conflict on
// event_id means this event was already applied by an earlier delivery, so
// it is treated as a successful no-op rather than an error. The caller
// commits the Kafka offset only once this returns nil.
func (h *Handler) HandleMessage(ctx context.Context, value []byte) error {
	if value == nil {
		return nil
	}

	var event orders.OrderCreated
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("decode order created event: %w", err)
	}
	if event.EventID == "" {
		return fmt.Errorf("order created event missing eventId")
	}

	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	_, err = tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1)`, event.EventID)
	if err != nil {
		if isUniqueViolation(err) {
			return nil
		}
		return fmt.Errorf("insert processed event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO order_notifications (order_id, customer_id, total_cents)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id) DO NOTHING`,
		event.OrderID, event.CustomerID, event.TotalCents)
	if err != nil {
		return fmt.Errorf("insert order notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolationCode
}
