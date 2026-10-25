package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/onahFran6/SearchSync/internal/orders"
	"github.com/onahFran6/SearchSync/internal/platform"
)

const pgUniqueViolationCode = "23505"

// ErrPermanent marks a failure that will never succeed on retry (a
// malformed event payload, a missing eventId). classify checks for it with
// errors.Is before falling back to Postgres-error-code-based
// classification.
var ErrPermanent = errors.New("notifications: permanent failure")

// permanentPostgresCodes are error classes that mean the input itself is
// invalid, not that the database is unavailable: not-null violation, check
// violation, invalid text representation, foreign-key violation. Retrying
// the exact same statement against the exact same payload would fail again
// forever, so these go straight to the DLQ.
var permanentPostgresCodes = map[string]bool{
	"23502": true, // not_null_violation
	"23514": true, // check_violation
	"22P02": true, // invalid_text_representation
	"23503": true, // foreign_key_violation
}

// classify distinguishes retryable Postgres/network failures (connection
// loss, serialization conflicts, deadlocks, timeouts) from permanent ones
// (constraint violations that indicate a malformed event, not an outage).
func classify(err error) platform.FailureKind {
	if errors.Is(err, ErrPermanent) {
		return platform.FailurePermanent
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && permanentPostgresCodes[pgErr.Code] {
		return platform.FailurePermanent
	}
	return platform.FailureTransient
}

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
		return fmt.Errorf("%w: decode order created event: %v", ErrPermanent, err)
	}
	if event.EventID == "" {
		return fmt.Errorf("%w: order created event missing eventId", ErrPermanent)
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
